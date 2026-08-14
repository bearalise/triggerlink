// 场景 B（PRD 4.2）：多步 AI 文档处理流水线。
// 演示：step 逐个推进、崩溃后 memo 恢复（已完成的 step 不重跑）。
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	triggerlink "github.com/bearalise/triggerlink/sdk"
	"github.com/bearalise/triggerlink/sdk/step"
)

func main() {
	signingKey := os.Getenv("TRIGGERLINK_SIGNING_KEY")
	if signingKey == "" {
		signingKey = "dev"
	}
	client := triggerlink.NewClient(triggerlink.Options{AppID: "docsvc", SigningKey: signingKey})

	fn := triggerlink.CreateFunction(
		triggerlink.FunctionOpts{ID: "process-doc", Event: "doc/uploaded", Retries: 4},
		func(ctx context.Context, in triggerlink.Input) (any, error) {
			var data struct {
				DocID string `json:"doc_id"`
			}
			if err := json.Unmarshal(in.Event.Data, &data); err != nil {
				return nil, err
			}

			parsed, err := step.Run(ctx, "parse", func(ctx context.Context) (string, error) {
				maybeCrash("parse")
				log.Printf("[app] EXECUTE parse (doc=%s)", data.DocID)
				return "parsed:" + data.DocID, nil
			})
			if err != nil {
				return nil, err
			}

			chunks, err := step.Run(ctx, "chunk", func(ctx context.Context) (int, error) {
				maybeCrash("chunk")
				log.Printf("[app] EXECUTE chunk")
				return 12, nil
			})
			if err != nil {
				return nil, err
			}

			vectors, err := step.Run(ctx, "embed", func(ctx context.Context) (string, error) {
				maybeCrash("embed")
				log.Printf("[app] EXECUTE embed (%d chunks, slow...)", chunks)
				time.Sleep(2 * time.Second) // 模拟慢速 embedding API
				return "vectors-ok", nil
			})
			if err != nil {
				return nil, err
			}

			_, err = step.Run(ctx, "store", func(ctx context.Context) (string, error) {
				maybeCrash("store")
				log.Printf("[app] EXECUTE store")
				return "stored", nil
			})
			if err != nil {
				return nil, err
			}

			return map[string]any{"doc_id": data.DocID, "parsed": parsed, "vectors": vectors}, nil
		},
	)

	http.Handle("/api/triggerlink", triggerlink.Serve(client, fn))
	log.Println("demo app listening on :8080 (serve: /api/triggerlink)")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

// maybeCrash：设置 TRIGGERLINK_CRASH_AT=<step_id> 时在该 step 内杀死进程，模拟崩溃。
func maybeCrash(stepID string) {
	if os.Getenv("TRIGGERLINK_CRASH_AT") == stepID {
		log.Printf("[app] 💥 crashing inside step %q", stepID)
		os.Exit(2)
	}
}
