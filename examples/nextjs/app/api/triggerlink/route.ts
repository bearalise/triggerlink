// TriggerLink serve 端点：平台 GET 内省 + POST 执行回调都打到这个路由。
// 函数：process-doc（对应 Go 示例 examples/pipeline 的 4 步流水线）、
//       build-report（网页按钮触发的耗时任务，见 app/page.tsx）。
import { createFunction, serve, StepInterrupt } from "@triggerlink/sdk";
import { triggerlink } from "../../lib/platform";
import { updateTask } from "../../lib/tasks";

export const runtime = "nodejs"; // SDK 使用 node:crypto，且需要较长执行时限
export const maxDuration = 300; // 单 step 时限须 < 平台回调超时（默认 5 分钟）

const processDoc = createFunction(
  { id: "process-doc", event: "doc/uploaded", retries: 4 },
  async ({ event, step }) => {
    const { doc_id } = event.data as { doc_id: string };

    // 副作用放进 step.run：重试/崩溃恢复时已完成的 step 由 memo 注入，不重跑
    const parsed = await step.run("parse", async () => {
      console.log(`[nextjs] EXECUTE parse (doc=${doc_id})`);
      return `parsed:${doc_id}`;
    });

    const chunks = await step.run("chunk", async () => {
      console.log("[nextjs] EXECUTE chunk");
      return 12;
    });

    const vectors = await step.run("embed", async () => {
      console.log(`[nextjs] EXECUTE embed (${chunks} chunks, slow...)`);
      await new Promise((r) => setTimeout(r, 2000)); // 模拟慢速 embedding API
      return "vectors-ok";
    });

    await step.run("store", async () => {
      console.log("[nextjs] EXECUTE store");
      return "stored";
    });

    return { doc_id, parsed, vectors };
  },
);

// 网页按钮触发的耗时任务（app/page.tsx → POST /api/report → 事件 report/requested）。
// 进度与结果写入应用侧任务状态表（app/lib/tasks.ts），页面轮询获知——
// M0 平台没有完成 webhook，最后一步写应用自有存储即"应用得到通知"的推荐做法。
const buildReport = createFunction(
  { id: "build-report", event: "report/requested", retries: 4 },
  async ({ event, step }) => {
    const { task_id } = event.data as { task_id: string };
    try {
      const collected = await step.run("collect", async () => {
        updateTask(task_id, { status: "running", stage: "collect" });
        console.log(`[nextjs] EXECUTE collect (task=${task_id})`);
        await new Promise((r) => setTimeout(r, 1000)); // 模拟查询业务库
        return { rows: 128, source: "orders-db" };
      });

      const summary = await step.run("analyze", async () => {
        updateTask(task_id, { status: "running", stage: "analyze" });
        console.log(`[nextjs] EXECUTE analyze (${collected.rows} rows, slow...)`);
        await new Promise((r) => setTimeout(r, 3000)); // 模拟慢速 LLM 调用
        return `营收环比 +12%（样本 ${collected.rows} 行，来源 ${collected.source}）`;
      });

      const report = await step.run("render", async () => {
        updateTask(task_id, { status: "running", stage: "render" });
        console.log("[nextjs] EXECUTE render");
        await new Promise((r) => setTimeout(r, 2000)); // 模拟渲染/写存储
        const result = {
          title: "每日经营报告",
          summary,
          rows: collected.rows,
          generated_at: new Date().toISOString(),
        };
        // 完成通知：最后一个 step 内把结果写回任务状态表（幂等，重试安全）
        updateTask(task_id, { status: "completed", stage: "done", result });
        return result;
      });

      return report;
    } catch (err) {
      // StepInterrupt 是 SDK 的正常控制流（step 中断），必须原样抛出，不能记为失败
      if (err instanceof StepInterrupt) throw err;
      updateTask(task_id, {
        status: "failed",
        error: err instanceof Error ? err.message : String(err),
      });
      throw err; // 平台按 RunError 走重试/终结
    }
  },
);

export const { GET, POST } = serve({ client: triggerlink, functions: [processDoc, buildReport] });
