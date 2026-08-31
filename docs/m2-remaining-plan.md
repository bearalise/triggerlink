# M2 收尾开发计划：流控三件套 / 可观测性 / Postgres 后端

> 目标：完成 PRD 第 6 章 M2 剩余功能——流控（debounce/throttle/batching）、可观测性（slog/metrics/保留策略）、Postgres 存储后端。
> 依据：`docs/triggerlink-prd.md` FR-4.4/4.5/4.7、FR-6.3/6.4/6.6、第 7 章存储设计。
> 现状基线：main @ c47d257 之后。SQLite 单后端；`store.Store` 接口约 40 个方法（`internal/store/store.go:122`）；执行器并发控制为内存 `inFlight` map（`internal/executor/executor.go:51`）；队列 = `queue_items` 表 + `UPDATE...RETURNING` 原子租赁；manifest 已有字段 id/event/match/cron/retries/cancel_on/timeout。
> 执行顺序：**阶段 B（可观测性，已完成）→ 阶段 A（流控，已完成）→ 阶段 C（Postgres）**。B 独立且是压测/发布门禁的前提；A 动队列调度核心，放在观测能力就绪后便于验证；C 工作量最大且依赖 A/B 的 Store 接口最终形态。
> 每个阶段独立成 PR：开分支 → 实现 → 全量测试（含 e2e）→ 推送开 PR → 合并。SDK 有面向用户改动时发 npm patch/minor。

---

## 阶段 B：可观测性（FR-6.3 / FR-6.4 / FR-6.6）✅ 已完成

分支 `feat/observability`，三个 commit（B1 slog / B2 metrics / B3 janitor）。与计划的差异：

- B2 的 `events_routed_total` 增加 `result="filtered"`（触发 match 不命中）：否则事件在路由层静默消失，无法排障。
- B2 的 `step_duration_seconds` 口径按 op 区分：`Run`/`SendEvent` = 回调内同步执行时长，`Sleep` = 目标时长，`WaitForEvent` = 挂起到解除的墙钟（`waits.created_at` 起算）。step_runs 表无时间戳，未为此加列。
- B3 的 queue_items 清理确认无需做：`CompleteQueueItem` 是 DELETE，残留租约由 `RequeueExpiredLeases` 对账。
- B3 的 `-retention-days` 语义：默认 30，flag=0 时读 env，小于 1 天拒绝启动，负数显式关闭清理。
- 依赖锁在 `prometheus/client_golang v1.22.0`：更高版本会把 go 指令顶到 1.24+，与 CI 的 1.23 冲突。

预估 1–1.5 天。全部为服务端改动，无协议变更，SDK 不动。

### B1. slog 结构化日志（FR-6.3）

- 全仓 `log.Printf` → `log/slog`。`main.go` 启动时按 `-log-level` flag（默认 `info`，env `TRIGGERLINK_LOG_LEVEL` 兜底）构造 `slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: ...}))` 并 `slog.SetDefault`。
- 关键日志带结构化字段：`run_id`、`function_id`、`event_id`、`attempt`、`duration`（如 `slog.Info("run completed", "run_id", run.ID, "attempt", attempt)`）。
- 范围：`internal/executor`、`internal/runner`、`internal/scheduler`、`internal/eventapi`、`internal/webhook`、`internal/dashapi`、`internal/appapi`、`cmd/triggerlink`。测试文件不动（测试里的日志断言只查状态不查日志格式，确认即可）。
- 验收：`-log-level=debug` 输出可见、error 级字段齐全；`go build ./... && go test ./...` 全绿。

### B2. Prometheus `/metrics`（FR-6.4）

- 依赖：`github.com/prometheus/client_golang`（PRD 7.5 既定选型）。
- 新包 `internal/metrics`：集中定义指标（避免各包重复注册），全部用包级 `promauto` 或显式 Registry（选显式 `prometheus.NewRegistry()`，便于测试隔离；handler 用 `promhttp.HandlerFor(reg, ...)`）。
- 指标清单（命名前缀 `triggerlink_`）：
  - `triggerlink_events_received_total`（counter，eventapi/webhook 接收）
  - `triggerlink_events_routed_total{result="routed|no_subscriber|paused"}`（counter，runner）
  - `triggerlink_runs_total{status="completed|failed|cancelled|timeout"}`（counter，executor）
  - `triggerlink_run_duration_seconds`（histogram，run 终态时按 created_at→ended_at）
  - `triggerlink_step_duration_seconds{op="Run|Sleep|WaitForEvent|SendEvent"}`（histogram）
  - `triggerlink_callback_duration_seconds`（histogram，executor HTTP 回调耗时）
  - `triggerlink_queue_depth`（gauge，pending 且 at<=now 的队列项数；10s 周期刷新，不挂 promhttp 查询路径）
  - `triggerlink_waits_active`（gauge，waiting 状态的 wait 数，同周期刷新）
- 挂载：`main.go` 加 `GET /metrics`，公开（无 basic auth，Prometheus 抓取惯例；文档注明公网部署应在反代层限制）。
- store 需加两个计数查询：`QueueDepth(ctx)`、`ActiveWaitCount(ctx)`（或复用现有查询组合，新接口方法二选一，倾向显式新方法）。
- 验收：启动后 `curl /metrics` 有上述序列；跑一条 e2e 流水线后 counter/gauge 数值变化正确（单测用 `testutil` 包断言注册值）。

### B3. 保留策略清理（FR-6.6）

- 配置：`-retention-days`（默认 30，env `TRIGGERLINK_RETENTION_DAYS`）；**校验：不得小于事件去重窗口（24h），小于则启动报错**。
- 新组件 `internal/janitor`（或挂 runner tick，倾向独立 goroutine 便于独立测试）：每小时执行一次清理。
- 删除规则（store 新方法）：
  - `DeleteRunsBefore(ctx, cutoff)`：删除 `ended_at < cutoff` 的终态 run 及其 `step_runs`（事务内级联删除两张表）。
  - `DeleteEventsBefore(ctx, cutoff)`：删除 `received_at < cutoff` 且**未被在途 run 引用**的事件（`NOT EXISTS (SELECT 1 FROM runs WHERE runs.event_id = events.id AND runs.status IN ('Queued','Running'))`）。
  - 顺带清理终态 waits、已完成 queue_items（`status='leased'` 残留由对账处理，这里只删 completed 无 lease 的历史项——检查现有 `CompleteQueueItem` 是 DELETE 还是置状态，若是 DELETE 则无需清理）。
- 验收：store 单测覆盖"过期删除/在途引用跳过/保留未到期"；janitor 单测驱动一次清理。

---

## 阶段 A：流控（FR-4.5 debounce / FR-4.4 throttle / FR-4.7 batching）✅ 已完成

分支 `feat/flow-control`，四个 commit（A0 基建 / A1 debounce / A2 throttle / A3 batching）+ A4 收尾。与计划的差异：

- A0：三个配置类型各自实现 Marshal/UnmarshalJSON，清单形态与落库形态是同一段 JSON，持久化只是原样存一列——不再需要"注册表结构 ↔ 落库字段"的手写映射。
- A1：store 方法比计划多一个 `UpdateQueueItemAt` 的返回值（改动行数），用于识别"续窗时队列项已被租赁"。
- A2：新增 `internal/flowkey` 共享包——key 求值同时发生在路由层与执行层，必须对同一事件算出同一个键；runner 内的实现已删除。
- A3：runs 表加 `batch` 列显式标记批 run，而非按"event_data 看着像数组"推断（普通事件的 data 本身就可能是数组）。`FlushBatch` 只删本次读过的 seq，flush 期间到达的事件不丢。
- 计划未提但做了：janitor 清理 24h 前的 throttle 配额行；events_routed_total 增加 debounced/batched 两个结果值；新增 runs_throttled_total。



预估 2.5–3.5 天。三个功能共享基础设施：函数配置扩展（manifest 三个新字段）+ registry 持久化三个新列 + runner 路由分流 + SDK 双端配置。**建议顺序：debounce → throttle → batching**（复杂度递增，batching 涉及协议 ctx.events 字段启用）。

### A0. 共享基建（先行）

- manifest/FunctionOpts 三个新配置（Go + TS 同构，JSON 字段名冻结）：
  - `debounce`: `{"period": "5m", "key": "data.user_id (可选 expr)", "timeout": "1h (可选，距首事件最长延迟)"}`
  - `throttle`: `{"limit": 10, "period": "1m", "key": "可选 expr"}`
  - `batch`: `{"max_size": 100, "timeout": "30s", "key": "可选 expr"}`
- `registry.Function` 加 `Debounce/Throttle/Batch` 结构体字段（自定义 UnmarshalJSON 解析 duration 字符串，参考现有 Timeout 的写法）；`functions` 表 ensureColumn 加三列（JSON 字符串），持久化/恢复链路同步。
- Go SDK `FunctionOpts` 加 `Debounce *DebounceOpts` 等；TS 加 `debounce?: {period: string|number, key?: string, timeout?: string|number}` 等（毫秒 number 复用 `msToGoDuration`）。
- runner/route 入口分流：`if f.Debounce != nil → debounce 路径；else if f.Batch != nil → batch 路径；else 现有直路由`。throttle 不在路由层（在执行层）。

### A1. Debounce（FR-4.5）

- 语义（对齐 Inngest 简化版）：同 `(function_id, key)` 的事件在 period 窗口内合并为一个 run，取**尾事件**（latest 覆盖）；`timeout` 为首事件起最长延迟（到时强制触发）。
- 存储：新表 `debounce_pending(function_id, key, run_id, first_at, trigger_at, PK(function_id, key))`。
  - 首个事件：CreateRun（Queued，event 快照 = 该事件）+ 入队 `at = now + period` + 写 debounce_pending（first_at=now, trigger_at=now+period；若配 timeout 则 trigger_at = min(now+period, first_at+timeout)）。
  - 后续事件（同 key 有 pending）：更新 run 的 event 快照为最新事件 + 更新队列项 `at = now+period`（受 timeout 上限约束）+ 更新 debounce_pending.trigger_at。**run ID 复用 pending 中的**（不新建）。
  - 队列项 dispatch 时（executor 侧或 dispatch 开头）删除 debounce_pending 记录（key 释放，下个事件重新开窗）。注意竞态：dispatch 删除 pending 与新事件写入的先后——删除用事务内"删除并校验 trigger_at<=now"或接受边缘重复（M2 语义：同 key 可能产生紧邻的两个 run，文档注明）。
- key 缺省 = 固定单 key（全函数合并为一个窗口）；key 表达式用 `evalTriggerMatch` 同款 expr 求值（环境只含 data），求值失败视为固定 key。
- store 新方法：`UpsertDebouncePending`、`GetDebouncePending`、`DeleteDebouncePending`、`UpdateRunEventSnapshot`（更新 run 的 event_data/ts）、`UpdateQueueItemAt`（按 run_id 改 pending 项的 at）。
- 测试：窗口合并（3 事件 1 run、event 快照为尾事件）、timeout 封顶、不同 key 独立窗口、暂停函数不建 pending、崩溃重启后 pending 从 DB 恢复（重启对账天然覆盖——队列项在库）。

### A2. Throttle（FR-4.4）

- 语义：per key 在 period 窗口内最多启动 limit 个 run 的**首次执行**（超限的 run 延迟到下一窗口，不丢弃）。
- 实现位置：executor dispatch 回调前（不是路由层——路由照建 run 入队，执行被推迟）。
- 存储：`throttle_state(constraint_key PK, window_start, count)`；constraint_key = `function_id + ":" + key求值`。dispatch 流程：查 fn.Throttle → 事务内 `INSERT ... ON CONFLICT` 读改写（窗口过期则重置）→ 超限则 `ReleaseLease(ctx, item.ID, window_end)` 延迟，不回调。
- key 表达式求值需要事件 data：dispatch 时 run.EventData 已在手。
- 崩溃恢复：throttle_state 在 DB，重启不丢（满足 PRD "DB 为事实源"）。
- 测试：窗口内 limit 计数、超限延迟到下一窗口、窗口过期重置、key 分组隔离。

### A3. Batching（FR-4.7）

- 语义：同 `(function_id, key)` 事件攒够 max_size 或距首条超过 timeout → 以**事件数组**触发一个 run。
- 协议启用预留字段：callbackRequest 的 `ctx.events`（8.2 草案中已有 `"events": null` 占位）：批触发时 `ctx.events = [event, ...]`、`ctx.event` 仍填首条（兼容）。SDK：`Input.Events []Event`（Go）、`ctx.events`（TS），批触发时填充。
- 存储：`batch_entries(function_id, key, seq, event_json, added_at)` + `batch_windows(function_id, key, first_at, PK(function_id, key))`。
  - 事件到达：append batch_entries + 若无窗口则建窗口；`COUNT >= max_size` →  flush：CreateRun（EventData = 数组 JSON、`events` 快照落 runs.event_data）+ 入队 + 清窗口与条目（事务）。
  - 超时 flush：runner 的 1s tick（或独立扫描）查 `first_at + timeout < now` 的窗口 → 同上 flush。
  - 崩溃恢复：条目全在 DB，重启后由超时扫描兜底 flush（可能延迟一个 tick，可接受）。
- 测试：按数量 flush、按超时 flush、key 分组、顺序保持（seq）、重启后超时兜底。

### A4. 收尾

- Dashboard：FunctionListPage 摘要显示三个配置的存在徽标（debounce/throttle/batch），Run 详情页批触发 run 显示事件数组（`ctx.events` 已在 run 快照 event_data 中，前端只读展示，改动小就做，复杂则跳过）。
- e2e：debounce 合并、throttle 限流延迟、batching 按数量/超时 flush 各一例。
- 文档：protocol.md（manifest 字段 + ctx.events）、user-guide.md（三个配置的用法与语义边界）、PRD 追溯矩阵勾销。
- 发版：sdk-ts 有面向用户的新配置 → minor（0.6.0）。

---

## 阶段 C：Postgres 后端（PRD 7.3 P1 方案 / 13 章部署形态）

预估 3–5 天。M2 验收项，工作量最大。

### C1. 选型与骨架

- 驱动：`pgx/v5`（stdlib 兼容模式 `database/sql`，PRD 7.5 既定）。依赖：`github.com/jackc/pgx/v5`。
- 新文件 `internal/store/postgres.go`：实现完整 `store.Store` 接口（约 40 个方法），与 `sqlite.go`（现 store.go）并列。
- `store.Open` 改为按 URI 分流：`postgres://` 前缀 → PostgresStore；否则 SQLite。`main.go` 加 `-postgres-uri`（或 `-db` 直接接受两种 scheme，倾向后者：`-db postgres://...` 一个 flag 两种后端，文档更清晰）。
- schema：同一套表，Postgres 方言——`TEXT` 主键不变；`event_data`/`data`/`output` 用 `JSONB`；时间列 `TIMESTAMPTZ`；schema 初始化用 `IF NOT EXISTS` 全量 DDL（与 SQLite 的 migrate 路径同思路，启动时执行）。

### C2. 方言差异点（逐条处理）

- 占位符：`?` → `$1..$n`（pgx stdlib 用 $n）。
- 幂等插入：`INSERT OR IGNORE` → `INSERT ... ON CONFLICT DO NOTHING`（`RowsAffected` 语义相同，CreateRun/InsertEvent/CreateWait 的 created bool 逻辑直接复用）。
- 原子租赁：`UPDATE ... WHERE id IN (SELECT ... LIMIT n) RETURNING *` Postgres 原生支持；多消费者安全版用 `FOR UPDATE SKIP LOCKED`（PRD 7.3 P1 方案）——M2 先保持与 SQLite 同语义（单消费者），SKIP LOCKED 作为实现注释预留。
- 时间：现有代码以字符串存时间（`fmtTime`），Postgres 实现统一转 `time.Time` 绑定 TIMESTAMPTZ；读侧 `time.Parse` 替换为直接 scan。注意 `expires_at NULL` 的可空处理。
- JSON：现有代码 JSON 存 TEXT 字符串，Postgres 侧 JSONB 列与 string 互转透明（`string(json.RawMessage)` 直接可写）。
- `ensureColumn` 迁移：Postgres 用 `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`。

### C3. 测试策略

- store 包测试矩阵化：现有 store 测试抽成"套件函数"（`func storeSuite(t *testing.T, open func(t) store.Store)`），SQLite 恒跑，Postgres 在 `TEST_POSTGRES_URI` 环境变量存在时跑（不存在则 skip）。重构成本注意控制：抽套件只搬测试函数体，不改断言。
- 本地：根目录 `docker-compose.yml` 加一个 `postgres:16` 服务（profile 隔离，默认不启）。
- CI：`.github/workflows/ci.yml` 的 go job 加 `services: postgres:16` + `TEST_POSTGRES_URI` 环境变量，矩阵恒跑。
- e2e：不必全量双跑——加一个 `TestSmokePostgres`（env 存在时跑）：起 Postgres store 的最小栈，跑通 happy path（事件→run→Completed）即可。

### C4. 收尾

- 文档：deployment.md（Postgres 启动方式、compose 示例含 postgres 服务、SQLite→Postgres 切换说明、备份差异）；user-guide.md 配置表；PRD 13 章部署形态表勾销。
- 压测（PRD R1 的闭环）：写一个 `internal/store` 的基准测试或独立 cmd 脚本，对比 SQLite/Postgres 的事件接收吞吐与写入放大，数据写进 deployment.md（量级正确即可，不追求精确压测报告）。
- 发布：打 `v0.2.0` tag（Postgres 是显著能力升级）。

---

## 全局约束与验收

- 每阶段完成后：`go build ./... && go vet ./... && go test ./... -count=1` 全绿 + `sdk-ts npm test` 全绿 + web build（若动前端）。
- 所有 schema 变更走 `CREATE TABLE IF NOT EXISTS` / `ADD COLUMN IF NOT EXISTS`，老库（含用户已有的 triggerlink.db）无损迁移；阶段 C 的 Postgres DDL 与 SQLite 保持逻辑等价。
- 协议变更（manifest 字段、ctx.events）两侧 SDK 必须同日对齐；npm 发版顺序 = 合并 main 后。
- 全程遵循最小改动：不重构无关代码，不动既有语义；每个阶段独立可发布。

## 风险

| 风险 | 缓解 |
|---|---|
| debounce/throttle 与既有队列租赁交互产生边缘竞态 | 状态全部落 DB（不用内存态），测试覆盖重启恢复 |
| 流控拉高 SQLite 写放大（R1） | 阶段 B 的 metrics 先行，A 完成后有数据可观测；C 提供 Postgres 出路 |
| store 测试矩阵化重构误伤既有断言 | 只搬不改；先跑通 SQLite 侧再接 Postgres |
| Postgres 时间/JSON 方言细节多 | C2 清单逐条对照；矩阵测试强制双后端同语义 |
