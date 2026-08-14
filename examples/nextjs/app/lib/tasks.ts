// 任务状态表：网页触发 → 后台执行 → 完成通知 的应用侧载体（demo 级内存实现）。
//
// 背景：M0 平台没有"run 完成 webhook"。但 durable 函数本身就运行在应用进程内
// （平台回调打进 /api/triggerlink），所以函数最后一个 step 把结果写进这张表，
// 页面轮询 /api/report?task_id= 即可获知完成——这就是 M0 下"应用得到通知"的推荐做法。
//
// 注意：生产环境应替换为 Redis/DB 等共享存储（多实例、重启保留）。
// 挂在 globalThis 上是为了在 Next.js dev 模式（HMR 多模块实例）下共享同一份状态。
export interface TaskState {
  status: "running" | "completed" | "failed";
  /** 当前执行到哪个 step（running 时有值） */
  stage?: string;
  /** 函数返回值（completed 时有值） */
  result?: unknown;
  error?: string;
  updatedAt: number;
}

const store: Map<string, TaskState> =
  ((globalThis as Record<string, unknown>).__triggerlinkTasks as Map<string, TaskState>) ??
  new Map<string, TaskState>();
(globalThis as Record<string, unknown>).__triggerlinkTasks = store;

export function updateTask(taskId: string, patch: Partial<Omit<TaskState, "updatedAt">>) {
  store.set(taskId, { ...store.get(taskId), ...patch, updatedAt: Date.now() } as TaskState);
}

export function getTask(taskId: string): TaskState | undefined {
  return store.get(taskId);
}
