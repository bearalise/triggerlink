// 网页按钮的服务端入口：
//   POST /api/report           → 发 report/requested 事件给平台，立即返回 task_id（异步触发）
//   GET  /api/report?task_id=  → 查询任务状态（页面轮询，直至 completed/failed）
import { NextResponse } from "next/server";
import { triggerlink } from "../../lib/platform";
import { getTask } from "../../lib/tasks";

export async function POST() {
  const taskId = crypto.randomUUID();
  try {
    await triggerlink.send({
      id: `report-${taskId}`, // 幂等键：重复提交不会重复触发
      name: "report/requested",
      data: { task_id: taskId },
    });
  } catch (err) {
    return NextResponse.json(
      { error: "platform rejected event", detail: err instanceof Error ? err.message : String(err) },
      { status: 502 },
    );
  }
  return NextResponse.json({ task_id: taskId });
}

export async function GET(req: Request) {
  const taskId = new URL(req.url).searchParams.get("task_id") ?? "";
  const task = getTask(taskId);
  // unknown：平台尚未完成首次回调（事件路由/排队中），属正常中间态
  return NextResponse.json(task ?? { status: "unknown" });
}
