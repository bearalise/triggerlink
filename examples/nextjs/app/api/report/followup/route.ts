// 后续操作（demo）：任务完成后页面自动调用，模拟"根据执行结果归档/通知下游"。
// 真实项目里这里是写业务库、发 IM 通知、触发下一个流程等。
import { NextResponse } from "next/server";

export async function POST(req: Request) {
  const { task_id, result } = await req.json().catch(() => ({}));
  console.log(
    `[nextjs] FOLLOWUP task=${task_id} 报告已归档：` +
      JSON.stringify(result ?? null),
  );
  return NextResponse.json({ archived: true, task_id });
}
