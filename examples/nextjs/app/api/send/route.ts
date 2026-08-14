// 演示生产者：POST /api/send 向平台发一个 doc/uploaded 事件。
// 真实项目里应在订单/上传/webhook 等业务代码里这样发事件。
import { NextResponse } from "next/server";
import { triggerlink } from "../../lib/platform";

export async function POST(req: Request) {
  const { doc_id = `doc-${Date.now()}` } = await req.json().catch(() => ({}));

  try {
    const result = await triggerlink.send({
      id: `doc-uploaded-${doc_id}`, // 幂等键：重复提交不会重复触发
      name: "doc/uploaded",
      data: { doc_id },
    });
    return NextResponse.json({ platform_status: result.status, event_ids: result.ids });
  } catch (err) {
    return NextResponse.json(
      { error: "platform rejected event", detail: err instanceof Error ? err.message : String(err) },
      { status: 502 },
    );
  }
}
