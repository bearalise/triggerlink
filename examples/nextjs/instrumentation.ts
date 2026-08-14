// Next.js 启动钩子：应用启动后通过 SDK client.register 向平台自注册（见 app/lib/platform.ts）。
// 平台未启动时后台重试（2s 间隔、最多 150 次 ≈ 5 分钟），不阻塞应用启动。
export async function register() {
  if (process.env.NEXT_RUNTIME === "nodejs") {
    const { triggerlink } = await import("./app/lib/platform");
    triggerlink.register(
      process.env.TRIGGERLINK_SERVE_URL ?? "http://localhost:3000/api/triggerlink",
    );
  }
}
