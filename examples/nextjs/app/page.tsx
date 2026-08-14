import ReportPanel from "./ReportPanel";

export default function Home() {
  return (
    <main style={{ fontFamily: "monospace", padding: "2rem", lineHeight: 1.8 }}>
      <h1>TriggerLink Next.js 示例</h1>
      <p>
        本应用通过 <code>@triggerlink/sdk</code> 在 <code>/api/triggerlink</code> 挂出
        serve 端点，注册了函数 <code>process-doc</code>（订阅 <code>doc/uploaded</code>）与{" "}
        <code>build-report</code>（订阅 <code>report/requested</code>）。
        应用启动时会通过 <code>client.register</code> 向平台自注册（见终端{" "}
        <code>[triggerlink] registered</code> 日志）。
      </p>

      <ReportPanel />

      <h2>命令行触发（process-doc）</h2>
      <pre>curl -X POST localhost:3000/api/send -d &apos;{`{"doc_id":"d1"}`}&apos;</pre>
      <p>然后观察终端：平台会分 4+1 次回调推进 parse → chunk → embed → store。</p>
      <p>完整步骤见 README.md。</p>
    </main>
  );
}
