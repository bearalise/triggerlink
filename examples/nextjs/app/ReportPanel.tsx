"use client";

// 耗时任务面板：点击按钮 → POST /api/report → 轮询状态 → 完成后执行后续操作。
// 后台执行期间可在平台 Dashboard（http://localhost:8288/dashboard）观察 run 时间线。
import { useEffect, useRef, useState } from "react";

type Phase =
  | { kind: "idle" }
  | { kind: "running"; taskId: string; stage?: string }
  | { kind: "completed"; taskId: string; result: unknown; followup?: string }
  | { kind: "failed"; taskId: string; error?: string };

const STAGE_LABELS: Record<string, string> = {
  collect: "① 采集数据",
  analyze: "② LLM 分析（慢）",
  render: "③ 渲染报告",
};

export default function ReportPanel() {
  const [phase, setPhase] = useState<Phase>({ kind: "idle" });
  const followupSent = useRef<string | null>(null);

  // 轮询任务状态，直到终态
  useEffect(() => {
    if (phase.kind !== "running") return;
    const timer = setInterval(async () => {
      const resp = await fetch(`/api/report?task_id=${phase.taskId}`);
      const task = await resp.json();
      if (task.status === "completed") {
        setPhase({ kind: "completed", taskId: phase.taskId, result: task.result });
      } else if (task.status === "failed") {
        setPhase({ kind: "failed", taskId: phase.taskId, error: task.error });
      } else {
        setPhase({ kind: "running", taskId: phase.taskId, stage: task.stage });
      }
    }, 1000);
    return () => clearInterval(timer);
  }, [phase]);

  // 完成后执行后续操作（每个任务只执行一次）
  useEffect(() => {
    if (phase.kind !== "completed" || followupSent.current === phase.taskId) return;
    followupSent.current = phase.taskId;
    void (async () => {
      const resp = await fetch("/api/report/followup", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ task_id: phase.taskId, result: phase.result }),
      });
      const body = await resp.json().catch(() => null);
      setPhase((p) =>
        p.kind === "completed"
          ? { ...p, followup: body?.archived ? "✅ 已归档（见应用终端日志）" : "后续操作完成" }
          : p,
      );
    })();
  }, [phase]);

  const start = async () => {
    setPhase({ kind: "running", taskId: "" });
    const resp = await fetch("/api/report", { method: "POST" });
    const body = await resp.json();
    if (!resp.ok) {
      setPhase({ kind: "failed", taskId: "", error: body?.error ?? `HTTP ${resp.status}` });
      return;
    }
    setPhase({ kind: "running", taskId: body.task_id });
  };

  const busy = phase.kind === "running";
  return (
    <section style={{ marginTop: "1.5rem" }}>
      <h2>端到端场景：生成经营报告（耗时任务）</h2>
      <button
        onClick={start}
        disabled={busy}
        style={{ padding: "0.5rem 1.2rem", fontSize: "1rem", cursor: busy ? "wait" : "pointer" }}
      >
        {busy ? "后台执行中…" : "生成报告"}
      </button>

      {phase.kind === "running" && (
        <p>
          状态：<b>{STAGE_LABELS[phase.stage ?? ""] ?? "排队/调度中"}</b>
          {"　"}（task_id: <code>{phase.taskId || "…"}</code>）
          <br />
          <small>
            函数正在后台由平台推进，可同时打开{" "}
            <a href="http://localhost:8288/dashboard" target="_blank" rel="noreferrer">
              平台 Dashboard
            </a>{" "}
            查看 run 时间线。
          </small>
        </p>
      )}

      {phase.kind === "completed" && (
        <div>
          <p>✅ 任务完成，平台 Run 详情可见同样结果。后续操作：{phase.followup ?? "执行中…"}</p>
          <pre style={{ background: "#f4f4f4", padding: "0.8rem" }}>
            {JSON.stringify(phase.result, null, 2)}
          </pre>
        </div>
      )}

      {phase.kind === "failed" && <p>❌ 任务失败：{phase.error ?? "未知错误"}</p>}
    </section>
  );
}
