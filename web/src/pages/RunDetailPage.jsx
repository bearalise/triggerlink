import { useEffect, useState } from 'react'
import { useParams } from 'react-router-dom'
import { fetchRun, cancelRun } from '../api'
import { usePolling } from '../usePolling'
import StatusBadge from '../components/StatusBadge'
import JsonTree from '../components/JsonTree'

const ACTIVE = new Set(['queued', 'running'])
const fmt = (s) => new Date(s).toLocaleString()

const bannerCls =
  'bg-amber-500/10 border border-amber-800 text-amber-400 text-sm rounded-md px-3 py-2'

const BAR_COLORS = {
  completed: 'bg-emerald-500/70',
  failed: 'bg-red-500/70',
  running: 'bg-blue-500/70 animate-pulse',
  queued: 'bg-neutral-500/50',
}

// 甘特式 step 时间线：每步一条横向 bar，位置/宽度按时间占比绘制。
// 起止为近似值：start = 上一个 step 的 updated_at（首步用 run.created_at），end = 本步 updated_at。
// 注意：step 只有在完成/失败（应用回调带 opcode 返回）后才会落库，执行中的 step 平台侧无记录——
// 因此 run 处于 running 时在末尾追加一条 "executing…" 占位 bar（平台此时不知道具体 step id）。
function StepGantt({ steps, run }) {
  const running = run.status === 'running'
  if (steps.length === 0 && !running) {
    return <div className="text-sm text-neutral-500">No steps recorded yet.</div>
  }
  const t0 = new Date(run.created_at).getTime()
  const nowMs = Date.now()
  const lastEndMs = steps.length > 0 ? new Date(steps[steps.length - 1].updated_at).getTime() : t0
  const tEnd = Math.max(
    run.ended_at ? new Date(run.ended_at).getTime() : 0,
    lastEndMs,
    running ? nowMs : 0,
    t0 + 1, // 防止除零
  )
  const range = tEnd - t0
  return (
    <ol className="space-y-1">
      {steps.map((s, i) => (
        <StepBar
          key={i}
          index={i + 1}
          step={s}
          startMs={i > 0 ? new Date(steps[i - 1].updated_at).getTime() : t0}
          endMs={new Date(s.updated_at).getTime()}
          t0={t0}
          range={range}
        />
      ))}
      {running && (
        <InFlightBar index={steps.length + 1} startMs={lastEndMs} endMs={nowMs} t0={t0} range={range} />
      )}
    </ol>
  )
}

// 执行中占位条：从上一步结束（或 run 创建）到当前时刻，蓝色脉冲。
function InFlightBar({ index, startMs, endMs, t0, range }) {
  const dur = Math.max(0, (endMs - startMs) / 1000)
  const left = Math.min(99, Math.max(0, ((startMs - t0) / range) * 100))
  const width = Math.max(1, Math.min(100 - left, ((endMs - startMs) / range) * 100))
  return (
    <li>
      <div className="flex items-center gap-3 text-sm">
        <span className="text-neutral-600 w-6 text-right">#{index}</span>
        <span className="w-56 shrink-0 font-mono text-neutral-500 italic">executing…</span>
        <div className="relative flex-1 h-5 rounded bg-neutral-900 border border-neutral-800 overflow-hidden">
          <div
            className="absolute top-0 h-full rounded-sm bg-blue-500/70 animate-pulse"
            style={{ left: `${left}%`, width: `${width}%` }}
          />
        </div>
        <span className="w-14 shrink-0 text-right text-xs text-neutral-500">{dur.toFixed(1)}s</span>
        <StatusBadge status="running" />
      </div>
    </li>
  )
}

function StepBar({ index, step, startMs, endMs, t0, range }) {
  const [open, setOpen] = useState(step.status === 'failed')
  const dur = Math.max(0, (endMs - startMs) / 1000)
  const left = Math.min(99, Math.max(0, ((startMs - t0) / range) * 100))
  const width = Math.max(1, Math.min(100 - left, ((endMs - startMs) / range) * 100))
  return (
    <li>
      <div className="flex items-center gap-3 text-sm">
        <span className="text-neutral-600 w-6 text-right">#{index}</span>
        <button
          onClick={() => setOpen(!open)}
          className="w-56 shrink-0 truncate text-left font-mono text-neutral-200 hover:underline"
          title={step.step_id}
        >
          {step.step_id}
        </button>
        <div className="relative flex-1 h-5 rounded bg-neutral-900 border border-neutral-800 overflow-hidden">
          <div
            className={`absolute top-0 h-full rounded-sm ${BAR_COLORS[step.status] || BAR_COLORS.queued}`}
            style={{ left: `${left}%`, width: `${width}%` }}
          />
        </div>
        <span className="w-14 shrink-0 text-right text-xs text-neutral-500">{dur.toFixed(1)}s</span>
        <StatusBadge status={step.status} />
        {/* attempt 是 step 落 memo 时的 run 回调轮次：仅 failed 时有重试语义，completed 下只是步骤序号，不显示 */}
        {step.status === 'failed' && step.attempt > 1 && (
          <span className="text-xs text-amber-400">×{step.attempt}</span>
        )}
      </div>
      {open && (
        <div className="ml-[4.75rem] mt-1 mb-2 space-y-1">
          <div className="text-xs text-neutral-500">{step.op}</div>
          {step.error && (
            <pre className="bg-red-500/10 border border-red-900 text-red-400 text-xs rounded-md p-2 whitespace-pre-wrap">{step.error}</pre>
          )}
          <JsonTree data={step.output} />
        </div>
      )}
    </li>
  )
}

export default function RunDetailPage() {
  const { id } = useParams()
  const [terminal, setTerminal] = useState(false)
  const [stopping, setStopping] = useState(false)
  const [stopError, setStopError] = useState(null)
  useEffect(() => setTerminal(false), [id])
  const { data, offline } = usePolling(
    async () => {
      const d = await fetchRun(id)
      if (!ACTIVE.has(d.run.status)) setTerminal(true)
      return d
    },
    2000,
    [id],
    !terminal,
  )

  if (!data) {
    if (offline) {
      return <div className={bannerCls}>Failed to load (run may not exist), retrying…</div>
    }
    return <div className="text-neutral-500">Loading…</div>
  }
  const { run, steps } = data

  const onStop = async () => {
    if (!window.confirm(`Stop run ${run.id}? It will be marked as cancelled and no further steps will run.`)) return
    setStopping(true)
    setStopError(null)
    try {
      await cancelRun(run.id)
      // 状态由下一轮轮询(≤2s)收敛为 cancelled 并停止轮询
    } catch (e) {
      setStopError(e.message)
    } finally {
      setStopping(false)
    }
  }

  return (
    <div className="space-y-4">
      {offline && <div className={bannerCls}>Connection lost, retrying…</div>}
      {stopError && <div className={bannerCls}>Stop failed: {stopError}</div>}
      <div className="flex items-center gap-3">
        <h1 className="text-lg font-semibold font-mono text-white">{run.id}</h1>
        <StatusBadge status={run.status} />
        {ACTIVE.has(run.status) && (
          <button
            onClick={onStop}
            disabled={stopping}
            className="ml-auto rounded-md border border-red-800 bg-red-500/10 px-3 py-1 text-sm text-red-400 hover:bg-red-500/20 disabled:opacity-50"
          >
            {stopping ? 'Stopping…' : 'Stop run'}
          </button>
        )}
      </div>
      <div className="text-sm text-neutral-400 space-x-4">
        <span>Function <span className="font-mono text-neutral-200">{run.function_id}</span></span>
        <span>Attempt {run.attempt}</span>
        <span>Queued {fmt(run.created_at)}</span>
        {run.ended_at && <span>Ended {fmt(run.ended_at)}</span>}
      </div>
      {run.error && (
        <pre className="bg-red-500/10 border border-red-900 text-red-400 text-xs rounded-md p-2 whitespace-pre-wrap">{run.error}</pre>
      )}
      <section>
        <h2 className="font-medium text-neutral-200 mb-1">
          Trigger event <span className="font-mono">{run.event_name}</span>
        </h2>
        <JsonTree data={run.event_data} />
      </section>
      {run.output && (
        <section>
          <h2 className="font-medium text-neutral-200 mb-1">Output</h2>
          <JsonTree data={run.output} />
        </section>
      )}
      <section>
        <h2 className="font-medium text-neutral-200 mb-1">Step timeline</h2>
        <StepGantt steps={steps} run={run} />
      </section>
    </div>
  )
}
