import { useState } from 'react'
import { fetchFunctions, pauseFunction, resumeFunction } from '../api'
import { usePolling } from '../usePolling'

export default function FunctionListPage() {
  const { data, offline, refresh } = usePolling(fetchFunctions, 5000)
  const [pending, setPending] = useState(null) // 正在操作的函数 id
  const [actionError, setActionError] = useState(null)
  const fns = data?.functions || []

  // 暂停 = 不接新触发，在途 run 继续；恢复则重新接收触发
  const onTogglePause = async (f) => {
    setPending(f.id)
    setActionError(null)
    try {
      await (f.paused ? resumeFunction(f.id) : pauseFunction(f.id))
      await refresh()
    } catch (e) {
      setActionError(`${f.paused ? 'Resume' : 'Pause'} ${f.id} failed: ${e.message}`)
    } finally {
      setPending(null)
    }
  }

  return (
    <div className="space-y-4">
      <h1 className="text-lg font-semibold text-white">Functions</h1>
      {offline && (
        <div className="bg-amber-500/10 border border-amber-800 text-amber-400 text-sm rounded-md px-3 py-2">
          Connection lost, retrying…
        </div>
      )}
      {actionError && (
        <div className="bg-amber-500/10 border border-amber-800 text-amber-400 text-sm rounded-md px-3 py-2">
          {actionError}
        </div>
      )}
      <table className="w-full text-sm">
        <thead>
          <tr className="text-left text-xs uppercase tracking-wider text-neutral-500 border-b border-neutral-800">
            <th className="py-2 font-medium">Function</th><th className="font-medium">Event</th>
            <th className="font-medium">App URL</th><th className="font-medium">24h Runs</th>
            <th className="font-medium">24h Success</th><th className="font-medium"></th>
          </tr>
        </thead>
        <tbody>
          {fns.map((f) => {
            const rate = f.stats_24h.total > 0
              ? `${Math.round((f.stats_24h.completed / f.stats_24h.total) * 100)}%`
              : '—'
            return (
              <tr key={f.id} className="border-b border-neutral-800/60 hover:bg-neutral-900/60">
                <td className="py-2 font-mono text-xs text-neutral-200">
                  {f.id}
                  {f.paused && (
                    <span className="ml-2 inline-block px-1.5 rounded-full bg-amber-500/10 border border-amber-800 text-amber-400 text-xs">
                      paused
                    </span>
                  )}
                </td>
                <td className="font-mono text-xs text-neutral-300">{f.event}</td>
                <td className="font-mono text-xs text-neutral-500">{f.app_url}</td>
                <td className="text-neutral-400">{f.stats_24h.total}</td>
                <td className="text-neutral-300">
                  {rate}
                  {f.stats_24h.failed > 0 && (
                    <span className="ml-1 inline-block px-1.5 rounded-full bg-red-500/10 border border-red-900 text-red-400 text-xs">
                      {f.stats_24h.failed} failed
                    </span>
                  )}
                </td>
                <td className="text-right">
                  <button
                    onClick={() => onTogglePause(f)}
                    disabled={pending === f.id}
                    className={`rounded-md border px-3 py-1 text-xs disabled:opacity-50 ${
                      f.paused
                        ? 'border-emerald-800 bg-emerald-500/10 text-emerald-400 hover:bg-emerald-500/20'
                        : 'border-amber-800 bg-amber-500/10 text-amber-400 hover:bg-amber-500/20'
                    }`}
                  >
                    {pending === f.id ? '…' : f.paused ? 'Resume' : 'Pause'}
                  </button>
                </td>
              </tr>
            )
          })}
        </tbody>
      </table>
      {fns.length === 0 && (
        <div className="py-16 text-center text-sm text-neutral-500">No functions were found.</div>
      )}
    </div>
  )
}
