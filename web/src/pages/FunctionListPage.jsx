import { fetchFunctions } from '../api'
import { usePolling } from '../usePolling'

export default function FunctionListPage() {
  const { data, offline } = usePolling(fetchFunctions, 5000)
  const fns = data?.functions || []
  return (
    <div className="space-y-4">
      <h1 className="text-lg font-semibold text-white">Functions</h1>
      {offline && (
        <div className="bg-amber-500/10 border border-amber-800 text-amber-400 text-sm rounded-md px-3 py-2">
          Connection lost, retrying…
        </div>
      )}
      <table className="w-full text-sm">
        <thead>
          <tr className="text-left text-xs uppercase tracking-wider text-neutral-500 border-b border-neutral-800">
            <th className="py-2 font-medium">Function</th><th className="font-medium">Event</th>
            <th className="font-medium">App URL</th><th className="font-medium">24h Runs</th>
            <th className="font-medium">24h Success</th>
          </tr>
        </thead>
        <tbody>
          {fns.map((f) => {
            const rate = f.stats_24h.total > 0
              ? `${Math.round((f.stats_24h.completed / f.stats_24h.total) * 100)}%`
              : '—'
            return (
              <tr key={f.id} className="border-b border-neutral-800/60 hover:bg-neutral-900/60">
                <td className="py-2 font-mono text-xs text-neutral-200">{f.id}</td>
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
