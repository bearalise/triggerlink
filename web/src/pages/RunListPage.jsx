import { useEffect, useState } from 'react'
import { Link, useSearchParams } from 'react-router-dom'
import { fetchFunctions, fetchRuns } from '../api'
import { usePolling } from '../usePolling'
import StatusBadge from '../components/StatusBadge'

const fmt = (s) => new Date(s).toLocaleString()

const selectCls =
  'bg-neutral-900 border border-neutral-700 rounded-md px-2 py-1 text-sm text-neutral-200 focus:border-blue-500 focus:outline-none'

export default function RunListPage() {
  const [searchParams] = useSearchParams()
  const [functionID, setFunctionID] = useState(searchParams.get('function_id') || '')
  const [status, setStatus] = useState(searchParams.get('status') || '')
  const eventID = searchParams.get('event_id') || ''
  const [extra, setExtra] = useState([])

  const params = { function_id: functionID, status, event_id: eventID }
  const { data, offline } = usePolling(() => fetchRuns(params), 2000, [functionID, status, eventID])
  const { data: fnData } = usePolling(fetchFunctions, 60000)

  useEffect(() => setExtra([]), [functionID, status, eventID])

  const runs = [...(data?.runs || []), ...extra]
  const loadMore = async () => {
    const last = runs[runs.length - 1]
    if (!last) return
    const d = await fetchRuns({ ...params, before: last.id })
    setExtra((prev) => [...prev, ...d.runs])
  }

  return (
    <div className="space-y-4">
      <h1 className="text-lg font-semibold text-white">Runs</h1>
      {offline && (
        <div className="bg-amber-500/10 border border-amber-800 text-amber-400 text-sm rounded-md px-3 py-2">
          Connection lost, retrying…
        </div>
      )}
      <div className="flex gap-2 items-center">
        <select value={functionID} onChange={(e) => setFunctionID(e.target.value)} className={selectCls}>
          <option value="">All functions</option>
          {(fnData?.functions || []).map((f) => (
            <option key={f.id} value={f.id}>{f.id}</option>
          ))}
        </select>
        <select value={status} onChange={(e) => setStatus(e.target.value)} className={selectCls}>
          <option value="">All statuses</option>
          {['queued', 'running', 'completed', 'failed'].map((s) => (
            <option key={s} value={s}>{s}</option>
          ))}
        </select>
        <span className="text-sm text-neutral-500">{runs.length} runs</span>
        {eventID && (
          <span className="text-sm text-neutral-500">
            Runs of event <span className="font-mono text-neutral-300">{eventID}</span>
          </span>
        )}
      </div>
      <table className="w-full text-sm">
        <thead>
          <tr className="text-left text-xs uppercase tracking-wider text-neutral-500 border-b border-neutral-800">
            <th className="py-2 font-medium">Status</th><th className="font-medium">Run ID</th>
            <th className="font-medium">Function</th><th className="font-medium">Trigger</th>
            <th className="font-medium">Attempts</th><th className="font-medium">Queued at</th>
          </tr>
        </thead>
        <tbody>
          {runs.map((r) => (
            <tr key={r.id} className="border-b border-neutral-800/60 hover:bg-neutral-900/60">
              <td className="py-2"><StatusBadge status={r.status} /></td>
              <td className="font-mono text-xs">
                <Link to={`/runs/${r.id}`} className="text-blue-400 hover:underline">{r.id}</Link>
              </td>
              <td className="font-mono text-xs text-neutral-300">{r.function_id}</td>
              <td className="font-mono text-xs text-neutral-400">{r.event_name}</td>
              <td className="text-neutral-400">{r.attempt}</td>
              <td className="text-neutral-500">{fmt(r.created_at)}</td>
            </tr>
          ))}
        </tbody>
      </table>
      {runs.length === 0 && (
        <div className="py-16 text-center text-sm text-neutral-500">No results were found.</div>
      )}
      {runs.length > 0 && (
        <button onClick={loadMore} className="text-sm text-blue-400 hover:underline">Load more</button>
      )}
    </div>
  )
}
