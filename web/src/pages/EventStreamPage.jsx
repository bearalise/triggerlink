import { useState } from 'react'
import { Link } from 'react-router-dom'
import { fetchEvents } from '../api'
import { usePolling } from '../usePolling'
import JsonTree from '../components/JsonTree'

const fmt = (s) => new Date(s).toLocaleString()

function EventRow({ ev }) {
  const [open, setOpen] = useState(false)
  return (
    <li className="border border-neutral-800 bg-neutral-900/50 rounded-md p-2">
      <div className="flex items-center gap-3 text-sm">
        <button onClick={() => setOpen(!open)} className="font-mono text-neutral-200 hover:underline">
          {ev.name}
        </button>
        <span className="text-neutral-500 text-xs">{fmt(ev.received_at)}</span>
        <Link to={`/runs?event_id=${ev.id}`} className="text-blue-400 text-xs hover:underline">
          Triggered {ev.runs.length} run{ev.runs.length === 1 ? '' : 's'}
        </Link>
      </div>
      {open && <div className="mt-1"><JsonTree data={ev.data} /></div>}
    </li>
  )
}

export default function EventStreamPage() {
  const [name, setName] = useState('')
  const { data, offline } = usePolling(() => fetchEvents({ name }), 3000, [name])
  const events = data?.events || []
  return (
    <div className="space-y-4">
      <h1 className="text-lg font-semibold text-white">Stream</h1>
      {offline && (
        <div className="bg-amber-500/10 border border-amber-800 text-amber-400 text-sm rounded-md px-3 py-2">
          Connection lost, retrying…
        </div>
      )}
      <input
        value={name}
        onChange={(e) => setName(e.target.value)}
        placeholder="Filter by event name, e.g. doc/uploaded"
        className="bg-neutral-900 border border-neutral-700 rounded-md px-2 py-1 text-sm text-neutral-200 placeholder-neutral-600 w-72 focus:border-blue-500 focus:outline-none"
      />
      <ul className="space-y-2">
        {events.map((ev) => <EventRow key={ev.id} ev={ev} />)}
      </ul>
      {events.length === 0 && (
        <div className="py-16 text-center text-sm text-neutral-500">No events were found.</div>
      )}
    </div>
  )
}
