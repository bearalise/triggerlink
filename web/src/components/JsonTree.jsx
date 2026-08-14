import { useState } from 'react'

function Node({ label, value }) {
  const [open, setOpen] = useState(false)
  if (value === null || typeof value !== 'object') {
    return (
      <div className="pl-4">
        {label !== null && <span className="text-violet-400">{JSON.stringify(label)}: </span>}
        <span className="text-neutral-300">{JSON.stringify(value)}</span>
      </div>
    )
  }
  const entries = Array.isArray(value) ? value.map((v, i) => [i, v]) : Object.entries(value)
  return (
    <div className="pl-4">
      <button onClick={() => setOpen(!open)} className="text-blue-400 hover:underline">
        {open ? '▾' : '▸'} {label !== null ? `${JSON.stringify(label)} ` : ''}
        {Array.isArray(value) ? `[${entries.length}]` : `{${entries.length}}`}
      </button>
      {open && entries.map(([k, v]) => <Node key={k} label={k} value={v} />)}
    </div>
  )
}

export default function JsonTree({ data }) {
  if (data === null || data === undefined) return <span className="text-neutral-600 text-xs">null</span>
  return (
    <div className="font-mono text-xs bg-neutral-900 border border-neutral-800 rounded-md p-2 overflow-x-auto">
      <Node label={null} value={data} />
    </div>
  )
}
