const STYLES = {
  queued: { dot: 'bg-neutral-400', text: 'text-neutral-300', bg: 'bg-neutral-500/10 border-neutral-700' },
  running: { dot: 'bg-blue-400 animate-pulse', text: 'text-blue-400', bg: 'bg-blue-500/10 border-blue-800' },
  completed: { dot: 'bg-emerald-400', text: 'text-emerald-400', bg: 'bg-emerald-500/10 border-emerald-800' },
  failed: { dot: 'bg-red-400', text: 'text-red-400', bg: 'bg-red-500/10 border-red-800' },
}

const FALLBACK = { dot: 'bg-neutral-400', text: 'text-neutral-300', bg: 'bg-neutral-500/10 border-neutral-700' }

export default function StatusBadge({ status }) {
  const s = STYLES[status] || FALLBACK
  return (
    <span className={`inline-flex items-center gap-1.5 rounded-full border px-2 py-0.5 text-xs font-medium ${s.bg} ${s.text}`}>
      <span className={`h-1.5 w-1.5 rounded-full ${s.dot}`} />
      {status}
    </span>
  )
}
