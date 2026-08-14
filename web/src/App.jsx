import { NavLink, Navigate, Route, Routes } from 'react-router-dom'
import RunListPage from './pages/RunListPage'
import RunDetailPage from './pages/RunDetailPage'
import EventStreamPage from './pages/EventStreamPage'
import FunctionListPage from './pages/FunctionListPage'

const itemCls = ({ isActive }) =>
  `block rounded-md px-3 py-1.5 text-sm transition-colors ${
    isActive
      ? 'bg-blue-600/20 text-blue-400 font-medium'
      : 'text-neutral-400 hover:text-neutral-100 hover:bg-neutral-800/60'
  }`

function NavGroup({ title, children }) {
  return (
    <div>
      <div className="px-3 pt-5 pb-1 text-xs font-medium uppercase tracking-wider text-neutral-600">
        {title}
      </div>
      <div className="space-y-0.5">{children}</div>
    </div>
  )
}

export default function App() {
  return (
    <div className="flex min-h-screen bg-neutral-950 text-neutral-200">
      <aside className="w-52 shrink-0 border-r border-neutral-800 px-3 py-4">
        <div className="flex items-baseline gap-2 px-3 pb-2">
          <span className="text-lg font-bold text-white">TriggerLink</span>
          <span className="text-[10px] font-medium uppercase tracking-wider text-neutral-500">
            Dev Server
          </span>
        </div>
        <nav>
          <NavGroup title="Monitor">
            <NavLink to="/runs" className={itemCls}>Runs</NavLink>
            <NavLink to="/events" className={itemCls}>Stream</NavLink>
          </NavGroup>
          <NavGroup title="Manage">
            <NavLink to="/functions" className={itemCls}>Functions</NavLink>
          </NavGroup>
        </nav>
      </aside>
      <main className="flex-1 min-w-0 p-6 max-w-6xl">
        <Routes>
          <Route path="/runs" element={<RunListPage />} />
          <Route path="/runs/:id" element={<RunDetailPage />} />
          <Route path="/events" element={<EventStreamPage />} />
          <Route path="/functions" element={<FunctionListPage />} />
          <Route path="*" element={<Navigate to="/runs" replace />} />
        </Routes>
      </main>
    </div>
  )
}
