const BASE = '/api/v1'

async function get(path) {
  const resp = await fetch(BASE + path)
  if (!resp.ok) throw new Error(`GET ${path}: ${resp.status}`)
  return resp.json()
}

function qs(params) {
  return new URLSearchParams(Object.entries(params).filter(([, v]) => v))
}

export const fetchRuns = (params = {}) => get(`/runs?${qs(params)}`)
export const fetchRun = (id) => get(`/runs/${id}`)
export const fetchEvents = (params = {}) => get(`/events?${qs(params)}`)
export const fetchFunctions = () => get('/functions')
