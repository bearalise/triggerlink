import { useCallback, useEffect, useRef, useState } from 'react'

// usePolling 每 intervalMs 调用 fn 拉取数据；active=false 时停止轮询。
// offline 表示最近一次拉取失败（连接中断横幅用），恢复后自动清零。
export function usePolling(fn, intervalMs, deps = [], active = true) {
  const [data, setData] = useState(null)
  const [error, setError] = useState(null)
  const [offline, setOffline] = useState(false)
  const fnRef = useRef(fn)
  fnRef.current = fn

  const tick = useCallback(async () => {
    try {
      const d = await fnRef.current()
      setData(d)
      setError(null)
      setOffline(false)
    } catch (e) {
      setError(e)
      setOffline(true)
    }
  }, [])

  useEffect(() => {
    if (!active) return undefined
    tick()
    const t = setInterval(tick, intervalMs)
    return () => clearInterval(t)
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [active, intervalMs, tick, ...deps])

  return { data, error, offline }
}
