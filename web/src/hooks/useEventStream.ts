import { useEffect, useRef, useState } from 'react'
import type { Snapshot, StreamEvent, WorkerConfig } from '../types'

/**
 * useEventStream opens an EventSource to the pipeline's SSE endpoint and
 * drives the dashboard state. The browser natively handles reconnects and
 * sends Last-Event-ID, so replayed events arrive in order. Events are
 * deduplicated by stream ID to cover the theoretical replay/live overlap.
 */
export function useEventStream() {
  const [events, setEvents] = useState<StreamEvent[]>([])
  const [snapshot, setSnapshot] = useState<Snapshot>({ at: '', per_second: {}, per_minute: {}, totals: {} })
  const [connected, setConnected] = useState(false)
  const seen = useRef<Set<string>>(new Set())

  useEffect(() => {
    const es = new EventSource('/api/events/stream')

    es.onopen = () => setConnected(true)
    es.onerror = () => setConnected(false)

    es.addEventListener('event', (e) => {
      if (!(e instanceof MessageEvent)) return
      const ev = JSON.parse(e.data as string) as StreamEvent
      if (seen.current.has(ev.id)) return
      seen.current.add(ev.id)
      setEvents((prev) => [ev, ...prev].slice(0, 200))
    })

    es.addEventListener('snapshot', (e) => {
      if (!(e instanceof MessageEvent)) return
      setSnapshot(JSON.parse(e.data as string) as Snapshot)
    })

    return () => es.close()
  }, [])

  return { events, snapshot, connected }
}

/**
 * useWorkerConfig loads and updates the live worker count. It reflects the
 * reported active count rather than assuming an instantaneous resize.
 */
export function useWorkerConfig() {
  const [config, setConfig] = useState<WorkerConfig>({ desired: 0, active: 0 })

  async function refresh() {
    const res = await fetch('/api/config/workers')
    if (res.ok) setConfig(await res.json())
  }

  useEffect(() => {
    refresh()
    const id = setInterval(refresh, 2000)
    return () => clearInterval(id)
  }, [])

  async function setCount(count: number) {
    const res = await fetch('/api/config/workers', {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ count }),
    })
    if (res.ok) setConfig(await res.json())
  }

  return { config, setCount }
}
