import { useCallback, useState } from 'react'

const EVENT_TYPES = ['click', 'view', 'purchase', 'login', 'error'] as const

/**
 * EmitButton posts a real event to POST /events and reports the resulting
 * stream ID. It generates live data through the real pipeline — no mocks.
 */
export function EmitButton() {
  const [busy, setBusy] = useState(false)
  const [lastId, setLastId] = useState('')

  const emit = useCallback(async () => {
    setBusy(true)
    setLastId('')
    const type = EVENT_TYPES[Math.floor(Math.random() * EVENT_TYPES.length)]
    const payload = JSON.stringify({ value: Math.floor(Math.random() * 1000) })
    try {
      const res = await fetch('/events', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          type,
          payload,
          occurred_at: new Date().toISOString(),
          source: 'dashboard',
        }),
      })
      if (res.ok) {
        const body = await res.json()
        setLastId(body.id)
      }
    } finally {
      setBusy(false)
    }
  }, [])

  return (
    <div className="flex items-center gap-3">
      <button
        onClick={emit}
        disabled={busy}
        className="border border-line bg-panel px-4 py-1.5 font-mono text-sm text-signal transition-colors hover:border-signal disabled:opacity-50"
      >
        {busy ? 'emitting…' : 'emit event'}
      </button>
      {lastId && <span className="font-mono text-xs text-dim">acked {lastId}</span>}
    </div>
  )
}
