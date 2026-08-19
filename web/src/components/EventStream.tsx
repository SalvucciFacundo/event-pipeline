import type { StreamEvent } from '../types'

/**
 * EventStream renders the live event list with the most recent event on
 * top. Each row names real data: stream id, type, timestamp, source.
 */
export function EventStream({ events }: { events: StreamEvent[] }) {
  return (
    <div className="border border-line bg-panel">
      <div className="flex items-center justify-between border-b border-line px-3 py-2">
        <span className="font-mono text-[10px] uppercase tracking-widest text-dim">live stream</span>
        <span className="font-mono text-[10px] text-dim tabular-nums">{events.length} events</span>
      </div>
      <ul className="max-h-80 divide-y divide-line overflow-y-auto font-mono text-xs">
        {events.length === 0 && (
          <li className="px-3 py-3 text-dim">no events yet — use “emit event” to send real traffic</li>
        )}
        {events.map((ev) => (
          <li key={ev.id} className="flex flex-wrap items-baseline gap-x-3 gap-y-1 px-3 py-1.5">
            <span className="text-dim tabular-nums">{ev.id}</span>
            <span className="text-signal">{ev.type}</span>
            <span className="text-dim tabular-nums">{new Date(ev.occurred_at).toLocaleTimeString()}</span>
            {ev.source && <span className="text-dim">@{ev.source}</span>}
            <span className="w-full truncate text-text/70">{ev.payload}</span>
          </li>
        ))}
      </ul>
    </div>
  )
}
