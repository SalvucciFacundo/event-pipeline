import type { Snapshot } from '../types'

function windowLabel(windowKey: string, granularity: 's' | 'm') {
  const n = Number(windowKey)
  const date = new Date(n * 1000)
  return granularity === 's'
    ? date.toISOString().substring(11, 19)
    : date.toISOString().substring(11, 16)
}

function typeBars(counts: Record<string, number>, granularity: 's' | 'm') {
  const max = Math.max(1, ...Object.values(counts))
  return Object.entries(counts)
    .sort((a, b) => a[0].localeCompare(b[0]))
    .map(([windowKey, value]) => {
      const pct = Math.round((value / max) * 100)
      return (
        <div key={windowKey} className="flex items-center gap-2">
          <span className="w-14 shrink-0 text-right font-mono text-[10px] text-dim">
            {windowLabel(windowKey, granularity)}
          </span>
          <div className="h-3 flex-1 border border-line bg-surface">
            <div className="h-full bg-signal/60" style={{ width: `${pct}%` }} />
          </div>
          <span className="w-6 shrink-0 font-mono text-[10px] text-text tabular-nums">{value}</span>
        </div>
      )
    })
}

/**
 * CountersPanel renders per-type totals and per-type bars for the latest
 * second and minute windows. Data is real aggregation output, never mocked.
 */
export function CountersPanel({ snapshot }: { snapshot: Snapshot }) {
  const latestSec = Object.keys(snapshot.per_second).sort().pop() ?? ''
  const latestMin = Object.keys(snapshot.per_minute).sort().pop() ?? ''
  const secCounts = snapshot.per_second[latestSec] ?? {}
  const minCounts = snapshot.per_minute[latestMin] ?? {}

  const total = Object.values(snapshot.totals).reduce((a, b) => a + b, 0)

  return (
    <div className="grid grid-cols-1 gap-4 md:grid-cols-3">
      <div className="border border-line bg-panel p-4">
        <div className="mb-2 font-mono text-[10px] uppercase tracking-widest text-dim">totals</div>
        <div className="font-mono text-3xl text-signal tabular-nums">{total}</div>
        <ul className="mt-3 space-y-1 font-mono text-xs">
          {Object.entries(snapshot.totals)
            .sort((a, b) => b[1] - a[1])
            .map(([type, count]) => (
              <li key={type} className="flex justify-between text-text">
                <span className="text-dim">{type}</span>
                <span className="tabular-nums">{count}</span>
              </li>
            ))}
        </ul>
      </div>

      <div className="border border-line bg-panel p-4">
        <div className="mb-2 font-mono text-[10px] uppercase tracking-widest text-dim">per second</div>
        {typeBars(secCounts, 's')}
        {Object.keys(secCounts).length === 0 && (
          <div className="font-mono text-xs text-dim">no traffic in the last second</div>
        )}
      </div>

      <div className="border border-line bg-panel p-4">
        <div className="mb-2 font-mono text-[10px] uppercase tracking-widest text-dim">per minute</div>
        {typeBars(minCounts, 'm')}
        {Object.keys(minCounts).length === 0 && (
          <div className="font-mono text-xs text-dim">no traffic in the last minute</div>
        )}
      </div>
    </div>
  )
}
