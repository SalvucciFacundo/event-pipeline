import type { WorkerConfig } from '../types'

/**
 * WorkerSelector lets the operator change the live worker count. It shows
 * the reported active count and reflects it rather than assuming the resize
 * happened instantly.
 */
export function WorkerSelector({ config, onSet }: { config: WorkerConfig; onSet: (n: number) => void }) {
  return (
    <div className="flex items-center gap-3 border border-line bg-panel px-4 py-2">
      <span className="font-mono text-[10px] uppercase tracking-widest text-dim">workers</span>
      <span className="font-mono text-sm text-text tabular-nums">
        <span className="text-signal">{config.active}</span>
        <span className="text-dim"> / {config.desired}</span>
      </span>
      <div className="flex gap-1">
        {[1, 2, 4, 8].map((n) => (
          <button
            key={n}
            onClick={() => onSet(n)}
            className={`border px-2 py-0.5 font-mono text-xs transition-colors ${
              config.desired === n
                ? 'border-signal text-signal'
                : 'border-line text-dim hover:border-amber hover:text-amber'
            }`}
          >
            {n}
          </button>
        ))}
      </div>
    </div>
  )
}
