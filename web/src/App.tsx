import { useEventStream, useWorkerConfig } from './hooks/useEventStream'
import { CountersPanel } from './components/CountersPanel'
import { EmitButton } from './components/EmitButton'
import { EventStream } from './components/EventStream'
import { WorkerSelector } from './components/WorkerSelector'

export default function App() {
  const { events, snapshot, connected } = useEventStream()
  const { config, setCount } = useWorkerConfig()

  return (
    <div className="min-h-screen bg-surface">
      <header className="flex items-center justify-between border-b border-line px-4 py-3">
        <div className="flex items-center gap-3">
          <h1 className="font-mono text-lg text-text">event pipeline</h1>
          <span
            className={`font-mono text-[10px] uppercase tracking-widest ${
              connected ? 'text-signal' : 'text-alert'
            }`}
          >
            {connected ? '● connected' : '○ reconnecting'}
          </span>
        </div>
        <EmitButton />
      </header>

      <main className="space-y-4 p-4">
        <WorkerSelector config={config} onSet={setCount} />
        <CountersPanel snapshot={snapshot} />
        <EventStream events={events} />
      </main>
    </div>
  )
}
