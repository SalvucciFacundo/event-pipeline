export interface StreamEvent {
  id: string
  type: string
  payload: string
  occurred_at: string
  source?: string
}

export interface Snapshot {
  at: string
  per_second: Record<string, Record<string, number>>
  per_minute: Record<string, Record<string, number>>
  totals: Record<string, number>
}

export interface WorkerConfig {
  desired: number
  active: number
}
