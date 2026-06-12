import { useCallback, useEffect, useRef, useState } from 'react'
import { subscribeOnFreshSocket } from '../lib/graphql'

export type SyncField = 'syncNaive' | 'syncSpawnBlocking'

export type RunPhase = 'idle' | 'connecting' | 'waiting' | 'streaming' | 'done' | 'error'

export interface SyncEvent {
  seq: number
  done: number
  total: number
  /** When the server PRODUCED this frame (ms since its loop started). */
  serverElapsedMs: number
  /** When this browser RECEIVED it (ms since Run was clicked). */
  clientElapsedMs: number
}

export interface SyncRun {
  phase: RunPhase
  events: SyncEvent[]
  /** ms since Run was clicked; live while running, frozen once done. */
  elapsedMs: number
  errorMessage: string | null
  start: () => void
}

interface ProgressData {
  [field: string]: { done: number; total: number; serverElapsedMs: number }
}

export function useSyncRun(field: SyncField): SyncRun {
  const [phase, setPhase] = useState<RunPhase>('idle')
  const [events, setEvents] = useState<SyncEvent[]>([])
  const [elapsedMs, setElapsedMs] = useState(0)
  const [errorMessage, setErrorMessage] = useState<string | null>(null)
  const startedAtRef = useRef(0)
  const disposeRef = useRef<(() => void) | null>(null)

  // Live elapsed timer while a run is in flight — this is what makes the
  // frozen "waiting for first frame… 3.4s" state visible.
  const running = phase === 'connecting' || phase === 'waiting' || phase === 'streaming'
  useEffect(() => {
    if (!running) return
    const ticker = setInterval(
      () => setElapsedMs(performance.now() - startedAtRef.current),
      100,
    )
    return () => clearInterval(ticker)
  }, [running])

  useEffect(() => () => disposeRef.current?.(), [])

  const start = useCallback(() => {
    if (disposeRef.current && running) return
    setEvents([])
    setErrorMessage(null)
    setElapsedMs(0)
    setPhase('connecting')
    startedAtRef.current = performance.now()

    let seq = 0
    disposeRef.current = subscribeOnFreshSocket<ProgressData>(
      `subscription { ${field} { done total serverElapsedMs } }`,
      {
        onConnected: () =>
          setPhase((p) => (p === 'connecting' ? 'waiting' : p)),
        onNext: (data) => {
          const p = data[field]
          if (!p) return
          seq += 1
          const event: SyncEvent = {
            seq,
            ...p,
            clientElapsedMs: performance.now() - startedAtRef.current,
          }
          setPhase('streaming')
          setEvents((prev) => [...prev, event])
        },
        onComplete: () => {
          setElapsedMs(performance.now() - startedAtRef.current)
          setPhase('done')
          disposeRef.current = null
        },
        onError: (err) => {
          setErrorMessage(err instanceof Error ? err.message : JSON.stringify(err))
          setElapsedMs(performance.now() - startedAtRef.current)
          setPhase('error')
          disposeRef.current = null
        },
      },
    )
  }, [field, running])

  return { phase, events, elapsedMs, errorMessage, start }
}
