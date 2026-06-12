import { useEffect, useRef, useState } from 'react'
import { subscribeOnFreshSocket } from '../lib/graphql'

/** Server emits a tick every 250ms; two missed ticks + margin = stalled. */
export const HEARTBEAT_INTERVAL_MS = 250
export const STALL_THRESHOLD_MS = 700

export interface HeartbeatState {
  status: 'connecting' | 'alive' | 'stalled'
  /** Completed inter-tick gaps in ms, most recent last (capped). */
  gaps: number[]
  /** ms since the most recent tick — grows live during a stall. */
  liveGapMs: number | null
}

const MAX_GAPS = 64

export function useHeartbeat(): HeartbeatState {
  const lastTickRef = useRef<number | null>(null)
  const [gaps, setGaps] = useState<number[]>([])
  const [nowMs, setNowMs] = useState(0)

  useEffect(() => {
    const dispose = subscribeOnFreshSocket<{ heartbeat: { tick: number } }>(
      'subscription { heartbeat { tick } }',
      {
        onNext: () => {
          const now = performance.now()
          if (lastTickRef.current !== null) {
            const gap = now - lastTickRef.current
            setGaps((prev) => [...prev.slice(-(MAX_GAPS - 1)), gap])
          }
          lastTickRef.current = now
        },
      },
    )
    // Drive the live "ms since last tick" display even while no ticks arrive
    // (that being the whole point of a stall).
    const ticker = setInterval(() => setNowMs(performance.now()), 100)
    return () => {
      clearInterval(ticker)
      dispose()
    }
  }, [])

  const liveGapMs = lastTickRef.current === null ? null : Math.max(0, nowMs - lastTickRef.current)
  const status: HeartbeatState['status'] =
    liveGapMs === null ? 'connecting' : liveGapMs > STALL_THRESHOLD_MS ? 'stalled' : 'alive'

  return { status, gaps, liveGapMs }
}
