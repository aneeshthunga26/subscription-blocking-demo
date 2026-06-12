import { useEffect, useRef, useState } from 'react'
import { queryHttp } from '../lib/graphql'

export interface HttpPingState {
  /** Round-trip of the last completed ping. */
  lastLatencyMs: number | null
  /** How long the current ping has been in flight — grows while the server is frozen. */
  inFlightForMs: number | null
}

/**
 * POST `{ ping }` once a second, one request in flight at a time. No timeout
 * on purpose: while the server's only worker is hogged the request simply
 * hangs, and `inFlightForMs` growing is the demo signal.
 */
export function useHttpPing(): HttpPingState {
  const inFlightSinceRef = useRef<number | null>(null)
  const [lastLatencyMs, setLastLatencyMs] = useState<number | null>(null)
  const [nowMs, setNowMs] = useState(0)

  useEffect(() => {
    let stopped = false
    const fire = async () => {
      if (stopped || inFlightSinceRef.current !== null) return
      const started = performance.now()
      inFlightSinceRef.current = started
      try {
        await queryHttp<{ ping: string }>('{ ping }')
        if (!stopped) setLastLatencyMs(performance.now() - started)
      } catch {
        // server unreachable — leave last latency as-is
      } finally {
        inFlightSinceRef.current = null
      }
    }
    fire()
    const interval = setInterval(fire, 1000)
    const ticker = setInterval(() => setNowMs(performance.now()), 100)
    return () => {
      stopped = true
      clearInterval(interval)
      clearInterval(ticker)
    }
  }, [])

  const inFlightForMs =
    inFlightSinceRef.current === null ? null : Math.max(0, nowMs - inFlightSinceRef.current)

  return { lastLatencyMs, inFlightForMs }
}
