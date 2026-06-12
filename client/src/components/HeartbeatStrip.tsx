import type { HeartbeatState } from '../hooks/useHeartbeat'
import { HEARTBEAT_INTERVAL_MS, STALL_THRESHOLD_MS } from '../hooks/useHeartbeat'
import type { HttpPingState } from '../hooks/useHttpPing'

function gapBar(gapMs: number, live: boolean, key: number) {
  // 250ms (healthy) ≈ 8px tall; a multi-second stall maxes out the strip.
  const height = Math.min(48, Math.max(4, (gapMs / HEARTBEAT_INTERVAL_MS) * 8))
  const color =
    gapMs < 400 ? 'bg-emerald-500/70' : gapMs < STALL_THRESHOLD_MS ? 'bg-amber-400' : 'bg-rose-500'
  return (
    <div
      key={key}
      title={`${Math.round(gapMs)}ms between ticks`}
      className={`w-1 shrink-0 rounded-t-sm ${color} ${live ? 'animate-pulse' : ''}`}
      style={{ height }}
    />
  )
}

export function HeartbeatStrip({
  heartbeat,
  ping,
}: {
  heartbeat: HeartbeatState
  ping: HttpPingState
}) {
  const { status, gaps, liveGapMs } = heartbeat
  const pingStalled = (ping.inFlightForMs ?? 0) > 1500

  return (
    <div className="flex items-center gap-6 rounded-xl border border-slate-800 bg-slate-900 px-5 py-4">
      {/* WS heartbeat status */}
      <div className="flex w-56 shrink-0 items-center gap-3">
        <span
          className={`h-3 w-3 rounded-full ${
            status === 'alive'
              ? 'animate-pulse bg-emerald-400'
              : status === 'stalled'
                ? 'bg-rose-500'
                : 'bg-slate-600'
          }`}
        />
        <div>
          <div className="text-sm font-medium text-slate-200">WS heartbeat</div>
          <div className={`text-xs ${status === 'stalled' ? 'font-semibold text-rose-400' : 'text-slate-400'}`}>
            {status === 'connecting' && 'connecting…'}
            {status === 'alive' && `tick every 250ms · last ${Math.round(liveGapMs ?? 0)}ms ago`}
            {status === 'stalled' && `STALLED — no tick for ${((liveGapMs ?? 0) / 1000).toFixed(1)}s`}
          </div>
        </div>
      </div>

      {/* inter-tick gap chart; the live (still-open) gap is the pulsing last bar */}
      <div className="flex h-12 min-w-0 flex-1 items-end justify-end gap-px overflow-hidden">
        {gaps.map((gap, i) => gapBar(gap, false, i))}
        {liveGapMs !== null && liveGapMs > 400 && gapBar(liveGapMs, true, -1)}
      </div>

      {/* HTTP liveness */}
      <div className="w-44 shrink-0 text-right">
        <div className="text-sm font-medium text-slate-200">HTTP ping</div>
        <div className={`text-xs ${pingStalled ? 'font-semibold text-rose-400' : 'text-slate-400'}`}>
          {pingStalled
            ? `no response for ${((ping.inFlightForMs ?? 0) / 1000).toFixed(1)}s`
            : ping.lastLatencyMs !== null
              ? `${Math.max(1, Math.round(ping.lastLatencyMs))}ms`
              : '—'}
        </div>
      </div>
    </div>
  )
}
