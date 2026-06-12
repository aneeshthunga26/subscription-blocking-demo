import { useEffect, useRef } from 'react'
import type { SyncRun } from '../hooks/useSyncRun'

const fmtSec = (ms: number) => `${(ms / 1000).toFixed(1)}s`
const fmtCount = (n: number) => n.toLocaleString('en')

const TONE = {
  rose: {
    chip: 'bg-rose-500/15 text-rose-400 border-rose-500/30',
    button: 'bg-rose-600 hover:bg-rose-500 disabled:hover:bg-rose-600',
    bar: 'bg-rose-500',
  },
  emerald: {
    chip: 'bg-emerald-500/15 text-emerald-400 border-emerald-500/30',
    button: 'bg-emerald-600 hover:bg-emerald-500 disabled:hover:bg-emerald-600',
    bar: 'bg-emerald-500',
  },
} as const

const PHASE_LABEL: Record<string, string> = {
  idle: 'idle',
  connecting: 'connecting',
  waiting: 'subscribed — waiting',
  streaming: 'receiving frames',
  done: 'completed',
  error: 'error',
}

export function SyncCard({
  badge,
  title,
  tone,
  description,
  code,
  run,
}: {
  badge: string
  title: string
  tone: keyof typeof TONE
  description: string
  code: string
  run: SyncRun
}) {
  const { phase, events, elapsedMs, errorMessage, start } = run
  const t = TONE[tone]
  const running = phase === 'connecting' || phase === 'waiting' || phase === 'streaming'
  const last = events.at(-1) ?? null
  const first = events[0] ?? null
  const pct = last ? Math.round((last.done / last.total) * 100) : 0

  const logRef = useRef<HTMLDivElement>(null)
  useEffect(() => {
    logRef.current?.scrollTo({ top: logRef.current.scrollHeight })
  }, [events.length])

  const buttonLabel = (() => {
    switch (phase) {
      case 'connecting':
        return `connecting… ${fmtSec(elapsedMs)}`
      case 'waiting':
        return `subscribed — waiting for first frame… ${fmtSec(elapsedMs)}`
      case 'streaming':
        return `integrating… ${fmtSec(elapsedMs)}`
      case 'done':
        return 'run again'
      default:
        return 'run sync'
    }
  })()

  return (
    <div className="flex flex-col gap-4 rounded-xl border border-slate-800 bg-slate-900 p-5">
      <div className="flex items-start justify-between gap-3">
        <div className="flex items-center gap-2.5">
          <span className={`rounded-md border px-2 py-0.5 text-sm font-semibold ${t.chip}`}>{badge}</span>
          <h2 className="font-mono text-base font-semibold text-slate-100">{title}</h2>
        </div>
        <span className="rounded-full bg-slate-800 px-2.5 py-1 text-xs text-slate-300">
          {PHASE_LABEL[phase]}
        </span>
      </div>

      <p className="text-sm leading-relaxed text-slate-400">{description}</p>

      <pre className="overflow-x-auto rounded-lg border border-slate-800 bg-slate-950 p-3 text-xs leading-5 text-slate-300">
        {code}
      </pre>

      <button
        onClick={start}
        disabled={running}
        className={`w-full rounded-lg py-2.5 text-sm font-semibold text-white transition-colors disabled:cursor-not-allowed disabled:opacity-70 ${t.button}`}
      >
        {buttonLabel}
      </button>

      {/* progress */}
      <div className="flex items-center gap-3">
        <div className="h-3 flex-1 overflow-hidden rounded-full bg-slate-800">
          <div
            className={`h-full rounded-full transition-[width] duration-150 ${t.bar}`}
            style={{ width: `${pct}%` }}
          />
        </div>
        <span className="w-10 text-right font-mono text-xs text-slate-400">{pct}%</span>
      </div>

      {/* stats */}
      <div className="grid grid-cols-3 gap-2 text-center">
        <div className="rounded-lg bg-slate-950/60 px-2 py-2">
          <div className="text-[11px] uppercase tracking-wide text-slate-500">first frame at</div>
          <div className="font-mono text-sm text-slate-200">
            {first ? fmtSec(first.clientElapsedMs) : '—'}
          </div>
          <div className="text-[11px] text-slate-500">
            {first ? `produced at ${fmtSec(first.serverElapsedMs)}` : ''}
          </div>
        </div>
        <div className="rounded-lg bg-slate-950/60 px-2 py-2">
          <div className="text-[11px] uppercase tracking-wide text-slate-500">frames</div>
          <div className="font-mono text-sm text-slate-200">{events.length}</div>
          <div className="text-[11px] text-slate-500">
            {last ? `${fmtCount(last.done)}/${fmtCount(last.total)}` : ''}
          </div>
        </div>
        <div className="rounded-lg bg-slate-950/60 px-2 py-2">
          <div className="text-[11px] uppercase tracking-wide text-slate-500">
            {phase === 'done' ? 'finished in' : 'elapsed'}
          </div>
          <div className="font-mono text-sm text-slate-200">
            {phase === 'idle' ? '—' : fmtSec(elapsedMs)}
          </div>
        </div>
      </div>

      {/* frame log: server-produced vs client-received timestamps */}
      <div
        ref={logRef}
        className="h-40 overflow-y-auto rounded-lg border border-slate-800 bg-slate-950 p-2 font-mono text-[11px] leading-5"
      >
        {events.length === 0 ? (
          <div className="flex h-full items-center justify-center text-slate-600">
            {running ? 'no frames received yet…' : 'frames will appear here'}
          </div>
        ) : (
          events.map((e) => {
            const delayed = e.clientElapsedMs - e.serverElapsedMs > 1000
            return (
              <div key={e.seq} className="flex gap-4 whitespace-nowrap">
                <span className="w-8 text-slate-600">#{e.seq}</span>
                <span className="w-36 text-slate-400">
                  {fmtCount(e.done)}/{fmtCount(e.total)}
                </span>
                <span className="w-28 text-slate-500">server +{fmtSec(e.serverElapsedMs)}</span>
                <span className={delayed ? 'font-semibold text-rose-400' : 'text-emerald-400'}>
                  client +{fmtSec(e.clientElapsedMs)}
                </span>
              </div>
            )
          })
        )}
      </div>

      {errorMessage && (
        <div className="rounded-lg border border-rose-500/30 bg-rose-500/10 p-2 text-xs text-rose-300">
          {errorMessage}
        </div>
      )}
    </div>
  )
}
