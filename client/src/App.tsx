import { useEffect, useState } from 'react'
import { HeartbeatStrip } from './components/HeartbeatStrip'
import { SyncCard } from './components/SyncCard'
import { useHeartbeat } from './hooks/useHeartbeat'
import { useHttpPing } from './hooks/useHttpPing'
import { useSyncRun } from './hooks/useSyncRun'
import { queryHttp } from './lib/graphql'

interface Config {
  workers: number
  defaultRecords: number
  progressEvery: number
}

const NAIVE_CODE = `// ❌ no spawn at all — work runs inside the stream's poll
async_stream::stream! {             // polled by the connection task
    let mut conn = open_seeded_db(total);
    for id in 0..total {
        integrate_one(&mut conn, id);  // blocks the runtime thread
        if (id + 1) % 5_000 == 0 {
            yield progress;  // returns Ready — NOT a scheduling point
        }
    }
}`

const FIXED_CODE = `// ✅ resolver hands the work to the blocking pool
let (tx, rx) = mpsc::channel(256);
tokio::task::spawn_blocking(move || { // dedicated blocking thread
    for id in 0..total {
        integrate_one(&mut conn, id);  // blocks a pool thread: fine
        tx.blocking_send(progress);    // no async context in here
    }
});
ReceiverStream::new(rx)`

export default function App() {
  const heartbeat = useHeartbeat()
  const ping = useHttpPing()
  const naive = useSyncRun('syncNaive')
  const fixed = useSyncRun('syncSpawnBlocking')
  const [config, setConfig] = useState<Config | null>(null)

  useEffect(() => {
    queryHttp<{ config: Config }>('{ config { workers defaultRecords progressEvery } }')
      .then((d) => setConfig(d.config))
      .catch(() => {})
  }, [])

  return (
    <div className="mx-auto flex min-h-screen max-w-6xl flex-col gap-5 px-6 py-8 text-slate-200">
      <header className="flex flex-wrap items-end justify-between gap-3">
        <div>
          <h1 className="text-2xl font-bold text-slate-50">
            <span className="font-mono text-emerald-400">spawn_blocking</span> — why it matters
          </h1>
          <p className="mt-1 max-w-2xl text-sm text-slate-400">
            Two GraphQL subscriptions run the same synchronous diesel "sync integration"; they
            differ only in which thread does the work. Watch the heartbeat and the frame
            timestamps.
          </p>
        </div>
        <div className="flex gap-2">
          <span
            className={`rounded-full border px-3 py-1 text-xs font-medium ${
              config?.workers === 1
                ? 'border-amber-500/40 bg-amber-500/10 text-amber-300'
                : 'border-sky-500/40 bg-sky-500/10 text-sky-300'
            }`}
          >
            {config ? `${config.workers} actix worker${config.workers === 1 ? ' (single-threaded)' : 's'}` : '…'}
          </span>
          <span className="rounded-full border border-slate-700 bg-slate-800/60 px-3 py-1 text-xs font-medium text-slate-300">
            {config ? `${(config.defaultRecords / 1000).toFixed(0)}k records/run` : '…'}
          </span>
        </div>
      </header>

      <HeartbeatStrip heartbeat={heartbeat} ping={ping} />

      <main className="grid gap-5 lg:grid-cols-2">
        <SyncCard
          badge="❌ no spawn"
          title="subscription { syncNaive }"
          tone="rose"
          description="No task, no channel: the diesel work runs inside the stream itself, driven by
            this connection's task on the runtime thread. Intuition says each yield lets the
            scheduler in — it doesn't. A yield just returns Ready to the caller, which polls the
            stream again; an await that is already Ready never reaches the scheduler. The thread
            is never released: same total starvation as the tokio::spawn version on main."
          code={NAIVE_CODE}
          run={naive}
        />
        <SyncCard
          badge="✅ fixed"
          title="subscription { syncSpawnBlocking }"
          tone="emerald"
          description="What synchroniser.rs does now: the identical loop handed to tokio's blocking
            thread pool. The runtime thread stays free to drive every socket, so each progress
            frame is delivered the moment it is produced and the heartbeat never skips."
          code={FIXED_CODE}
          run={fixed}
        />
      </main>

      <footer className="rounded-xl border border-slate-800 bg-slate-900/60 p-5 text-sm text-slate-400">
        <h3 className="mb-2 font-semibold text-slate-200">What to look for</h3>
        <ul className="list-disc space-y-1.5 pl-5">
          <li>
            <span className="text-rose-400">❌ no spawn:</span> with 1 worker the progress bar
            sits at 0%, the heartbeat flatlines, HTTP ping hangs — then every frame lands at
            once. The yields changed nothing: <span className="font-mono">server +t</span> spreads
            across the run while <span className="font-mono">client +t</span> is identical for
            every row. Yielding <em>items</em> is not yielding the <em>thread</em> — only a
            Pending (e.g. <span className="font-mono">yield_now().await</span>) reaches the
            scheduler, and only <span className="font-mono">spawn_blocking</span> (the{' '}
            <span className="font-mono">main</span> branch's ✅ card) actually fixes it.
          </li>
          <li>
            <span className="text-emerald-400">✅ spawn_blocking:</span> frames stream live
            (server ≈ client time), heartbeat steady — while the exact same diesel work runs.
          </li>
          <li>
            Restart the server with <span className="font-mono">WORKERS=4</span>: the naive run no
            longer kills the heartbeat (it lives on another worker thread) — but its own frames{' '}
            <em>still</em> burst at the end, because its socket and its blocking task share one
            worker. actix workers are isolated single-thread runtimes with no work-stealing: more
            workers shrink the blast radius and <em>hide</em> the bug; only{' '}
            <span className="font-mono">spawn_blocking</span> fixes it.
          </li>
        </ul>
      </footer>
    </div>
  )
}
