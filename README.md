# subscription-blocking-demo

End-to-end demo of **when and why `spawn_blocking` is needed** when you serve
GraphQL subscriptions from actix while doing synchronous (diesel) DB work —
the exact bug open-msupply hit with its V7 sync, made visible in a browser.

The problem of one request blocking others is not only a subscription issue and can happen with regular http requests (graphql queries/mutations etc.) as well. 

This demo builds upon [`runtime-blocking-demo`](https://github.com/andreievg/runtime-blocking-demo) which is a terminal version of the same lesson; this demo adds the full stack similar to what [`open-msupply](https://github.com/msupply-foundation/open-msupply)
actually run in production: actix workers, `async-graphql` subscriptions over
WebSocket, a diesel "DB layer", and a React + Tailwind SPA so you can *watch*
the starvation happen.

| open-msupply                                                    | this demo                                  |
| --------------------------------------------------------------- | ------------------------------------------ |
| `HttpServer::workers(n)` (current-thread runtime per worker)    | same, `WORKERS` env, default **1**         |
| `async_graphql` subscriptions over `/graphql/ws`                | same routes, same crate versions           |
| V7 sync integration loop (sync diesel over `sync_buffer`)       | `integrate_one()` loop over `sync_buffer`  |
| progress via `logger.progress()` → channel → subscribers        | progress via mpsc → subscription stream    |
| fix: `spawn_blocking` in `synchroniser.rs`                       | `subscription { syncSpawnBlocking }`       |
| the bug: integration ran directly on the runtime                | `subscription { syncNaive }`               |

## Run it

```bash
# terminal 1 — server (single actix worker by default)
cd server && cargo run

# terminal 2 — SPA on http://localhost:5173 (proxies /graphql to :8088)
cd client && npm install && npm run dev
```

Open http://localhost:5173. GraphiQL is also at http://localhost:8088/graphql.

No browser handy? `node scripts/probe.mjs syncNaive` does the same check
headlessly and prints a verdict.

## What you'll see

The page keeps two always-on liveness signals: a **WS heartbeat** subscription
(server ticks every 250 ms) and an **HTTP ping** (1/s). Then two buttons, each
opening one GraphQL subscription that runs the *same* ~4 s diesel integration
(400k `sync_buffer` records) and streams progress frames. Every frame carries
the time it was **produced** (server clock); the UI logs when it was
**received** (client clock).

### ❌ `syncNaive` — the bug

```rust
tokio::spawn(async move {              // lands on this worker's runtime thread
    for id in 0..total {
        integrate_one(&mut conn, id);  // synchronous diesel — never yields
        tx.send(progress).await;       // channel has room → returns Ready → no yield
    }
});
```

With one worker the whole server freezes for the duration: heartbeat flatlines,
HTTP ping hangs, progress bar sits at 0%, the *other* button can't even
connect. When the loop finishes, **every frame lands at once** — the log shows
frames produced at `server +0.1s … +3.4s` all received at `client +3.8s`.
The work was never "slow to report"; the reports could not be delivered.

### ✅ `syncSpawnBlocking` — the fix

```rust
tokio::task::spawn_blocking(move || {  // dedicated blocking thread pool
    for id in 0..total {
        integrate_one(&mut conn, id);
        tx.blocking_send(progress);    // no async context in the closure
    }
});
```

Identical work, identical sends — but the runtime thread stays free, so frames
stream live (`server +t ≈ client +t`) and the heartbeat never skips. This is
the shape of the real fix in open-msupply's `synchroniser.rs`
(`integrate_and_translate_sync_outer`): the closure must be `Send + 'static`,
so the DB connection is created *inside* it — open-msupply threads its
`SyncLogger` across the same boundary via `SyncLoggerHandle`.

### ⚠️ More workers *hide* it, they don't fix it

```bash
WORKERS=4 cargo run
```

Re-run `syncNaive`: the heartbeat and ping now survive (they live on other
worker threads), so at a glance the server "works". But the naive run's **own
frames still arrive in one burst** — its WebSocket and the task it spawned
share one worker thread, and actix workers are *isolated single-thread
runtimes with no work-stealing* (unlike `#[tokio::main(flavor =
"multi_thread")]`, which is what makes the sibling demo's variant 04 pass).

Note that more workers only improve the *odds*, and not a guarntee that other tasks won't get starved. Each
new connection is simply assigned to the next worker in turn, and a spawned
task stays on the worker that spawned it. So the blocking task can still land
on the same worker as another task (the heartbeat, someone else's request) —
and when it does, that task starves just as badly as with one worker.

That's the production trap: on a many-core server the bug shrinks to "this one
subscription is weirdly bursty", and on the single-core box in the field it
becomes "the UI freezes for the whole sync". Verified headlessly:

```
$ node scripts/probe.mjs syncNaive            # WORKERS=1
  first frame:        server +42ms, client +3809ms
  max heartbeat gap:  4002ms
  ❌ STARVED: frames arrived in a burst at the end, heartbeat flatlined

$ node scripts/probe.mjs syncSpawnBlocking    # WORKERS=1
  first frame:        server +41ms, client +464ms
  max heartbeat gap:  251ms
  ✅ LIVE: frames streamed as produced, heartbeat kept ticking

$ node scripts/probe.mjs syncNaive            # WORKERS=4
  first frame:        server +42ms, client +3847ms
  max heartbeat gap:  251ms
  ⚠️  HIDDEN: own frames still burst, but the heartbeat survived on another worker
```

## Why this happens (the one-paragraph version)

An actix worker is one OS thread running a cooperative scheduler. `await` only
hands control back when a future returns `Pending` — diesel calls never
`await`, and `mpsc::Sender::send` on a channel with free capacity returns
`Ready` immediately. So an "async" task whose body is sync DB work holds the
thread from start to finish, and every other future scheduled there (WebSocket
writers, heartbeat timers, HTTP handlers) is *ready but never polled*.
`spawn_blocking` moves the work to tokio's blocking pool, which exists for
exactly this; `blocking_send` (panics on a runtime thread, correct on a pool
thread) is the tell that you're on the right side of the boundary.

## Layout

```
server/            actix + async-graphql + diesel (SQLite in-memory, bundled)
  src/db.rs        the "DB layer": sync_buffer schema, seed, integrate_one()
  src/graphql.rs   syncNaive / syncSpawnBlocking / heartbeat resolvers
  src/main.rs      routes + WORKERS handling
client/            Vite + React 19 + Tailwind v4 + graphql-ws
  src/hooks/       useSyncRun, useHeartbeat, useHttpPing
  src/components/  SyncCard (frame log), HeartbeatStrip (gap chart)
scripts/probe.mjs  headless verifier (zero deps, hand-rolled graphql-transport-ws)
```

One detail worth copying: the client opens **one WebSocket per subscription**
(no multiplexing) so that with `WORKERS>1` actix can place the heartbeat and
the sync runs on different workers — multiplexed onto one socket they would
all share a worker and the `WORKERS=4` experiment would show nothing.

In open-msupply, see `server/service/src/sync/synchroniser.rs`
(`integrate_and_translate_sync_outer`) for the real fix,
`server/service/src/subscription.rs` for the worker that fans triggers out to
subscribers, and `server/graphql/general/src/subscriptions/` for the streams.
