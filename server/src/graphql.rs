//! GraphQL schema: one query root, three subscriptions.
//!
//! * `syncNaive`          — ❌ no spawn at all: blocking diesel work runs
//!                          INSIDE the stream — yielding items does NOT
//!                          yield the thread; same starvation as a spawn
//! * `syncSpawnBlocking`  — ✅ same loop handed to tokio's blocking pool
//! * `heartbeat`          — 4 Hz tick; freezes whenever this worker thread is
//!                          hogged, making starvation visible from the UI
//!
//! The two sync subscriptions are intentionally written out long-hand so they
//! can be read side by side — the diff between them is the entire lesson.

use std::time::{Duration, Instant};

use async_graphql::{Context, Object, SimpleObject, Subscription};
use futures_util::{Stream, StreamExt};
use tokio::sync::mpsc;
use tokio_stream::wrappers::{IntervalStream, ReceiverStream};

use crate::db::{integrate_one, open_seeded_db};

/// Default number of sync_buffer records one "sync" integrates. Tuned so a
/// run takes a handful of seconds — long enough to watch the heartbeat die.
pub const DEFAULT_RECORDS: i64 = 400_000;
/// Emit a progress event every N records (mirrors open-msupply's
/// PROGRESS_INTERVAL in the V7 integration loop).
pub const PROGRESS_EVERY: i64 = 5_000;
/// More slots than progress events for a full run, so the producer's sends
/// always find room and never block/yield — same as the real bug, where the
/// channel never exerted backpressure.
const CHANNEL_BUFFER: usize = 256;

const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Clone, Copy)]
pub struct DemoConfig {
    pub workers: usize,
}

#[derive(SimpleObject, Clone)]
pub struct SyncProgress {
    /// Records integrated so far.
    pub done: i32,
    pub total: i32,
    /// Milliseconds since this run started, stamped ON THE SERVER when the
    /// event was produced. Compare against when the client receives it: under
    /// starvation these spread 0..runtime but all arrive at once at the end.
    pub server_elapsed_ms: i32,
}

#[derive(SimpleObject, Clone)]
pub struct HeartbeatTick {
    pub tick: i32,
}

#[derive(SimpleObject)]
pub struct ConfigNode {
    /// Number of actix worker threads the server was started with.
    pub workers: i32,
    pub default_records: i32,
    pub progress_every: i32,
}

pub struct QueryRoot;

#[Object]
impl QueryRoot {
    /// Trivial query the UI polls over HTTP to show whether this server can
    /// answer plain requests at all while a sync runs.
    async fn ping(&self) -> &str {
        "pong"
    }

    async fn config(&self, ctx: &Context<'_>) -> ConfigNode {
        let config = ctx.data_unchecked::<DemoConfig>();
        ConfigNode {
            workers: config.workers as i32,
            default_records: DEFAULT_RECORDS as i32,
            progress_every: PROGRESS_EVERY as i32,
        }
    }
}

pub struct SubscriptionRoot;

#[Subscription]
impl SubscriptionRoot {
    /// ❌ NO SPAWN AT ALL — the work runs inside the stream itself.
    ///
    /// There is no task and no channel here: the resolver returns a stream
    /// whose `poll` does the diesel work directly, driven by the connection
    /// task that owns this WebSocket — i.e. ON the runtime thread.
    ///
    /// The intuition trap: "it yields an item every 5,000 records, surely
    /// that lets the scheduler in?" Measured answer: NO. `yield` makes this
    /// poll return `Ready(Some(item))` — control goes to the CALLER (the
    /// connection task's forwarding loop), not to the scheduler. The caller
    /// `.await`s things that are also instantly `Ready` and polls this
    /// stream again. Ready-awaits never reach the scheduler, so from first
    /// poll to last the thread is never released: no WS write is flushed, no
    /// heartbeat fires, and every frame arrives in one burst at the end —
    /// byte-for-byte the same starvation as the `tokio::spawn` version on
    /// the main branch.
    ///
    /// (The band-aid that WOULD make this stream live is an explicit
    /// `tokio::task::yield_now().await` after each `yield` — a real Pending
    /// — which is variant 02 in the sibling runtime-blocking-demo. The fix
    /// is still spawn_blocking.)
    async fn sync_naive(&self, records: Option<i32>) -> impl Stream<Item = SyncProgress> {
        let total = records.map(|r| r as i64).unwrap_or(DEFAULT_RECORDS);

        async_stream::stream! {
            // Everything below runs on the runtime thread, starting with the
            // seed itself (the first poll blocks for the whole seed).
            let mut conn = open_seeded_db(total);
            let started = Instant::now();
            for id in 0..total {
                integrate_one(&mut conn, id);
                if (id + 1) % PROGRESS_EVERY == 0 || id + 1 == total {
                    // The only points where this stream's poll returns and
                    // the scheduler gets a look-in.
                    yield SyncProgress {
                        done: (id + 1) as i32,
                        total: total as i32,
                        server_elapsed_ms: started.elapsed().as_millis() as i32,
                    };
                }
            }
        }
    }

    /// ✅ THE FIX — what open-msupply does now (synchroniser.rs,
    /// `integrate_and_translate_sync_outer`).
    ///
    /// `spawn_blocking` moves the loop to tokio's dedicated blocking thread
    /// pool. The runtime thread stays free to drive this WebSocket, the
    /// heartbeat, and everyone else, so each progress event is delivered the
    /// moment it is produced. Inside the closure there is no async context —
    /// sending becomes `blocking_send`, which is exactly what those pool
    /// threads are for.
    async fn sync_spawn_blocking(&self, records: Option<i32>) -> impl Stream<Item = SyncProgress> {
        let total = records.map(|r| r as i64).unwrap_or(DEFAULT_RECORDS);
        let (tx, rx) = mpsc::channel::<SyncProgress>(CHANNEL_BUFFER);

        tokio::task::spawn_blocking(move || {
            // Identical work to sync_naive — only the thread differs. Note
            // the Send + 'static boundary: the connection is created INSIDE
            // the closure (open-msupply threads its SyncLogger across the
            // same boundary via SyncLoggerHandle).
            let mut conn = open_seeded_db(total);
            let started = Instant::now();
            for id in 0..total {
                integrate_one(&mut conn, id);
                if (id + 1) % PROGRESS_EVERY == 0 || id + 1 == total {
                    let progress = SyncProgress {
                        done: (id + 1) as i32,
                        total: total as i32,
                        server_elapsed_ms: started.elapsed().as_millis() as i32,
                    };
                    // Blocking send: parks THIS pool thread if the channel is
                    // full (it would panic if called on a runtime thread).
                    if tx.blocking_send(progress).is_err() {
                        break; // subscriber went away
                    }
                }
            }
        });

        ReceiverStream::new(rx)
    }

    /// Liveness probe for this worker thread. Emits a tick every 250ms — when
    /// the runtime thread is hogged, the gap between ticks is the visual
    /// proof. `Delay` keeps recovery clean (a gap, then steady ticks) instead
    /// of a burst of catch-up ticks.
    async fn heartbeat(&self) -> impl Stream<Item = HeartbeatTick> {
        let mut interval = tokio::time::interval(HEARTBEAT_INTERVAL);
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        IntervalStream::new(interval)
            .enumerate()
            .map(|(i, _)| HeartbeatTick { tick: i as i32 })
    }
}
