//! GraphQL schema: one query root, three subscriptions.
//!
//! * `syncNaive`          — ❌ blocking diesel loop spawned onto the runtime
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
    /// ❌ THE BUG — how open-msupply's V7 sync used to run.
    ///
    /// `tokio::spawn` puts the task on this worker's current-thread runtime,
    /// i.e. the SAME OS thread that drives this WebSocket, the heartbeat
    /// stream, and every other connection on this worker. The task's body is
    /// synchronous diesel work; `tx.send(...).await` always finds channel
    /// room so it returns `Ready` without ever yielding. The loop therefore
    /// holds the thread from first record to last: progress events pile up
    /// in the channel and the subscriber receives the whole run in one burst
    /// at the end.
    async fn sync_naive(&self, records: Option<i32>) -> impl Stream<Item = SyncProgress> {
        let total = records.map(|r| r as i64).unwrap_or(DEFAULT_RECORDS);
        let (tx, rx) = mpsc::channel::<SyncProgress>(CHANNEL_BUFFER);

        tokio::spawn(async move {
            // Everything below blocks the runtime thread, starting with the
            // seed itself.
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
                    // Async send — but it only yields when the channel is
                    // full, which it never is. No yield, no scheduling, no
                    // frames out.
                    if tx.send(progress).await.is_err() {
                        break; // subscriber went away
                    }
                }
            }
            // tx drops here → stream completes after the burst is drained.
        });

        ReceiverStream::new(rx)
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
