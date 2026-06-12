//! Tiny diesel + SQLite "DB layer" — open-msupply's repository crate at
//! doll-house scale.
//!
//! Naming mirrors the real thing: a `sync_buffer` table holding records that
//! arrived from sync, and an "integrate" step that processes one buffered
//! record (here: flag it integrated + write a log row; in open-msupply:
//! translate + upsert into real tables).
//!
//! The one property that matters for the demo: every function here is
//! SYNCHRONOUS. Diesel has no async API — each call blocks the calling OS
//! thread until SQLite finishes. The caller decides which thread pays that
//! price, and that decision (runtime thread vs blocking pool) is the entire
//! point of this demo.

use diesel::prelude::*;
use diesel::sqlite::SqliteConnection;

diesel::table! {
    sync_buffer (id) {
        id -> BigInt,
        payload -> Text,
        integrated -> Bool,
    }
}

diesel::table! {
    integration_log (id) {
        id -> Nullable<BigInt>,
        record_id -> BigInt,
        message -> Text,
    }
}

#[derive(Insertable)]
#[diesel(table_name = sync_buffer)]
struct NewBufferRow {
    id: i64,
    payload: String,
    integrated: bool,
}

#[derive(Insertable)]
#[diesel(table_name = integration_log)]
struct NewLogRow {
    record_id: i64,
    message: String,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = sync_buffer)]
pub struct SyncBufferRow {
    pub id: i64,
    pub payload: String,
    pub integrated: bool,
}

/// Open a fresh in-memory database and seed `records` rows into
/// `sync_buffer`, as if a sync pull had just filled the buffer.
///
/// Per-run in-memory DBs keep concurrent runs independent; the blocking
/// characteristics are identical to a file DB.
pub fn open_seeded_db(records: i64) -> SqliteConnection {
    use diesel::connection::SimpleConnection;

    let mut conn = SqliteConnection::establish(":memory:").expect("open sqlite");
    conn.batch_execute(
        "CREATE TABLE sync_buffer (
            id INTEGER PRIMARY KEY,
            payload TEXT NOT NULL,
            integrated BOOLEAN NOT NULL DEFAULT 0
         );
         CREATE TABLE integration_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            record_id BIGINT NOT NULL,
            message TEXT NOT NULL
         );",
    )
    .expect("create tables");

    // Chunked bulk insert: 3 binds per row, SQLite caps bound variables at
    // 32766, so 10k rows per statement is comfortably inside the limit.
    for chunk_start in (0..records).step_by(10_000) {
        let chunk: Vec<NewBufferRow> = (chunk_start..(chunk_start + 10_000).min(records))
            .map(|i| NewBufferRow {
                id: i,
                payload: format!("{{\"table\":\"item\",\"record_id\":\"{i}\"}}"),
                integrated: false,
            })
            .collect();
        diesel::insert_into(sync_buffer::table)
            .values(&chunk)
            .execute(&mut conn)
            .expect("seed sync_buffer");
    }

    conn
}

/// "Integrate" one buffered record — a miniature of open-msupply's
/// `integrate_and_translate_sync_buffer` per-record work:
///
///   1. read the buffer row            (fetch)
///   2. parse its JSON payload         (translate)
///   3. mark it integrated + log row   (upsert + activity log)
///
/// Cheap individually, but a loop over the whole buffer holds the calling
/// thread for seconds — and none of these steps can ever yield to tokio.
pub fn integrate_one(conn: &mut SqliteConnection, record_id: i64) {
    let row: SyncBufferRow = sync_buffer::table
        .filter(sync_buffer::id.eq(record_id))
        .select(SyncBufferRow::as_select())
        .first(conn)
        .expect("fetch sync_buffer row");

    if row.integrated {
        return; // already processed in an earlier pass
    }

    let translated: serde_json::Value =
        serde_json::from_str(&row.payload).expect("parse payload");
    let table = translated["table"].as_str().unwrap_or("?");

    diesel::update(sync_buffer::table.filter(sync_buffer::id.eq(row.id)))
        .set(sync_buffer::integrated.eq(true))
        .execute(conn)
        .expect("update sync_buffer");

    diesel::insert_into(integration_log::table)
        .values(NewLogRow {
            record_id: row.id,
            message: format!("integrated {table} record {record_id}"),
        })
        .execute(conn)
        .expect("insert integration_log");
}
