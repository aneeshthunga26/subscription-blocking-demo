#!/usr/bin/env node
// Headless check of the demo server (no browser, no deps — uses Node's
// built-in WebSocket and speaks graphql-transport-ws by hand).
//
//   node scripts/probe.mjs syncNaive
//   node scripts/probe.mjs syncSpawnBlocking [records]
//
// Opens one socket with the heartbeat subscription, then a second socket
// running the requested sync subscription, and reports:
//   * when each progress frame arrived vs when the server produced it
//   * the largest heartbeat gap observed while the sync ran
// Under starvation (syncNaive, WORKERS=1) the heartbeat flatlines and all
// progress frames arrive in a burst at the end.

const field = process.argv[2] ?? 'syncNaive'
const records = process.argv[3] ? `(records: ${process.argv[3]})` : ''
// Point at the vite dev server instead with WS_URL=ws://localhost:5173/graphql/ws
const URL = process.env.WS_URL ?? 'ws://localhost:8088/graphql/ws'

function gqlSocket(query, { onNext, onComplete }) {
  const ws = new WebSocket(URL, 'graphql-transport-ws')
  ws.onopen = () => ws.send(JSON.stringify({ type: 'connection_init' }))
  ws.onmessage = (e) => {
    const msg = JSON.parse(e.data)
    if (msg.type === 'connection_ack') {
      ws.send(JSON.stringify({ id: '1', type: 'subscribe', payload: { query } }))
    } else if (msg.type === 'next') {
      onNext(msg.payload.data)
    } else if (msg.type === 'complete') {
      onComplete?.()
      ws.close()
    } else if (msg.type === 'error') {
      console.error('subscription error:', JSON.stringify(msg.payload))
      process.exit(1)
    }
  }
  ws.onerror = (e) => {
    console.error(`websocket error (is the server running on :8088?)`, e.message ?? '')
    process.exit(1)
  }
  return ws
}

// ── socket 1: heartbeat ──
let lastTick = null
let maxGapDuringSync = 0
let syncRunning = false
gqlSocket('subscription { heartbeat { tick } }', {
  onNext: () => {
    const now = performance.now()
    if (lastTick !== null && syncRunning) {
      maxGapDuringSync = Math.max(maxGapDuringSync, now - lastTick)
    }
    lastTick = now
  },
})

// give the heartbeat a second to settle, then start the sync
setTimeout(() => {
  syncRunning = true
  const t0 = performance.now()
  let first = null
  let count = 0

  console.log(`subscribing to ${field}${records} ...`)
  gqlSocket(`subscription { ${field}${records} { done total serverElapsedMs } }`, {
    onNext: (data) => {
      const p = data[field]
      const clientMs = Math.round(performance.now() - t0)
      first ??= { clientMs, serverMs: p.serverElapsedMs }
      count++
      if (count % 10 === 0 || p.done === p.total) {
        console.log(
          `  frame #${String(count).padStart(2)}  ${p.done}/${p.total}  server +${p.serverElapsedMs}ms  client +${clientMs}ms`,
        )
      }
    },
    onComplete: () => {
      syncRunning = false
      // Include the still-open gap: under total starvation no tick arrives
      // during the sync at all, so the only evidence is how stale the last
      // tick is right now.
      if (lastTick !== null) {
        maxGapDuringSync = Math.max(maxGapDuringSync, performance.now() - lastTick)
      }
      const totalMs = Math.round(performance.now() - t0)
      console.log(`\n=== ${field} ===`)
      console.log(`  frames received:          ${count}`)
      console.log(`  first frame:              server +${first.serverMs}ms, client +${first.clientMs}ms`)
      console.log(`  run finished:             client +${totalMs}ms`)
      console.log(`  max heartbeat gap:        ${Math.round(maxGapDuringSync)}ms`)
      const burst = first.clientMs > totalMs * 0.8
      const heartbeatDied = maxGapDuringSync > 1000
      if (burst && heartbeatDied) {
        console.log('  ❌ STARVED: frames arrived in a burst at the end, heartbeat flatlined — whole worker frozen')
      } else if (burst) {
        console.log('  ⚠️  HIDDEN: own frames still burst at the end, but the heartbeat survived on another worker — extra workers shrink the blast radius without fixing the subscription')
      } else {
        console.log('  ✅ LIVE: frames streamed as produced, heartbeat kept ticking')
      }
      process.exit(0)
    },
  })
}, 1200)
