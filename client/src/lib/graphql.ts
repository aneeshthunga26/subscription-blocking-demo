import { createClient } from 'graphql-ws'

const WS_URL = `${location.protocol === 'https:' ? 'wss:' : 'ws:'}//${location.host}/graphql/ws`

export interface SubscribeHandlers<T> {
  /** WebSocket open + connection_ack received — the subscription is registered. */
  onConnected?: () => void
  onNext: (data: T) => void
  onComplete?: () => void
  onError?: (err: unknown) => void
}

/**
 * Run one subscription on its OWN WebSocket and return a cleanup function.
 *
 * One fresh socket per subscription is deliberate: graphql-ws normally
 * multiplexes everything over a single connection, which would pin the
 * heartbeat and both sync runs to the SAME actix worker thread — and the
 * WORKERS=4 experiment would show nothing. Separate sockets give actix the
 * chance to place them on different workers.
 */
export function subscribeOnFreshSocket<T>(
  query: string,
  handlers: SubscribeHandlers<T>,
): () => void {
  const client = createClient({
    url: WS_URL,
    retryAttempts: 0,
    shouldRetry: () => false,
  })
  if (handlers.onConnected) client.on('connected', handlers.onConnected)

  const unsubscribe = client.subscribe<T>(
    { query },
    {
      next: (value) => {
        if (value.data) handlers.onNext(value.data as T)
      },
      complete: () => {
        handlers.onComplete?.()
        setTimeout(() => client.dispose(), 0)
      },
      error: (err) => {
        handlers.onError?.(err)
        setTimeout(() => client.dispose(), 0)
      },
    },
  )

  return () => {
    try {
      unsubscribe()
    } finally {
      client.dispose()
    }
  }
}

/** Plain HTTP POST /graphql — used for config and the liveness ping. */
export async function queryHttp<T>(query: string): Promise<T> {
  const res = await fetch('/graphql', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ query }),
  })
  const json = await res.json()
  if (json.errors?.length) throw new Error(json.errors[0].message)
  return json.data as T
}
