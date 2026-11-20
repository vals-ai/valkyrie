# Request draining reference

See the [setup guide](DRAINING.md) to turn draining on. CBS adds no control endpoint and no traffic routing.

## Local API

| Call | Behavior |
| --- | --- |
| `DrainState.create(path)` | Creates a closed store at revision `-1`: no controller command yet. |
| `DrainState(path)` | Opens an existing store. |
| `set_admission(accepting=..., revision=...)` | Opens or closes the store. Nonnegative revisions order commands. Older commands do nothing. Conflicts at the same revision raise errors. |
| `snapshot()` | Returns `store_id`, `revision`, `accepting`, and `in_flight`. |
| `admit(request_id)` | Accepts a request only while open. |
| `complete(request_id)` | Removes that request. Repetition cannot reduce another request's count. |

- Middleware calls `admit` and `complete` for HTTP requests and WebSocket connections, including error responses and awaited cleanup.
- Crashed workers leave their requests counted. A missing, replaced, locked, or corrupt store raises an error rather than reporting completion.

## Rejection response

- Closed workers return HTTP 503 with `X-Benchmark-Service-Draining: 1`, before any benchmark work starts. `is_drain_rejection(status, headers)` recognizes that status and header together.
- WebSockets are rejected during connection setup, before the client sends its payload. Without ASGI `websocket.http.response`, the rejection has no retry header.

## Retry rules

- The client keeps starting retries for up to 60 seconds. That window doesn't limit grading time.
- HTTP retries turn off keep-alive in a separate pool, to skip idle connections to old tasks. Accepted requests keep their connections.
- WebSocket retries open a new connection with a 60-second connection timeout. Once connected, the loop never replays the payload.
- Ordinary 503 responses and uncertain failures don't trigger draining retries, and older HTTP retries are unchanged.
- DNS caches can still return an old task.
