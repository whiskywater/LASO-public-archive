# Durable session SSE contract

LASO exposes durable session events at `GET /api/v1/sessions/{session_id}/events/stream`. The server replays journal entries after the numeric `Last-Event-ID` cursor, then streams later entries. The first per-session sequence is 1; IDs are monotonically increasing journal sequence numbers. An omitted cursor starts at 0. A supplied cursor must be decimal and no larger than `INT64_MAX`; malformed or out-of-range cursors receive HTTP 400 before stream admission. Clients should persist the last fully processed ID, deduplicate by sequence, and reconnect with that ID. Delivery is replayable and may be duplicated across a disconnect boundary; it is not exactly-once. The event page endpoint accepts `after` and `limit` for explicit replay.

## Admission

The limit is per LASO server process, not cluster-wide. `max_session_sse_streams` configures the maximum from 1 through 128 and defaults to 32. Multi-instance deployments therefore have an independent limit on each process; this does not provide global fairness or a distributed admission queue.

When a request is authorized, its session and cursor are validated, and capacity is exhausted, LASO rejects the request before writing SSE response headers with HTTP `429 Too Many Requests`, `Content-Type: application/json`, `Cache-Control: no-store`, and `Retry-After: 1`. The body is a small sanitized JSON error. Rejection does not occupy a slot, subscribe to the session journal, mutate session state, or advance the event cursor. Clients should wait at least the supplied delay and reconnect with the same last successfully processed `Last-Event-ID`.

LASO does not queue waiting clients. Admission order follows server request scheduling; FIFO fairness is not promised. Operators should size the process limit for available connection/thread and storage capacity rather than remove the bound.

## Stream lifecycle

Admitted streams receive HTTP 200 and `Content-Type: text/event-stream`. A heartbeat comment is emitted after 15 seconds without an event. A stream ends when the client disconnects, the session is closed and its final event is delivered, a write/storage failure ends the handler, server shutdown closes sockets, or the 30-minute maximum stream lifetime is reached. The server watches for TCP peer closure during idle periods and cancels the journal poll when closure is observed, so the admission slot is released without waiting for the next heartbeat. A network black hole that does not produce a TCP close is detectable only when a later event/heartbeat write fails or the maximum lifetime expires. The admission slot is released as the stream handler exits.

After transport interruption, clients should reconnect using the most recent successfully handled numeric event ID. Events committed after that cursor are replayed from durable session storage. A client may see a duplicate if it received an event but did not durably record its cursor before disconnecting. There is no claim of exactly-once delivery. A full `429` response has not begun an SSE stream and therefore does not update `Last-Event-ID`.

Active, accepted, rejected, and closed stream counts plus configured capacity are available through the in-process `HttpServer::metrics()` snapshot for local operator instrumentation. They contain only counts and configured capacity, not session identifiers or event content.
