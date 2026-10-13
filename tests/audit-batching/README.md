# Audit batching comparison

The opt-in `TestAuditDeliveryPerformance` uses the real gateway audit middleware,
signed events, durable spool and ClickHouse repository. It requires disposable
ClickHouse 24.8 and S3-compatible storage. Its credentials and team/sandbox IDs
are synthetic fixtures; never point this test at a production endpoint.

`Audit batching check` runs race regressions, then measures the base and candidate
on fresh, identically configured backends. Each implementation receives three
30-second workloads at 10, 100 and 400 requests/second, with 5% canonical-sync
mutations and 95% durable-async reads. Sixteen clients and a 32-entry producer
queue bound concurrency. The traffic generator slows under backpressure rather
than spawning unbounded goroutines.

The comparison artifact records API P95/P99, acknowledged-event visibility
P95/P99, observed request throughput, new parts, merges, merged bytes, object
uploads, insert CPU and peak insert memory. Every successful request must have
both audit phases visible in the canonical table. Failed requests or missing
events fail the test. Ratios are evidence for rollout review, not cloud-billing
savings or an assertion that an optimization target has been reached.

The durable-async replay policy flushes at a fixed one-second boundary, 500
pending records or a 2 MiB encoded-event budget. New arrivals do not extend the
deadline. Restart and recovery drain existing backlogs immediately. A single
valid oversized record is delivered alone instead of blocking the queue.
Canonical foreground requests retain their short batching window and ACK
requirement. Network-audit ingestion retains its existing canonical ACK and
producer-side batching contract.

Replay skips individually owned events, not all foreground traffic. One replay
worker shares the bounded four-slot canonical writer pool. Persistent backlog
admission is capped at one million records or 1 GiB; capacity exhaustion tries
the existing canonical fallback and rejects an event if neither durable path
succeeds. Accepted records are never removed before canonical acknowledgement.
The in-memory spool and foreground queues remain bounded at 2048 entries.
Pending-event/byte gauges and canonical visibility histograms expose backlog
and delivery delay. Existing signing, retention and retry cooldown are preserved.
