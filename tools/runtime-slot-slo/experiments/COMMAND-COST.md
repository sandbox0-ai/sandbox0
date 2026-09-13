# D-COMMAND-COST: response waiting versus node CPU work

2026-09-12. Evidence: `/tmp/sandbox0-command-cost.wjKOrB`.
Six new ordinary-layout guests; no object publication or regional claim.

## Result

The slow first cold guest has nearly the same node CPU work and immutable
demand as the final cold guest, but substantially longer HTTP first-byte waits.
This narrows the roughly one-second variation to the response-wait path rather
than a doubling of local CPU work. It does not distinguish OSS processing/cache,
network delay and client callback scheduling, or establish a universal bound.
Local decode, integrity verification and decryption are independently material.

Three pairs use the exact optimized ordinary Node descriptor from D-LOOP-NET.
Each creates a fresh provider/encrypted wrapper and empty node Reader/header
caches, then runs cold and cached-new COW/runsc/procd identities. HTTP callbacks
are present in all pairs. Only O2 adds Go CPU sampling and execution trace.

| Pair / cohort | Local authenticated ready, ms | node-v, ms | Local operation to command done, ms | Reader-process CPU, ms | Owned-cgroup CPU, ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| O1 cold | 1008.522 | 1314.206 | 2322.850 | 857.271 | 1035.455 |
| O1 cached-new | 161.890 | 93.574 | 255.523 | 143.207 | 286.171 |
| O2 cold, profiled | 549.186 | 793.279 | 1342.559 | 890.474 | 1054.620 |
| O2 cached-new, profiled | 166.362 | 93.316 | 259.767 | 146.847 | 284.851 |
| O3 cold | 563.479 | 754.477 | 1318.042 | 866.646 | 1033.219 |
| O3 cached-new | 197.437 | 94.477 | 291.998 | 147.117 | 298.721 |

CPU values are consumed CPU time, not wall time. The owned cgroup includes the
Reader process, runsc/procd and diagnostic helpers; it is not a pure guest CPU
counter. CPU work overlaps object requests and can use two cores. Do not add
CPU and request-wait sums, or subtract them to invent an exact residual.

The local operation includes network/key setup, Reader construction, NBD/mount,
runsc and authenticated procd plus the real command. It excludes provider and
credential preparation, regional ingress, manager/PG transactions and occupied
production width. Profile flushing and teardown occur after the command timing.
Preserve O1's local >1s readiness and >2s combined result. These are **zero new
regional acceptance samples**, not a production SLO or size-independence proof.

## Request timing

| Cold pair | HTTP attempts / reused connections | Ciphertext returned, bytes | Write-to-first-byte P50 / P95 / max, ms | First-byte-to-close P50, ms |
| --- | ---: | ---: | ---: | ---: |
| O1 | 265 / 262 | 48,914,479 | 16.329 / 42.213 / 101.010 | 0.229 |
| O2 | 262 / 260 | 48,799,651 | 6.448 / 15.320 / 37.581 | 0.240 |
| O3 | 264 / 261 | 48,848,863 | 6.978 / 17.629 / 34.130 | 0.233 |

All 791 HTTP attempts return206, match one raw source operation by exact source
ID, key hash, requested range and returned bytes, and close without error. All
six guests read 93,223,424 unique NBD bytes before command completion. Cached-new
guests make zero source/HTTP requests while still issuing new NBD reads; cache
snapshots match exactly across each pair's new identities.

Approximately99% of HTTP attempts reuse connections. Total connection-acquisition
durations are about71/74/71ms per cold pair (concurrent sums); repeated fresh TLS
handshakes do not explain most of the first guest's extra second. The union of
write-to-first-byte intervals is1895.573/950.657/947.526ms. A union means at least
one pending response; it is not necessarily a critical-path stall and may overlap
useful CPU work. First-byte callbacks themselves can be delayed by scheduling.

The observer only checks `x-oss-request-id`, which is absent in these recorded
responses. No validated provider request IDs or new server-side SLS timing were
captured. Do not imply an exact server-log correlation from the source IDs.
Provider caches remain uncontrolled even though all node caches start empty.

## CPU and scheduling attribution

O2 cold consumes890.474ms self CPU before profiler stop; its CPU profile samples
880ms at the normal10ms sampling granularity. Cumulative sampled paths include:

- Range decode and checksum verification:330ms, including zstd and digest work.
- zstd-related stacks:180ms (overlaps the decode category).
- Encrypted frame read/decrypt/write path:110ms; not all of this is AES itself.
- TLS-related stacks:80ms; RSA object-key unwrap stacks:10ms.
- Background GC-worker stacks:50ms.
- Diagnostic NBD content hashing:50ms cold and70ms cached-new. This is observer
  work, not product checksum overhead. Cached-new has no sampled range decode,
  encrypted-frame decryption or TLS work.

The cold Node-command phase alone uses615.293ms Reader-process CPU and666.071ms
owned-cgroup CPU during793.279ms wall time. Cold controls show similar Reader CPU
in that phase:587.335/596.109ms. The cache removes both object waits and most
local transformation work, not just network latency.

The execution trace has93,589 cold and17,267 cached event headers, with complete
successful parsing. Cold runnable waits have P95=0.740ms and max=9.795ms. Their
sum across goroutines is1617.979ms but interval union is272.137ms; neither is
an exact command-path penalty. Idle HTTP loops, observers and parallel workers
contribute. There are20 GC sweep and20 GC mark-termination pauses totaling
2.304ms. Stop-the-world GC is not a seconds-long pause in this profile; allocation
and concurrent GC CPU costs are separate.

All-process sampled host accounting is42.15% busy,37.99% idle and19.86% iowait,
with no observed steal, owned-cgroup throttling or OOM. CPU PSI is present. These
unoccupied2-vCPU/8GiB samples do not exonerate host contention at production
density. NBD wait can contribute to I/O pressure; it does not prove a slow disk.

O2 remains near the later lightweight control, but O1's response conditions
differ. The surrounding controls do not produce an exact profiler-overhead
correction or randomized performance comparison.

## Decision and next admitted change

Keep the ordinary format/control and original requirements. Do not reopen the
layout ABBA, enlarge timeouts/caches or sweep machine sizes from this evidence.
Two costs remain: response-wait variation and repeatable node transformation work.

Current `decryptEncryptedObjectFrames` allocates ciphertext and separate AEAD
plaintext buffers for each frame. The measured frame path makes a bounded
per-read scratch/in-place-decryption experiment admissible: keep every AEAD,
range, checksum, stale-header retry and partial-output contract, use no global
pool, and require identical object requests/bytes plus actual CPU/allocation
improvement. This is a **candidate**, not an implementation or a claimed speedup.
The whole sampled frame path is only110ms; eliminating its allocation overhead
alone cannot be credited with solving the full regional/density gate. Similar
copy reduction must preserve cache backing-array ownership and accounting.

Do not remove integrity checks because their CPU cost is visible. Keep response
waiting as an independent track; these results do not justify calling the issue
only S3, only a small machine, or only user-process startup.

## Verification and lifecycle

The SDK, credential provider, retryer and transport remain original. A one-line
compile overlay adds only a per-Get HTTP decorator; cache counter hooks are
additive. Runtime passes the concrete Reader directly to Branch. No storage,
codec, cache, concurrency, timeout, readahead or product algorithm changed.
Source staging remains detached throughout; executable publication mode is
rejected before I/O. No OSS object writes, image rebuild, SLS collection, service
restart, production rollout, merge/tag, deletion or local e2e.

Nineteen Go race tests/vet/build pass. Seven offline tests/1636 assertions
reproduce the analysis and verify HTTP/source binding, new identities and profile
accounting. Preserve two offline observer failures: Go1.21 outside the module
could not parse the Go1.25 trace; selecting the original module toolchain fixes
that. Verbose parsed stacks then exceed a64MiB artifact bound; stream/hash the
complete output and retain every event header within16MiB, using pprof for stack
attribution. Neither correction reruns a guest or changes captured profiles.

The single remote campaign succeeds; all six commands/proofs and cleanup checks
pass. All39 remote artifacts transfer with hashes. Final checks inspect236
process mount tables and verify owned loop/NBD/mount/cgroup/veth absence and
unchanged original runtime files/PIDs, PG rows, Nomad job and host network.
SSH closes; stop completes and a fresh13:00:30UTC read confirms
Stopped/StopCharging/no public IP. Preserve all prior files and `/data`.
All1353 frozen product files and193 dirty worktree entries remain; only requested
experiment notes change. Full populated-root/upper-history, semantics/authority,
occupied production-width and regional readiness plus immediate command gates
remain unproven. Generic shared carriers and claim-time RootFS binding stay fixed.

Follow-up: [FRAME-SCRATCH.md](FRAME-SCRATCH.md) verifies the proposed scratch
candidate. Real cold-read process allocation falls about19%, but the reverse
control is faster and late CPU is nearly unchanged. Exact object request sets
also differ. Retain it as an allocation candidate, not a demonstrated startup
fix; do not keep tuning this small path instead of the full regional/density gate.
