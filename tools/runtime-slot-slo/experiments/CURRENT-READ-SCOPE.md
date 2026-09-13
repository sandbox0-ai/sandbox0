# D-CURRENT-READ-SCOPE: current-format reads and closure of the legacy pool lane

2026-09-13 Asia/Beijing. Evidence root: `/tmp/sandbox0-pool-health.E5nSZs`.
This is read-only fixture diagnosis and offline reuse of existing measurements,
not a new startup sample, runtime implementation or SLO pass.

## Current-format request binding

The retired format10005 trace rejected by D-NODE-BINDING is not needed to obtain
current-format volume reads. D-MAPPING-TRACE already captured the exact current
Node `2b5208c9...` and Coding `21a2f3ba...` format2 artifacts. Bind its active
resource leases, exact filesystem/generation/writer epochs, sandbox samples and
raw/parsed A/B traces. Reverify13 selected trace files against the pinned seal,
all65 D-NODE-BINDING files and1359 unchanged product files.

All14373 existing NBD reads bind to16 original sandbox identities:7187 cold,
7186 cached-new, zero standby reads. Both cohorts have four Node and four Coding
claims, and every first command is the original literal `node -v`. A request's
start determines its phase; no actual request crosses a claim/command boundary.
The32 per-sandbox phase groups independently match a512-byte-sector set oracle.
Four unit tests/eight assertions cover interval union, invalid ranges,
half-open phase boundaries and exact nanosecond timestamp conversion.

| Exact artifact / phase | Unique requested volume bytes per sandbox | Requests per sandbox |
| --- | ---: | ---: |
| Node / claim | 22,284,800 | 304 |
| Coding / claim | 22,350,848 | 317–327 |
| Node / first command | 70,938,624 | 575 |
| Coding / first command | 71,806,976 | 598 |

Each row's unique volume-byte result is identical in the cold and cached-new
cohorts. These are whole-volume NBD coordinates, including speculative kernel
read-ahead and filesystem reads: NOT Node-file offsets, minimum guest working
set, S3 transferred bytes, HTTP counts or sequential critical-path requests.
Do not sum unique-byte rows without cross-phase deduplication. They are not new
remote measurements; D-READER-COST already replayed this same14373-read schedule.

For these two images, the64-fold logical-volume-size difference does not produce
a comparable increase in observed startup volume reads. This does not isolate
size causally: Coding is sparse, the populated contents/dependencies differ, and
mapping layout/cache/source-service time remain distinct. It proves neither a
universal size-independent bound nor that memory/CPU/S3 are innocent.

The trace used the historical stock runsc20260810 and existing diagnostic
instrumentation. Its artifact/volume coordinates are useful; its times cannot
replace the current-pin regional reference or occupied-width acceptance. Before
any generic ELF real-read proposal, obtain exact CURRENT file extent translation
and account for extra reads/decode/cache pressure. Do not reuse the retired65MB
union or its42MiB conditional delta. No new broad tracing run is justified.

## D-POOL-HEALTH: close the already-fixed recovery lane

The previous boot `9b2e1ddd-d7a6-46bb-ad97-16a2f02d0f9f` has two exact old-driver
`decode recovered task config: EOF` events at20:17:39UTC. Same-allocation retry
then fails registration409, no-restart policy terminates it, and both slots are
terminal with `allocation_missing` by20:17:44. This is the already-reproduced
D-PERSISTED-CONFIG defect, not a new S3 timeout or candidate regression.

This turn performs only the normal ECS start and read-only observation. Boot
`ae04a502-2e09-462a-9964-e550d7ce6911` automatically obtains NEW allocations
`86406f20-2878-ad3b-2aab-f2bb811d9d75` and
`f80eeed1-81ae-a559-8f71-5205f0bde0ad`, both ready. JobModifyIndex72102, original
binary/config hashes and running service PIDs are unchanged during inspection.
No manual job mutation, restart, repair or candidate installation occurred.

The default fixture still uses the OLD driver `28a95851...`. The corrected driver
`ed647ff9...` plus manager refill was already remotely qualified in
[RECOVERY-REMOTE.md](RECOVERY-REMOTE.md). Do not implement or qualify that fix
again merely because the deliberately restored old fixture fails another boot.
Legacy readiness is not current-candidate readiness. An independently guarded
candidate installation needs exact idle/physical-absence, authority, history and
backup checks; it must not make old ready-count2 an unrelated prerequisite.
Timed claims still require the installed candidate's exact current-boot healthy
carriers, compatible pins, capacity and requested width. Never weaken that gate.

The old fixture's disk remains96% used,5,436,145,664 bytes available. Logs show
Nomad GC pressure, not ENOSPC or measured disk service latency. No guest, claim,
object read, database mutation or production change is initiated this turn.
This diagnosis does not explain the ready-carrier regional2.221/2.668s maxima.
Reuse existing request-ID-bound OSS evidence; do not restart machine/S3 sweeps.

## Completion of this bounded diagnostic

Original263 and formal2072 sandbox rows remain, all64NBD devices detached,
no runtime files/configs changed and no historical data deleted. Owned SSH is
closed. Fresh CLI observation at20:49:43.710UTC confirmsStopped/StopCharging,
original2CPU/8GiB shape and no public IP. Only experiment notes and temporary
diagnostic scripts/results change. No local e2e, merge, tag or rollout.

The overall cold/cached-new, populated-root/history, occupied-width and1s/2s
requirements remain open. This closes repeated legacy-pool diagnosis and
provides a hash-bound current-format coordinate input, not a speedup claim.
