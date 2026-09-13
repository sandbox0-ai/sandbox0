# D-LOOP-NET: encrypted OSS to real guest commands

2026-09-12. Qualified evidence: `/tmp/sandbox0-loop-net-qualified.GuLqwt`.
Preserved failed attempt: `/tmp/sandbox0-loop-net.XEArUb`.

## Result and decision

The direct-loop wrapper reduces object requests, but this comparison does not
establish a repeatable latency advantage over ordinary layout. The reverse
ordinary control catches up. Do not adopt the unchosen nested-XFS/EROFS candidate
or claim the first control's entire difference as an optimization win.

All eight guests complete authenticated readiness and real `node -v`, returning
v22.23.2 with distinct procd and sandbox identities. Each pair constructs a new
encrypted store and empty 128MiB Reader cache, then starts cold and cached-new
guests. Cold means empty node data/mapping/header caches; provider caches are
uncontrolled, and the wrapper objects were just published.

| Pair / cohort | Local operation to authenticated ready, ms | node-v, ms | Local operation to command done, ms | Object HTTP attempts |
| --- | ---: | ---: | ---: | ---: |
| O1 cold | 881.330 | 1272.261 | 2153.593 | 265 |
| O1 cached-new | 191.014 | 96.893 | 287.909 | 0 |
| W1 cold | 580.386 | 768.059 | 1348.447 | 207 |
| W1 cached-new | 182.904 | 90.339 | 273.245 | 0 |
| W2 cold | 603.388 | 755.834 | 1359.223 | 203 |
| W2 cached-new | 196.800 | 91.839 | 288.641 | 0 |
| O2 cold | 569.582 | 785.361 | 1354.945 | 263 |
| O2 cached-new | 193.644 | 95.696 | 289.342 | 0 |

The local operation includes ephemeral network/key setup, Reader construction,
NBD/mount, stock runsc and procd. It excludes per-pair provider/credential
preparation, regional ingress, manager/PG claim, production occupancy and
teardown. These are **zero new regional acceptance samples**. Preserve O1's
local combined >2s result; subsequent samples do not erase it. No timeout changed.

| Cold pair | NBD unique bytes | Encoded plaintext returned | Ciphertext returned | Client ciphertext acquire-to-close P50 / P95, ms |
| --- | ---: | ---: | ---: | ---: |
| O1 | 93,223,424 | 44,627,117 | 48,881,671 | 15.365 / 42.883 |
| W1 | 90,952,192 | 45,061,041 | 48,303,386 | 9.034 / 22.887 |
| W2 | 90,952,192 | 44,955,156 | 48,139,346 | 9.146 / 23.506 |
| O2 | 93,223,424 | 44,642,676 | 48,865,267 | 8.718 / 20.528 |

Mean request count is 264 versus 205, a 22.35% reduction. Encoded plaintext
volume is slightly higher in the wrapper; ciphertext volume decreases only
about 1.3%. The two ordinary controls have nearly equal demand and transferred
bytes but very different request durations and elapsed time. A first-control-only
comparison would substantially overstate the candidate's benefit.

Object durations cover client Get acquisition through body Close, including
client scheduling/consumption; they are not pure OSS processing or network RTT.
Raw and encrypted durations overlap, and concurrent sums are not critical-path
time. All recorded requests return HTTP206; all bodies close without failure.
Cached-new guests make no source Get or HTTP requests while still issuing fresh
NBD reads. Their cache snapshots exactly inherit the prior guest's cache; they
are not reused guest processes or local-file-backed runs.

## Machine and S3 interpretation

Read-only 500ms samples cover 8.010 seconds of the guest campaign on the retained
2-vCPU / 8GiB ECS. Host accounting is 47.41% busy, 32.08% idle and 20.51% iowait,
with zero steal ticks. CPU PSI some increases 1.618 seconds; I/O PSI some/full
increase 2.990/1.677 seconds. The owned cgroup consumes 5.857 CPU-seconds, with
zero throttle and memory OOM events; minimum MemAvailable is 6,139,740KiB.
These are campaign-level sampled observations, not per-stage causal attribution
or production-density evidence. NBD wait can contribute to I/O pressure; iowait
alone does not establish a slow physical disk. CPU scheduling remains relevant
even without steal, throttling or OOM.

The cold/cached-new gap shows a substantial remote-read/decode dependency in
this path. It does not distinguish OSS backend cache/processing from transport,
TLS/decode work or local CPU scheduling. O1's slower request durations and O2's
recovery make machine/provider effects an active investigation track, not an
excuse to increase timeouts or to prewarm tenant roots. Previous exact
client/OSS correlation remains separate evidence; this run did not re-enable
SLS collection or collect new request-ID-correlated server timing.

Next admissible attribution should split client response wait and node CPU/
scheduler cost for the same real demand, with explicit provider-cache limits.
Do not replay this layout ABBA or sweep machine size, cache, readahead or
concurrency merely to obtain a passing sample. A new format needs a benefit
that survives the reverse control, in addition to complete semantics/authority.

## Construction and preservation

The ordinary control is the existing verified format-two Node artifact with
xfs-file-ranges-v1 and contiguous mappings, not a weaker rebuilt control. The
wrapper reuses the exact retained logical image from D-LOOP-DIO, publishes via
the frozen materialized-file builder with the global 64KiB grid, and preserves
16KiB authenticated encryption frames. Six conditionally created objects total
98,925,660 encoded plaintext bytes under the test-only prefix
`rootfs/diagnostic-loop-net-gulqwt/wrapper/v2`. They remain retained; there is no
manager/PG artifact or runtime generation publication. Original objects are
read-only. All object inventory, input/output identities and receipts are kept.

Full logical wrapper identity remains
`sha256:a972e11701e9d9ed5672321c728100c7cc24ae4705828175a539812a18ef7a8d`.
Source staging is readonly/noload and detached before any guest. Runtime has
no local image fallback. The concrete `*rootfsblock.Reader` is passed directly
to Branch; read-only cache counters use additive compile overlays, not alternate
Reader algorithms. Fixed stock runsc, 512-byte loop/NBD sectors, 4096KiB
readahead and actual loop DIO=1 are checked. No kernel trace instrumentation.

The first attempt fails during its first conditional object publication, before
any guest or descriptor. Its runner omitted the original service's AWS config/
profile selection. Restore those four verified nonsecret environment values in
a fresh diagnostic root; do not log credential values. Publication then succeeds.
The initial generic error did not preserve a precise AWS failure code, so it is
not evidence of an OSS latency or availability fault. Also fix the independent
command/descriptor receipt filename collision. Preserve the failed attempt's
19 reports and unsuccessful object intent/result; no claim of remote object
absence follows from a failed publication return.

Both builds pass 12 diagnostic Go race tests, vet and build. Qualified offline
verification passes six tests / 50 assertions, reproduces analysis, checks all
63 transferred reports, and preserves the first attempt's files. Qualified
unit ends inactive/success/0; all eight guest cleanup results pass. Final checks
inspect 231 process mount tables: owned mounts, loops, NBD, cgroups and veths are
absent, original host network/runtime files/PIDs/rows/job are unchanged within
the boot. SSH closes. Stop completes; fresh 12:39:38UTC cloud read confirms
Stopped/StopCharging/no public IP. The earlier Stopping receipt is retained.

All 1,353 frozen product files and 193 dirty worktree entries remain. Only
requested experiment notes change. No production rollout, merge, tag, deletion
or local e2e. Generic shared carriers, claim-time tenant RootFS binding and
disposable nodes are unchanged. Populated large roots/upper histories, actual
occupied production width, full sparse/hardlink/rebase and writer/cleanup
authority, and preferred 1s / accepted 2s regional readiness plus immediate
real-command acceptance remain open. This single Node image cannot prove them.

Follow-up: [COMMAND-COST.md](COMMAND-COST.md) now separates real ordinary-guest
HTTP first-byte timing and node CPU samples. Similar CPU work with very different
response waits explains why the initial control was misleading, while decode,
verification and frame processing remain actionable CPU costs. No format adoption
or original regional/density gate closure follows.
