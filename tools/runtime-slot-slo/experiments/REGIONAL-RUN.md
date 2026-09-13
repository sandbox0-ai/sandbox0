# D-REGIONAL-RUN: current-pin regional cold/cache comparison

2026-09-12. Evidence: `/tmp/sandbox0-regional-run.qKMiRv`.
Actual guest run, not preparation or a standalone Reader microbenchmark.

## Fixed inputs and scope

Reused the frozen1353-file optimized candidate, the existing pure manager/ctld/
driver binaries and exact Ready Node/Coding artifacts. Stock runsc now matches
infra main release-20260817.0, with its corresponding canonical compatibility
digest `sha256:d7fdba49664702dbee206ce7fbcb32dd217c50d5fe93d920d92a0978454a1dd7`.
No frame-scratch candidate, product edits, imports or tenant prewarming.

Singapore test ECS temporarily uses the same16CPU/64GiB shape as the previous
full regional trial. Eight generic carriers each lease1750millicores/1792MiB;
four claims use each artifact. The first cohort starts after a fresh boot, with
zero NBD reads and zero ctld RootFS outgoing packets; the second uses eight new
identities on the same node. Provider-side object caches are not claimed cold.
Requests traverse the private regional TLS fixture and authenticated procd,
followed immediately by `node -v`. Request budgets remain10s, with no POST retry.

## Results

Every cell below is the maximum of four samples. Marginal maxima are not additive;
original per-sample combined clocks and correlated phase records are retained.

| Node cache / artifact | Regional claim | Command only | Claim to command completion | Combined >2s |
| --- | ---: | ---: | ---: | ---: |
| Empty / Node | 0.903s | 1.322s | 2.221s | 4/4 |
| Empty / Coding | 1.579s | 1.088s | 2.668s | 4/4 |
| Cached-new / Node | 0.605s | 0.427s | 1.029s | 0/4 |
| Cached-new / Coding | 0.877s | 0.813s | 1.673s | 0/4 |

All16 claims and16 commands succeed, with exact `v22.23.2\n` and exit0.
Four cold Coding claims exceed1s; no claim exceeds2s. All16 combined samples
exceed1s and all eight cold samples exceed2s. Cold command-only samples all
exceed1s; none exceeds2s. Timing misses are retained despite functional success.

The slowest combined cold Coding sample has RootFS ensure577.018ms, runsc
create234.271ms/start220.338ms, procd probe465.728ms, and command1074.047ms.
These nested/sequential records must not be summed with their parent node-claim
duration. Claim was1577.490ms and combined2668.477ms for this exact identity.

Historical old-pin D-MAPPING-COLD maxima were Node/Coding combined1.767/2.825s
cold and0.954/0.674s cached-new. This run improves one cold image but worsens
other cells, especially cached Coding command latency. It is not a randomized
runsc A/B: storage response and cache residency may differ between dates/cohorts.
Do not label the version change either an adopted latency fix or a proven cause
of regression. It does establish that the current stock pin still misses the gate.

## Machine versus storage evidence

Added read-only100ms host CPU/pressure and ctld process counters to the existing
RSS/NBD/queue observers, without modifying product binaries or enabling tracing.
Across the bracket containing the cold cohort, average busy CPU is12.605% over
16CPUs, steal is0, and reported iowait is20.682%. The cached cohort is13.759%
busy with0 steal and17.234% iowait. All16 lease cgroups have zero throttling,
OOM and memory-max events. Host available memory never falls below58.799GiB.
Eight sandbox cgroups occupy only about868MiB after commands; this is emphatically
not an occupied-memory density test. Both ctld processes peak at502.598MiB RSS
combined; that is not total RootFS subsystem memory.

Cold host I/O pressure increments are1.178s `some` and1.051s `full`; CPU `some`
increments52.430ms and memory pressure is negligible. These are host-scoped,
sampled intervals, not per-request blocking spans or additive command time.
They do not support whole-host CPU/memory exhaustion as this cohort's main cause.
They do not rule out transient contention, single-thread CPU limits or occupied
nodes. NBD is backed by the userspace RootFS/S3 path: Linux iowait is not proof
of slow physical disk or object-server processing. No new per-request S3 server/
network/client wait split was measured here; prior request-ID correlation remains
separate evidence. Cold-versus-cached savings also include avoided decoding,
decryption and filesystem work, not only S3 latency.

## Failures and restoration

Preserve preparation mistakes separately from startup samples: the wrapper's
initial field-name compile failure; SSH attempts before boot readiness; a failed
stage before a master existed; and the manager rejecting a temporary catalog
selector. The last failure correctly enforces deployment-pinned asset paths.
A separate intent backs up the original catalog and owned manager config, then
installs the version-only catalog at its required path. No path/auth validation
was bypassed. Original install/database receipts are retained unchanged. Service
stops use their existing grace periods; they are outside claim measurement.
An early completion observation rejected a still-running cleanup; the same run
was observed to completion without replay. Early local analysis before download
completion and two malformed read-only summary commands produced no results.

Final test verification proves2088 test rows (2072 original plus16 claims), eight
ready replacement carriers, zero active leases, no branch/import scratch, and
all64 NBD devices detached. Original source databases/artifact inventories are
guarded. Restoration receipts and final cloud state are retained with the evidence.

## Decision

Current-pin full-path comparison is now measured. The2s cold combined gate is
not met; neither universal size independence nor production acceptance is proven.
The Coding root is sparse1TiB with about5GiB allocated, not populated1TiB.
Populated-root/upper-history and genuinely occupied configured-width testing remain
required. No production rollout, merge, tag, local e2e or product adoption.

Stop repeating binary/catalog preparation or machine/layout/cache-size sweeps.
Reuse the actual regional phase budget. Follow-up [FIRST-TOUCH.md](FIRST-TOUCH.md)
corrects the initial tracing premise: cached Coding command variation already
appears in historical full-path runs and is not new with this stock pin. Existing
trace evidence includes payload eviction/reload, but does not explain each current
sample. Its cold-path retention-only coverage is too small to justify that lane
as the main2s fix under a fixed-other-cost screen. Prioritize first-touch payload
dependencies and remote response cost with quantified net benefit. Do not repeat
a full trace just to rediscover known cached behavior or substitute a standalone
helper/S3 throughput test for the actual gate.
