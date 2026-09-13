# D-CODING-LOOP-NET: Coding cold benefit, not format admission

2026-09-13. Evidence: `/tmp/sandbox0-coding-loop-net.XgviED`.

## Result and decision

Unlike the earlier Node-only [LOOP-NET.md](LOOP-NET.md) comparison, both Coding
direct-loop wrapper cold samples beat the reverse ordinary control in local
combined latency and object cost. This closes the actual Coding coverage gap;
it does not establish a production-ready filesystem replacement.

| Pair / cohort | Local operation to authenticated ready, ms | Literal node-v, ms | Local operation to command done, ms | HTTP attempts |
| --- | ---: | ---: | ---: | ---: |
| O1 cold | 1814.735 | 1721.184 | 3535.920 | 338 |
| O1 cached-new | 194.315 | 98.409 | 292.726 | 0 |
| W1 cold | 764.982 | 1039.835 | 1804.819 | 217 |
| W1 cached-new | 195.470 | 103.137 | 298.609 | 0 |
| W2 cold | 695.610 | 1115.462 | 1811.075 | 217 |
| W2 cached-new | 202.619 | 174.898 | 377.519 | 0 |
| O2 cold | 1203.127 | 1003.544 | 2206.673 | 337 |
| O2 cached-new | 201.548 | 95.285 | 296.836 | 0 |

Use O2, not the much slower O1, as the conservative comparison. W1/W2 save
401.854/395.598ms combined, with438.145/507.517ms saved before authenticated
readiness. Their command stages are36.291/111.918ms **slower**, not faster.
Both ordinary cold samples exceed2s at this component boundary; retain them.
No population tail estimate or p99 follows from two samples per layout/cohort.

Cached-new combined time is1.773/80.683ms higher than O2. The smaller difference
alone is not proof of a systematic regression, but the predeclared conservative
no-regression screen is not met. W2's174.898ms command has **zero source Gets
and zero HTTP attempts**. Its intersecting NBD read-time union is29.773ms versus
O2's17.852ms; sums are46.060/20.607ms and must not be treated as critical-path
time. This rules out a fresh S3 read as the cause of that cached sample, not all
local scheduling, gVisor, filesystem or measurement effects. No causal removal
budget or blanket hardware exoneration follows.

Decision: retain the cold-read opportunity, do not promote the wrapper/default,
and do not replay this same ABBA to obtain a cleaner outcome. The candidate's
cached local-command behavior and complete layered lifecycle contracts remain
unqualified. The first-command cost is still material, even where readiness
improves. Broad command-triggered ELF prefetch remains rejected separately.

## Read cost and scope

| Cold pair | Distinct source objects | NBD unique bytes | Encoded plaintext returned | Ciphertext returned | Client ciphertext duration P50 / P95, ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| O1 | 50 | 94141440 | 48319915 | 53198091 | 19.742 / 46.614 |
| W1 | 19 | 91805184 | 47590960 | 50823478 | 10.178 / 31.212 |
| W2 | 19 | 91805184 | 47606146 | 50839882 | 10.262 / 24.147 |
| O2 | 50 | 94141440 | 48273787 | 53132475 | 8.667 / 23.383 |

Against O2, each wrapper removes120 of337 HTTP attempts (35.61%), while
ciphertext falls only2.293–2.309MB (about4.3%). Lower request count is not a
proportional bandwidth or latency reduction. Cached mapping occupancy falls
from16025918B to5933568B within the unchanged128MiB Reader cache; headers cover
50 versus19 objects. All four cached-new guests inherit their respective cold
guest's cache exactly and require no source requests, despite fresh NBD reads.

Request-initiation phase records show O2's211 command ciphertext calls versus
134 for each wrapper, but command payload is nearly unchanged:
38752440B versus38729988B. Fewer requests do not establish faster execution.
Before the command, object calls decrease126 to83, and ciphertext decreases
14380035B to12093490/12109894B. Phases can overlap: do not add independent
duration sums or reinterpret these labels as a mandatory dependency graph.

Cold means a fresh process-local data/mapping/header cache, not a guarantee
about OSS backend caches. The wrapper objects were just published. O1/O2 have
almost identical traffic but substantially different client Get durations;
machine/provider effects remain relevant. Acquisition-to-body-Close timing
includes client scheduling and consumption, not pure OSS service time or RTT.

The measured boundary includes isolated network/key setup, concrete encrypted
Reader construction, COW/NBD/mount, stock runsc, authenticated procd readiness
and the immediate real command. It excludes provider credential preparation,
already-loaded descriptor setup, regional ingress, manager/PostgreSQL claim,
production occupancy and teardown. There are **zero new regional acceptance
samples**, not new end-to-end2s passes. HTTP/readiness request budgets stay10s.

Read-only500ms host samples cover11.514s on2CPU/8GiB:41.74% busy,39.34% idle,
18.92% iowait, zero steal ticks. The owned cgroup consumes7.376CPU-seconds,
with zero observed throttle or OOM events; minimum MemAvailable6027544KiB.
CPU PSI some increases1.867s, I/O some/full3.878/2.302s. These campaign-level
values neither prove a saturated physical disk nor production-density safety.

## Inputs, preparation and preservation

Reuse the existing format2 ordinary Coding artifact
`sha256:21a2f3bac82e85bf353d9d625165c79c9d915f5c107fe2f36c349cc84e806ceb`
and the exact retained complete wrapper from
[WRAPPER-DEMAND.md](WRAPPER-DEMAND.md). No OCI pull, image rebuild, filesystem
scan, command training, cache/timeout/concurrency change or Node replay.
The ordinary artifact already uses xfs-file-ranges-v1 and contiguous mappings;
it is not a weaker freshly rebuilt control.

Historical sealed source/compact trees and fixed procd/Node hashes are checked
again locally. Current publication rehashes the full logical wrapper before
and after reading it; both equal
`sha256:a145ef17911b5279307130a286eaa02dca02cf5da92d212041cb4832698b6074`.
Inner plain EROFS is4506877952B, SHA256
`a902d2345927a44e2f33cd4afe2cbee4509b265ca0b8c299cb45fd0e84a5a10d`.
Coding has4489374053B actual file contents in a sparse1TiB image, not a populated
or history-bearing1TiB root. Full-tree fidelity is not full lifecycle fidelity.

One offline conditional publication creates86 test-prefix objects totaling
1688988275 encoded plaintext bytes. Recorded build/post-identity duration is
68.389s; this is not claim time. Publication is bounded at128objects/6GiB/600s
inside a900s isolated unit, without changing the10s runtime request budget.
The prefix is `rootfs/diagnostic-loop-net-xgvied/wrapper/v2`; original objects
and manager generations are untouched. New descriptor SHA256 is
`3697911906ba254d793d345740ecde708a6cdcefb98caca565d0fe0ec755217c`.
These objects and all local/remote diagnostic data remain retained.

Source staging is selected by exact disk serial/UUID, mounted readonly/noload
in a private mount namespace, and normally unmounted before guests. Runtime
receives the concrete encrypted Reader, never the local source file. Each
wrapper has actual loop DIO1,512-byte sectors,4096KiB readahead, exact inner
inode132 on NBD63, checked before/after. Stock runsc is the retained20260810
diagnostic pin, not a claim of latest production parity. Warm-carrier behavior
and claim-time tenant binding are not changed by this component harness.

The one campaign completes all eight authenticated new procd/sandbox identities
and literal `node -v` outputs v22.23.2. There is no failed or replayed remote
workload in this run. Local harness porting removes a stale Node-specific
request-count assertion in favor of independent raw-attempt tallying and
strengthens literal-command/identity checks; it does not change measured code.
Go diagnostic race tests pass12tests x3; vet/build pass. Offline verification
passes8tests/70assertions including13negative real-evidence mutations, verifies
223 recovered report hashes/sizes, publication receipts and1359 unchanged
runtime source files. Existing user changes are preserved.

Final checks inspect229 process mount tables: no owned mount/loop/NBD/cgroup/
veth or trace instance remains, global tracing/network and original runtime
files/PIDs/job73058/database rows are unchanged. Owned SSH closes explicitly;
the master returns255 on requested closure with no output, not a workload
failure. One stop call completes; independent00:54:36UTC cloud read confirms
Stopped/StopCharging, original2CPU/8192MiB and no public IP. This follows the
remote-test skill lifecycle, not its obsolete Kubernetes bootstrap paths.

No production change, merge, tag, data deletion or local e2e. All original
regional preferred1s/accepted2s readiness/command gates, genuinely occupied
production width and populated/history-bearing roots remain open. In
particular, the existing rebase scanner rejects the EROFS representation;
logical coverage, writable-device attribution, sparse/hardlink/copy-up,
snapshot/fork/restore/rebase, writer/terminal absence and GC must be qualified
without weakening current checks before any lifecycle integration.
