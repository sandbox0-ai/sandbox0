# D-INDEXED-LIVE: indexed read groups in real claim and command execution

2026-09-13. Evidence root: `/tmp/sandbox0-indexed-live.teiDkw`.
Previous continuation made progress by completing the qualified runtime builds.
This experiment follows D-INDEXED-NET with actual regional TLS claims and literal
`node -v`, not a serialized retained-read replay.

## Decision

Do not promote indexed grouping as a general cold-start fix. Both candidate
runs improve cold Node combined latency against both baseline runs, but Coding
does not improve consistently. The second candidate's cold Coding result is
worse than either baseline. Keep every run, including that regression.

All 64 distinct claims and commands succeed. All 64 claim/readiness samples
are below 2s, but 24 claim-start-to-first-command-finish samples exceed 2s.
No hard bound, populated-large-RootFS, occupied-density or production acceptance
has been established. Four boot-level runs are not a tail-latency guarantee.

## Inputs and boundaries

- Order: baseline, indexed, indexed, baseline. Each arm uses a new boot and
  two synchronized eight-claim cohorts: four Node and four Coding per cohort.
  The second cohort uses new sandbox identities on the same cached node.
- Same diagnostic ECS at 16CPU/64GiB, eight generic single-use carriers, and
  exact 1750m/1792MiB leases. No resident-load fixture. This is not occupied
  production-width or reclaim acceptance.
- Every cold cohort proves empty RootFS branches, all NBDs detached before use,
  zero configured NBD counters and zero ctld tenant TCP443 outgoing packets.
  No tenant RootFS, executable hint or command is warmed before claim.
  Provider-side storage caches are not controlled.
- Requests use the regional TLS hostname and the established private hosts
  binding, with certificate verification. This is not public production NLB
  or cross-host ingress acceptance. Claim starts before the client's POST;
  the combined clock continues through the complete first command response.
- Literal argv is `node`, `-v`; no command environment override, wrapper or
  shell interpolation. Every response confirms stopped CMD, exit 0 and exact
  `v22.23.2\n`. No claim/command POST retry; public timeout remains 10s.
- Only ctld's three-file indexed-group overlay differs between arms. Manager,
  driver and harness are built from the same 1361-file frozen worktree.
  Official stock runsc is release-20260817.0, matching inspected infra main.
- Retain the same ordinary Node artifact `sha256:2b5208c97335f4e25312150a1d980ed6039efe1884139129efd79f00a7f1b155`
  and Coding artifact `sha256:21a2f3bac82e85bf353d9d625165c79c9d915f5c107fe2f36c349cc84e806ceb`.
  Both descriptor and attestation are checked before each arm and actual lease
  bindings after commands. Node is logical 16GiB; Coding is sparse logical
  1TiB, NOT populated/history-bearing 1TiB.
- The frozen UTC procd in those artifacts is unchanged. This does not test the
  newer copied-session-owner reset fix in the worktree.
- Clone source database `s0_mapping_oci_uqo1av` into owned
  `s0_indexed_live_teidkw`; no image reimport. Disable background object
  deletion/squashing, isolate discovery to the two qualified images, and keep
  source authority untouched. Owned rows progress 2072 -> 2088 -> 2104 ->
  2120 -> 2136, matching exactly 64 new identities.
- Keep 128MiB decoded/mapping cache, protected mapping budget, 8MiB/1024-entry
  encrypted-header cache, eight source permits, 1MiB coalescing, 128KiB NBD
  request cap and 4096KiB readahead. No cache/concurrency/timeout enlargement.

## All measured maxima

Seconds; each image has four samples in each row. Maxima are independently
selected, so claim and command columns must not be added to reconstruct combined.

| Arm | Node-cache state | Node claim | Node command | Node combined | Coding claim | Coding command | Coding combined |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 00 baseline | Cold | 0.900386 | 1.110699 | 2.007869 | 1.557156 | 1.068415 | 2.614394 |
| 00 baseline | Cached-new | 0.664465 | 0.466369 | 1.123789 | 0.860684 | 0.839962 | 1.688262 |
| 01 indexed | Cold | 0.775213 | 1.090589 | 1.846701 | 1.482879 | 1.029471 | 2.503405 |
| 01 indexed | Cached-new | 0.591009 | 0.459453 | 1.030133 | 0.787165 | 0.762792 | 1.544122 |
| 02 indexed | Cold | 0.796592 | 1.197826 | 1.974899 | 1.629619 | 1.131642 | 2.740504 |
| 02 indexed | Cached-new | 0.669056 | 0.449112 | 1.104205 | 0.772201 | 0.088502 | 0.855212 |
| 03 baseline | Cold | 0.823039 | 1.254900 | 2.067729 | 1.509770 | 0.981684 | 2.479726 |
| 03 baseline | Cached-new | 0.599100 | 0.441875 | 1.031990 | 0.845847 | 0.793596 | 1.637024 |

The candidate's cold Node margin below 2s falls to only 25.1ms in its second
run. Coding cold maximum varies by 237.1ms between identical candidate builds;
its cached-new command maximum varies from 762.8ms to 88.5ms. These are useful
variance observations, not proof of the underlying cause or stable SLO gains.

## Reading and resource evidence

| Arm | Completed NBD read bytes | Matched incoming TCP443 bytes | ctld peak RSS bytes |
| --- | ---: | ---: | ---: |
| 00 baseline | 1533916160 | 86943886 | 463134720 |
| 01 indexed | 1533974016 | 83464903 | 474488832 |
| 02 indexed | 1533957632 | 77539441 | 482562048 |
| 03 baseline | 1533990400 | 87057158 | 484302848 |

NBD counters include cleanup and reflect completed device requests, not S3
downloads or the minimum executable working set. Matched TCP bytes include
protocol overhead/retransmissions and may undercount packets lacking early
socket association; they are not GET counts or verified HTTP payload bytes.
The previous serialized encrypted-source results must not substitute for
per-command live GET measurements, which this experiment does not collect.

Every observed lease has zero quota-throttle and memory OOM/max events. No
host CPU steal is observed; minimum available host memory is 63093358592 bytes.
These facts do not rule out all host/ctld or storage delays, and the mostly idle
host is not a high-density test. Whole-run CPU samples include cleanup.

All 64 claim phase records are correlated to exact sandbox IDs. For example,
baseline 00 cold Coding RootFS Ensure is 466.0–503.6ms, runsc create/start roughly
460ms together, and procd probe 507.7–521.0ms, followed by a roughly 1.06s command.
Node subphases are nested within node_claim; marginal ranges are not additive.
Detailed per-image ranges and the exact slowest combined sample are retained in
the four `phase-analysis-*.json` files. No command-internal critical-path trace
was added.

## Qualification and retained corrections

- Local race-enabled runtime/claim/harness suites: 493 test pass events;
  independent driver module: 113. Opt-in privileged/soak skips remain skips,
  not acceptance passes. The preceding sealed indexed-reader qualification
  remains the candidate's mechanism/security regression evidence.
- Independent verifier checks all 132 exported arm JSON artifacts, 64 unique
  identities, four boot IDs, exact ctld variants, cold counters, resource leases,
  eight noatime guest mounts per cohort, command results and terminal cleanup.
- A local helper generator initially replaced a substring inside ARM; its
  guard stopped before staging. Word-boundary replacement fixed the generator.
- An early installed-state read ran while the install process was still
  stopping original services, and correctly rejected a partial file set.
  The original regional-gateway reached its 90s systemd stop timeout. The
  same install process completed; no install or claim was replayed.
- A local analyzer was invoked before its existing export process completed
  and found no receipt. It was run only after that same export became terminal.
- `phase-analysis.rb` contains the retained initial syntax failure;
  `phase-analysis-v2.rb` fixes the brace and reads the same saved data.
- A read-only configuration/hash audit in arm 03 ran during cleanup, between
  cold command finish 06:42:26.928724Z and cached claim start 06:43:19.948052Z.
  Its local source was created 06:42:32.884Z and remote audit finished
  06:42:36.464900Z. It did not overlap either claim/command measurement window;
  whole-arm host observer totals nevertheless include this unmatched audit.
- Explicit SSH master closure succeeds; wrapper exit 255 after requested
  closure is retained separately. Do not treat it as a benchmark failure.

## Next decision boundary

Do not replay this unchanged candidate or enlarge caches/timeouts from its Node
gain. Any follow-up needs a predicted whole-path benefit for Coding and mixed
cache pressure. Reuse existing full-demand/cache traces before another remote
campaign. Historical B-RECENCY-ABBA already rejected recency-only changes;
do not relabel that experiment as a new fix. Provider/host variability and
shared-cache effects remain distinct hypotheses, not established causes here.

RootFS-size independence, populated/history-bearing roots, occupied real-width
first commands and durable lifecycle correctness remain required. No production
rollout, PR merge, tag or product-default adoption is authorized by these results.

## Verified restoration

At 06:47:49.749746Z the original service executable hashes, original config
hashes and original two-group job definition were restored. Job modify index
is now 74750; it legitimately changed from 73058 during the experiment.
Both original carriers were ready, all owned experiment units were absent,
all 64 NBDs were detached and no active test RootFS/resource leases remained.
Original/source database row and artifact checks passed; the older retained
trial still has 2121 rows, while this owned clone retains exactly 2136.

All 14 additional restoration exports passed byte-count/SHA checks. Private
configs and the database dump were not exported. Temporary systemd drop-ins
were moved to the owned `removed/` directory, so they remain recoverable;
original data and all `/data` experiment artifacts were preserved.

One stop and one resize-back request completed. Fresh Aliyun read at
06:50:42.360525157Z confirms Stopped/StopCharging, original ecs.g9i.large,
2CPU/8192MiB. Explicit SSH control closure succeeded and its wrapper terminated.
No production change, PR merge, tag, product-default adoption or timeout change.
The final seal is `/tmp/sandbox0-indexed-live.teiDkw/evidence.json`.
