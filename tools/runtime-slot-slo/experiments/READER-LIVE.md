# D-READER-LIVE admission

2026-09-13. Diagnostic qualification, not an optimization comparison.
Local evidence: `/tmp/sandbox0-reader-live.Q4hLga`.
Remote owned root: `/data/sandbox0-reader-live-Q4hLga`.

## Hypothesis and one variable

D-DEMAND-COVERAGE established zero ordinary-root fragment eligibility;
D-READER-ATTRIBUTION qualified counters locally but measured material cache-hit
cost. New evidence sought: on actual claim and literal first-command workloads,
ordinary Node/Coding Readers dispatch zero fragment batches; history Readers can
dispatch batches, and source work belongs to actual leaders, not every waiter.
The independent variable is diagnostic collection: uninstrumented V2 candidate
versus the same V2 with enabled per-Reader counters. This is not another old
baseline-versus-fragment-candidate ABBA optimization trial.

Two fresh-boot arms, `00-baseline` (uninstrumented V2) then `01-candidate`
(observed V2). Each performs eight synchronized cold NEW claims then eight
cached-node NEW claims, two each of Coding, ordinary Node, History00 and
History16. Total32 measured identities, each immediately running literal
`node -v`, expected `v22.23.2\n`. Ten-second requests, zero POST retries.
Single boot per mode can show obvious perturbation, not prove exact observer
overhead, cross-boot equivalence, algorithm speedup or provider causality.

## Frozen inputs and boundaries

- Main `0f09220460581bfc1fdc331f34ebc85bf38381e7`; infra main
  `1bdd57f2c41144b6da60ec7f07ab295eedca0ca7`, both freshly fetched.
- Original1362file inventory SHA256
  `8c7ebcef841b6ec05e6b2e4ebeb7971012f93fccd3b02e678c787c273afbd546`.
- Unobserved ctld `e5d0bf5d80bc1dce926a2eff6ee959b7f0ae5a21654b6162ceff6a2dd12aebaa`;
  observed `f4042deb54140914e037374e530199548a4f052495b24f04444b16acd217e6bd`.
- Manager `e31e700b9ee72297e6d21ce8b3709cc5e886f2601965e8a0e0c1c3205413b036`;
  driver `dd76415ecc9d21426c108ae470b35a37d047cb73c79aeca3822e19397abaa430`.
- Stock runsc Aug17 `048b89aada69dc3333422e139d6e9d02f8ab06bda52398060e0fbdacca00074c`;
  existing frozen UTC procd `29dedac3bce92b6a9a3d507889110927ede8727f87186376524e577617c69f2d`.
  This guest predates the separate copied-session-owner fix; not release parity.
- Harness `7aef2fd5cedcc26558a2fc7ba87e080b9e9fde32b94b3395df1151e420c80921`.
  Cases, descriptors and configurations checked by exact staged hashes and DB
  authority, retaining D-DEMAND-LIVE inputs. No imports, template changes or
  target RootFS prewarm. Source admission8, decoded cache128MiB, NBD128KiB,
  kernel readahead4096KiB; generic carriers have no guest before claim.
- Dedicated diagnostic ECS `i-t4n2wfukm75q4wie9j5f`, Singapore zone1d. Original
  2CPU/8GiB stopped/StopCharging freshly verified. Temporary16CPU/64GiB, eight
  standard generic carriers,1750m/1792MiB per claim. No resident-pressure load;
  do not call this occupied production width or fully populated large roots.
- Isolated retained DB `s0_history_live_gjreyw`: initial2142 sandbox rows;
  original sandbox DB263, source2072, indexed fixture2136 remain untouched.
  Preserve paused seed, three history templates, all caches and `/data`.
- Private-hosts TLS regional ingress, not production NLB/cross-host authority.
  Lifetime counters include construction/cleanup and overlapping durations;
  source byte counts precede envelope encryption, not HTTP wire/GET counts.

## Decision and rollback fixed before execution

Capture exact consumed writer/generation binding after commands and before
deletion. Join exclusive private Reader reports to that binding, descriptor
identity and boot/PID. Preserve and reject missing, extra, conflicted, failed,
nonquiescent or warning-bearing reports; never reinterpret missing data as zero.
No descriptor payload, object key, raw error or credential in counter exports.
Local collector has19positive/negative checks; prior overlay race1252pass/18skip.

Report both cold/cached-new readiness, claim, command and combined distributions
and every1s/2s miss. Counters can accept/reject direct fragment coverage, not
explain shared timing causally by lifetime sums. If ordinary batches appear,
reject the previous coverage model and inspect identity/dispatch first. If
history batches are zero, reject benefit attribution for this runtime demand.
No product-default promotion follows any outcome of this diagnostic.

Record a single launch receipt and systemd unit per arm. Poll that exact unit;
do not replay on observation timeout. On completion/failure clean up only owned
identities through public lifecycle, verify writer/mount/NBD/resource absence,
restore backed-up binaries/configs/exact two-carrier job, remove only owned
dropins recoverably, export receipts, stop compute and restore2CPU/8GiB
Stopped/StopCharging. Current active handles are recorded by the execution
receipts; no cloud write or runtime launch has occurred at admission time.

## Result: complete Reader attribution, incomplete external queue sampling

Two fresh boots and32 NEW identities completed; all32 literal `node -v`
commands returned the expected version. Before deletion, all32 consumed writer
grants were joined to exact RootFS/generation/writer epoch/head and boot. Both
ordinary artifact heads and both history heads match the intended template.
The observed arm produced exactly16 private Reader reports, no Reader warnings,
no conflicting bindings, no active work at report time and no read/source/
fragment errors. The control correctly produced no Reader reports.

The observed arm's external queue-setting sampler failed with
`No such device @ io_fread - /sys/block/nbd0/pid`. The workload exited0 but its
outer systemd runner exited1; retain this failed observation, do not label the
whole arm green. Exact failure time is unavailable. Completed NBD-counter and
memory observers and independently verified final physical absence do not fill
the queue-setting coverage gap. No arm or POST was replayed. Per-Reader counts
are separately qualified; latency comparison is not full-observer acceptance.

Observed seconds below are ranges, not guarantees or causal overhead estimates:

| Mode / cohort | n | Readiness | Claim | Command only | Claim + command | Combined >2s |
| --- | ---: | --- | --- | --- | --- | ---: |
| Unobserved V2, cold | 8 | 0.906–1.632 | 0.925–1.650 | 0.973–1.268 | 2.192–2.623 | 8/8 |
| Unobserved V2, cached NEW | 8 | 0.529–0.687 | 0.533–0.690 | 0.856–0.999 | 1.532–1.545 | 0/8 |
| Observed V2, cold | 8 | 0.892–1.508 | 0.912–1.526 | 1.165–1.216 | 2.128–2.692 | 8/8 |
| Observed V2, cached NEW | 8 | 0.465–0.578 | 0.469–0.581 | 1.027–1.139 | 1.607–1.608 | 0/8 |

All32 claims/readiness observations are numerically below2s; four cold Coding
samples miss1s. All16 cold combined samples miss2s. The timing differences are
nonuniform: observed cold Node combined is about65ms lower but Coding about69ms
higher. With one boot per mode and the queue observer failure, do not calculate
a fixed correction, claim negligible observer cost or assert algorithm gain.

## Actual coverage and source work

- Ordinary Node/Coding: eight Readers across cold/cached-NEW, zero fragment
  helper entries or batches. This confirms D-DEMAND-COVERAGE in current runtime.
- History00: four Readers,12 helper entries but zero admitted fragment batches.
- History16: four Readers, two batches and17 planned immutable units per Reader,
  covering262144 requested bytes each. No busy fallback or fragment error.
- Each eight-Reader cohort has four batches covering524288 bytes out of about
  766million Reader-requested bytes (about0.0685percent). This is byte coverage,
  NOT a latency fraction or an Amdahl bound: small metadata can remain critical.

Only the observed arm has counters. Per-cohort sums include Reader construction
and cleanup, not just the measured claim/command interval:

| Observed cohort | Reader calls | Reader requested bytes | Actual source calls | Source requested bytes | Mapping flight calls | Coalesced leaders / followers |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Cold | 7347 | 765743616 | 530 | 55906303 | 564 | 127 / 3609 |
| Cached NEW | 7362 | 765866496 | 508 | 59631316 | 2 | 173 / 3565 |

Cached-node NEW is not an all-data-cache-hit condition. Mapping flight calls
nearly vanish, but data-source and shared-flight work remains. Mapping flights
are not source GETs; the two cached mapping calls belong to History16 (one
leader and one follower). Source calls/bytes are below envelope encryption,
not HTTP attempts/ciphertext, and shared sources are charged to actual leaders.
Overlapping lifetime wait sums must not be added into a startup critical path.
This is consistent with D-CRITICAL-BUDGET's older payload-wait finding, not a
newly discovered universal cache/transport cause or a reason to replay it.

Both full-run NBD counter observers see about1.530GB across cold+cached+cleanup,
not S3 bytes. Minimum host available memory remains over62.85GB; summed ctld RSS
peaks at about606MB unobserved and585MB observed. These are single-run values,
not memory savings or occupied-density evidence.

## Decision, preserved mistakes and cleanup

Stop treating history-fragment scheduling as a general ordinary-root cold-start
fix. Preserve History16 mechanism evidence without default promotion. Further
work needs a new bounded ordinary-payload delivery mechanism and whole-startup
cost model; do not repeat unchanged cache-capacity/source-width/readahead sweeps
or the existing shared-wait diagnosis merely with new labels.

Preserve two local analysis corrections without runtime replay: the first
unobserved aggregate emitted synthetic zero totals; version2 uses unavailable
null counters. Initial latency summaries called functional completion
`full_arm`; version2 separates functional completion from external-observer
completion and records the failure. Old JSON remains but is not authoritative
for these conclusions. The explicit reader-count join, public samples and
generation bindings remain unchanged.

Original readiness1/2 reproduced before installation, with no job resubmission
to hide it. After testing, all32 temporary identities were deleted,64 NBD devices
detached, original binaries/configs/exact two-carrier job restored, and2ready
carriers verified. The retained history DB has2174 rows (2142+32); original263,
source2072 and indexed fixture2136 are preserved. Seed remains paused at17 and
all three history templates remain. Only owned dropins were moved to the owned
`removed` archive; caches, backups and `/data` remain recoverable/preserved.
Final cloud receipt at2026-09-13T11:17:32.511249688Z verifies2CPU/8GiB,
Stopped/StopCharging. The126local-artifact seal and checksummed remote exports
are in `evidence.json`, SHA256
`ede4e1934174b071d1f0d88c692bdea5cab7b494135551c93452b8fca3b596dd`.

No default/runtime algorithm/product-source change, tenant RootFS prewarm,
timeout increase, local e2e, production change, merge or tag. All original
populated-root, actual occupied width and regional1s/2s requirements remain open.
