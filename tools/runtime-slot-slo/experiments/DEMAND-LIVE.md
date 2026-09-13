# D-DEMAND-LIVE: demand-fragment scheduling in real claims

Date: 2026-09-13. Decision: retain the diagnostic candidate for its history-claim
signal; no product-default adoption or universal startup acceptance. The later
[DEMAND-COVERAGE.md](DEMAND-COVERAGE.md) audit finds zero partial views in ordinary
Coding/Node roots: Coding's improvement here is a mixed-workload association,
not proof of direct fragment scheduling on its own RootFS. D-HISTORY-DEMAND's
encrypted component result justified this full-path trial, not a parameter sweep.

## Scope and invariant inputs

- Four fresh-boot arms in fixed ABBA order: baseline, candidate, candidate,
  baseline. Each arm synchronizes eight NEW sandbox claims, immediately runs
  literal `node -v`, cleans them up, then repeats with eight NEW identities
  on the cached node. Total: 64 measured claims/commands, 32 in each cache
  regime, two independent boots per implementation. Same-root concurrent lanes
  can share in-flight loads; they are not independent cold-node repetitions.
- Each cohort has two lanes each of `mixed-xe3dad-node22`,
  `cold-coding-agent-v060`, `history-gjreyw-00`, and `history-gjreyw-16`.
  The history templates and their immutable descriptors come from the retained
  D-HISTORY-LIVE authority; no image import or fixture rewrite is repeated.
- 16 CPU / 64 GiB Singapore ECS; eight generic single-use carriers; each claim
  leases 1750m CPU / 1792 MiB. This is an otherwise unoccupied test machine,
  not actual resident-memory pressure or production acceptance. The private
  TLS regional hostname routes through the regional/cluster gateways to procd
  on this test installation, not the public production NLB/cross-host topology.
- Generic carriers contain no tenant RootFS before claim. Every cold arm checks
  a distinct boot, eight ready/live carriers, empty runtime/branches, zero
  tenant transport packets and zero NBD counters before requests. Read-only
  template metadata checks do not fetch tenant blocks. Provider caching is
  not controlled.
- Source admission remains eight, decoded cache 128 MiB, NBD request cap
  128 KiB, observed readahead 4096 KiB. Explicit request timeout remains 10s,
  with zero claim/command POST retries. Authentication, trusted readiness,
  writer fencing, integrity and physical-cleanup requirements are unchanged.
- Only two runtime source files differ in the candidate overlay: `reader.go`
  and the new bounded demand-fragment helper. It schedules only the existing
  ReadAt demand, not a command profile or tenant prefetch. Ordinary coalescing
  and complete compression/authentication units still cause read amplification.
  RootFS identity becomes known at claim; an arriving ReadAt and its verified
  mapping leaf determine which immutable units this helper schedules. It does
  not predict a future command or schedule neighboring logical reads. The
  caller's request may itself originate from unchanged kernel readahead, so
  demand-scoped scheduling is not a claim of zero speculative I/O end to end.
- Both arms use the same stock runsc Aug 17 binary, frozen guest procd,
  manager, driver, gateway binaries, config and templates. Baseline ctld is
  byte-identical to D-INDEXED-LIVE's baseline. The dirty product worktree is
  not silently replaced, committed, merged, tagged or deployed to production.

## Provenance

| Input | SHA-256 / identity |
| --- | --- |
| sandbox0 main and diagnostic HEAD | `0f09220460581bfc1fdc331f34ebc85bf38381e7` |
| infra main, freshly fetched | `1bdd57f2c41144b6da60ec7f07ab295eedca0ca7` |
| ctld baseline | `28b3d1ba5a27a513a3efe2d82cfbb53d938de65da3bd5094dec23de7ff6985c4` |
| ctld candidate | `e5d0bf5d80bc1dce926a2eff6ee959b7f0ae5a21654b6162ceff6a2dd12aebaa` |
| timed harness | `590fb44c3c63efccc7d45ed258d189d0e940b42168d5633b88b1742514ce708f` |
| stock runsc | `048b89aada69dc3333422e139d6e9d02f8ab06bda52398060e0fbdacca00074c` |
| guest procd | `29dedac3bce92b6a9a3d507889110927ede8727f87186376524e577617c69f2d` |
| descriptor input | `1c2a0ecf49029917ca7a08f2b3c90b885f52146518220c9e0d9e41cca5095147` |
| existing authority | `s0_history_live_gjreyw`, initially 2076 sandbox rows |

Local overlay race tests for rootfsblock, rootfssession and nomadruntime pass
1237 test events. Eighteen explicit privileged/external/large-model/soak skips
are retained, not counted as passes. No local e2e or subagent is used.

## Results: distinguish claim from the first command

Cold medians below contain four lane samples per implementation, from only
two independent boots. Small ABBA samples are not a confidence interval or
an absolute guarantee. Marginal medians need not add to the combined median.

| Case | Claim baseline / candidate s | Command baseline / candidate s | Combined baseline / candidate s |
| --- | --- | --- | --- |
| Original Node | 0.821 / 0.846 | 1.288 / 1.259 | 2.109 / 2.096 |
| History00 | 0.835 / 0.841 | 1.274 / 1.257 | 2.109 / 2.096 |
| History16 | 0.911 / 0.845 | 1.198 / 1.252 | 2.109 / 2.096 |
| Coding | 1.612 / 1.421 | 1.166 / 1.043 | 2.778 / 2.464 |

History16 claim ranges do not overlap in this trial: baseline 0.900–0.933s
versus candidate 0.827–0.861s. Its trusted readiness median is
0.895793 -> 0.827111s. This is a repeated claim signal, not a general command
improvement: history16 command-only median rises and its combined result has
no consistent per-boot direction.

Coding's candidate combined range, 2.452051–2.475294s, is below both baseline
boots' 2.671232–2.884520s range. The pooled median decrease is about 314ms
(11.3%). Keep this signal, but do not infer a universal RootFS-size bound or
attribute every difference to a known source request without a critical-path
trace. Cross-boot/provider variation and shared-lane interactions remain.

Follow-up correction: D-DEMAND-COVERAGE confirms that this ordinary Coding
immutable root has no partial views, so its own data views do not enter the
fragment helper. The observed improvement could be indirect through history
activity or reflect other variation; neither is isolated here. Do not treat it
as evidence of direct Coding fragment parallelism.

The per-boot combined medians show why a pooled Node-family median alone is
not adoption evidence:

| Arm | Original Node s | History16 s | Coding s |
| --- | ---: | ---: | ---: |
| 00 baseline | 2.121821 | 2.121810 | 2.884470 |
| 01 candidate | 2.183398 | 2.183470 | 2.452059 |
| 02 candidate | 2.008355 | 2.008311 | 2.475279 |
| 03 baseline | 2.096490 | 2.096524 | 2.671234 |

All 64 measured claims and commands succeed with `v22.23.2` and unique
identities. Readiness max is 1.681618s and claim max 1.696987s; both are below
2s, but retain eight canonical 1s misses, all cold Coding. Cold combined is
2.008237–2.884520s: **32/32 exceed 2s**, including the near-2s samples.
Cached-node NEW identities are 1.439570–1.819655s combined, **0/32 over 2s**.
Do not turn arbitrary process startup into a changed platform readiness
contract, or report cached-new results as first-node-cache-cold success.

## Resource and phase evidence

Read-only external observers sample NBD, both ctld processes and host counters
every 100ms. Enclosing claim-plus-command windows read about 765MB of completed
NBD blocks in every arm (765,017,600–765,050,880 bytes). These are not OSS
transfer bytes, HTTP request counts, or the minimal Node working set. There
is no meaningful reduction of this block-read volume in the candidate.

Cold-window host busy fraction is 13.8–15.4% across 16 CPUs, iowait 21.5–25.0%,
steal zero; minimum host available memory exceeds 58.7 GiB. The machine is
not under the required resident/reclaim pressure. Iowait alone cannot identify
pure OSS server time. Baseline/candidate sampled ctld RSS ranges overlap;
this trial does not establish a general memory saving. D-HISTORY-DEMAND's
whole-component CPU/RSS costs remain part of the decision record.

All 64 manager claim-phase records are exported. First-arm cold nested
node-timing fields are absent; do not fill them from later arms. Node-claim
and procd-probe phases remain available, while command-internal attribution
is not collected. Windows overlap unfinished sibling claims and cannot be
added or treated as pure command-only profiles. Wall/monotonic timestamps
and observer ordering are separately cross-checked.

## Failure preservation and semantic qualification

The original two-carrier installation again has only one ready carrier after
boot, now on 16 CPUs. This weakens a simple two-CPU capacity explanation but
does not identify the cause. Job index 75270 was unchanged, with no queued
placement; the missing group alternated after reboot. Do not label it fixed.
The separate diagnostic installation reaches eight ready carriers on all four
fresh boots without reducing the acceptance width.

A post-delete attempt to inspect writer-grant generations returns zero rows;
terminal deletion removes that evidence. It is retained as unavailable, not
fabricated into an exact historical binding proof. After all timed cohorts,
two additional NEW baseline-runtime sandboxes from history00/history16 verify
their live consumed writer's exact expected head and full Node/user-file SHA256.
These are semantic checks, excluded from the latency comparison.

Two local read/analysis invocations were attempted while the final collection
process was still live. Missing-receipt guards stopped them before any probe
staging or new claim. The same collection handle was awaited to completion;
only then were the dependent operations performed. No remote test was replayed.
A transient SSH observation failed during reboot; the existing boot observer
continued and proved the new boot/eight-ready state without another reboot.

## Cleanup and remaining work

Final verification confirms all 64 measured and two semantic-probe identities
deleted, all 64 NBD devices detached, and no active test writer/branch/lease.
The paused seed, three history templates, durable objects and original data
remain. Original binaries, four configs and the exact two-carrier job definition
are restored, with two ready/live allocations before shutdown. This restoration
does not establish a fix for the initial post-boot pool anomaly.

The export audit verifies 132 arm JSON receipts and 48 final JSON receipts;
the local artifact index seals 173 files. All 1362 product source files and the
file inventory are unchanged. A fresh cloud observation at
2026-09-13T09:59:57.370894688Z confirms the original 2CPU/8GiB instance shape,
Stopped/StopCharging. No production change, merge, tag or timeout increase.

Keep this candidate diagnostic. Next quantify its coverage on the actual
Coding/Node command critical path and the remaining waits using the current
artifact identities; do not restart already-closed admission/cache/readahead
sweeps without new evidence. Occupied physical-width behavior, fully populated
large roots, and the original 1s/2s requirements remain unproven. No timeout
increase or prewarming of a user's RootFS is part of the proposed solution.

Evidence root: `/tmp/sandbox0-demand-live.Q2NOx7`. `summary.json`, per-arm
analysis/phase/window/clock reports and 132 checksummed arm JSON receipts retain
all measured successes and misses. `evidence.json` and `artifact-index.json`
seal the completed campaign. Remote artifacts remain under
`/data/sandbox0-demand-live-Q2NOx7`; no private config or credential is exported.
