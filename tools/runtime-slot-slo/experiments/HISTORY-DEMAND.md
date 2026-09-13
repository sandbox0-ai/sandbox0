# D-HISTORY-DEMAND: real encrypted demand-fragment reads

Date: 2026-09-13. Status: positive component evidence; no runtime/default
adoption and no startup SLO acceptance.

## Trigger and unchanged requirements

This candidate is demand scheduling, not predictive RootFS prefetch. In the
runtime path, claim still selects the tenant RootFS. The candidate is reached
only by an existing `Reader.ReadAt(offset, length)` encountering an uncached
compressed partial view. It considers at most 128 KiB of that caller's demand
inside an already verified mapping leaf, deduplicates the required immutable
units, and uses the existing eight-source admission budget to read them.
It neither knows the next command nor fetches a neighboring mapping page.

Complete compression/authentication units can contain bytes outside the logical
request. The existing ordinary-range coalescing path also remains unchanged;
do not describe the whole Reader as having zero speculative reads. The new
fragment scheduler itself does not predict future demand or prewarm tenants.

The component harness deliberately supplies eight fixed read intervals from
the retained history fixture. Those offsets are test inputs, not a startup
profile injected into the runtime. This campaign makes no claim and executes
no guest command, so it does not yet attribute these intervals to the critical
path of actual `node -v` or establish end-to-end savings.

## Design and controls

- Read-only real encrypted OSS access on the existing Singapore ECS at
  2 CPU / 8 GiB. No resize, runtime installation, RootFS import, object write,
  database mutation, cache deletion, service restart, or Nomad job submission.
- Reuse immutable descriptors from D-HISTORY-LIVE: original image, populated
  baseline, first rewrite, and sixteenth rewrite. The actual 64 MiB user file
  and all history objects remain retained; this is not a fully populated large
  RootFS acceptance test.
- ABBA order: baseline, candidate, candidate, baseline. Each arm makes three
  repetitions of eight 128 KiB demands in three regimes: a fresh Reader and
  encrypted-header/cache instance; a NEW Reader sharing the warmed cache;
  and eight simultaneous Readers sharing one fresh cache. Total: 288 reads,
  96 in each regime. Cache capacity remains 128 MiB and source concurrency 8.
- Explicitly use the verified remote service credential profile. Credential
  preparation precedes timing and is checked to perform zero object requests.
  Per-source request timeout remains 10 seconds. The four-minute Go test
  timeout bounds an entire arm and is not a changed request timeout.
- All returned content hashes agree across arms and regimes. Request bodies
  are accounted through close; workers/admission are empty at completion.
  Local harness race checks pass 15 test events with zero skips. No local e2e.
- Fresh client caches are controlled; provider-side caching is not. Mixed
  Readers share real immutable content and are not eight unrelated tenants
  or an occupied production-width benchmark.

## Results

Medians combine six samples per implementation. These are small ABBA samples,
not confidence intervals. Read time excludes Reader construction.

| Empty-cache interval | Baseline read ms | Candidate read ms | Source calls, baseline/candidate | HTTP attempts, baseline/candidate |
| --- | ---: | ---: | --- | --- |
| History16, 18 fragments | 321.370 | 216.362 | 20 / 20 | 36 / 36 |
| History16, metadata8 | 155.831 | 74.333 | 10 / 10 | 18 / 18 |
| History16, metadata6 | 116.159 | 64.441 | 8 / 8 | 14 / 14 |
| Original superblock | 33.367 | 34.602 | 3 / 3 | 3 / 3 |
| History01 fragment control | 46.452 | 47.490 | 4 / 4 | 5 / 5 |

For the 18-fragment interval, open-plus-read median is 341.888 -> 235.884 ms;
read ranges are 272.465–579.367 versus 199.455–254.830 ms. Both paths issue
two mapping and eighteen data-source requests, returning 228,436 encoded
source bytes (182,043 data bytes). Peak outstanding source bodies rises from
one to eight. This is parallel-wait improvement, not reduced request/byte
amplification. Encoded source bytes are compressed plaintext storage units
before envelope encryption, not ciphertext network-byte accounting.

The prior full-tree inventory's 42 mapping pages are NOT current-demand GETs:
this read needs only two mapping calls. Metadata8 likewise keeps 72,347 source
bytes; metadata6 changes 67,310 -> 66,250 bytes. Other intervals' unchanged
ordinary coalescing and cache races can change source bytes; consult the raw
request-shape distributions rather than generalizing the 18-fragment result.

All 96 cached NEW Reader reads perform zero additional source or HTTP I/O.
Cached microsecond timings do not establish a CPU improvement. Mixed-eight
makespan median is 286.835 -> 231.949 ms, with ranges 268.655–338.615 versus
218.913–249.523 ms. Shared content and varying coalescing/cache races limit
that comparison; the earlier synthetic eight-unrelated-Reader saturation
counterexample (no throughput gain) remains valid.

Whole-test-process costs, including harness, credential preparation, cache
construction, logging, and all read regimes:

| Arm | User CPU seconds | System CPU seconds | Max RSS KiB |
| --- | ---: | ---: | ---: |
| 00 baseline | 2.035501 | 0.087692 | 33508 |
| 01 candidate | 2.452975 | 0.101370 | 40604 |
| 02 candidate | 2.499757 | 0.089672 | 40612 |
| 03 baseline | 2.046201 | 0.100632 | 35760 |

CPU/RSS increases must be carried forward into high-density evaluation. These
are not isolated ctld per-sandbox increments. The transient systemd unit was
garbage-collected after completion, so final unit-level peak accounting is
unavailable; persisted per-arm reports establish results, not default values
from a missing unit.

## Preserved anomaly and cleanup

The initial strict preflight failed because the original two-carrier pool had
only one ready carrier after this boot. Read-only inspection found unchanged
job index 75270, two terminal prior allocations, one new running warm-1, and
no queued placement. This campaign did not determine the missing warm-0's
cause and did not resubmit the job to hide the anomaly. The component-only
guard records ready=1 before and after; it does not claim a healthy pool or
relax a startup acceptance gate. Diagnose this separately before live claims.

Original runtime binaries/configs and recorded service PIDs/start ticks remain
unchanged during the test. The 2076-row fixture authority is unchanged; there
are no mounted tenant writers or attached NBD devices. All four arms exit zero,
21 exported JSON receipts have verified checksums, and no owned test executable
remains. SSH closes explicitly. A fresh cloud observation at
2026-09-13T08:59:43.834502188Z confirms 2 CPU / 8192 MiB,
`Stopped` / `StopCharging`. Data and existing caches are preserved.

## Decision and evidence

Keep the candidate diagnostic. The next useful test is an isolated full-path
comparison using the retained history templates: verified fresh-node and
cached-node NEW sandbox identities, regional claim/readiness and literal first
`node -v` separately, then occupied physical width. The 1s/2s requirements,
fresh Coding gap, and populated-root coverage remain open. Do not replace
these requirements with this component result or increase the 10s timeout.

Local evidence: `/tmp/sandbox0-history-demand.h9Ota4/evidence.json`;
raw export: `collect.remote.json`; analysis: `analysis.json` and `effects.json`;
sealed file checksums: `artifact-index.json` in the same directory.
Remote retained artifacts: `/data/sandbox0-history-demand-h9Ota4`.
All 1362 product source files remain unchanged from D-HISTORY-READ; only this
experiment record and its ledger/index entries are added to the worktree.
