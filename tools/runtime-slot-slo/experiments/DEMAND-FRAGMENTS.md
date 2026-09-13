# D-DEMAND-FRAGMENTS: demand-only parallel reads for history fragments

2026-09-13. Evidence root: `/tmp/sandbox0-demand-fragments.Cz3DHh`.

## Scope and decision

This follows D-HISTORY-READ, not another indexed/speculative-group replay.
Keep the implementation in an isolated Go overlay until real RootFS/transport
and full-path qualification. No product-default adoption or cold-start SLO
claim. The unchanged 1362-file worktree remains the baseline.

The candidate considers only fragments serving the bytes of an actual ReadAt
within its already authenticated mapping leaf. It does not fetch another
mapping page, use a command/path hint or warm a tenant RootFS before claim.
It still fetches complete authenticated/compressed units: demand-only scheduling
does not imply zero byte amplification. Ordinary complete entries keep the
existing coalescing path.

## Mechanism and bounds

- Start only at an uncached partial compressed view; schedule at most 128 KiB
  of the current caller's demand within that leaf. Deduplicate verified unit
  identities. Coarse units over 64 KiB fall back to the existing reader.
- All workers call the existing readRange and share checksum validation,
  singleflight, Reader lifetime and eight-source per-node admission. No source
  limit, decoded/header-cache budget, format or 10s public timeout change.
- At most eight private batches per ReadCache, with at most eight workers
  each. Busy readers fall back immediately; there is no second waiting queue.
  Private output payload is at most 1 MiB per node/cache. Each worker can retain
  one at-most-64-KiB verified unit during copying: at most another 4 MiB of unit
  references outside existing cache/source limits. These are payload bounds,
  not a measured RSS or allocator/codec/goroutine-overhead bound.
- Workers write disjoint pieces to private output, never to caller memory.
  Join every worker, choose the earliest failed logical piece and expose only
  its verified prefix. Later successful pieces remain invisible after an
  earlier error. Hole and composite-tail overrides schedule no covered source
  unit. Failure may still fetch other requested units concurrently; it need
  not stop issuing I/O at the serial reader's first-error position.
- Legacy Get-only sources retain their existing inability to cancel an active
  Get. A batch must join all workers before device/branch cleanup can finish.

## Evidence collected

- Actual production builders construct 16-write, 128 KiB fragmented fixtures.
  A fake clock adds exactly 20ms per source call, not an observed S3 delay.
  With 128 MiB initially empty cache: serial18 calls/360ms versus candidate18
  calls/60ms, the same98,360 stored data bytes. With cache disabled: serial32
  calls/640ms/328,128bytes versus candidate18/60ms/98,360bytes. Candidate peak
  is eight source bodies. These numbers are not sandbox or network predictions.
- Check unaligned/EOF/small demands, rejection of source ranges outside actual
  demand, checksum failure and untouched caller suffix, hole/tail precedence,
  lifetime cancellation and recovery, and actual encrypted-store round trip.
  The encryption test uses the existing in-memory store and test key wrapper,
  not a remote S3 provider or production key service.
- Twelve unique Reader generations share one cache. The first eight hold the
  batch budget; four late Readers fall back. Controlled source grants prove
  those late owners progress through the existing fairness queue before every
  earlier fragment finishes. All worker/source/batch counts return to zero.
  This is a concurrency invariant test, not occupied physical-width acceptance.
- The existing 64 MiB/256-generation history fixture is rerun with a mutex
  around its test-source counters so newly parallel Get calls can be counted
  safely. The test-only adaptation stays in the overlay. Full-image hashes and
  all prefix contents still verify. In its disabled-cache fragmented case,
  demand mapping reads32->1 and data reads32->18; the128MiB case retains1/18.

## Changes, failures and CPU screen

- The initial coarse-fallback test constructed an invalid raw partial view;
  mapping validation rejected it before the candidate ran. Retain the failed
  five-repeat safety receipt, failed package receipt and original fixture source.
  The corrected fixture uses a valid small compressed view followed by a
  full coarse raw range. No validation was relaxed.
- V1 corrected safety tests pass85 parent/subtest events across five race
  repeats. Full package race passes951 events with two opt-in skips; vet passes.
- A separate baseline/candidate/candidate/baseline in-memory benchmark uses
  GOMAXPROCS1/8 and three samples per case/arm, with no test-process overlap.
  It measures Reader construction/read CPU and allocation costs, not runtime
  startup. V1 cold128MiB-cache reads are slower and allocate about137kB more
  per operation. Cached-new Readers retain240B/1allocation, but cost roughly
  1–2 microseconds more because the initial view hit was looked up twice.
- V2 removes that duplicate cache lookup and reuses the verified cached view
  directly. The V1 implementation and all results remain archived separately.
  V2 full package race passes951 events with the same two skips. The subsequently
  added saturation model separately passes25 events across five race repeats.
  Final vet includes that model and passes. No skipped test is promoted to an
  acceptance pass.

### V2 in-memory cost screen

Each median combines six samples from the two matching ABBA arms; retain all
144 raw benchmark samples across V1/V2. This is a small local screen without
confidence intervals, not a wall-clock latency or production-density gate.

| Case | GOMAXPROCS | Baseline median | V2 median | V2 allocation-byte change |
| --- | ---: | ---: | ---: | ---: |
| Disabled cache | 1 | 1927.147us | 496.241us | -78.5% |
| Disabled cache | 8 | 3318.352us | 968.519us | -78.5% |
| Empty 128MiB cache | 1 | 478.869us | 530.372us | +27.2% |
| Empty 128MiB cache | 8 | 743.237us | 1046.819us | +27.3% |
| Cached-new Reader | 1 | 10.612us | 10.573us | 0% |
| Cached-new Reader | 8 | 12.373us | 12.693us | 0% |

Cached-new ranges overlap and both retain240B/one allocation; do not claim
equivalence from the medians alone. Empty-cache no-network reads retain a
10.8%/40.8% time increase and about137–138kB additional allocation per operation.
Disabled-cache gains include avoiding repeated decode/view and mapping reads.

### Saturated source-budget counterexample

The controlled20ms-per-source model uses the unchanged128MiB cache, unique
generations and one shared eight-source budget. Every read requires18 distinct
source calls and there is no prior data cache warming.

| Simultaneous Readers | Total data reads | Serial per-request path makespan | Candidate makespan |
| --- | ---: | ---: | ---: |
| 1 | 18 | 360ms | 60ms |
| 8 | 144 | 360ms | 360ms |

All five race repeats preserve these fake-clock results and verified content.
This is NOT real occupied-node acceptance. It is a concrete counterexample to
extrapolating the single-Reader6x model result to source-saturated throughput:
the candidate does not reduce distinct requests in the current-cache case or
create new node I/O capacity. Keep the candidate diagnostic; do not admit it as
a universal high-density fix from these results.

## Remaining acceptance

No remote/cloud call, actual claim, guest command, production change, merge or
tag occurs in this experiment. The last remote-stop receipt is historical,
not freshly verified here. No local e2e or privileged mount is run.

Qualify the candidate on an actual history-bearing XFS artifact and encrypted
remote transport before live admission. The new mechanism specifically covers
history-induced views and does not establish a fix for fresh Coding's current
full-path gap. Loss of prior speculative neighbor reuse, extra CPU/allocation,
cache pressure and occupied-node behavior must be measured, not inferred from
the fake-latency model. Keep regional claim/readiness, literal first node-v and
combined latency separate for cold-node and cached-node NEW sandbox identities.
Populated large roots, real occupied width and the original1s/2s gates remain.
