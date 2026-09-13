# D-INDEXED-NET: real encrypted full-demand comparison

2026-09-13. Evidence: `/tmp/sandbox0-indexed-net.gtvXYg/evidence.json`.
Previous turn made progress: local indexed mixed-unit candidate qualified.
This turn performs six actual remote encrypted-storage replays. It does not
deploy the candidate into ctld or claim/execute a new sandbox.

## Decision

The candidate reduces total source calls, actual HTTP attempts and response
body bytes on the retained mixed-root workload, consistently across three
processes per arm. Advance to isolated live claim-plus-command comparison;
do not replace the product default or declare general cold-start acceptance.

The result is not uniform: Coding gains little in request count and reads
slightly more bytes in both cohorts. Its cached-reader time does not improve
consistently. Retain this regression and test both images in live concurrent
execution; do not hide it behind aggregate Node savings.

## Controlled inputs and coldness

- Original Aliyun test ECS: ecs.g9i.large, 2CPU/8192MiB, no resize. Host services
  and tenant-neutral Nomad carrier job remain untouched. No guest runtime starts.
- Arms execute baseline, indexed, indexed, baseline, baseline, indexed. Each
  is a new static linux/amd64 process, provider and encryption wrapper, with
  fresh 128MiB block/mapping and 8MiB/1024-entry encrypted-header caches.
- Each process constructs eight Readers and executes the exact recorded-start
  serialized schedule of 7187 original cold requests, then eight NEW Readers
  and 7186 requests sharing that process's caches. These are Reader identities,
  NOT new sandbox identities or production-width concurrency.
- All 14373 logical reads per process bind to the exact retained ordinary Node
  and Coding descriptors, offsets, lengths and output SHA256. Input SHA:
  c9a5855573a497db68bcb59d328f201bccfb47cf433ddcb10f280cbb3e3bc065.
- Runtime candidate is precisely the preceding sealed three-file overlay.
  Common HTTP/crypto observers are identical in both arms; undoing their
  instrumentation call sites reproduces current product files byte-for-byte.
- Keep 8 source permits, at most1MiB coalesced windows and 10s source deadline.
  No user RootFS prewarm, file/command hint, cache increase or local payload
  replay. Real reads go to the existing encrypted internal OSS authority.
- HTTP observer allows only bounded GETs of the57 retained objects, with
  16MiB/request, 4096-attempt and 256MiB/requested-body campaign-per-process
  diagnostic ceilings. It rejects writes/foreign targets. These limits are
  not product settings or a relaxation of the public request timeout.
- OSS backend cache state is not controlled. Fresh node-style caches do not
  prove a cold provider backend; alternating order and all samples are retained.
  This is not a measurement of disposable-node boot or regional startup.

## Stable complete-workload costs

All three repetitions per arm have identical counts/bytes below. HTTP response
bytes are observed response-body reads, including object encryption framing,
not TCP/TLS wire overhead. Source bytes are post-object-decryption stored-range
bytes, still compressed where applicable, not full decoded RootFS size.

| Across both cohorts, per process | Baseline | Indexed | Change |
| --- | ---: | ---: | ---: |
| Verified logical reads | 14373 | 14373 | identical |
| Verified constructors | 16 | 16 | identical |
| Source range calls | 547 | 436 | -20.29% |
| Source range bytes | 73819484 | 72336511 | -2.01% |
| Real HTTP attempts | 581 | 470 | -19.10% |
| HTTP response body bytes | 82513100 | 79248704 | -3.96% |
| HTTP requested range ceilings | 82700816 | 79436420 | -3.95% |
| Cold-header crypto spans | 57 | 57 | identical |

| Cohort | Baseline HTTP | Indexed HTTP | Baseline body bytes | Indexed body bytes |
| --- | ---: | ---: | ---: | ---: |
| Fresh process caches | 389 | 301 | 59104366 | 57070270 |
| Same-cache new Readers | 192 | 169 | 23408734 | 22178434 |

No source/HTTP failures, rejected requests or bad logical output. Total across
six runs: 86238 verified logical outputs and96 verified constructors.

### Per-image regression is not waived

| Image / cohort | Baseline HTTP | Indexed HTTP | Baseline body bytes | Indexed body bytes |
| --- | ---: | ---: | ---: | ---: |
| Node / fresh caches | 242 | 157 | 47913835 | 45715699 |
| Node / cached Readers | 101 | 81 | 15813682 | 14517766 |
| Coding / fresh caches | 147 | 144 | 11190531 | 11354571 |
| Coding / cached Readers | 91 | 88 | 7595052 | 7660668 |

Coding body bytes rise1.47% cold and0.86% cached. Its source bytes rise from
9514964 to9717489 cold and6043279 to6157965 cached. Node accounts for most
savings. This does not establish a universal read-amplification improvement.

## Time and resource observations, NOT startup latency

The following seconds are sums/elapsed time for the whole SERIALIZED
eight-Reader cohort, including thousands of retained reads. They must never
be shown as a single sandbox's claim/readiness/node-v latency.

| Run order | Arm | Cold cohort wall seconds | Cached cohort wall seconds | Process CPU seconds |
| --- | --- | ---: | ---: | ---: |
| 00 | Baseline | 6.947103 | 2.164466 | 2.944611 |
| 01 | Indexed | 4.064809 | 2.115970 | 2.805681 |
| 02 | Indexed | 3.610720 | 1.739329 | 2.774750 |
| 03 | Baseline | 4.254509 | 2.171192 | 2.857880 |
| 04 | Baseline | 4.508327 | 2.113033 | 2.873054 |
| 05 | Indexed | 3.899655 | 2.076711 | 2.798649 |

Median cold cohort wall time is4.508s versus3.900s; cached2.164s versus2.077s.
The first baseline is substantially slower even than later fresh-process
baselines. Do not attribute all this timing difference to grouping: backend
and environmental variability remain, and three samples are not tail SLO proof.

Node cold Reader-time sums span2.353–3.767s baseline versus1.757–2.044s indexed.
Coding cold1.405–2.680s versus1.360–1.525s is much less decisive; Coding cached
0.651–0.756s versus0.622–0.839s shows overlapping/regressing observations.
No per-image timing improvement is promised from the aggregate result.

Process CPU median is2.873s versus2.799s, including verification/observation
overhead. Peak RSS is318196–323892KiB baseline and318576–319808KiB indexed;
the run order and all values remain in analysis.json. No host steal ticks are
observed; minimum host available memory stays above5975032KiB. This is not
occupied-node memory reclaim or high-density acceptance.

## Verification and corrections

- Both static probe builds pass nine race-enabled observer/input/gate tests.
  The baseline independently reproduces all frozen source/cache shapes.
  Candidate shape changes are measured, but exact logical output and cache,
  HTTP, source integrity constraints remain mandatory.
- Independent verifier checks all output populations, per-cohort ownership,
  body/request accounting, twelve HTTP interval unions and exact original
  state before/after each arm. HTTP overlaps are unions, not sums presented
  as a serialized critical path.
- Initial read-only post-boot preflight reaches Nomad before port4646 accepts
  connections. A subsequent unit check finds original services active, and
  preflight-ready passes without restart/configuration mutation. Initial stage
  invocation fails closed before creating files; campaign launches once.
- Two read-only discovery commands fail (absent optional helper; nested JSON
  key casing). No measurement is restarted because of these observer errors.
- Initial analyzer wrongly equates requested Range length with observed body
  bytes and exits1. Each run has26 shorter EOF-complete responses. Their exact
  object/range/body hashes and187716-byte total difference are identical across
  all six runs. Separate analyze-v2 validates the distinction; no remote replay,
  checksum bypass or timeout relaxation. The original failed analyzer remains.
- Explicit SSH control exit succeeds after capture/final verification; its
  master wrapper exits255 afterward. Retain this separately from all six
  successful probe executions.

## Restoration and open gates

All33 exported JSON artifacts (66509163 bytes) verify after transfer. Original
services retain exact PID/start-tick/binary/config identities; original Nomad
job modify index73058 and trial sandbox rows2121 are unchanged. All64 NBD
devices remain detached, no owned probe process remains, unit is inactive/
success/exit0, and original source database/file checks pass.

The test machine remains2CPU/8192MiB and is freshly confirmed
Stopped/StopCharging at2026-09-13T05:52:51.596924688Z. /data artifacts are
preserved. No production rollout, merge or tag.

All1361 current product files and51 preceding candidate artifacts verify
unchanged. User-requested experiment records are updated despite the remote
skill's generic preference not to add summary docs.

Next: isolated baseline/candidate live regional claim/readiness plus immediate
literal node -v, recording command-only and combined time, cold-node and
cached-node NEW sandbox cohorts. Evaluate Coding separately and retain the
new read-amplification regression. Do not reopen closed cache/timeout/worker
sweeps or replace the goal with a favorable Node-only component result.

Populated/history-bearing large RootFS, occupied real production width, current
stock-runsc parity and original1s/2s gates remain unproven. Goal stays active.
