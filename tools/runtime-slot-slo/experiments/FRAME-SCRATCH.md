# D-FRAME-SCRATCH: less allocation, no demonstrated startup speedup

2026-09-12. Evidence: `/tmp/sandbox0-frame-scratch.81ED3t`.
This follows D-COMMAND-COST's measured encrypted-frame processing cost.

## Decision

A per-read scratch buffer with in-place AEAD opening substantially reduces
allocation, but this campaign does **not** establish lower whole-command CPU or
cold-start latency. Retain the isolated candidate, not a production change or a
latency win. Do not tune this mechanism further to chase the 1s/2s target.
The response-wait and complete regional/occupied-width requirements remain open.

The candidate copies each provider frame into read-owned scratch, authenticates
with `AEAD.Open(ciphertext[:0], ...)`, synchronously delivers the selected range,
and only then reuses scratch. It changes neither provider buffers nor cached
metadata. Go's AEAD contract explicitly permits this exact overlap; `io.Writer`
must not retain the input slice. There is no global pool or cross-reader sharing.
Allocation follows the validated actual frame length, not a maximum header hint.
Every frame/AAD/nonce check, plaintext digest, partial-output rule, stale-header
refresh, cancellation path, cache bound and request timeout remains unchanged.

## Actual encrypted OSS to authenticated procd and node-v

One control/candidate/candidate/control campaign on the same 2-vCPU/8GiB ECS.
Each pair starts a fresh provider, encrypted-header cache and 128MiB Reader cache,
then uses cold and cached-new COW/runsc/procd identities. No image rebuild or object
publication; source staging remains detached. Both binaries use the same HTTP
observer and low-overhead Go allocation counters; CPU/Go trace profiling is off.

| Order | Variant | Cold local ready, ms | Cold node-v, ms | Cold local combined, ms | Reader-process CPU, ms | Cumulative allocation, bytes |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| O1 | Control | 827.107 | 1310.968 | 2138.183 | 937.087 | 518,996,816 |
| O2 | Scratch | 613.726 | 777.534 | 1391.363 | 898.031 | 421,142,072 |
| O3 | Scratch | 572.715 | 842.663 | 1415.499 | 888.035 | 419,877,408 |
| O4 | Control | 584.293 | 688.279 | 1272.784 | 894.609 | 516,755,352 |

Cold allocation is about 19% lower with scratch. This is total bytes allocated
over the operation by the diagnostic Reader process, including its observers;
it is not RSS, live heap, peak memory, or a demonstrated density improvement.
The Node-command phase accounts for about 307MB with scratch versus 380-382MB
with control. Cached-new stays around 104MB for both, with zero source requests.

The late control's CPU is essentially equal to the two candidate samples, and
its elapsed time is lower. Do not compare only O1 with O2 to claim a 747ms gain.
O1's combined >2s sample remains part of the result. Cached-new combined times
are 287.976 / 285.102 / 294.495 / 273.732ms; commands are 96.416 / 95.725 /
102.255 / 93.500ms. All eight distinct procd instance IDs authenticate readiness
and all eight real commands return `v22.23.2` with exit code zero.

The measured local operation includes ephemeral network/key preparation,
Reader construction, NBD/mount, runsc and authenticated procd followed by the
command. It excludes provider/credential preparation, regional ingress,
manager/PG claim and occupied production width. There are zero new regional
acceptance samples. Current main/infra architecture was rechecked; this retained
fixture still does not establish current production runsc-pin parity.

## Demand and response conditions

All guests demand 93,223,424 unique NBD bytes before command completion, but the
exact encoded/ciphertext request multisets differ. The strict same-request
comparison required for a latency attribution therefore does not pass.

| Cold order | HTTP attempts | Ciphertext returned, bytes | Write-to-first-byte P95, ms |
| --- | ---: | ---: | ---: |
| O1 | 265 | 48,898,075 | 39.155 |
| O2 | 265 | 48,963,691 | 20.927 |
| O3 | 264 | 48,865,267 | 18.261 |
| O4 | 268 | 48,553,591 | 17.311 |

All 1,062 requests return 206, close successfully and bind one-to-one to source
IDs, key hashes, exact HTTP ranges and returned byte counts. About 99% reuse a
connection. Node caches start empty; provider/backend caches are uncontrolled.
First-byte callbacks cannot distinguish pure OSS processing, network delay and
client scheduling. This observer captures no validated server request IDs; the
earlier correlated-reader experiment already has a two-header allowlist for
`x-oss-request-id` and `x-amz-request-id`. Reuse that existing mechanism if service
correlation is needed in a future actual regional run, not another broad baseline.

Host sampling over 8.010s shows 48.09% busy, 33.61% idle and 18.30% iowait;
zero observed steal, cgroup throttling or OOM. Minimum MemAvailable is
6,105,892KiB. CPU/I/O pressure is present; these unoccupied two-core observations
do not prove production density or a physical disk bottleneck.

## Narrow benchmark and correctness evidence

The same frozen decryptor is retained as a differential test reference. Both
AES-GCM and ChaCha20 cover every byte truncation and single-byte corruption of
a multi-frame fixture, ranges, wrong object/chunk AAD, nonzero start chunks,
writer failure after authenticated prefixes, unchanged source bytes and 32
independent concurrent reads. The entire objectstore race suite passes, including
existing cancellation, stale-header, bounded independent-read and corruption tests.
RootFS block and objectstore package race tests also pass (181.020s / 2.692s).

On the actual remote amd64 machine, a synthetic 1MiB AES read using 16KiB frames
has median 536.470us / 2,237,753 allocated bytes for the frozen loop versus
224.152us / 27,708 bytes for scratch (three samples). ChaCha20 is 859.900us versus
521.588us, with the same allocation reduction. Synthetic speedup is not whole
Reader, real-command or regional speedup; the latter remains unproven above.

Both real-guest harness variants pass race tests, vet and builds. A cloned AST
test initially inspected the previous retained runtime file; it was corrected
to this experiment and the fourth pair, then both complete harness race suites
passed again without changing the built binaries. Seven offline analysis tests /
42 assertions reproduce results and reject range, cold-cache, command, counter,
cleanup and scope mutations. Preserve the initial benchmark formatter's missing
Ruby `end`; correcting offline formatting reuses the original successful output
and does not rerun guests.

## Preservation and next action

All 42 remote reports transfer with verified hashes. Campaign and physical cleanup
finish successfully with the original services, PG rows, Nomad job, host network
and detached source staging unchanged. The owned SSH session closes and the
instance receives an explicit StopCharging request. The first status read during
shutdown still reports Stopping; it is retained, not treated as terminal. Stop
completes at 13:32:58UTC and a fresh 13:34:05UTC read confirms
Stopped/StopCharging/no public IP. No production rollout, merge, tag, object publication,
deletion, local e2e or historical Kind bootstrap occurs.

The 1,353 frozen product files and 193 dirty worktree entries remain unchanged;
only requested experiment notes are added. Scratch is an isolated compile overlay,
not an adopted change. Its allocation benefit can be considered for occupied-node
validation, but does not justify more isolated latency tuning. Resume the full
regional claim + immediate-command causal budget using the ordinary layout and
existing traces, accounting for exact demand, response waiting and actual node
occupancy. Populated large roots, upper histories, authority/semantics and real
production width remain required. Generic carriers, claim-time RootFS selection,
no user-root prewarm and the original 1s/2s gates remain fixed.
