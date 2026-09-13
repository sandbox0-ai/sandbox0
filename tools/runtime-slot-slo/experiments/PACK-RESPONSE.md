# D-PACK-RESPONSE: test total-object size versus range response cost

2026-09-12. Evidence: `/tmp/sandbox0-pack-response.HWZGyw`.
An actual remote encrypted-storage comparison, not sandbox startup acceptance.

## Decision

Do not split RootFS packs just to pursue an assumed large-object response penalty.
For this fixed range,1MiB objects do not show a consistent first-touch advantage
over64MiB objects. They win only6/12 pairs; the two order strata disagree. The
predeclared admission threshold is not met. This rejects the size-only lane as
the main cold-start fix on this corpus, not every possible object geometry or
every contribution from S3/network/client waiting.

Every repeated-source target read is faster than its first-target counterpart,
despite every call still traversing OSS and returning the same ciphertext bytes.
This is evidence against attributing all repeated-test improvement to a node
payload cache. Provider-side state, request ordering and network/client effects
remain possible; neither an OSS defect nor a pure server-time decomposition is
established.

## Controlled scope

Use the original2CPU/8GiB Singapore test ECS and existing internal OSS authority.
No production, runtime binary/config/job change, guest launch, import or artifact
registration. Publish12 synthetic pairs with the existing conditional-create
encrypted writer, same configured key/algorithm and16KiB frames. Each pair has
identical plaintext through the first1MiB; only total size changes1MiB/64MiB.
Keys and envelopes are necessarily distinct. No existing object is overwritten.

All target reads use plaintext offset256KiB, length64KiB. Actual ciphertext
requests are exactly `bytes=263278-328893`,65616bytes, for every target in both
arms and repetitions. Exact AEAD/length/checksum verification and one HTTP request
per target are required. The underlying provider/SDK/transport is unchanged;
a compile overlay adds only the already qualified per-GetObject observation
option. Dual x-oss/x-amz request-ID handling and owned-socket TCP observation are
reused rather than replacing the provider or enabling another SLS campaign.

One-byte envelope preparations also read each object's first frame, with ranges
proved disjoint from the target. This is first-touch PAYLOAD with prepared
headers, not a wholly cold claim. Publication and header reads can warm provider
storage beyond requested bytes; provider-side caches are not controlled. No
RootFS/decoded/ciphertext payload cache exists in this standalone caller, and
all48 target operations are independently bound to real HTTP responses.

First-target order is S,L / L,S by pair, six pairs each; repeated-source order
reverses each pair. All objects are published before header preparation/reading.
The10s per-operation limit is unchanged. There is no claim retry or startup
timeout change. Sequential two-core reads are not production-width evidence.

## Results

Twelve samples per row. First-byte wait is client WroteRequest to
GotFirstResponseByte, not pure OSS processing. Operation includes authenticated
decode and fixture checksum verification. Marginal medians are not additive.

| Source access / object size | First-byte wait median | First-byte wait P90 | First-byte wait max | Full operation median | Process CPU median |
| --- | ---: | ---: | ---: | ---: | ---: |
| First target /1MiB | 7.731ms | 8.804ms | 12.411ms | 8.137ms | 0.572ms |
| First target /64MiB | 7.250ms | 8.766ms | 35.819ms | 7.665ms | 0.577ms |
| Repeated source /1MiB | 4.932ms | 5.848ms | 5.911ms | 5.464ms | 0.559ms |
| Repeated source /64MiB | 4.607ms | 6.289ms | 6.480ms | 5.222ms | 0.644ms |

All72 encrypted read operations succeed:24 disjoint header preparations and48
targets. The provider records exactly24 PutObject,24 HeadObject and72 GetObject
attempts, with no extra retries. All target connections are reused. Every target
response has exact bytes, complete body/EOF/close and an unambiguous request ID.
TCP RTT samples at first byte span0.496-0.741ms; these cumulative transport
estimates cannot be subtracted from first-byte wait as exact server time.

Small-object first-target median is0.481ms higher, not the required at least50%
and5ms lower. Only6/12 pair differences favor small objects, versus the required
9/12. Median paired differences are+0.380ms in one order stratum and-0.473ms in
the other. Preserve the35.819ms large-object outlier without declaring that one
sample proof of a size-caused tail.

Paired first-to-repeat wait reductions are positive for all12 objects in EACH
arm: medians2.880ms small and2.572ms large. Similar process CPU and identical
network bytes do not explain away the response-wait change. This is not evidence
of zero first-touch storage cost, nor permission to count repeated-source timings
as empty-node sandbox acceptance.

## Safety, preparation failures and validation

The first SSH master attempt terminates unsuccessfully before staging. A separate
read-only connection succeeds, then a new owned master is opened. No machine or
test replay follows an observation timeout.

The first staging identity check correctly rejects an assumed two ready carriers:
this boot's original job71527 instead has zero ready carriers. Independent checks
confirm original binaries/configurations, processes,263/2072/source2072 rows,
artifacts, empty physical runtime and64 detached NBDs. Because this is a storage-
only probe, retain that anomaly and explicitly correct the fixture guard to
require the observed zero-ready state unchanged before/after; do not alter Nomad
or claim runtime health. Original/qualified guard and manifest bytes, failed
receipt and separate correction intent are retained. The experiment starts only
after qualification, once, as `s0-pack-response-HWZGyw.service`.

Eight Go tests pass with race detection, plus vet/static amd64 build. Four
analyzer tests/14 assertions and48 independent HTTP endpoint differences pass;
the complete analysis reproduces and actual ordering is checked. All67 remote
files match size/SHA. Frozen1353 product files and193 worktree status entries are
preserved. Product source is not changed or adopted.

Execution and before/after guards succeed. The transient unit is inactive and
garbage-collected, no owned probe PID remains, and owned ciphertext scratch is
empty. Its final systemd memory/CPU counters are unavailable after collection;
do not promote the initial launch snapshot to a peak/RSS/density result. Original
services/files/job/rows/NBD state are unchanged. The owned SSH master is closed
and the workspace stop workflow is used; final cloud receipt accompanies evidence.

Retain24 create-only synthetic objects in `diagnostics/pack-response-hwzgyw/`:
817889280 plaintext bytes,818907216 encrypted stored bytes. They contain generated
test data, no tenant RootFS content, and are not registered as durable generations.
No existing source or diagnostic evidence is deleted.

## Handoff

Stop this size-only tuning lane; do not run a pack-size sweep or reimport either
real image from this result. A different response-cost mechanism needs its own
identified cause and net full-path coverage. Smaller packs would add headers,
objects, mapping/inventory/import and GC work, none admitted here.

There are zero new regional claims/commands in this turn. The latest real current-
pin cold combined maxima remain Node2.221s/Coding2.668s. Shared tenant-neutral
carriers, claim-time RootFS binding, stateless nodes and encrypted durable authority
remain unchanged.1s/2s, populated-root/upper-history and genuinely occupied
physical-width gates remain open; the goal is not complete.
