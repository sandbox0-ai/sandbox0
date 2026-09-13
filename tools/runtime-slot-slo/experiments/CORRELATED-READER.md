# D-CORRELATED-READER: exact client, OSS and socket evidence

2026-09-12. Evidence: `/tmp/sandbox0-correlated-reader.T03MNR`.
One ordinary Reader diagnostic; not a sandbox startup or production-width test.

## What changed in the conclusion

All **581 client requests now match exactly one OSS log record**, with the same
request ID, object SHA256, status206 and returned bytes. No missing, duplicate
or mismatched rows. This closes the full-workload correlation gap left by
D-OSS-SERVICE; its old failed ID capture remains failed and unchanged.

The machine/S3 hypothesis has stronger evidence than a shared time window.
Of121cold requests with client request-written-to-first-byte at least20ms,
116also have OSS response time at least20ms;62have server processing at least
20ms. Socket smoothed RTT is generally below1ms. Large waits on these exact
requests cannot be explained solely by local key unwrap or physical-network
round-trip distance. This does not identify an OSS defect or its internal queue,
storage/cache/frontend/response substage, and does not exonerate all host/network
effects or machines under production occupancy.

| Exact request example | Client post-write | OSS processing | OSS response | TCP RTT at first byte |
| --- | ---: | ---: | ---: | ---: |
| Coding cold, ID387 | 97.942ms | 61ms | 98ms | 0.497ms |
| Node cold, ID89 | 73.503ms | 69ms | 73ms | 0.706ms |
| Node cold, ID2 | 72.134ms | 67ms | 71ms | 0.738ms |
| Coding cold header, ID25 | 56.441ms | 53ms | 56ms | 0.911ms |
| Node cached-new, ID477 | 48.322ms | 3ms | 48ms | 0.514ms |

Processing and response durations are distinct provider fields, not synchronized
subspans of the client clock. Do not subtract them into exact network RTT or add
parallel service durations into startup. ID477 also shows why attributing all
response delay to backend processing alone would be wrong.

## Fixed scope and checks

First query the three slowest cached-new request IDs from the retained combined
transport campaign. All three have no corresponding SLS records; the old
measurement cannot be retrospectively correlated. No RootFS GET or compute start
is needed for that read-only check. The failed lookup is retained.

Then run ONE original ordinary Reader schedule, with14,373ReadAt plus16Reader
constructors. All output checks,547source shapes, both full cache snapshots and
57crypto operations match the frozen input. All581HTTP attempts are successful,
closed206responses. The SDK/provider, authority, encryption and immutable objects
are unchanged. No combined-format objects are requested or uploaded.

The node-side Reader starts with128MiB data cache,16MiB mapping protection and
8MiB/1024encrypted headers, retains them for cached-new identities, and uses the
same10s requests/source admission. Provider caches are not controlled; this is
not a claim of a cold OSS server. The input serializes eight logical identities
per cohort; it does not execute eight live guests or model occupied scheduling.

| Per-request distribution | Cold median / P95 / max | Cached-new median / P95 / max |
| --- | ---: | ---: |
| Client post-write, ms | 12.607 / 34.585 / 97.942 | 6.729 / 13.027 / 48.322 |
| OSS processing, ms | 7 / 28 / 69 | 3 / 5 / 14 |
| OSS response, ms | 13 / 36 / 98 | 6 / 12 / 48 |

Cold has389HTTP/355source calls; cached-new192/192. Three cached-new post-write
waits exceed20ms; all three have provider response at least20ms, but none has
processing at least20ms. Connections are reused577of581times. Credential
preparation167.651ms is separate and not charged per object.

The serialized component ReadAt+constructor interval unions are7005.886ms cold
and1845.815ms cached-new; HTTP coverage6136.257/1616.859ms, crypto outside HTTP
248.713/0ms. These multi-identity helper totals are NOT per-sandbox startup,
new2s passes/misses or a valid absolute comparison to another observer version.
Returned ciphertext59,104,366/23,408,734bytes matches the ordinary workload.

## Socket and machine observations

The only new connection observer reads Linux TCP_INFO at got-connection,
first-byte and body-close. It never changes socket flags/deadlines/traffic.
It records its own cost: median3.526us/P959.415us/max48.864us; interval union
7.390ms across1743samples. Of these,1740are available. Body-close samples for
IDs191/426/450are unavailable and remain explicit; all got-connection and
first-byte samples are present. No missing value is interpreted as zero RTT.

Across available samples, smoothed RTT median0.528ms/P950.801ms/max1.116ms.
There are four HTTP/1.1 connection tuples carrying150/150/150/131requests.
TCP_INFO is cumulative connection state, not an exact per-request network trace.
One connection's retransmission counter increases by2, with observed increments
across IDs567and572: client post-write13.239/19.050ms versus provider response
5/7ms. These identify limited transport contribution worth preserving, not an
exact subtraction assigning the residual to those packets. The large examples
above do not show a retransmission-counter increase across their samples.

506host samples span10.083s. Host aggregate idle80.060%, steal0, no observed CPU
quota throttling or memory PSI/OOM. CPU-some pressure is528.945ms host-wide and
223.014ms for the diagnostic; contention is not literally zero. Host IO-some/
full11.612/9.364ms, diagnostic IO pressure0. Process CPU3.291s includes observer
and verification work; RSS peak320884KiB is not per-sandbox memory/density cost.
The host sampler itself costs median0.616ms/max3.852ms; no observer-off startup
calibration is claimed.

## Lifecycle, authority and verification

Fetch both main refs before work: sandbox0 remains0f092204, infraaf04ea978.
Production main is Nomad-only with official runsc release-20260817.0 and fixed
worker type ecs.u2a-c1m4.4xlarge. This diagnostic remains on the existing2CPU/
8GiB Singapore ecs.g9i.large fixture with historical runtime pins, one ready
carrier and two warm-job groups. It is not production parity or width proof.

Only policy `s0-cold-correlated-t03mnr`, in instanceMode for the exact Singapore
test bucket, is enabled. Existing SLS service/role/logstore are reused: no IAM,
account-wide collection, production resources, retention or shard changes.
The existing logstore remains7days/two shards. One owner preserves the original
07:25:34.999UTC deadline and disables collection early at07:07:47.165UTC after
the sole run. Fresh07:11:38UTC readback confirms disabled. The disabled rule and
retained logs remain for audit, not an assertion of automatic resource deletion
or zero storage cost. Two bounded GetBucketInfo readiness polls precede data
reads; the first returns0records and the second1. No extra RootFS probe runs.

The unit finishes inactive/success/0/MainPID0. All ten report artifacts transfer
and match SHA256. Before/after original binaries/configuration/PIDs, boot
eeb5433f-7ba8-4888-898e-dd5174ded77a, job70668, rows2072/2072/263, empty runtimes
and64detached NBD devices match. Makefile remote-skill start/stop only; no Kind,
bootstrap, local e2e or runtime service restart. Preserve `/data`. SSH is closed;
the owned master exits255 after explicit control-close, not a failed Reader.
Keep the interimStoppingreceipt. Final07:11:37.693UTC cloud read confirms
Stopped/StopCharging/no publicIP. No live diagnostic or collection owner remains.

14Go race tests pass, including original input/source/cache/crypto checks and
real TLS/S3 ID/connection observation. Ruby7tests/15assertions cover exact
correlation, output/cohort completeness, source lifetime, response identity and
TCP/phase geometry. All1353frozen product files remain unchanged. Only experiment
notes change in the repository; user work is preserved. No production rollout,
merge, tag, RootFS write or material deletion.

## Next decision

The request-ID observer and this ordinary Reader correlation are now complete;
do not repeat them as another baseline. Preserve both service-side delays and
the two transport-counter increments. Further attribution inside OSS needs
provider evidence, not renaming every response residual as network or CPU.

Optimization must reduce actual mandatory/dependent remote reads or demonstrate
that a different generic durable layout reduces those service waits without
added decode/cache/density costs. Header-only packing, connection-pool changes,
larger caches/timeouts/hardware and unchanged broad replay are not justified by
these results. Reuse the sealed real-startup dependency evidence to choose a
materially wider mechanism before implementation; never project these helper
quantiles directly onto its guest/kernel critical path.

Generic carriers, claim-time tenant RootFS, disposable workers, cryptography,
checksums, authenticated readiness, writer fencing and cleanup remain unchanged.
Current-version regional ingress-to-procd, immediate real node-v, empty-node/
cached-new identities, populated-large roots, actual occupied production width
and1s target/2s fallback still require end-to-end verification. Goal not complete.
