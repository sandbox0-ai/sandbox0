# D-OSS-SERVICE: provider processing observed; exact correlation failed

2026-09-12. Evidence root: `/tmp/sandbox0-oss-service.svgQQ9`.
No product change, runtime acceptance or performance improvement is claimed.

## Decision

The machine/S3 hypothesis now has provider-side evidence. The original Reader
still spends most of its time in HTTP, and OSS itself reports nonzero processing
time for the matching test-window population. A CPU-only explanation is
insufficient for this component observation. This is not proof of an OSS defect,
an exact network/service split, or adequate hardware at occupied production width.

Keep ordinary layout, fixed caches and all original requirements. Prioritize
mandatory remote-read dependencies on the actual startup critical path, not
another RSA/cache/hardware/concurrency sweep. Do not multiply aggregate request
counts by latency and call that startup savings.

## Fixed workload and retained failure

One original full-Reader schedule ran on the existing 2CPU/8GiB Singapore test
machine. It serialized two eight-identity schedules, cold then cached-new; these
are neither actual claims nor live eight-wide occupancy. The shared Reader cache
starts empty at128MiB, with16MiB mapping protection; source admission remains8,
encryption-header cache8MiB/1024entries and timeout10s. RootFS objects, crypto,
input sequence and output hashes match D-READER-COST. OSS internal caches are not
controlled by this observation; it is not an assertion of a cold storage server.

All14373ReadAt checks and16constructors validate, all547source shapes and both
cache states match, and all581HTTP attempts return206 with successfully closed
bodies. There are57crypto operations and577reused HTTP connections. Nevertheless,
the diagnostic exits1: all581new client request-ID validations fail. The observer
only reads `x-oss-request-id`, discarding values that are not24hex characters.
It did not retain presence/length or the S3-compatible `x-amz-request-id` header.
Absent versus unsupported headers cannot be distinguished retrospectively.
The service logs do contain581unique24hex IDs; that does not recover client IDs.

No probe replay, changed validator, rebuilt binary or replacement result erases
this failure. The terminal systemd unit remains failed/exit-code/1, with no live
process. Read correctness is recorded separately from correlation correctness.

## What the matching service window establishes

An exact-bucket GetObject query covers04:25:20–04:25:34UTC. It returns581unique
records, all206, totaling82,513,100response bytes. Object SHA256 and response-byte
multiplicities exactly match the client population. Only hashes and allowlisted
numeric/ID fields are exported; no raw object keys, signed URIs, access IDs or
private configuration are copied from SLS. This is whole-window correspondence,
not a per-attempt, range-offset, subsecond or cold/cached service-time assignment.

| Provider log duration | Median | P95, nearest rank | Maximum | Mean |
| --- | ---: | ---: | ---: | ---: |
| `server_cost_time`, ms | 3 | 25 | 68 | 7.492 |
| `response_time`, ms | 7 | 32 | 71 | 11.775 |

OSS defines these as server processing and response time in milliseconds; they
are not interchangeable with client write-to-first-byte or body-close intervals.
See the official [log field definitions](https://help.aliyun.com/zh/sls/oss).
No difference of averages, sum of service times or aligned maxima is presented
as network RTT, exclusive critical-path cost or potential startup savings.

## Client and machine evidence

| Serialized Reader component | Cold | Cached-new |
| --- | ---: | ---: |
| ReadAt + constructor interval union, ms | 6286.661 | 1758.829 |
| HTTP interval coverage, ms | 5384.857 | 1529.133 |
| Crypto outside HTTP, ms | 274.656 | 0 |
| Write-to-first-byte median / max, ms | 10.042 / 71.530 | 6.539 / 27.471 |

Do not compare these multi-identity helper totals to the2s startup gate. The
preceding identical workload's cold Reader union was7674.903ms; changed timing
without a product change is variation, not an admitted optimization effect.

463host samples over9.227s show78.654%CPU idle, no steal, cgroup throttling or
memory PSI. CPU-some pressure is443.053ms host-wide and193.397ms in the probe;
this is not zero contention. Host IO-some/full pressure is5.279/4.033ms; probe IO
pressure is zero. Global TCP counters include one retransmission, zero timeouts
and abort-on-data/close+2/+1; those are not attributed to this client's sockets.
The longest71.530ms wait encloses three regular host samples and no retransmit,
IO or memory pressure. This narrows a whole-runtime pause hypothesis, not all
per-goroutine effects. No observer-off or different-machine comparison was run.

## Scoped logging setup and cleanup

SLS service and `AliyunServiceRoleForSLSAudit` already existed. One uniquely named
collection policy, `s0-cold-service-svgqq9`, used instanceMode with only the exact
Singapore test bucket and no centralization/resource-directory scope. No IAM,
account-wide logging, production resource or native bucket-log shipping change.
The original20minute deadline was04:30:13UTC and was never extended.

SLS created its regional managed `oss-log-store`, two shards, with7day retention.
The rule was enabled once and disabled after the single replay; readback verifies
disabled at04:26:45.690962500UTC. Queries afterward read retained logs, not new
RootFS requests. The disabled policy and managed project/logstore remain for
audit; seven days is data retention, not automatic resource deletion or a promise
of zero cost. See [CloudLens assets and limits](https://www.alibabacloud.com/help/en/sls/cloudlens-for-oss/).

This boot has one ready carrier, versus two in D-ARRIVAL-PATH. The object-only
guard originally allowed zero/two but omitted one; it was corrected only for
this direct Reader diagnostic. No claim or full-runtime readiness gate was
relaxed. Before/after boot, original binaries/configuration/PIDs, job70668/two
groups, source/formal/original rows2072/2072/263, empty physical runtime and64
detached NBD devices match. No restart/re-register/repair or RootFS object write.

Local setup errors are preserved: eventual PolicyNotExist and default empty
directory-field normalization during policy readback; an invalid raw GetLogs
CLI route and unexpected SQL metadata projection; the object-only ready-count
guard; and executing a relative-require launcher via stdin. The latter failed
before launch intent; an absolute-path wrapper verified the unchanged staged
launcher and started exactly one unit. None is an OSS latency sample. The owned
cleanup watcher adopted the existing rule and original deadline, without a
second enable operation.

Remote skill usage was limited to workspace Makefile lifecycle; no Kind,
bootstrap, local e2e, production rollout, merge or tag. `/data` is preserved.
Final cloud read04:30:08.485154375UTC isStopped/StopCharging,2CPU/8GiB/no publicIP.
Eleven diagnostic Go race tests passed before the run; their synthetic OSS header
does not establish compatibility of actual S3-protocol response headers. Eight
analysis tests/616assertions separately protect interval accounting and prevent
read/HTTP/cache failures from being reclassified as ID-only failures.

## Next bounded step

Before any further full correlation run, validate a single bounded actual
response's allowlisted OSS/S3 request-ID fields and bind it to its provider log.
Do not repeat581requests to discover whether the observer works. Reuse sealed
runtime dependency/arrival evidence to rank removable serial remote waits;
whole-window service quantiles alone cannot approve a format or runtime change.
Current-version ingress→procd, real `node -v`, empty-node/cached-new identities,
populated-large-root, occupied production-width and durable-lifecycle gates all
remain open. Generic carriers, claim-time RootFS binding, crypto, proofs and the
1s target/2s fallback are unchanged.
