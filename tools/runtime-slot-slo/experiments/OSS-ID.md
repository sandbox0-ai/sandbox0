# D-OSS-ID: one actual S3-compatible response correlated to OSS

2026-09-12. Evidence root: `/tmp/sandbox0-oss-id.Tgsr7d`.
This closes an observer prerequisite, not a startup or performance acceptance.

## Result

The actual provider response contains one valid24hex `x-amz-request-id` and no
`x-oss-request-id`. The preceding D-OSS-SERVICE observer only read the latter.
The new observer therefore explains the missing IDs on this confirmed actual
S3-compatible path; the previous581discarded IDs cannot be reconstructed or
retrospectively marked as correlated.

Exactly one1KiB GET reads an existing immutable ciphertext header through the
unchanged original objectstore provider. Its SHA matches preceding attempt9,
object SHA795619cb33c7d7e4… and body SHA5283745fb97f04f3…. No extra RootFS range,
decrypted payload, object write, claim or guest command. HTTP admission permits
only this method/host/object/range and refuses a second network attempt. Product
timeouts and provider behavior are not changed; the diagnostic lifetime is10s.

Request ID `6AA4D92CF4F7BE35316C0BD1` matches exactly one GetObject log record,
same object SHA, status206 and1024response bytes. The recorded intervals are:

| Boundary | Duration |
| --- | ---: |
| Client connection acquisition, fresh connection | 13.919ms |
| Client request-written to first-byte callback | 20.971ms |
| Client HTTP attempt through body close | 35.092ms |
| OSS `response_time` | 20ms |
| OSS `server_cost_time` | 7ms |

These boundaries differ. Do not subtract them into an exact network RTT, infer
a distribution from one observation, or call35ms a sandbox cold start. Original
provider credential preparation is122.258ms before the HTTP trace origin and is
not hidden inside these intervals. This was not a reused-provider startup trial.

Five Go race tests cover OSS-only/S3-only IDs, missing/malformed/duplicate values,
conflicting valid IDs, real TLS/body closure, rejection of a second request and
exact range admission. Only validated IDs and header presence/count/length are
exported; malformed values, unrelated headers and response bodies are not.
The static linux/amd64 probe is SHA1f96d4fc82da4967…; its sole remote unit ends
inactive/success/0. No failed observation was retried as a new RootFS request.

## Logging and remote lifecycle

Existing SLS service/role and the existing managed regional logstore are reused.
Only a new exact-test-bucket instanceMode policy, `s0-cold-id-tgsr7d`, is enabled.
The prior rule is verified disabled. No IAM, account-wide collection, production,
project/logstore creation, retention or shard-count change. The original10minute
deadline is04:55:16.624569219UTC; disable readback succeeds04:46:58.601605469UTC.
The disabled policy remains for audit alongside7day log retention/two existing
shards. Seven days is not automatic resource deletion or a zero-cost guarantee.

Two bounded GetBucketInfo-log readiness polls precede the one RootFS GET; first
returns0, second1. The exact-ID query succeeds without another object request.
A local policy-list guard first assumed one array in the response. Actual CLI
JSON has both `data` and `statistics`; a fields/types-only check identifies the
shape, then the corrected guard reads `data`. This fails before any policy write
and is not an OSS latency sample. All receipts retain that correction.

The fresh boot3714c164… has one ready carrier, not full-runtime readiness. Before
and after, original binaries/configuration/PIDs, job70668/two groups, rows
2072/2072/263, physical-runtime absence and64detached NBD devices match. Only an
object diagnostic is admitted. No carrier repair, deployment, bootstrap, Kind or
local e2e. Remote skill uses workspace Makefile start/stop and preserves `/data`.
Final cloud04:51:14.557638750UTC isStopped/StopCharging,2CPU/8GiB/no publicIP.

## Structural next action, using history instead of another trace

D-HTTP-DEPENDENCY already maps1282startup-NBD HTTP attempts and validates5128
subphase maps. Its historical Coding samples would need48.15–68.00%reduction in
recorded single-outstanding payload-wait exposure to close2s with all other costs
fixed. This is an exposure screen, not a complete mandatory guest/kernel path or
guaranteed savings. Header/data already overlap; another cache/pool-only change
is not justified. The30sealed dependency artifacts are reused, not rerun.

The unified conditional mapping/data delivery candidate has a bounded-cache
prototype, but still needs net encrypted-transport/decode cost and safe lifetime/
concurrency behavior. Reviewing it exposes a concrete cached-parent authority
gap, now independently reproduced and corrected in
[COMBINED-PARENT-BINDING.md](COMBINED-PARENT-BINDING.md). Do not run another broad
baseline merely because request-ID observation now works. Finish the candidate's
remaining guards, then qualify net cost on identical complete streams and fixed
cache/admission before considering an actual runtime comparison.

No new product edit, startup improvement, populated-root/occupied-width result,
current-production-parity or achieved1s/2s claim. All original requirements remain.
