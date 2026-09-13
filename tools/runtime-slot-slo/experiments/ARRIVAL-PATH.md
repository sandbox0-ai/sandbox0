# D-ARRIVAL-PATH: existing arrival proof, carrier recovery and service logs

2026-09-12. Read-only diagnosis; no product change or new startup acceptance.
Evidence root: `/tmp/sandbox0-arrival-path.acMT7K`.

## Decision

Keep the original layout/control and all cold-start requirements. Machine and
storage remain separate hypotheses; neither another RSA cache nor a generic
kernel/TLS trace is justified by the current evidence. The unresolved storage
boundary is network versus OSS service processing, bound to the exact request.

The preceding full-Reader run is useful component evidence, but its proposed
next kernel-arrival experiment overlooked already sealed history. Correct that
plan rather than repeating the work. No new full-Reader or object-range replay
was run here. No performance improvement is claimed.

## Reused evidence and its limits

Reverified all207indexed files in `/tmp/sandbox0-packet-correlated.lFHESn`
and all19in `/tmp/sandbox0-cold-path-audit.aQuWrO`. Their evidence SHA256s are
`b3d53c901e5bebe2e3d2c35dc1fa5ac26d5bc2b0a4780b564855c021a7de35ff`
and `e137139e43433c7fd6caab24d03e0764be0c027ec56de20196db46918bf88716`.

The September9packet run contains36real claims and `node -v` commands,
1186successfully mapped GETs and4906raw-read coverage proofs, with no capture
drops. Cold GET write-to-first-byte callback averages18.573ms; earliest relevant
kernel packet arrival to callback averages0.156ms. Cached-new values are7.507ms
and0.123ms. Post-arrival Go/TLS work was not the main wait in that run.

All18cold combined-path samples still exceeded2s (2.159–2.989s); all18cached-new
samples were below2s (1.149–1.384s). Those are historical, instrumented samples,
not this turn's result or production-width/occupied-node acceptance. The older
source/artifact inventory differs from today's full-Reader fixture. These
numbers cannot retrospectively supply arrival times for its175ms outlier.
Pre-arrival time still includes request transmission, network and provider work.

D-READER-COST independently reports88.525%HTTP interval coverage and3.296%
exclusive crypto interval coverage in serialized cold Reader time, with81.729%
host CPU idle and no observed throttling/steal/memory pressure. That two-core
component run does not establish production machine adequacy at occupied width.

## Carrier diagnosis, separate from storage

This boot is `174f2ed1-9049-4df3-95ad-3b970215c45b`. The existing test system job
still has two groups, `restart.attempts=0`, job modify index70668. Node and driver
are healthy. PostgreSQL reports two current-boot, unclaimed `fastpath_ready`
carriers. No job update, stop/re-register, database deletion or manual repair was
performed. Nomad's node-update evaluation created new allocations automatically.

The exact prior failed allocations are07dad576… and3fea784a…. Their actual
DriverFailure message is a409registration conflict on
`runtime_slots_cluster_id_allocation_id_key`, followed by `Policy allows no
restarts`. Both database slots belong to boot651e249b…, are terminal and have
neither sandbox nor resource lease bound. Their replacements are9546ce02… and
951b3aa7…, respectively, bound to the current boot. The277failed/74complete
counts are accumulated history, not277new failures.

Current source `RegisterRuntimeSlot` retains one immutable allocation incarnation
with a unique(cluster, allocation)constraint, and the task driver registers
against the current boot. The observed failed re-registration is not evidence
of an OSS timeout; weakening uniqueness or restarting a consumed carrier is not
a remedy. Why replenishment did not complete during the preceding boot remains
a separate refill/lifecycle question. This observation does not establish a
current-production defect or a healthy production-parity acceptance fixture.

## Read-only storage control checks

Aliyun CLI confirms the exact test bucket is in Singapore, Standard/LRS. Its
internal endpoint agrees with the preceding actual-OSS probe configuration;
ECS is also in Singapore. This rules out a simple region/endpoint mismatch for
that configured probe, not packet-path or individual-object-tier problems.

`GetBucketLogging` returns `LoggingEnabled:null`: no bucket log shipping.
`ListCollectionPolicies`, filtered by this exact bucket, product `oss` and
`access_log`, returns0of0rules. No logging configuration or cloud resource was
created. After correcting CLI path/host parameters and a duplicated project
endpoint (without bypassing TLS), the exact legacy Singapore OSS-log project
lookup returns `ProjectNotExist`. All preceding failed attempts remain in the
receipts; they were local CLI/TLS mistakes, not the evidence for absence.

OSS documents `request_id`, `response_time` and `server_cost_time` in its access
logs. These are the relevant fields for service correlation; client first-byte
time alone is not server processing time. See the official
[log field definitions](https://help.aliyun.com/zh/sls/oss) and
[exact-instance policy query](https://help.aliyun.com/en/sls/developer-reference/api-sls-2020-12-30-listcollectionpolicies).
The [GetBucketLogging contract](https://www.alibabacloud.com/help/en/oss/developer-reference/getbucketlogging)
is distinct from SLS real-time collection, so both were checked separately.

## Validation and remaining work

Before/after checks retain exact original binaries/configuration/PIDs, job index,
source artifacts and sandbox row counts2072/2072/263. No active writer/runtime,
64NBD devices detached; two unclaimed carriers remain ready before shutdown.
No new claim, guest command, RootFS object read/write, import or runtime rollout.
Existing1353product files and164trace files revalidate; no local e2e was run.

Local harness issues are retained: a remote-call basename validation error,
an unused/unavailable REXML require before bucket API calls, and legacy SLS CLI
path/host parameter validation and duplicated-project endpoint failures. None
is a storage latency sample.
Intentional SSH master close returns255; lifecycle shutdown is checked
independently in cloud receipts. The remote-test skill uses only workspace
Makefile start/stop and preserves `/data`; old Kind/bootstrap instructions are
not used. Final cloud read at2026-09-12T03:58:44.867428750Z reports
Stopped/StopCharging,2CPU/8GiB and no public IP. See the sealed evidence for
ownership and source checks.

Next establish request-ID-bound service evidence through an explicitly scoped,
bounded test logging setup before scheduling a correlation-only runtime run.
Do not repeat kernel capture or claim legacy logs were retroactively enabled.
All current-version, empty-node, cached-new, populated-large-root, real `node -v`,
occupied production-width and durable-lifecycle gates remain open. Generic warm
carriers, claim-time RootFS binding, crypto/proofs and10s timeout are unchanged.
