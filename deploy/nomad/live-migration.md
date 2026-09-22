# System-owned live migration

This document specifies the internal execution-state migration path. Migration
is a system lifecycle operation, selected by manager during planned node
evacuation. It is not a sandbox API, SDK method, CLI command, template option,
or user-selected scheduling constraint. A user cannot request migration,
select its destination, or download an execution image.

## Implementation status

### Current performance evidence (2026-09-22 UTC)

Point-to-point execution-image transfer uses pinned mutual TLS and overlaps
with encrypted regional publication. Regional durability and exact source
fencing still precede target execution. RootFS already reuses unchanged COW
blocks; execution memory is a separate checkpoint, not a full disk copy.
The source seals only changed RootFS blocks and the corresponding mapping into
an immutable regional generation. The destination attaches that generation,
reuses local cache entries and reads missing blocks on demand. COW does not
make anonymous process memory incremental: the destination still needs the
memory and runtime state required by the stock-runsc execution checkpoint.
The tentative peer cache avoids sending unchanged checkpoint chunks twice
within one migration; it is not cross-migration memory dirty-page tracking.

The timer runs from the source drain trigger to the first authenticated
command response through the regional ingress from the committed destination
generation. These fixtures use `cluster-gateway` as the single-cluster regional
entry point; they do not run a separate `regional-gateway` process.
For a preserved Python process holding 128 MiB of random memory, the
manifest-cache and resource-comparison cohorts measured:

| Public memory tier / derived CPU | Samples | Target usable, seconds | Median | At or below 3 seconds |
| --- | --- | --- | --- | --- |
| 512 MiB / 150 millicores | 6 | 3.428–3.710 | 3.629 | 0 / 6 |
| 1 GiB / 250 millicores | 4 | 2.815–2.945 | 2.855 | 4 / 4 |
| 2 GiB / 500 millicores | 6 | 2.234–2.572 | 2.287 | 6 / 6 |

The 512-MiB row is the September 22 manifest-cache cohort described below.
It and the 1-GiB row include the bounded-prefetch-acknowledgement fix; the
2-GiB cohort predates that fix. The resource rows were measured in separate
acceptance cohorts, not a controlled comparison of resource tiers or fixes.

The subsequent integrated growing regional-upload cohort at 150 millicores /
512 MiB took 3.575–3.924 seconds across six moves (median 3.736 seconds), with
zero samples at or below three seconds. Early point-to-point receipt is now
integrated: a subsequent six-move cohort with the same resource limits and
128-MiB random-memory workload took 3.442–3.621 seconds (median 3.505), also
with zero samples at or below three seconds. All six moves used early receipt
and final repair. The observed median is 231 ms lower than the growing-upload
cohort; these small sequential cohorts are not production percentiles or a
controlled attribution of every latency difference.

These cohorts used the unchanged 4-GiB-per-CPU policy, 100-ms CPU quota period
and original `shmem_enabled=never` host setting. All complete-memory, process,
file and physical-cleanup checks passed. These are serial migrations with warm
carriers/caches, not production percentiles or a guarantee for arbitrary active
memory, dirty disks, cold caches or concurrent migrations. Source cleanup is
measured separately and may finish after the destination becomes usable.

A separate host-setting experiment enabled `shmem_enabled=advise` on both
temporary nodes without changing CPU/memory quotas or binaries. Six moves took
3.019–3.464 seconds (median 3.294), with zero at or below three seconds. This
experiment is separate from the original `never` baseline and does not change
the production default. Its stock-runsc restore median was 796 ms, versus
1002 ms in the preceding baseline cohort; the small sequential samples do not
isolate all sources of variation.

A later verification-overlap revision reduced observed final repair from a
187-ms to a 109-ms median under the `never` setting. Its full six-move median
was nevertheless 3.678 seconds; one 7.146-second move included a 3.768-second
regional-publication interval. The same revision with `advise` took
3.145–3.433 seconds (median 3.299), again zero of six at or below three seconds.
All state and cleanup checks passed, and the host setting was restored. These
results establish a shorter repair stage, not an end-to-end SLO improvement.

A subsequent final-chunk upload overlap candidate moved missing execution
chunks into the RootFS seal interval. Six `never` moves took 3.550–3.693 seconds
(median 3.602); six separate `advise` moves took 3.216–4.255 seconds (median
3.371). Neither cohort had a sample at or below three seconds. Final publication
medians fell to 94 and 87 ms, but sealing also took longer, so a faster total
has not been demonstrated. All twelve state/cleanup checks passed and both
host settings were restored.

The final tested handoff-retry candidate preserved all state across six default
`never` moves at 150 millicores / 512 MiB with 128 MiB random memory: 3.403–3.640
seconds, median 3.518. All six reused the prefetched image. The separate `advise`
experiment had a 3.944-second median and 4–5-second tails; the host setting was
restored. The release keeps the original CPU quota and `never` setting. The
observed 3.x-second default cohort is a workload-specific result, not a hard
latency bound for arbitrary memory, dirty RootFS, cache state or concurrency.

The default 150-millicore/512-MiB tier remains above the requested three-second
total for this workload. A separate 64-MiB random-memory cohort at that same
default quota took 2.847–3.028 seconds (median 2.976), with three of four samples
at or below three seconds. Smaller memory therefore changes the result, but
still does not establish a stable three-second bound. The resource cohorts
establish measured conditions, not a change to defaults or proof that
250 millicores is the minimum.
Detailed historical samples below retain their original workload, configuration
and measurement boundaries. Production rollout requires independent validation
of the deployed immutable bundle, node admission and durable cleanup.

The full Nomad/ctld/block-COW path completed five unattended consecutive moves
of the **same running sandbox** between two independent Linux hosts on
2026-09-20 UTC: A → B → A → B → A → B. Generation advanced from 1 through 6.
The unmodified procd and Python workload preserved PID, an in-memory token and
advancing counter, an open unlinked file and its offset, shared mmap contents,
runtime `/tmp` data and persistent RootFS files. Each move released the source
lease and both staging reservations. The official stock runsc bundle was
`release-20260914.0`, with Nomad 1.11.3 and encrypted regional S3-compatible
objects. Trigger-to-terminal-cleanup times were 21.34, 19.03, 21.23, 19.24 and
21.29 seconds. These are small-workload observations, not a latency guarantee;
requests encountered temporary errors during the consistency cut and handover.
The largest gaps between sampled successful responses were 12.3–16.4 seconds;
two-second polling and request timeouts do not establish exact downtime.

A sixth move, B → A, deliberately restarted manager after capture entered the
`publishing` phase and before the publication receipt existed. Recovery reused
the original transaction and completed generation 7, preserving the same
process and state; source capacity and both staging reservations were released.
The measured trigger-to-cleanup interval was 23.29 seconds under this injection.
The first five execution-image bindings were then observed with automatic GC
complete. This test covers one manager restart boundary, not source loss or
all possible interruption points.

A seventh move, A → B, also retained an established guest-loopback TCP pair: an
echo exchange through the same Python socket objects succeeded after generation
8 committed. The other process, FD, mmap and filesystem checks still passed.
This is internal loopback state continuity; it does not cover ctld proxy
connections, SSH/streaming sessions or external TCP continuity.

An eighth move, B → A, interrupted the isolated regional object store during
publication. The source stayed held, no target execution was published during
the outage, and automatic retries completed generation 9 after storage
recovered. The same process, open files, mmap, tmpfs and loopback TCP pair
survived; source capacity and staging were released. This run took 127.77
seconds overall. The planned 15-second injection lasted **108.06 seconds**:
stopping the test's transient MinIO unit unloaded it, so the recovery helper's
`systemctl start` failed. The original storage service definition was restored
without changing migration state or replaying an execution image. This fixture
error limits the timing claim; the preserved workload and resumed transaction
were checked after actual storage recovery.

A separate fault test killed the exact ctld primary after durable cut intent but
before XFS freeze and the synced WAL sequence. A second injection killed the
primary after successful XFS freeze but before the sequence was durable. In
both runs the standby identified owner loss and automatically completed the
capture-failure protocol: no image was published, no target was restored, and
both leases and staging reservations were released, and the sandbox became paused at generation 2. The last committed
RootFS head was unchanged. Recovery completed about five to six seconds after
each injection, including physical thaw and cleanup for the frozen filesystem.
This validates failure containment, not process continuity; dirty state that had
never committed was not promoted. The injection used a test-only Go overlay,
absent from the production binary.

A third primary-loss injection occurred after the immutable RootFS cut was
durable but before thaw. This time recovery retained the valid cut and completed
A → B migration, rather than entering capture failure. The workload contained
128 MiB of random process memory; its SHA-256, original PID, open unlinked FD,
mmap and tmpfs all survived. Source finalization and both staging releases were
observed by 25.39 seconds after drain. The first allowed-egress check timed out
because the test-only destination address lacked an IPIP return route from A to
B; a direct B-host request failed identically. After repairing that fixture
route, the preserved process could reach the allowed port and was still denied
the other port under its retained `block-all` policy. No migration state or
workload process was repaired. This run establishes cut recovery and retained
policy behavior, but its initial network fixture failure precludes a clean
end-to-end network timing claim.

With the fixture route stable and both nodes back on the production ctld
binary, the same 128 MiB workload completed an unattended B → A return. The
original process state and the explicit allowed/denied egress behavior both
survived. Terminal cleanup was observed at 24.25 seconds; final workload and
network verification completed at 26.33 seconds. The test covered one migration
at a time, one 512 MiB lease and one explicit TCP allow/deny policy.

A destination-loss test then killed ctld after runsc was independently observed
running but before the complete-restore observation was journaled. The region
never received a successful restore receipt or committed that runtime. Recovery
classified execution as uncertain, stopped the target, finalized source and
target artifacts, and released both leases and staging reservations. The
sandbox became paused at the consumed generation 4, retaining exactly the
published RootFS cut rather than target dirty state. Recovery converged about
62 seconds after injection (80.79 seconds after drain); this is not a low-latency
failure-recovery guarantee. The execution image was not replayed and the
entrypoint was not restarted to manufacture success. This was another test-only
ctld overlay, removed after evidence collection.

An isolated PostgreSQL outage test stopped the database for 15.12 seconds after
capture entered publication. With the same transaction and no image replay,
manager resumed after database recovery and completed A → B, preserving the
original Python process, memory, FD, mmap, tmpfs and persistent files. Both
staging reservations and the source lease were released. Drain-to-verification
was 98.05 seconds, including approximately 61 seconds waiting for the existing
node cooldown from the preceding failure test. This covers a short outage at
one publication boundary, not arbitrary database loss or all lease expirations.

After the acceptance runs, all test sandboxes were deleted through the public
API. The regional database reported zero live sandboxes, active resource leases,
nonterminal migrations and published execution images awaiting GC. Fault hooks
were confined to remote build overlays and were removed from both nodes.

Previously, `release-20260817.0` passed individual fresh A-to-B and B-to-A moves,
but a second move of the same workload stalled procd HTTP in both directions.
The guest remained alive while its existing listener stopped serving requests.
The newer official release includes the upstream
[epoll ready-list restore repair](https://github.com/google/gvisor/pull/14249),
and the same failure did not recur in the five-move test. This supports the
upstream defect as the cause; it is not a bisect of every intervening change.
Older and unqualified release evidence is now rejected before source
preparation, without making historical recovery metadata unreadable.

The feature remains experimental pending the remaining failure, network and
capacity acceptance gates below. These successful moves do not establish
uninterrupted TCP sessions, large-memory performance, concurrent migration
capacity or recovery of volatile state from a dead source. Migration is
system-owned; users cannot trigger it or choose its destination.

### End-to-end latency optimization samples (2026-09-21 UTC)

A new isolated two-host fixture uses Nomad 1.11.3, stock runsc
`release-20260914.0`, real PostgreSQL, disk-backed MinIO with application-layer
encryption, block-COW RootFS and four ready standard carriers on each host.
Both hosts are four-vCPU/8-GiB ECS instances in the same zone. These samples use
one Python REPL workload with a 512-MiB memory lease and the derived default
150-millicore CPU lease, without an additional large random-memory allocation. The control services and object store share
host A; this is not production capacity or concurrency acceptance.

The timer starts immediately before committing the source drain fence and ends
only after the target runtime generation commits and an authenticated regional
gateway REPL command returns the preserved state. PostgreSQL observation polls
at 20 ms plus query time; the complete request round trip is included. Source
cleanup is measured separately. The same PID, in-memory token, increasing
counter, open-unlinked descriptor and offset, mmap, `/tmp` and persistent file
contents are checked after every move.

| Change under test | Direction | Target command usable | Final cleanup and verification |
| --- | --- | --- | --- |
| P2P, progress wakeups and zero-page exclusion | B → A | 7.697 s | 9.801 s |
| Revalidated warm CPU evidence; no repeated capture-complete progress | B → A | 3.277 s | 5.023 s |
| Same process returning; 250-ms capture observation window | A → B | 4.023 s | 5.431 s |
| One-second observation window, before transient seal-busy retry | B → A | 4.273 s | 6.276 s |
| Bounded retry includes concurrent source sealing | A → B | 2.455 s | 4.552 s |

The second run recorded 533 migration-port TCP packets with approximately
14.46 MB of summed TCP payload. Source-execution advance reports fell from 178
to two. Publication still completes in the encrypted regional store before
peer transfer; durability and fencing were not relaxed. The 250-ms observation
window expired before the third run's capture completed, so it still incurred
the periodic reconciliation delay. The next run encountered concurrent
driver-owned RootFS sealing and still backed off on temporary node
unavailability. The bounded wait now includes that transient result. These
individual samples do not establish a
three-second SLO, a percentile, or a large-memory bound.

After adding 128 MiB of random memory to that same preserved Python process,
B → A completed with an identical full-memory SHA-256 and all previous state
checks. Target generation commit was observed at 5.539 seconds; the regional
REPL response including a fresh full-memory hash completed at 6.653 seconds.
Final cleanup plus the repeated hash verification completed at 8.352 seconds.
The measured command itself took about 1.114 seconds, so this is a conservative
usability measurement with substantial verification work. Even the earlier
generation-commit observation exceeded three seconds. This larger-memory sample
therefore does not satisfy the requested total-latency target.
A separate repeat of the hash command took 0.957 seconds and consumed about
0.147 CPU-seconds. Its leaf cgroup (`cpu.max = 15000 100000`) was throttled in
all nine measured periods, while the two-CPU parent was not throttled. The
sandbox quota is therefore material to interpreting these timings; four host
vCPUs do not mean the guest received four CPUs. The restore claim log measured
1.107 seconds in the restore/start stage, 0.190 seconds in create and 0.068
seconds in RootFS attachment.

The instrumented repeat retained the same 150-millicore limit and 128-MiB random
allocation. It took 7.546 seconds through the verifying REPL response, with
0.610 seconds in stock checkpoint, 0.219 seconds syncing the execution image,
0.108 seconds sealing RootFS, 0.466 seconds publishing the encrypted image and
0.751 seconds preparing its 149,105,181 bytes over the peer transport. The
successful destination journal log explicitly reported `transport=peer`.
The source's one-second observation window expired about six milliseconds
before RootFS sealing finished, adding another one-second retry backoff. The
window was consequently extended to two seconds; this changes bounded waiting,
not the required durability or execution authority.

The same process returning A → B with the two-second window completed the
verifying command in 6.028 seconds, with generation commit observed at 5.029
seconds and final cleanup plus verification at 7.678 seconds. No migration-pass
errors were recorded. RootFS sealing completed in 0.109 seconds and source
recovery advanced about 20 milliseconds later, without the previous one-second
backoff. Checkpoint took 0.418 seconds, image sync 0.232 seconds, regional image
publication 0.940 seconds, peer preparation 0.464 seconds and restore/start
1.140 seconds. Direction and stage timings differ between samples, so the
total difference cannot be attributed solely to the observation window.

RootFS migration already uses the incremental block-COW generation builder:
unchanged immutable data ranges are reused, while dirty blocks and updated
mapping metadata are published. The 151,171,392-byte peer image in the return
sample is the separate execution checkpoint, including the workload's 128 MiB
of random memory; it is not a full RootFS copy. Disk COW does not provide a
destination baseline for anonymous process memory. The three-second target
therefore still requires improvements beyond avoiding full RootFS transfer.

A subsequent B → A move of the same process used a lightweight preserved-state
REPL command for readiness and performed the full 128-MiB SHA-256 separately
after cleanup. Generation commit was observed at 5.728 seconds, the readiness
response at 6.005 seconds, and final cleanup plus full-state verification at
8.212 seconds. All preserved-state checks passed. This distinction removes
expensive verification work from the readiness metric; it is not an
implementation speedup, and the result still exceeds three seconds.

Drain discovery now also listens for PostgreSQL commit notifications. Migration
090 adds a payload-free notification for entering, changing or removing a drain
fence; unchanged fences do not notify. Each manager's evacuation worker holds
one pool connection, scans after subscribing or reconnecting, and retains its
one-second periodic scan during listener outages. Notifications never carry
execution authority. Unit/race tests cover immediate wakeup, bounded reconnect,
fallback scans and cancellation; isolated PostgreSQL tests verify commit,
rollback and unchanged-fence behavior.

With that change and the same lightweight readiness command, the preserved
128-MiB random-memory process completed A → B in 5.247 seconds and B → A in
5.032 seconds. Reservation was observed at 0.054 and 0.052 seconds respectively,
compared with 0.921 seconds in the preceding B → A sample. Generation commits
were observed at 5.033 and 4.853 seconds; final cleanup plus full-memory hash
verification finished at 8.054 and 8.228 seconds. Both full-state checks passed
with the same 150-millicore/512-MiB lease. These single samples confirm removal
of discovery polling delay, but do not establish a three-second SLO.

`BenchmarkPeerImageMaterialization` measures a 128-MiB pipe transfer including
both chunk-hash checks and destination fsync on one host. Three five-iteration
runs on host A measured 0.956–0.994 seconds per operation. A two-buffer source
read-ahead experiment passed unit and race tests but measured 1.025–1.076 seconds
and added 8 MiB of buffer allocation. That experiment was reverted; the
benchmark remains available. These local disk/pipe results do not characterize
cross-host network throughput or the end-to-end migration SLO.

### Planned publication and concurrent peer-transfer prototype

`runtimecheckpoint.Store.PlanLocal` now describes a retained completed image
without uploading objects. `PublishPlanned` and `WritePlannedPeerImage` can
consume that exact prospective reference concurrently. Publication rereads and
verifies every planned chunk, publishing the immutable regional manifest last.
The plan is explicitly not a durable publication receipt or restore authority.
Tests gate regional uploads while allowing the peer to finish, then exercise
both successful publication and cancellation. Changed source bytes, inventories
and bindings are rejected. Existing checkpoint/node-runtime tests, architecture
checks and checkpoint race tests pass. Planning a warm 128-MiB local image took
70.95–73.20 ms in three ten-iteration runs with `GOMAXPROCS=2`.

A separate transfer-only prototype compared serial publication then peer
transfer with planned parallel publication/transfer. It uses 128 MiB of random
fixture data, the real application-encrypted object store, pinned TLS 1.3 peer
identities, chunk verification and crash-durable destination materialization.
Successful HTTP completion also waits for regional publication; receiving all
peer bytes alone cannot produce success. The timer includes planning, upload,
transfer, verification and destination fsync. It excludes sandbox checkpoint,
regional transaction orchestration and guest restore, and the prototype uses
test-owned authorization rather than the production migration protocol.

With MinIO and the destination sharing host A's 100-GiB ESSD PL0 system disk,
successive serial and parallel samples both approached 2.05 seconds. A third
isolated four-vCPU/8-GiB host with its own 100-GiB ESSD PL0 disk then hosted only
the encrypted object-store backend for the prototype. All three hosts remain
in the test VPC with automatic release; the live Nomad fixture still uses its
original backend. With independent storage but back-to-back transfers, both
modes approached 1.00 second, consistent with a sustained destination-write
throughput limit.

Spacing otherwise identical transfers by three seconds separated single-run
latency from that sustained load:

| Transfer-only mode | Three samples | Median |
| --- | --- | --- |
| Serial publication then peer | 0.832, 0.852, 0.814 s | 0.832 s |
| Plan then concurrent publication/peer | 0.513, 0.555, 0.538 s | 0.538 s |

The 0.294-second median difference includes the additional planning scan. It
supports further protocol integration, not a three-second end-to-end claim.
The live coordinator still requires committed publication before target
preparation. Prospective target authorization and interrupted-preparation
cleanup must be integrated before enabling overlap in actual sandbox migration.

Source ctld now supports reading the planned image while its publication
worker uploads the same checked chunks. A speculative read explicitly selects
the planned stream and must match the publication request digest and reserved
destination certificate. The worker retains its exclusive source slot until
all admitted readers exit. Closing admission and admitting readers use the same
mutex, so cleanup cannot race past a late reader. Failed publication cancels
the readers and expires blocked HTTP write deadlines before releasing custody.
After successful publication, authorized cleanup can preempt a stalled reader
without discarding the durable publication receipt. A stream completing early
is only speculative cache data; it does not prove regional publication.

The peer listener also closes handler admission and joins its active handlers
before ctld releases the journal and runtime. Closing sockets alone does not
provide that ownership guarantee. Node-runtime tests cover concurrent transfer
with a gated regional upload, absent publication authority after early peer
completion, changed identity/operation rejection, failed-upload interruption,
successful-upload custody retention, cleanup preemption and joined shutdown.
The speculative transfer integration is installed in the isolated performance
fixture and has completed the two-node measurements described below. It adds
a planning scan when peer publication is enabled; that cost must be included
in the eventual complete migration measurements.

The authenticated node channel now exposes `migration_publication_plan` as an
optional capability. It starts or joins the same bounded source upload worker
and returns a separate prospective-reference response as soon as planning is
complete. `migration_publish` still waits for the durable publication receipt.
Repeated observers share the upload; canceling one observer cannot interrupt it.
A late planned HTTP read can use the exact completed publication after the
worker exits, with the original destination certificate and request checks.
Protocol tests reject using a plan as a publication response, mixed response
payloads, another source binding and unsupported or stale-boot node channels.

Migration 091 adds immutable target-prefetch authorization to PostgreSQL. Its
request binds the prospective source reference and peer endpoint to the exact
existing destination staging reservation. Authorization requires a previously
committed publication request, both staging receipts, current destination
capacity and no staging release or already-started target preparation. It does
not create a publication or target-preparation receipt, move the writer, or
advance runtime generation. The transfer projection reads this authorization
after manager restart. Isolated PostgreSQL tests cover concurrent exact retries,
changed plans, expired target admission, immutability and the unchanged durable
publication gate.

The coordinator now dispatches this optional target cache fill concurrently with
ordinary source publication. Unsupported peers and failed cache fills retain the
ordinary image-preparation path. Only the real publication receipt can advance
the regional transaction. Target journal version 14 owns partial and completed
cache directories within the existing staging reservation; both ctld instances
must understand that version before enabling the path. Cache acknowledgements
are separate from preparation receipts and grant no execution authority.

Ordinary authorized preparation verifies cached files against the published
regional manifest before promoting the directory. Partial, corrupt or interrupted
promotion falls back to retrieving the exact published image. Cleanup cancels
and joins an active fill, fsyncs physical cache absence, and then records the
cache tombstone before releasing staging. Tests cover restart reuse, quota and
identity rejection, interrupted promotion, cleanup races, lost fill responses,
and a target receiving the complete image before gated regional publication.
Isolated PostgreSQL integration tests also exercise real coordinator overlap,
unsupported peers, failed peer transfer and failed regional publication, with
the publication gate unchanged. These tests do not establish an end-to-end
latency improvement in the live fixture.

The first mixed-version rollout attempt aborted before image transfer. Although
the driver observed OCI `stopped` after saving, ctld subsequently observed
`running` and correctly rejected sealing the execution/RootFS pair. The installed
driver already included the OCI exit polling loop; a missing polling fix was
ruled out by inspecting the binary. Stock runsc can report a stopped guest while
its host sentry is still exiting, then fall back to host PID existence if the
control RPC fails. The adapter now pins the running source with a Linux pidfd
before checkpoint, rechecks its identity, and waits for that process to exit
before accepting OCI stopped. This does not kill or resume the source. Tests
cover early OCI stopped, changed PID and cancellation retaining image custody;
the complete gvisorcli race suite passes. Two subsequent real migrations with
this change preserved the same process and all checked state. The failed
attempt is not a performance result. The relevant upstream behavior is in
[CheckStopped](https://github.com/google/gvisor/blob/release-20260914.0/runsc/container/container.go#L2274)
and its fallback
[IsRunning](https://github.com/google/gvisor/blob/release-20260914.0/runsc/sandbox/sandbox.go#L2063).

The previous REPL had a one-hour lifetime and was no longer present before the
mixed-version attempt, so that attempt used a replacement context. After its
failed capture cleanup, a fresh sandbox was claimed for the subsequent pair.
It retained the same 150-millicore CPU limit, 512-MiB memory limit and 128-MiB
random-memory workload. Neither comparison silently increases guest resources.

With speculative transfer and physical-exit confirmation installed, B → A took
5.257 seconds from the drain trigger to a lightweight authenticated preserved
REPL command; generation commit was observed at 4.957 seconds. RootFS sealing
took 0.102 seconds, checkpoint 0.468 seconds and image sync 0.197 seconds.
Regional publication took 0.535 seconds while target prefetch took 1.188 seconds
for 149,190,423 bytes. Subsequent authorized preparation reported
`transport=prefetch` and took 0.166 seconds; target claim took 1.641 seconds,
including 1.207 seconds in restore/start. Final cleanup and a separate complete
memory-hash check finished at 7.509 seconds, with no migration-pass errors.

The same process returned A → B in 4.946 seconds, with generation commit
observed at 4.751 seconds and final cleanup plus the full-memory check at
7.794 seconds. Both directions preserved PID, counter continuity, the full
128-MiB SHA-256, an unlinked open descriptor and its offset, persistent and
runtime-only files, and an mmap. These samples confirm end-to-end correctness
of overlap and cache promotion; they do not meet the three-second total target
or establish a consistent latency improvement over the earlier serial path.
Storage and peer transfer share fixture resources, and the longer overlapping
prefetch must remain in the measured critical path rather than being excluded.

An isolated restore-loading experiment used the same stock runsc, 128 MiB of
random process memory and a verified 512-MiB cgroup memory limit. At 150
millicores, four foreground restores took 0.900–0.920 seconds and four
`--background` restores took 0.897–0.915 seconds. Each incurred nine CPU
throttling periods; background loading did not materially reduce this stage.
Complete memory hashes and preserved process state passed in both modes.

A separate, explicitly changed-resource comparison at 1,000 millicores took
0.098–0.144 seconds for three foreground restores and 0.108–0.145 seconds for
three background restores. This identifies CPU quota as a substantial restore
cost in this fixture, but is not an end-to-end migration result or evidence for
the three-second target at 150 millicores. The actual migration sandbox retains
its original resource limit, and production restore still uses foreground
loading. Reproduce the isolated Linux experiment with
`SANDBOX0_RUN_RESTORE_LOADING_PROBE=1` and
`SANDBOX0_RESTORE_PROBE_CPU_MILLICORES=150` (or explicitly `1000`), running
`TestPrivilegedRestoreLoadingLatency` in `pkg/gvisorcli`. It owns separate
cgroups and runtime roots and requires root and stock runsc.

Local image verification now reuses the bounded publication scanner, checking
all chunk hashes with at most four workers. Exact file inventory, the regional
manifest binding and the full restore-intent recheck remain mandatory. This
raises the maximum chunk-buffer allocation per verification from 8 to 32 MiB;
it does not increase the guest CPU or memory lease. On the same host with
`GOMAXPROCS=2`, three five-iteration `BenchmarkVerifyLocal` runs for 128 MiB
improved from 126.96–127.85 ms to 69.24–70.33 ms. Checkpoint race tests passed,
including corruption in every parallel worker's chunk and the short final
chunk, cancellation, changed inventories and planned-image binding checks.
The affected node-runtime migration tests also passed.

After deploying this verification change only to the temporary fixture, the
same 150-millicore workload completed another B → A → B pair, advancing runtime
generation 3 → 4 → 5. Trigger-to-command-ready times were 5.484 and 4.584 seconds;
generation commit was observed at 5.279 and 4.350 seconds. Image preparation
took 83.65 and 73.71 ms, versus 165.84 and 149.83 ms in the preceding pair.
The images contained 151,608,948 and 151,433,309 bytes. Both moves preserved the
full random-memory hash, PID, increasing counter, open unlinked descriptor and
offset, persistent file, runtime-only file and mmap; source capacity and both
staging reservations were released with no migration-pass errors. Cleanup plus
the separate full-memory check finished at 7.876 and 7.382 seconds. Regional
publication varied from 1.425 to 0.654 seconds and restore/start from 1.160 to
1.066 seconds. The stage-level verification gain is established; this pair
does not establish a consistent total-latency gain or meet the three-second
target.

An additional end-to-end CPU comparison used a separate sandbox with a real
1,000-millicore lease, the same 512-MiB memory limit and 128 MiB of random
process memory. The isolated manager's memory-per-CPU policy was temporarily
set to `512Mi` only while claiming this sandbox through the normal memory
override API, then restored byte-for-byte before any timed migration. The
original sandbox retained its 150-millicore lease and passed another full
preserved-state check. It was subsequently paused, with its lease released,
so reverse drains would migrate only the comparison workload. Source and
destination cgroup reads confirmed `cpu.max = 100000 100000` and
`memory.max = 536870912` for the new workload. This is a changed-resource
comparison, not an improvement to the default 150-millicore result.

The same one-core process then completed four alternating migrations:

| Direction | Trigger to command-ready | Cleanup plus full-memory verification |
| --- | --- | --- |
| A → B | 2.979 s | 4.764 s |
| B → A | 3.589 s | 5.131 s |
| A → B | 3.072 s | 5.395 s |
| B → A | 3.594 s | 5.071 s |

All four preserved the same PID, random-memory hash, counter continuity,
unlinked open descriptor and offset, persistent and runtime-only files, and
mmap. All source leases and staging reservations were released, with no
migration-pass errors. Checkpoint took 75.6–98.9 ms and restore/start took
147.1–210.9 ms. Peer prefetch was 522.9–555.3 ms when targeting B and
1,209.9–1,211.3 ms when targeting A. The final B → A image was 151,400,528
bytes; host A's physical disk counters increased by 311,025,664 written bytes
and 1,142 I/O milliseconds between triggering the drain and command readiness.
These are whole-host counters, so they do not isolate per-service I/O. Their
approximately doubled byte count is consistent with the target and regional
MinIO writing the image on the same disk. Independent storage is the next
experiment needed to test that contention hypothesis. One sub-three-second
sample does not establish a three-second SLO, even with the larger CPU lease.

The live test region was subsequently moved to the independent storage host.
The completed synthetic workload was verified and paused, all compute leases
were released, and the test manager and both nodes' ctld instances were stopped
before copying. All 78 remaining objects (33,735,954 ciphertext bytes) were
copied conditionally into a separate test bucket and individually SHA-256
verified. Source inventory stability and exact destination inventory were also
checked. Application encryption keys and settings were retained; only the test
endpoint, bucket and storage credentials changed. The original MinIO service
was then stopped, retaining its data, so successful new claims and migrations
could not silently use it. No production configuration was changed.

A fresh, otherwise equivalent one-core/512-MiB workload completed A → B in
3.023 seconds and B → A in 3.068 seconds, with full preserved-state checks and
physical cleanup passing in both directions. Regional publication took 599.2
and 562.2 ms; peer prefetch took 507.0 and 492.2 ms. The former slow direction's
prefetch therefore improved substantially from approximately 1.21 seconds,
consistent with removing shared-disk contention. Full migration still exceeded
three seconds, including preparation, restore, regional commit and the
authenticated preserved-context command. Cleanup plus full-memory verification
finished at 5.061 and 5.143 seconds. Neither run reported a migration-pass error.

The observer was then changed from spawning `psql` on every poll to one
persistent pgx connection. Twenty isolated queries measured a median of 5.886
ms with a process per query and 0.178 ms with the persistent connection. This
is a measurement change, not a runtime optimization. The same workload moved
A → B in 3.002188 seconds and B → A in 2.946016 seconds with the persistent
observer, preserving the full state and releasing source and staging leases.
The first result still exceeds three seconds; neither pair establishes a
stable sub-three-second guarantee. The readiness probe excludes the full
128-MiB memory hash, which remains required in post-readiness verification.

Destination restore preparation now overlaps image-custody verification with
attachment of the exact authorized RootFS writer. Request and CPU validation
still precede both operations; `runsc create` and restore wait for both to
succeed. A failed operation cancels its peer and joins both calls before the
driver enters uncertain migration custody. The writer, image and carrier stay
owned by the regional failure protocol, including a late successful peer
response. Ordinary non-migration claims retain their existing attach path.
The complete independent driver module passed its tests; migration-restore
race tests also passed, including both completion orders and cancellation with
a delayed successful response in either direction.

With the same one-core workload, independent storage and persistent observer,
eight alternating moves measured 2.975087, 3.117464, 2.989010, 3.023820,
3.049723, 3.171024, 3.148818 and 3.126458 seconds (starting A → B). Full memory,
PID, counter, open-unlinked descriptor, durable file, tmpfs and mmap checks
passed on every move, as did staging release and physical source cleanup.
No migration-pass errors were recorded. Target claim durations ranged from
337.2 to 418.4 ms. Only two of these eight complete migrations met three
seconds; the stage optimization does not establish the requested latency
bound, and the one-core results do not replace the separate 150-millicore
baseline or cold-cache/concurrent acceptance.

A subsequent repeated-migration comparison alternated the serial and parallel
driver binaries on empty destination nodes. Early immediate-after-restart
samples included CPU-preflight socket-unavailable retries and reached 5.464
seconds; those remain rollout-readiness evidence, not clean preparation-cost
comparisons. After explicitly observing four warm control sockets for five
consecutive seconds, the final eight comparisons had no migration-pass errors:

| Source generation | Preparation | Trigger to usable (seconds) | Target claim (ms) |
| --- | --- | ---: | ---: |
| 21 | Serial | 3.632092 | 447.583 |
| 22 | Serial | 3.515025 | 406.342 |
| 23 | Parallel | 3.579752 | 308.116 |
| 24 | Parallel | 3.771630 | 409.715 |
| 25 | Serial | 3.618464 | 381.211 |
| 26 | Serial | 3.689449 | 430.443 |
| 27 | Parallel | 3.787623 | 385.108 |
| 28 | Parallel | 3.784553 | 485.509 |

All preserved-state and cleanup checks passed. Median target claim was 418.393
ms serial and 397.412 ms parallel, but complete-migration medians were 3.625278
and 3.778091 seconds respectively. These small, sequential samples do not prove
an end-to-end benefit; latency increased over repeated moves in both modes.
Image prefetch remained approximately 460–488 ms, regional publication 543–633
ms, and source image sync approximately 203–216 ms in these final samples.
Stored migration request sizes were effectively unchanged from generation 10
through 28, ruling out growing individual authority payloads in this sample.
Idle two-second samples showed ctld using approximately 20% of one CPU on each
node. Additional profiling is required before attributing the remaining drift
to a specific control-plane or node-runtime path.

Profiling the retained-history node showed repeated full journal validation
inside registration-abort and adoption scans. Code inspection found the same
full-history validation on synchronous staging-pool admission paths. These
scans now retain bounded SHA-256 fingerprints of fully validated records that
cannot contribute work or admission usage without changing their bytes. Each
of the three scan caches holds at most 256 fingerprints, retains no custody
payloads, and starts empty after restart. Changed bytes, eviction, candidate
records and actual execution still use full validation. Young unacknowledged
registrations are never excluded because time alone can make them eligible.
Staging exclusions require no image custody, no retained-count admission and
no exclusive reservation; active reservations remain checked in the same Bolt
transaction. No journal format, writer fencing or regional durability boundary
changed.

The complete node-runtime package passed in 12.811 seconds; targeted race tests
covering registration, scan exclusions, adoption, staging, image preparation
and prefetch passed in 11.584 seconds. Tests cover grace-period expiry without a
write, changed corrupt records, restored unacknowledged receipts, bounded
concurrent eviction, and a cached empty slot becoming an exclusive reservation.
The existing competing-reservation tests also pass with the new admission scan.

After updating only destination B, the same one-core workload migrated in
3.560441 seconds. With both ctld nodes updated, five following alternating moves
measured 2.784806, 2.440921, 2.327040, 2.381981 and 2.276783 seconds. The final
four have a 2.354511-second median, with full preserved-state checks and physical
cleanup passing and no migration-pass errors. This uses 1 CPU, 512 MiB of
memory limit, 128 MiB of random process memory, ready carriers, warm caches and
independent regional object storage. It is evidence for that bounded serial
workload, not a cold-cache, high-concurrency or arbitrary-memory guarantee.

The one-core process was then fully verified and normally paused. A fresh
otherwise equivalent workload used the unchanged default CPU policy: 150
millicores for a 512-MiB limit. Its six alternating moves measured 3.753032,
4.127922, 4.231964, 4.240846, 4.011404 and 4.064627 seconds. All state and cleanup
checks passed; the third move recorded one retried source-execution transaction
conflict. The default profile therefore still fails the three-second target.
Median checkpoint time was 608.5 ms and restore execution 1,205.7 ms, compared
with 93.8 and 175.8 ms in the final four one-core samples. Prefetch remained
approximately 492 ms and regional publication 568 ms in the default samples;
process capture and restore under the CPU quota are the principal measured
remaining difference. These experiments do not change the default CPU policy.

An isolated follow-up kept the 150-millicore quota ratio and 512-MiB memory
limit while comparing 100-ms, 20-ms and 10-ms CPU bandwidth periods. The
foreground restore probe used stock runsc, 128 MiB of random memory, its own
cgroups and runtime directories on the empty destination host. Nine serial
runs alternated periods in the order 100, 20, 10, 10, 20, 100, 100, 20, 10 ms.
Every run checked the actual source and destination `cpu.max`, destination
`memory.max`, process token/PID, open-unlinked file offset, temporary data,
CPU feature digest and complete restored memory hash.

| CPU period | Checkpoint median | Restore median | Restore range |
| --- | --- | --- | --- |
| 100 ms | 502.2 ms | 994.1 ms | 901.3–1,005.8 ms |
| 20 ms | 534.4 ms | 999.7 ms | 976.9–1,221.4 ms |
| 10 ms | 534.0 ms | 843.2 ms | 829.9–980.4 ms |

The 10-ms samples suggest a modest restore improvement, but capture did not
improve consistently and three samples per period do not establish a stable
tail-latency benefit. These are primitive timings, not trigger-to-command-ready
migrations; they exclude regional publication, peer transfer and handoff.
This result does not close the default profile's three-second gap. The probe
accepts an explicit `SANDBOX0_RESTORE_PROBE_CPU_PERIOD_US` of 100000, 20000 or
10000 and preserves the quota ratio. Product leases still use the canonical
100000-us period: changing it also affects exact lease validation and the
supported minimum CPU allocation, so no production contract was changed.

A second isolated comparison enabled the stock restore `--direct` option,
which bypasses the host page cache for the pages file. Six alternating runs
used the unchanged 150-millicore/100-ms quota and the same complete state
checks. Buffered restore took 822.3, 994.7 and 901.0 ms; direct restore took
1,122.0, 1,115.8 and 1,133.0 ms. All six passed, but direct I/O was slower in
this ext4 host-filesystem primitive test. It is not evidence for the separate
project-quota XFS migration staging mount, and does not justify enabling the
option in the production adapter. `SANDBOX0_RESTORE_PROBE_DIRECT=1` exposes
this experiment explicitly; its default remains buffered foreground restore.
After both comparisons, no probe runtime directories, cgroups or mounts
remained, and both destination ctld HA readiness probes passed.

External, cgroup-filtered `perf` sampling then localized the remaining restore
CPU work without enabling runsc profiling or changing its seccomp policy. In
three isolated restores, 104 of 148 samples included the asynchronous pages
reader, with kernel stacks in shmem page faults, allocation, clearing and
`pread` copying. State decoding appeared in 15 samples and Go GC in 12; these
categories are not exclusive and this small sample is diagnostic, not an exact
cost breakdown. An initial PID-only capture produced metadata without samples
and was rejected. The optional `SANDBOX0_RESTORE_PROBE_PROFILE_DIR` probe now
requires a private output directory and nonempty decoded samples, and joins
the recorder before cleanup. Profiled timings are excluded from SLO evidence.

The default perf build-ID cache also created a hard link to the watched sentry
executable on the empty destination. Its hash still matched the source, but
the inode metadata change correctly invalidated the driver's CPU-launch
monitor. The first subsequent full migration was rejected during CPU preflight
and automatically aborted after about 121 seconds, before source preparation or
capture; the source remained at generation 7. That failed attempt is retained
and is not a latency sample. Only the empty destination's driver was restarted
after verifying the matching runsc/sentry hashes, followed by five consecutive
seconds of four warm control sockets. The profiling probe now passes
`--no-buildid-cache`; integrity checks were not relaxed to accommodate sampling.
A subsequent cgroup-filtered capture with that option preserved all watched
runsc/sidecar device, inode, ctime and hard-link counts, produced real samples,
and passed the complete restored-state check.

The empty test host used Linux `6.6.102-7.alnx4.x86_64` with shared-memory THP
disabled (`shmem_enabled=never`), even though runsc enables application huge
pages by default. Six unprofiled foreground restores alternated `never` and
`advise`, keeping the 150-millicore/100-ms CPU quota, 512-MiB memory limit,
128-MiB random workload, anonymous-memory THP settings and `defrag=defer`
unchanged. The host setting was restored to `never` in a `finally` block.

| Shared-memory THP | Checkpoint median | Restore median | Restore CPU median | Target shmem THP |
| --- | --- | --- | --- | --- |
| `never` | 504.4 ms | 909.5 ms | 134.5 ms | 0 |
| `advise` | 408.2 ms | 526.1 ms | 81.8 ms | 132 MiB |

All six runs preserved the complete process/memory evidence. The three
`advise` restore samples were 526.1, 522.9 and 710.7 ms, so this is not a
tail-latency guarantee. The measured cgroup `shmem_thp` confirms that the
optimization actually changed backing-page allocation. The kernel describes
the allocation and memory-usage tradeoffs in its
[transparent hugepage documentation](https://www.kernel.org/doc/html/latest/admin-guide/mm/transhuge.html),
and the pinned stock runsc
[flag definition](https://github.com/google/gvisor/blob/release-20260914.0/runsc/config/flags.go)
requires host shared-memory THP support for application huge pages. These
primitive results motivate full migration testing, not a production rollout.

Full-path testing then enabled `shmem_enabled=advise` on empty node B, moved
the existing default-CPU process to B, and enabled it on the now-empty A. The
first move after repairing the profiling interference completed with state and
cleanup intact, but its observer selected the earlier aborted transaction at
the same runtime generation. Its command-ready instant was missed; the later
observation is explicitly not an SLO sample. The harness now excludes all
preexisting migration operation IDs before triggering a timed move.

Five following clean moves of that existing process took 3.992011, 3.949547,
4.186502, 4.184549 and 4.100762 seconds. The recovered workload used only 2 MiB
of shared-memory THP, so changing the host setting did not give its existing
128-MiB allocation the same benefit as the primitive's newly created process.
All retained-state and cleanup checks passed. After the last move, the fixture
was fully checked and normally paused; its resource lease was released.

A new otherwise equivalent Python workload was created while both hosts used
`advise`: default 150 millicores, 512-MiB hard limit and 128 MiB of random
memory. Six alternating A/B migrations took 3.679277, 3.759631, 3.651437,
3.781331, 3.719128 and 3.776454 seconds, with a 3.739379-second median. All six
preserved PID, token, counter continuity, open-unlinked FD state, mmap, tmpfs,
durable files and full memory hash, and completed source/staging cleanup with
no recorded migration-pass errors. The final target's actual limits remained
`cpu.max=15000 100000` and `memory.max=536870912`; its `shmem_thp` was 136 MiB.
These are serial, warm-carrier/cache tests on the same three-host fixture.

| Stage | Earlier default profile median | Newly created THP workload median |
| --- | --- | --- |
| Checkpoint | 608.5 ms | 460.9 ms |
| Image sync | 205.7 ms | 201.5 ms |
| RootFS cut | 103.3 ms | 100.2 ms |
| Regional publication | 567.6 ms | 562.2 ms |
| Peer prefetch | 492.2 ms | 480.3 ms |
| Target claim | 1,516.5 ms | 1,196.3 ms |
| Restore execution, included in claim | 1,205.7 ms | 907.8 ms |

Shared-memory THP therefore improves this full migration workload without
raising its CPU quota, but does not meet the three-second target and is not a
retroactive guarantee for existing allocations. The host setting is enabled
only on the temporary test nodes. Production bootstrap and rollout remain
unchanged; cold/fragmented-memory and density behavior still require validation
before making a fleet-wide host-policy recommendation.

A further background-loading comparison removed a workload bias from the
earlier primitive: continuous full-memory hashing could compete with restore.
With `SANDBOX0_RESTORE_PROBE_LIGHT_WORKLOAD=1`, the test payload instead emits
lightweight fresh counter/state evidence, then recomputes the complete memory
hash only after receiving a new random 128-bit verification challenge. The
parent records first response separately and requires that exact challenge in
the subsequent matching full-memory proof. A cached pre-checkpoint hash cannot
satisfy this check. The original continuous-scan workload remains the default.

Six alternating foreground/background restores used shared-memory THP,
150 millicores, a 100-ms quota period, a 512-MiB memory limit and 128 MiB of
random memory. Foreground first-response times were 719.1, 638.3 and 638.7 ms;
background times were 738.6, 729.2 and 620.5 ms. Median restore-command times
were 618.3 and 708.8 ms respectively. All six passed fresh full-memory and
process-state validation, with images retained until target teardown. These
small primitive samples show no stable background-loading advantage for this
workload and do not support enabling it in the migration adapter. Production
restore remains foreground, with its existing image-custody boundary.

A source-writeback experiment then targeted the roughly 200-ms serialized
image sync after capture. It retained the 150-millicore/100-ms quota,
512-MiB limit and continuous full-memory validation. Three baseline and three
candidate runs alternated in each comparison. All 18 restores passed the
complete process/memory checks; these remain isolated primitive timings.

| Filesystem and early writeback | Baseline capture + sync median | Candidate median | Candidate final sync median |
| --- | --- | --- | --- |
| Host ext4, `sync_file_range(WRITE)` | 627.0 ms | 498.1 ms | 15.0 ms |
| Isolated loop XFS with project quotas, `sync_file_range(WRITE)` | 599.1 ms | 551.1 ms | 147.1 ms |
| Isolated loop XFS with project quotas, 16-MiB `fdatasync` batches | 606.3 ms | 436.6 ms | 6.4 ms |

The first XFS range-writeback candidate took 1,045.4 ms, so the ext4 benefit
cannot be generalized to XFS or to tail latency. The three XFS batched-data-sync
samples were 438.6, 404.1 and 436.6 ms, including their final full sync. Both
isolated XFS mounts and loop devices were detached after runtime/cgroup/mount
cleanup, and the destination ctld HA probes passed. The probe accepts
`SANDBOX0_RESTORE_PROBE_WRITEBACK=0`, `1` or `2` for these explicit comparisons
and disables the production helper to avoid measuring two helpers together.

The production checkpoint adapter now starts one bounded Linux writeback
worker during stock runsc capture, requesting data sync after each additional
16 MiB per file. It retains at most 256 regular-file descriptors, rejects
replacement/truncation, joins the worker, observes a final sync on those same
descriptors and propagates every writeback error. Retaining the descriptors
matters because reopening a file only after writeback could miss an earlier
asynchronous I/O error. The driver's complete file-inventory and directory
`fsync` remains mandatory before capture completion is journaled; early
writeback is not a substitute for that durability boundary. Physical sentry
exit, regional publication, full destination verification and writer fencing
are unchanged. Tests cover delayed worker exit, retained-descriptor sync
errors, unsafe inventory, descriptor limits and rejection of capture success
after a writeback failure. Targeted Linux race tests and the full driver suite
passed before temporary-node installation. Architecture tests also passed in
the remote build tree. The local architecture run failed on preexisting,
untracked legacy `netd` and repository-local `.cache/go-mod` toolchain source;
those unrelated directories were left untouched, and the local failure is not
reported as a passing check.

After updating only empty test nodes, six full migrations of the same retained
THP-backed process took 3.559876, 3.673645, 3.547004, 3.828961, 3.614136 and
3.561874 seconds, with a 3.588005-second median. The earlier six-move median was
3.739379 seconds. Every measured move preserved PID, token, counter continuity,
open-unlinked FD, mmap, tmpfs, durable files and the complete 128-MiB memory
hash; source/staging cleanup completed and no migration-pass errors were
recorded. The preliminary move with an old source driver took 4.014937 seconds
and is retained separately, outside the six candidate samples.

The new combined capture-plus-sync median was 494.0 ms. The adapter's
`checkpoint_us` now includes joining early writeback and its retained-descriptor
sync, so the driver's 0.179-ms final `image_sync_us` must not be presented as the
entire persistence cost. Regional publication, peer prefetch and target restore
medians were 614.3, 488.2 and 910.7 ms respectively. These are serial, warm-carrier
three-host tests at the unchanged 150-millicore/512-MiB lease. The improvement
does not meet the three-second total target or establish concurrent/cold-cache
performance. Production rollout remains stopped.

A follow-up raw-TCP diagnostic sent 151 MiB of synthetic bytes from A to B,
A to the independent storage host C, and both destinations simultaneously.
Three samples per mode had medians of 45.4, 44.4 and 132.5 ms respectively.
The receiver acknowledged the complete bounded byte count. This excludes TLS,
hashing, file sync and object-store processing, so it is a network diagnostic,
not a migration result. It does not support attributing the roughly 614-ms
publication stage solely to the source link. The temporary private listeners
were stopped and their ports verified absent before further migrations.

The encrypted object writer issued separate scratch-file writes for each
16-KiB frame's length and ciphertext and allocated a fresh ciphertext slice
for every frame. It now buffers up to 64 KiB and reuses one ciphertext buffer,
without changing encryption algorithms, keys, nonces, authenticated frame
geometry, object names or immutable publication semantics. The final buffered
flush must succeed before upload; tests verify that a deferred write error is
returned and that AES-GCM and ChaCha20 framed objects with partial tails remain
readable through the existing reader. Existing corruption, range-read and
conditional-publication coverage also passed in the remote race run of
`pkg/objectstore`, `pkg/rootfsobjectstore` and `pkg/runtimecheckpoint`.

For an 8-MiB object with 16-KiB frames, three ten-iteration Linux benchmarks
reduced median encrypted scratch preparation from 9.157 to 5.692 ms and
allocated bytes from approximately 9.57 MB to 0.215 MB per operation. The
benchmark uses fixed-cost test key wrapping and a discard upload sink, and
therefore does not measure regional publication or end-to-end migration.
The local object-store suite's separate native-OSS virtual-host fixture
returned an HTTP EOF, including with upper-case proxy variables cleared;
the complete corresponding remote race suite passed. That local failure is
retained, not counted as a passing test.

Six following full migrations with buffered encrypted writes took 3.938354,
3.608897, 3.544732, 3.576641, 5.644309 and 3.623497 seconds. Their
3.616197-second median does **not** improve the preceding 3.588005-second
whole-path median, despite reducing median regional publication from 614.3
to 543.4 ms. All six retained-state, full-memory and source/staging cleanup
checks passed without migration-pass errors. The preliminary mixed-version
move took 3.985873 seconds and is recorded separately.

In the 5.644309-second sample, target claim took 3,062.4 ms, including
2,802.0 ms in the enclosing restore-execution stage. That existing timer also
includes node observations and journal commits, so it does not prove that the
stock `runsc restore` command itself consumed the whole interval. The driver
now separately logs executing-intent persistence, CPU observation before and
after the command, the restore command, completion persistence and overall
success. This adds no execution retry and changes none of the authority gates.
The targeted driver race suite passed before installing the diagnostic build
on empty temporary nodes. The three-second total target remains unmet.

Six diagnostic moves with the new restore timers took 3.645248, 3.720036,
3.531109, 3.631447, 3.529552 and 4.774701 seconds; all complete state and
cleanup checks passed. The median stock restore-command interval was
839.1 ms, compared with 965.9 ms for the enclosing restore-execution stage.
Median executing-intent persistence and completed-restore persistence were
60.2 and 83.8 ms; the two CPU observations were only 2.2 and 3.6 ms.
These component medians must not be summed as if they described one run.

The new 4.774701-second outlier had a 998.9-ms restore command and a
1,138.0-ms enclosing restore stage, not a multi-second runsc command. Its node
claim took 1,320.2 ms. Comparing the source-fence observation with the target
claim's start identifies approximately one second **before** the node claim,
in addition to ordinary variation inside restore. The earlier 5.644309-second
outlier predates these detailed timers and remains unattributed within its
2,802.0-ms enclosing restore stage. Further diagnosis must preserve both
possibilities instead of explaining every tail as memory loading. At the end
of this run, generation 27 was active on A, the migration work queue was empty,
and actual limits remained 150 millicores/512 MiB with 138 MiB of shmem THP.

A dedicated migration-planning observer now records the existing planner
phases without feeding migration samples into the ordinary claim SLO observer.
Six subsequent default-resource moves took 3.763600, 3.817328, 3.681479,
3.798431, 3.748271 and 3.644055 seconds (median 3.755935 seconds). All preserved
state, full-memory hashes and source/staging cleanup checks passed without
migration-pass errors. Planning before the node-claim phase took 23.7–39.7 ms
(median 35.0 ms). RootFS metadata, slot acquisition, network preparation and
writer authorization therefore do not explain a fixed one-second delay in
these samples. The earlier pre-claim outlier did not recur and remains
unattributed; these diagnostics alone establish no whole-path improvement.
Comparing planner start with the periodically observed source fence yields
small negative differences within the observer interval, not execution before
the required fence. Generation 33 was active on A with an empty migration work
queue and unchanged 150-millicore/512-MiB limits.

Remote Linux race tests passed for `runtimeslotclaim` and `nodeauthority`, and
the targeted manager runtime tests and manager build passed. Locally, the
planner tests passed but the manager build ran out of disk space; that local
command is recorded as failed, not as completed validation. No production
rollout was performed.

Additional source-dispatch timing isolated another synchronous history scan.
Across the six planner-diagnostic moves, capture authorization preceded the
asynchronous checkpoint worker by approximately 381–432 ms. With source
admission timers installed, three moves took 3.789301, 3.697169 and 3.935992
seconds. Their ctld capture-intent recording calls consumed 397.4, 450.0 and
408.7 ms; lock waiting was negligible and CPU rechecks took only 5.8–7.0 ms.
The preliminary move with the previous source driver took 3.638642 seconds.

The first source capture's receipt-count scan still decoded every historical
journal record, unlike the neighboring staging-pool and destination admission
scans. It now reuses `migrationPoolScanRecord`, including the same bounded
fingerprints, full validation of changed records and active custody, and the
same Bolt write transaction. This does not remove an fsync, weaken admission,
or release any retained source. New tests first cache empty records, then prove
that newly retained captures still exhaust the original count limit and that
changed corrupt records reject admission without committing a capture intent.
The targeted node-runtime race tests passed in 1.170 seconds. The full
node-runtime package subsequently passed in 12.711 seconds and architecture
tests passed in 0.025 seconds. The diagnostic driver capture/CPU race tests
passed in 110.371 seconds before installing it on empty test nodes.

The first mixed-ctld move took 4.026939 seconds. With both nodes updated, six
alternating default-resource moves took 3.994846, 3.375438, 3.590098, 3.223503,
3.452657 and 3.207910 seconds, with a 3.414048-second median. No sample is
removed for being slow. All preserved state, full-memory hashes and physical
source/staging cleanup checks passed without migration-pass errors. Median
capture-intent recording dropped to 26.0 ms and complete synchronous dispatch
to 42.5 ms. This supports the measured reduction in source admission overhead;
it still does **not** meet the three-second complete-migration target. The
3.994846-second sample also spent about 0.60 seconds reaching preparation
following the temporary daemon restarts, while later samples remained faster.
At completion, generation 44 was active on B, migration work was empty, and
actual limits remained 150 millicores/512 MiB with 138 MiB of shmem THP.
Production rollout remains stopped.

Further isolated publication experiments did not establish another material
whole-path improvement. They used private random files of 128 MiB, 16 MiB and
64 KiB, the existing encrypted regional MinIO store, and a unique operation per
sample. Each sample collected its own regional objects and removed its local
fixture. They are publication primitives, not migrations or a claim about the
actual guest checkpoint's per-file geometry.

With `GOMAXPROCS=2`, six samples per upload width gave median publication times
of 519.0 ms at four workers, 498.2 ms at eight and 499.5 ms at sixteen. With
`GOMAXPROCS=4`, six samples each gave 512.2 ms at four workers and 459.1 ms at
eight. The running temporary ctld services have no explicit `GOMAXPROCS`
override. No default concurrency increase was retained; additional chunk-buffer
memory and these noisy primitive results do not prove an end-to-end gain.
A Go CPU profile of three default-width publications attributed 48.5% of CPU
samples to SHA-256 and 4.3% to AES-GCM. The profile also includes fixture
creation, cleanup and waits; it is diagnostic, not an SLO sample. AWS request
payload signing contributed to the hashes on this HTTP test-storage endpoint.

A second prototype shared the existing four-chunk budget across multiple files
instead of processing files serially. Six interleaved samples per version gave
plan-plus-publication medians of 594.0 ms before and 579.1 ms after, with wide
overlap between individual samples. Its complete checkpoint race suite passed,
including shared admission, cancellation joining, unpublished manifests during
partial uploads, and canonical file/chunk ordering. The small measured benefit
did not justify retaining the additional scheduling code: the prototype and its
new tests were removed, the original sources were restored on the build host,
and no ctld or driver service was updated for these experiments. The retained
full-migration result remains a 3.414048-second median, above the requested
three-second total.

A subsequent change overlaps stopped-image inventory hashing with RootFS
sealing. It starts only after the exact durable capture and physically stopped
source have been checked, and joins the hash worker before releasing source
reconciliation custody. Failed sealing cancels and joins the worker. The
bounded, disposable cache holds hashes for at most two image directories;
eviction, cancellation or daemon restart falls back to the ordinary scan. The
exact RootFS/runtime binding is attached only after sealing. Publication and
peer transfer still reread and verify every chunk, and regional durability,
destination verification and writer fencing are unchanged. Tests cover overlap,
cancel/join behavior, optional inventory failure, cache consumption and eviction,
independent plan ownership and rejection of changed bytes.

The preceding sandbox and REPL approached their two-hour fixture TTL. They were
verified and normally paused before creating another 150-millicore/512-MiB
sandbox with 128 MiB of random memory. Its initial setup parser failed to find
JSON interleaved with the REPL prompt; reattaching to that same context with the
existing prompt-tolerant parser completed verification without recreating or
modifying the workload. New baseline and candidate samples are kept separate
from the preceding fixture. Four baseline moves took 3.107766, 4.050194,
3.366544 and 3.711814 seconds (median 3.539179). The mixed-version transition
move took 4.111589 seconds and is retained separately.

With both temporary nodes updated, six alternating moves took 4.119391,
3.342984, 4.042411, 3.172958, 3.581725 and 3.311214 seconds. Their median was
3.462354 seconds, with zero samples meeting the three-second total target.
Median image publication decreased from 605.0 to 527.2 ms, while median RootFS
sealing increased from 102.0 to 121.1 ms. These phase medians are not additive.
The small, differently sized cohorts do not establish a stable whole-path
improvement: direction-specific medians increased, and restore-command times
in the candidate cohort ranged from 691.0 to 1,568.9 ms. The first candidate
also spent about 0.65 seconds reaching preparation after a daemon restart; it
is included. All process, FD, mmap, tmpfs, persistent-file, complete-memory-hash
and source/staging cleanup checks passed without migration-pass errors.

Targeted Linux race tests passed for checkpoint planning/inventory and node
inventory/custody behavior. The complete checkpoint and node-runtime package
tests and architecture tests passed before building ctld with the unchanged
bundled procd. Only empty temporary nodes were updated, with ctld B then A
readiness checks. Generation 12 ended on node B; its actual limits remained
`cpu.max=15000 100000` and 512 MiB, with 136 MiB of shmem THP. The three temporary
VMs retain automatic deletion at 2026-09-22 02:22 UTC. No production rollout or
PR update was performed.

Additional node-side restore-observation timers separate image verification,
session enumeration, runsc state queries and journal reads/commits. They do not
change execution ordering or receipt validation. Targeted Linux restore race
tests and architecture checks passed before updating empty temporary nodes.
Four diagnostic moves took 4.166280, 3.887645, 3.627687 and 3.543710 seconds;
all state and cleanup checks passed without migration-pass errors. These are
diagnostic samples, not evidence of another latency improvement.

At restore intent, median image verification took 61.0 ms and the complete
observation 74.7 ms. Before execution, the observation took 19.8 ms, including
6.5 ms in runsc state and 8.8 ms in journal commit. After restoration, it took
62.3 ms, including 44.1 ms in runsc state and 14.2 ms in journal commit. Session
enumeration took approximately 0.1 ms in both cases, rejecting the hypothesis
that a node-wide session scan explains this receipt delay. Exact lookup was
therefore not substituted as a speculative performance change. Separate claim
logs showed approximately 115–150 ms in RootFS attachment and 128–182 ms in
runsc create. All durability and physical state checks remain in place.

An isolated HTTP/HTTPS publication comparison used the same storage host,
reverse-proxy implementation, encrypted random-file fixture and four upload
workers. HTTPS validated a one-day, IP-SAN test certificate through a
process-scoped trust file; certificate verification was never disabled. Six
interleaved samples per protocol produced median publication times of 485.2 ms
over HTTP and 465.4 ms over HTTPS. The first HTTPS publication took 776.0 ms and
is retained. Proxy counters observed 120 payload-signed HTTP PUTs and 120
`UNSIGNED-PAYLOAD` HTTPS PUTs, including the ordinary manifests. Thus the SDK's
transport-dependent signing avoids a ciphertext hash pass on HTTPS, but this
small primitive gain does not prove an end-to-end improvement. Application
encryption, chunk verification and create-only object semantics were unchanged.
Each sample collected only its own operation's objects. The temporary proxy
was stopped and its private key removed; production and migration-fixture
storage configuration were not changed.

Fresh resource-boundary tests retained 512 MiB of sandbox memory and the same
128-MiB random-memory workload while requesting real 500-millicore and
1,000-millicore leases. Each cohort used a fresh sandbox/context and six
alternating A/B migrations of that same process. The previous fixture was
fully verified and normally paused before each cohort. The isolated manager's
memory-per-CPU ratio was temporarily changed only to create each comparison
lease, then its exact original configuration was restored before measurement.
The public API still exposes memory only; no CPU override or production policy
change was introduced. These fixed-memory comparisons do not by themselves
validate the corresponding memory tier under the normal 4-GiB-per-CPU policy.

| CPU lease | Trigger to authenticated preserved-command response, seconds | Median | Samples at or below 3 seconds |
| --- | --- | --- | --- |
| 500 millicores | 2.194244, 2.323686, 2.259201, 2.242401, 2.565536, 2.250063 | 2.254632 | 6 / 6 |
| 1,000 millicores | 2.109569, 2.114734, 2.123841, 2.126140, 2.083836, 2.058408 | 2.112152 | 6 / 6 |

All samples retained PID, token, counter continuity, the open-unlinked FD and
offset, mmap, tmpfs, durable files and the full memory SHA-256; physical source
and both staging reservations were released without migration-pass errors.
The 500-millicore cohort had median capture/restore-command times of
264.4/200.4 ms; the one-core cohort had 259.2/95.7 ms. Regional publication
remained approximately 515/521 ms and first-command execution approximately
76.7/76.0 ms, respectively. These measurements support a resource-dependent
three-second result for this workload, not a guarantee for 150 millicores,
larger active memory, dirty disks, cold image/artifact caches, simultaneous
migrations, storage outages or arbitrary latency percentiles.

Final cgroup observations confirmed `cpu.max` values of `50000 100000` and
`100000 100000`, 512-MiB memory limits and 136 MiB of shmem THP. The one-core
fixture ended at generation 7 on A with a fresh complete-state verification,
an empty migration queue, healthy regional storage and the manager's original
4-GiB-per-CPU policy restored. The default 150-millicore target remains unmet.

A subsequent cohort used the normal public claim API with a 2-GiB memory
request and the unchanged 4-GiB-per-CPU policy. It received a real
500-millicore/2,048-MiB lease without modifying or restarting the manager.
The same preserved process held 128 MiB of random memory. Six alternating
moves took 2.125331, 2.213728, 2.230282, 2.246828, 2.178387 and 2.160274
seconds (median 2.196058); all six met the three-second total target. Full
state, memory-hash and physical-cleanup checks passed without migration-pass
errors. The final cgroup had `cpu.max=50000 100000`, a 2-GiB memory limit and
98 MiB of shmem THP. These measurements validate this normal resource tier
under the test's warm, serial conditions, but still include the temporary
host's `shmem_enabled=advise` setting.

Both temporary workers were then returned to their recorded original
`shmem_enabled=never` setting, changing each only while it held no guest
RootFS. The mixed-setting transition took 2.276733 seconds and is reported
separately. Six subsequent alternating migrations of the same native 2-GiB
fixture took 2.234022, 2.249935, 2.312968, 2.260903, 2.571836 and 2.373535
seconds (median 2.286935). All six met the trigger-to-target-command target
without the shmem THP tuning. Each preserved full process/file/memory state
and completed source lease and both staging releases without migration-pass
errors. Cleanup plus the post-timing complete-memory probe finished in
4.081–5.154 seconds; those background cleanup times are not the target-usable
metric and are not hidden by the three-second result. This remains a serial,
warm-cache, 128-MiB-active-memory result, not a general latency guarantee.
A fresh generation-14 probe verified the complete memory hash again, an empty
migration queue and healthy regional storage. The destination cgroup confirmed
`cpu.max=50000 100000`, `memory.max=2147483648` and zero `shmem_thp` bytes.

An isolated destination-writeback experiment tested whether overlapping file
sync with peer reception would shorten materialization. It used the empty
source host's XFS staging mount, a 128-MiB fixture, the existing peer stream,
all chunk hashes and final file/directory syncs, with `GOMAXPROCS=2`. One
bounded worker synced the same output descriptor after each additional
16, 32 or 64 MiB; it joined before final sync/close and propagated errors.
Five iterations per mode averaged 970.0 ms for the first baseline, 1,000.0 ms
at 16 MiB, 1,024.9 ms at 32 MiB, 1,220.3 ms at 64 MiB and 999.3 ms for the
last baseline. These local transfer primitives do not measure full migration,
but they provide no evidence to retain this extra worker. The temporary
prototype was removed, the original build-host source restored byte for byte
and its isolated staging directory removed. No service binary was updated.

A read-only correlation of the retained source driver and ctld journals also
checked the interval before publication in two default-resource samples
(3.207910 and 3.543710 seconds total). Dispatch took 46.9/44.4 ms, checkpoint
569.5/654.5 ms, and RootFS sealing 96.2/141.3 ms. Subtracting the logged
publication duration from its completion timestamp places publication's start
57.9/51.5 ms after RootFS-cut completion. That publication timer includes local
planning and final receipt persistence, so it must not be confused with the
first network byte. These two samples do not reveal another fixed, second-long
source scheduling gap; they do not explain every earlier outlier.

The lower normal public tier was then tested without changing manager policy:
a fresh 1-GiB sandbox received a 250-millicore/1,024-MiB lease and held the same
128-MiB random-memory workload. Six alternating moves took 2.554258, 2.775323,
2.877027, 2.713561, 2.818680 and 2.786959 seconds (median 2.781141). All six
met the total target and retained the full process/file/memory evidence without
migration-pass errors. Source leases and both staging reservations were
released; cleanup plus post-timing verification took 4.853–5.101 seconds.
The hosts remained at `shmem_enabled=never`. These samples narrow the tested
resource boundary below 500 millicores, but do not establish a 150-millicore
result or a tail-latency guarantee. A fresh generation-7 command reverified
the complete memory hash and an empty migration queue. The cgroup confirmed
`cpu.max=25000 100000`, `memory.max=1073741824` and zero `shmem_thp` bytes.

A coordinator audit found an avoidable failure tail after successful regional
publication: `publishWithPrefetch` joined its optional cache observer without
a separate completion bound. A lost prefetch acknowledgement therefore held
publication progress until the entire caller deadline expired. A new real
PostgreSQL regression reproduced the failure under a five-second deadline,
even though source publication had succeeded.

The coordinator now allows up to 250 ms for the optional acknowledgement after
`PublishMigration` succeeds, then cancels and joins that observer. The remote
node retains staging custody independently; ordinary authorized preparation
still verifies the image or cancels/joins an unfinished fill and downloads the
exact regional reference. Failed publication cancels immediately and still
cannot authorize preparation or fencing. This bounds the post-publication
observer wait, not checkpoint, source transfer, disk sync or all network errors.
The database regression checks the cancelled observer has exited before return,
the real publication is committed before ordinary preparation, and no fence is
authorized early. It passed with race detection alongside success, unsupported
peer, failed prefetch, failed upload, authority, retry and expiry cases. The
isolated test databases were removed. Coordinator race tests, node preemption
race tests and architecture checks also passed before building the temporary
manager. The five-second reproduction is failure evidence, not an end-to-end
latency comparison.

The bounded-acknowledgement manager was installed only in the empty temporary
fixture after its tests passed. A fresh normal-policy 1-GiB/250-millicore
sandbox with 128 MiB of random memory then completed four alternating moves
in 2.824309, 2.885032, 2.815430 and 2.944924 seconds (median 2.854671).
All four retained the complete process/file/memory evidence and released source
leases and both staging reservations without migration-pass errors. These
healthy-path checks are below three seconds but do not demonstrate a latency
improvement over the preceding cohort; the fix addresses lost acknowledgements.
The temporary manager artifact was
`sha256:2c06b03a41a7041b643ff287ec12f1d977929c3eca5fe73e5b1391e62b2af264`.

Two following default-policy cohorts retained the 150-millicore/512-MiB lease,
100-ms quota period and original `shmem_enabled=never` host setting. Each
used a fresh preserved process and four alternating migrations. With 64 MiB
of random memory, times were 2.957157, 2.995659, 2.847181 and 3.027947 seconds
(median 2.976408; three of four at or below three seconds). With 128 MiB,
times were 3.436222, 6.317229, 3.544834 and 3.726760 seconds (median 3.635797;
zero of four at or below three seconds). Every complete-memory, process, file,
source-lease and staging-release check passed without migration-pass errors.
Both final cgroups confirmed `cpu.max=15000 100000`, a 512-MiB memory limit
and zero `shmem_thp` bytes; both fixtures were normally paused after final
verification. These samples cannot justify a stable default-tier guarantee,
and the slow sample is retained.

In the 6.317229-second move, stock runsc restore took 3,688.2 ms and the
complete restore-execution interval took 3,805.0 ms. Target planning plus
claim took 4,186.9 ms. Peer prefetch was 453.0 ms and preparation reused the
prefetched image in 77.7 ms. This localizes the extra delay to the restore
command, rather than the newly bounded prefetch acknowledgement. The stage
logs alone cannot split that command's elapsed time into CPU work, cgroup
throttling and host scheduling; no narrower cause is claimed from them.

A subsequent four-move diagnostic sampled each temporary node's exact
150-millicore cgroup `cpu.stat` every 20 ms, without changing binaries or limits.
It ran for at most four minutes and was explicitly stopped after collection.
The first attempt on B used a missing temporary directory and failed before
starting; it was corrected before this diagnostic workload was created.
The observed total times were 3.449397, 3.663978, 3.506246 and 4.044578 seconds;
these include the sampler and are kept separate from uninstrumented acceptance.
All state, memory-hash and cleanup checks passed.

The four restore commands took 978.6, 1,091.3, 993.7 and 1,273.6 ms. Counter
windows bracketed each command with 1.1–15.3 ms before and 2.9–12.3 ms after.
They recorded 151.8, 168.1, 158.4 and 197.9 ms of total cgroup CPU, respectively,
including 112.4, 136.3, 109.3 and 147.0 ms of kernel CPU. Every counted quota
period was throttled (10/10, 11/11, 10/10 and 13/13). This supports CPU-quota
limitation during these ordinary restores. The aggregated `throttled_usec`
values are not additive with wall time and must not be interpreted as an
exact wall-clock breakdown. The earlier 3,688.2-ms restore was not sampled and
its extra delay remains unattributed. The diagnostic fixture was verified
again and normally paused, releasing its lease; both sampling services stopped.

The driver now also records bounded, read-only cgroup CPU counters immediately
around the restore command. It keeps the same `cpu.stat` file descriptor across
the command, rejects missing, duplicate, overflowing or decreasing required
counters, and reports validity separately. Unavailable accounting never changes
restore authorization, execution or retry behavior. Targeted Linux race tests
covered restore behavior and counter parsing, retained-descriptor identity and
path confinement. The initial build used an incorrect command directory after
the tests passed; building the independent driver module at its root succeeded.
Both empty temporary nodes received driver artifact
`sha256:edb8a3e9e3dd61ec8f06541cce50e1328a803a47453cc9d4b3b96a31ac911eea`.

With this accounting and no external sampler, a fresh default 150-millicore /
512-MiB sandbox containing 128 MiB of random memory completed four moves in
3.513457, 3.805475, 4.282889 and 3.774819 seconds (median 3.790147; zero of four
at or below three seconds). All complete-memory, PID, counter, open-file offset,
mmap, tmpfs, durable-file and cleanup checks passed. The final lease was normally
paused and released. Original `shmem_enabled=never`, the 100-ms quota period,
regional durability and stock runsc were retained; accounting was valid in all
four commands. Restore wall times were 906.0, 1,089.8, 994.3 and 1,096.3 ms;
cgroup CPU times were 145.1, 160.2, 147.4 and 168.3 ms, including 107.5–132.7 ms
of kernel CPU. All counted quota periods were throttled (9/9, 11/11, 10/10 and
11/11). These observations do not establish a speedup or explain the older
3,688.2-ms restore outlier.

The 4.282889-second move instead included a 706.5-ms target restore-intent
observation: image verification took 64.9 ms and the journal operation took
639.8 ms. Its subsequent journal operations took approximately 8 ms each.
Peer prefetch took 471.7 ms and image preparation reused that cache in 69.3 ms.
The journal operation reads and writes one slot; it does not scan historical
records. Its original timer includes acquiring the Bolt write transaction,
record decoding/encoding and commit, so the 639.8 ms cannot yet be attributed
to disk sync. Additional diagnostic fields split transaction acquisition,
record processing and completion of `Update`; completion still includes
commit/rollback and scheduling, not exclusively filesystem sync. The temporary
instances' automatic release was extended to 2026-09-22 02:52 UTC for this bounded
investigation. No production rollout or PR was created.

The journal phase instrumentation passed the targeted Linux restore race tests
and architecture tests, then was installed on both empty temporary nodes as
ctld artifact
`sha256:ea5342da32c3b4bd5166ef7d6dfc3558eecfbf98c766916d9e3c6fee4f4a5aa4`.
A fresh default-tier 128-MiB-random-memory round trip took 4.849152 and 3.466498
seconds. Both moves preserved all state and released source/staging custody;
the final full-memory hash passed before normal pause released the final lease.
All six restore observations completed their journal operations in 6.4–13.8 ms:
transaction acquisition took 7–67 microseconds, record work 1.7–6.3 ms, and
transaction completion 3.5–5.7 ms. The remaining interval includes observation
validation before acquiring the transaction. The earlier 639.8-ms journal tail
did not reproduce and remains unattributed; this is diagnostic coverage, not
a demonstrated journal latency improvement.

In the 4.849152-second move, the regional observer first saw the source barrier
at 1.547 seconds after triggering drain. The later peer transfer took 460.8 ms
for 148,797,893 bytes, rootfs cut 128.7 ms, and stock restore 932.7 ms. Restore
consumed 144.3 ms of cgroup CPU and all nine counted periods were throttled.
The return restore took 1,090.0 ms and 158.9 ms of cgroup CPU, with 11/11 periods
throttled. This first sample's extra delay preceded the recorded source barrier;
it was not another restore-journal spike. No cause narrower than that observation
boundary is established by these timings.

The manager phase log narrowed the 1.547-second pre-barrier interval: the two
staging passes completed 748 and 729 ms apart, immediately after CPU preflight.
Both nodes had just restarted. Startup `Prune` already fully validates every
retained journal record, but previously discarded that work before the first
migration-pool scan. It now feeds only eligible, fully validated byte fingerprints
into the existing bounded exclusion cache. Every current payload is still hashed;
changed records, active custody, misses and eviction retain full validation.
No durable authority, expiry, cleanup rule or cache size changed. Reservation
logs additionally separate intent, kernel quota admission and ready persistence.

Targeted Linux race tests covered startup-validation reuse, changed reservations,
corruption, staging serialization/recovery and journal pruning; architecture tests
also passed. Both empty nodes received ctld artifact
`sha256:63bd8178b2f8df2d6fb889e1284373e4d263843daf41c0bd72e310bfa31ed6c8`.
Their first subsequent default-tier 128-MiB-random-memory migration reached the
source barrier at 0.140713 seconds and command-ready at 3.701416 seconds. Local
source/target staging reservations took 19.5 and 18.4 ms (kernel budget checks
44 and 32 microseconds), with no intervening migration to prime the cache.
The return migration took 3.673645 seconds, with 32.5 and 23.8-ms staging
reservations. Both passed complete state and source/staging-release checks;
final memory hash, unchanged 150-millicore/512-MiB lease and zero shared THP were
verified again. These two samples support removal of this startup-related
critical-path scan, not a general latency distribution or a three-second SLO.
Restore still took 993.1 and 1,017.6 ms. Earlier slow samples remain part of the
evidence; the 639.8-ms journal tail is not proven to have the same cause.

The corresponding driver claim logs account for much of the target interval:
RootFS attachment took 130.5/120.2 ms, stock `runsc create` 159.9/117.2 ms, and
the complete restore-execution intervals 1,056.4/1,209.7 ms. The first preserved
REPL command after the first migration committed returned HTTP 200 directly
and took another 247.9 ms; this command time remains inside the reported total.
These records do not show a fixed route-cache or coordinator sleep to remove.

A small-buffer verification prototype was evaluated separately on macOS/arm64
(Apple M1 Pro, Go 1.25.4, GOMAXPROCS=2). The standalone primitive scanned the same
128-MiB random file in four workers and checked each 8-MiB chunk digest. Across
three six-iteration runs, median times were 35.846 ms for 8-MiB worker buffers,
35.976 ms for 64-KiB buffers, 34.338 ms for 256-KiB buffers and 33.678 ms for
1-MiB buffers. Allocations fell from about 32 MiB to 0.25/1/4 MiB, respectively.
This is not a Linux checkpoint-store or end-to-end result; the roughly 2-ms best
primitive improvement does not justify a migration speedup claim. The candidate
was retained only as a temporary experiment, without changing runtime code.

An attempted 15-minute instance extension failed with
`InvalidAutoReleaseTime.Malformed`. The original 2026-09-22 02:52 UTC automatic
release remained effective: a subsequent exact-ID inventory returned zero of
three requested instances, and the new Linux probe failed before dispatch with
`InvalidInstance.NotFound`. No result is attributed to that unexecuted probe.
The preceding real-migration logs and results were already exported locally,
and the last sandbox had been verified and normally paused with its lease
released before instance expiry.

A possible next experiment is a separately admitted temporary CPU reservation
for destination restore, followed by returning to the configured CPU quota before
publishing readiness. This is a proposal, not implemented behavior or measured
performance. Whether migration may temporarily exceed the configured sandbox
CPU cap is an unresolved resource-policy decision. Existing fixed-500-millicore
samples cannot establish the behavior of a temporary-lease transition.

The pinned stock runtime does not itself prohibit resource-limit differences:
[`release-20260914.0` restore spec validation](https://github.com/google/gvisor/blob/release-20260914.0/runsc/specutils/restore.go#L444)
warns about changed Linux resources instead of failing that comparison. This
source inspection does not validate a live limit transition. Sandbox0 currently
binds the exact resource snapshot and digest into restore commands and CPU launch
lineage, so changing `cpu.max` directly would violate its own contract even when
stock runsc accepts the OCI specs. A reviewable implementation would require
PostgreSQL admission of additional capacity, exact node/boot/operation binding,
crash-recoverable application and release of the temporary allocation, and proof
of restored normal quota before command-ready. Metering must account for the
actual leased intervals, and source/target overlap must remain admitted. CPU
feature exposure, memory limits, writer fencing and stock spec validation remain
unchanged. Tests would need to cover quota-reset failure, lost acknowledgements,
node/manager restart, exhausted capacity and a second migration after the reset;
a faster steady-500m benchmark is insufficient evidence for these transitions.

An unconnected protocol candidate now separates an early
`MigrationRestoreCPUReservation` from a later `MigrationRestoreCPUGrant`.
Capacity can therefore be reserved before source capture, while the grant binds
the eventual exact restore request. The base resource lease remains unchanged.
A reset receipt binds the grant, durable cgroup identity, original CPU period
and original quota; a receipt from a different restore cannot release it.
These types do not allocate capacity, alter cgroups, authenticate a node receipt
or enforce the regional one-time grant transaction. PostgreSQL admission,
durable application/reset recovery and metering remain unimplemented. No
runtime path calls the candidate and no temporary CPU policy is enabled.
The candidate's protocol tests passed locally with Go 1.25.5. On a fresh
isolated amd64 Linux host, the protocol tests and a real node-fixture restore
binding test passed with the race detector, followed by the architecture suite.
They reject changed reservation amounts/boots, retained elevated quotas,
replacement cgroup identities and reset receipts from another restore. These
are contract tests, not privileged quota-transition or migration benchmarks.

A follow-up source audit considered overlapping target RootFS attachment and
container creation with image transfer without changing CPU limits. The current
transaction graph does not admit that reordering: source fencing requires the
committed target-image receipt, and destination writer issuance requires the
exact physical source-fence proof plus the installed captured RootFS head.
`TestNomadMigrationImagePreparationGatesSourceFenceIntegration` explicitly checks
that publication alone and download intent cannot detach the source. Removing
those checks is not a scheduling optimization; an earlier target-preparation
phase would need its own recoverable authority and failure-state design. This
was a code audit, not an executed test or evidence of a latency improvement;
the existing ordering was retained.

A later checkpoint-store change caches successfully read and digest-verified
regional manifests by their exact binding/reference pair, with limits of 16
entries and 8 MiB of payload. It does not accept speculative peer plans as
publication evidence. Returned manifests own their slices, failed reads are
not cached, eviction/restart falls back to regional reads, and every local file
and chunk is still revalidated before restore. A read-count test proves that
repeated local verification avoids a second remote manifest read without
accepting changed local bytes or a different reference.

On an isolated amd64 Linux source snapshot, the complete `runtimecheckpoint`
and `nomadruntime` suites passed with the race detector (6.985 and 43.388 seconds),
followed by architecture checks (0.024 seconds). Local ordinary checkpoint tests
also passed; local race compilation ran out of disk space, and local architecture
checks encountered ignored legacy directories/module-cache files, so neither
local failure was counted as a pass. Restore-intent verification already overlaps
RootFS attachment, so an avoided manifest read must not be reported as an equal
reduction in trigger-to-command-ready time.

The subsequent September 22 two-node comparison used two independent
`ecs.c7.xlarge` workers and a third host for PostgreSQL and encrypted MinIO
storage. Both variants were built from the same source snapshot; a Go overlay
disabled only manifest-cache lookup/insertion for the baseline. Both ctld
binaries bundled the same procd, used stock runsc `release-20260914.0`, and kept
the 150-millicore/512-MiB lease, 100-ms CPU period and `shmem_enabled=never`.
The Python image was pinned to
`sha256:9b8dad7f66b5c7751df6cb7a64a07812e86bed85d0116efe82b3a11209f1440d`.
Each cohort preserved one Python process with 128 MiB of random memory through
six consecutive alternating moves, A → B → A → B → A → B → A.

| Manifest reads | Six trigger-to-command-ready samples, seconds | Median | At or below 3 seconds |
| --- | --- | --- | --- |
| Baseline, cache disabled | 3.695768, 3.614868, 3.601334, 3.537350, 3.664418, 3.703311 | 3.639643 | 0 / 6 |
| Bounded manifest cache | 3.587332, 3.625934, 3.632852, 3.428451, 3.659378, 3.709689 | 3.629393 | 0 / 6 |

The approximately 10-ms median difference does not establish an end-to-end
improvement: these small sequential cohorts were not interleaved, and the first
cohort also populated shared artifact caches. The cache reduces repeated
regional reads, but this experiment does not establish a stable latency gain
or the requested three-second bound. Baseline median RootFS cut, execution-image
publication and runsc restore were approximately 120, 514 and 1,004 ms;
publication overlapped peer transfer and these durations must not be summed.

All twelve migrations preserved PID, the advancing counter, memory SHA-256,
open unlinked FD and its offset, mmap, tmpfs and persistent RootFS contents.
The timed first command checked state continuity without hashing all 128 MiB;
the complete hash was verified after migration cleanup. Every move released its
source lease and both staging reservations. Both final guests were verified,
normally paused and their final leases released. This remains a serial warm
carrier test, with no temporary CPU boost or production rollout.

A subsequent stock-runsc source audit examined capture/transfer overlap. In
`release-20260914.0`, the local pages writer queues offset-based writes. Its
explicit file pre-extension is enabled only for successful O_DIRECT AIO setup;
the current non-direct checkpoint uses the serial Go queue instead. Consequently,
pre-extension is not evidence of a bottleneck in the current configuration.
Neither a file length nor an mtime is a completed-checkpoint receipt. See the
pinned [FDWriter implementation](https://github.com/google/gvisor/blob/release-20260914.0/pkg/sentry/state/stateio/fdwriter.go)
and [asynchronous page saver](https://github.com/google/gvisor/blob/release-20260914.0/pkg/sentry/pgalloc/save_restore.go).

An isolated XFS probe nevertheless compared the existing 16-MiB size-triggered
writeback with an experimental mtime-triggered variant that also flushed writes
within an unchanged rounded size. Eight interleaved, foreground-restore runs
retained 150 millicores, a 100-ms quota period, a 512-MiB limit, 128 MiB of random
memory and `shmem_enabled=never`. Capture plus final complete-image sync took
509.915, 485.979, 589.788 and 498.855 ms for the baseline (median 504.385 ms),
and 590.950, 598.772, 489.684 and 502.783 ms for the experiment (median
546.867 ms). All eight process/memory restore checks passed. These are isolated
primitive timings, not migration totals. The slower experimental source changes
were reverted; its source and evidence were retained outside the repository.
The probe's runtime/cgroups were cleaned and its XFS mount and loop detached.

Earlier speculative byte transfer alone cannot remove the regional-publication
wait: current chunk keys include the full image binding, including the captured
RootFS descriptor, which is available only after execution capture and RootFS
sealing. The collector uses that same exact binding. Overlapping encrypted
publication with an unfinished capture therefore requires a separately
authorized, bounded staging scope with recoverable cleanup, followed by final
binding and complete chunk validation. No early-publication path or relaxed
execution/fencing authority was enabled by these experiments.

The next store-layer implementation introduces `BindCapture` and
`OpenCaptureStaging`. The capture scope reuses the operation, exact source
writer/boot binding, assignment revision, runtime compatibility and CPU-feature
digest, while deliberately excluding the not-yet-known final RootFS cut.
Tentative chunks retain their original encrypted object keys. An immutable
reservation fixes the plaintext chunk budget; each distinct candidate consumes
an entire 8-MiB slot, including short chunks and candidates discarded after
final verification. Recovery lists the bounded existing scope, so restarting
does not reset that budget. Regional accounting must additionally reserve
envelope/metadata overhead and exclusive producer ownership.

Final publication rereads and hashes the complete retained image, reuses only
matching successful uploads, binds exactly one final RootFS descriptor, and
publishes a version-2 manifest last. Existing version-1 publications and reads
remain supported. The legacy planned publisher rejects version-2 plans rather
than placing their chunks under incorrect keys. Peer transfer and local
verification accept the version-2 manifest with the same complete chunk checks.
Capture collection retains its final-binding marker through partial deletion
and also removes a manifest whose successful publication reply was lost.

`CaptureStager` now also produces prospective version-2 peer plans from either
a retained inventory or a fresh completed-image scan. Its planned publisher
checks the same reference and every current local chunk, reuses successful
tentative uploads, and rejects changed files instead of silently repairing an
already-exposed peer plan. Independent chunks retain four-way upload
concurrency; equal hashes share one pending immutable write. Peer receipt still
cannot authorize execution when regional publication fails.

`UploadGrowing` watches the producer-owned private directory without creating
it. It copies each observed full 8-MiB file range once per worker lifetime,
using at most four parallel chunk buffers. Concurrent or preallocated bytes
are tentative candidates only: final publication rehashes the completed image
and uploads replacements when necessary. Short tails wait for that final scan.
Cancellation joins uploads before releasing the staging gate; callers must
cancel and join before planning, publication, cleanup or primary handover.
The existing distinct-object budget also charges discarded candidates.

An isolated stock-runsc experiment on September 22 compared the legacy planned
publisher with growing-capture uploads against encrypted regional MinIO on a
separate host. Eight interleaved runs used the same static Go payload, 128 MiB
of random memory, 150 millicores, a 100-ms quota period, 512 MiB of memory,
16-MiB source writeback, an isolated XFS loop filesystem and
`shmem_enabled=never`. The order was legacy, growing, growing, legacy, legacy,
growing, growing, legacy. All used the same test binary
`b7b490dd0fac510393d402396de18267daf5775e2ee34b9b9e71c4c2d44c5ba5`.

| Publication path | Capture + final sync + publication samples, ms | Median, ms | Median publication work remaining after sync, ms |
| --- | --- | --- | --- |
| Legacy planned publication | 1089.860, 1105.916, 1088.610, 1093.525 | 1091.693 | 496.375 |
| Growing capture uploads | 768.403, 809.113, 801.007, 873.354 | 805.060 | 229.792 |

The measured primitive median improved by 286.633 ms (26.3%). Every run
downloaded the published encrypted image and restored from that downloaded
copy, preserving PID, progress, open-file offset, tmpfs, CPU flags and the full
memory SHA-256. Download was measured separately (median 825.298/853.098 ms)
and excluded from the capture/upload comparison. Each exact test object scope
was fully collected; test runtimes and cgroups were removed, and the XFS mount
and loop were detached. Both worker ctld HA probes remained ready.

These are component timings, not trigger-to-command-ready migration results.
They exclude regional staging admission, the real RootFS cut, P2P transfer,
manager coordination and restore. Restore ran on the same worker using a
static fixture filesystem, not a migrated block-COW writer. The measured
286.633-ms difference cannot simply be subtracted from the full migration
median: regional publication already overlaps the current peer transfer.
Earlier destination chunk reception is also needed to remove that remaining
transfer wait. No new three-second full-migration result is established.

The subsequent integration wires the growing source uploader into ctld capture
custody and reserves its immutable scope through regional staging admission.
Migration 092 bounds outstanding regional reservations to 64 GiB, including
metadata/encryption overhead and failed captures until exact GC completion.
Exhausted admission falls back to legacy publication. GC requires terminal
lifecycle and physical custody proofs, including captures that failed before
publication and lost publication replies. Journal envelope 15 retains the grant
and rejects downgrade. Capture outcome, cleanup and primary handover cancel and
join the uploader before releasing reconciliation custody. Earlier destination
reception is not yet wired into node or regional lifecycle coordination. Source exit, consistent RootFS, successful
regional publication and destination verification remain execution prerequisites.

Only the temporary test cluster received this integration. Six consecutive
A-to-B/B-to-A moves retained the same Python process, 128 MiB of random memory,
150 millicores, 512 MiB memory and a 100-ms quota period. Trigger-to-first-command
times were 3.789280, 3.671244, 3.903905, 3.683171, 3.923541 and 3.575256 seconds
(median 3.736226 seconds; zero of six within three seconds). Every move preserved
PID, counter progress, open-file offset, durable files, tmpfs, mmap and the full
memory SHA-256. The fixture was verified again and paused after generation 7,
with its resource lease released. These sequential samples do not establish an
end-to-end improvement over the earlier cohorts. No production rollout or
temporary CPU boost was performed.

RootFS already uses incremental block-COW publication in
`pkg/rootfssession/migration.go`: unchanged data objects are reused, while the
stopped writer's final dirty blocks and mapping changes form the new generation.
This is separate from the runsc execution image streamed by P2P. RootFS COW
does not provide incremental anonymous-memory or tmpfs checkpoints. Destination
cache misses can still require object reads, so a cold target is not a zero-I/O
restore even when its RootFS generation shares most blocks.

The integration passed Linux race suites for runtime-slot contracts (1.163 s),
checkpoint storage (10.346 s), migration coordination (3.172 s) and node runtime
(45.404 s), plus architecture checks (0.175 s), the separate Nomad driver suite
(25.059 s) and manager command compilation. Targeted PostgreSQL integration
tests ran against a separate database and passed in 27.604 s without skips,
covering concurrent budget admission, unused/failed capture collection,
post-deletion GC, staging, publication, and existing image-GC recovery.

The next destination-transfer primitive is `CapturePeerCache`. It accepts
source-scope-bound full 8-MiB ranges before a final manifest exists. Disk bytes,
file/directory counts and cumulative frame count are bounded; reconnecting does
not reset admission. Its inventory describes only complete received prefixes.
The final source rereads its entire retained image, compares the tentative
inventory with the final plan and sends only changed/missing ranges. The target
rehashes every reused range, repairs the same files in place, checks exact final
framing and syncs files/directories before acknowledging receipt. False hints,
corruption and interrupted frames cannot produce a completed final image.
Unexpected paths require authorized cache disposal and normal full transfer. Original write descriptors remain open through final fsync to preserve
writeback-error observation; path/inode substitution and delayed sync errors
invalidate the cache. Reusable ranges are checked with bounded parallel scans
at source and destination instead of serial hashing around each reuse flag.


`UploadGrowingWithPeer` shares bounded source buffers between regional upload
and an optional peer consumer, joining both before custody release. The legacy
uploader remains unchanged when no peer callback is installed. The new stream
is not exposed by a node endpoint yet: regional source/destination peer grants,
node journal ownership, restart disposal and integration with normal image
preparation remain required before enabling it. Capture-cache methods do not
authorize a destination or replace successful regional publication.

An opt-in 128-MiB random-data probe (`SANDBOX0_CAPTURE_PEER_PROBE=1`,
`TestCapturePeerGrowing128MiBTransferVolume`) checks that the target receives the
first chunk while the source file is still only 8 MiB long. It then compares
full and repair stream volumes and independently hashes the final local image.
The probe uses local pipes and synthetic image data, with no runsc, regional
RootFS cut or manager lifecycle; its byte savings are not migration timings.
On isolated amd64 Linux, the full stream was 134,220,027 bytes. Early receipt
was 134,218,522 bytes, with 1,745 bytes of inventory metadata. The final repair
stream was 2,315 bytes when unchanged and 8,390,923 bytes after rewriting one
8-MiB range. Both final images passed independent full-hash verification. These
are remaining-transfer savings; all original data still crosses the early
stream, and rewrites increase total traffic.

The checkpoint and node-runtime race suites passed in 14.822/45.440 seconds,
with architecture checks in 0.153 seconds. Including the opt-in volume probe,
the final checkpoint race suite passed in 19.472 seconds. Coverage includes
canceled consumers retaining custody until joined, incomplete frames, false
reuse hints, damaged destination data, exact scope/ref binding, bounded repeated
streams, symlink escape rejection, and canonical inventory decoding.

A subsequent stock-runsc probe restored actual 128-MiB random-memory captures
from the repaired peer directory and checked PID, progress, open-file offset,
tmpfs, CPU flags and the full memory hash. It used 150 millicores, 512 MiB memory,
a 100-ms CPU period and encrypted MinIO on another host. Source capture and
peer receipt ran on the same worker and isolated loop-backed XFS disk through
local pipes, so it still excludes real inter-node transport and regional
RootFS/lifecycle coordination.

Initial interleaved tests found essentially no capture-to-publication/peer-ready
gain despite reducing the final wire payload to a few MiB. Detailed timings
showed final target fsync, rather than remaining network bytes, dominating the
repair phase (418.764 ms in an instrumented early-receipt sample). The receiver
now submits each full received range for Linux asynchronous writeback, retaining
its original write descriptor until final fsync. Unsupported filesystems fall
back to ordinary final synchronization; writeback errors invalidate the cache.
This hint is not a durability acknowledgement and adds no detached worker.

Eight interleaved tests of binary
`cf62e62e1350f8f2222befa6d2c7ef97d4026e10a21ce6a1cfbb823da26eb358`
used full, early, early, full, full, early, early, full order. Both paths used
regional growing upload; only the early path also received during capture.

| Local-pipe path | Capture + sync + publication + peer verification median, ms | Work remaining after source sync median, ms | Final target sync median, ms |
| --- | --- | --- | --- |
| Complete peer transfer after capture | 1248.317 | 702.982 | 356.952 |
| Early receipt with asynchronous writeback | 1437.619 | 283.204 | 31.251 |

All eight restores and exact-scope object collections passed. Test runtimes,
cgroups, loop mounts and backing files were cleaned; both worker ctld HA probes
remained ready. Early receipt reduced the remaining phase by 419.778 ms, but
increased the combined primitive median by 189.302 ms on the shared disk.
Source/destination disk contention is a hypothesis to test on separate nodes,
not evidence of an end-to-end improvement. This prototype has not been wired
into regional peer grants, node journals or the actual migration path, and no
three-second result is established. The final checkpoint race suite, including
the volume probe and writeback-failure tests, passed on Linux in 19.215 seconds.

The follow-up network probe used independent source B and receiver A hosts and
isolated loop-backed XFS filesystems, with encrypted regional storage on C.
Its test-only receiver required pinned mutual TLS over the private network;
it was never registered on ctld. Binary
`f72374a1181837cc1cae9b3da643445b69bb002beda8dd71267cc61f866788c5`
passed two initial trials and eight interleaved trials in full, early, early,
full, full, early, early, full order. The workload and CPU policy were unchanged.

| Separate-node path | Capture + sync + publication + peer verification median, ms | Work remaining after source sync median, ms | Final target sync median, ms |
| --- | --- | --- | --- |
| Complete peer transfer after capture | 1144.235 | 577.346 | 156.235 |
| Early receipt with asynchronous writeback | 946.110 | 368.915 | 156.730 |

The combined component improved by 198.126 ms (about 17%). Early receipt held
96–128 MiB before final repair; final transfer ranged from 4,543,401 to
38,064,871 bytes, compared with approximately 139 MB on the full path. These
are shifted bytes, not avoided total memory traffic. Final synchronization
remained around 157 ms on the separate receiver, unlike the local-pipe result.
The comparison supports a component benefit under these conditions, not a
general causal claim about storage contention.

Each timing ended after A durably received the exact image and verified it
against the encrypted regional publication. After that timer, the probe copied
the verified image back to B for stock-runsc restore and full state validation;
that return copy took 390–411 ms and was explicitly excluded. All eight restores,
memory hashes and exact-scope regional collections passed. This experiment
excludes peer admission, manager coordination, the RootFS cut, and restoration
on A, so it is not a complete cross-node migration acceptance result. The
test receiver was shut down and its cache released after the cohort.

Node integration now retains each private endpoint and certificate alongside
the original staging receipt. Retries and primary changes cannot replace that
historical identity or enable transport for an older reservation. The internal
`migration_capture_peer` node-channel command binds both receipts, both exact
placements, the regional capture scope and the existing staging budget. It
records authority only; source capture intent and final regional publication
remain separate prerequisites.

Journal envelope 16 retains endpoint/grant state and rejects older envelopes.
An early destination grant owns its derived cache path and staging admission
until synchronized physical absence is recorded. Cancellation retains a
tombstone, so delayed grants cannot recreate released paths. Generic carrier
cleanup and journal pruning cannot bypass this custody. The source grant owns
no destination cache path and cannot be introduced after capture begins.
The ctld source uploader now shares its bounded chunk buffers with an optional
pinned-TLS stream to the reserved destination. A slow or failed peer cancels
only that consumer; regional upload continues. The destination retains original
write descriptors under journal custody until final repair, synchronization or
cleanup. Prefetch cancels and joins tentative receipt, obtains a bounded reuse
inventory, and repairs the same files against the final planned or published
reference. Restarted caches are discarded rather than reopened as trusted data.
Repair failure removes the old cache before full transfer, preserving the single
image staging budget. Normal preparation still verifies the committed regional
manifest before acknowledging a usable destination image.

Actual TLS node tests passed with the race detector in 5.532 s. They exercise
the source capture uploader, unchanged-chunk reuse (an 8-MiB tentative chunk
requires less than 32 KiB of final wire data), changed source chunks, corrupt
target chunks, lost process-local cache state, wrong certificates/grants, and
receiver cancellation while regional upload continues. A blocked manifest
publication test proves final repair can finish before publication without
creating destination execution custody. Complete Linux race regressions passed
for node runtime (50.384 s), checkpoint storage (14.817 s), runtime-slot contracts
(1.180 s), migration coordination (3.731 s) and architecture (1.135 s); manager
and ctld command compilation and the separate driver suite (24.125 s) passed.
These tests prove protocol behavior, not the three-second migration SLO.

The first integrated six-move probe exposed an admission bug: only the initial
source accepted its early-peer grant. Later sources retained their previous
destination-adoption history, which was incorrectly treated as an active image
preparation. Those five moves safely fell back to full image transfer, so that
cohort is not evidence for repeated early receipt. The guard now permits a
historical adopted destination only as the next source, using the existing
exact adopted-runtime matcher for generation, process, assignment, writer
binding and resource identity. A changed generation remains rejected. The
expanded node authority/stream race tests passed in 6.556 s; the complete node
runtime race suite then passed in 50.066 s.

After that fix, one fresh 150-millicore/512-MiB sandbox holding 128 MiB of random
memory completed A → B → A → B → A → B → A, advancing generation 1 through 7:

| Move | Trigger to target command-ready |
| --- | --- |
| A → B | 3.441619 s |
| B → A | 3.477313 s |
| A → B | 3.469087 s |
| B → A | 3.532972 s |
| A → B | 3.621430 s |
| B → A | 3.575501 s |

The median was 3.505143 s; zero of six met the three-second total target. All
six PostgreSQL grants retained both acknowledgements without fallback, and all
six destination logs recorded successful early-cache repair. Tentative cache
inventories contained 96–136 MiB; these are received bytes, not a claim that
every byte remained reusable in the final checkpoint. Median final repair was
207.309 ms, prefetch 221.259 ms, published-image preparation 74.452 ms, RootFS
cut 125.037 ms, and regional publication 176.375 ms. These overlapping stage
medians must not be added to derive total time. Stock-runsc restore alone still
took 986–1091 ms (median 1002.228 ms) under the original CPU quota.

Every move preserved PID, counter progress, the full memory SHA-256, the open
unlinked file and offset, mmap, persistent files and tmpfs. Both staging
reservations and source leases were released. After final verification the
fixture was paused; no active guests, migrations, resource leases or outstanding
capture-upload budget remained, and all six capture scopes were collected.
The acceptance ctld hash was
`86d259c0123c7a2a3600bddd3aec14721539576e519d8697b40b9ed2e67fde94`
and manager hash was
`cab4b169aa5228b006593eecbe441d3485732ed0326aa59750f27caf57aa462d`.
The stock runsc, driver, procd, CPU/memory limits, 100-ms quota period and
`shmem_enabled=never` setting were unchanged. No production rollout occurred.

A subsequent host-setting comparison retained those same binaries, workload,
150-millicore/512-MiB resources and 100-ms quota period, changing only the two
temporary nodes from `shmem_enabled=never` to `advise`. Observed guest cgroup
shared-memory huge pages were 144,703,488 bytes before migration and 142,606,336
bytes after the sixth move. The six trigger-to-target times were 3.019164,
3.331543, 3.113738, 3.255509, 3.387829 and 3.463501 seconds (median 3.293526;
zero of six at or below three seconds). All six early-peer grants retained both
acknowledgements without fallback, and all six repairs completed. Median final
repair was 215.404 ms, RootFS cut 119.685 ms, regional image publication
207.522 ms, checkpoint 558.711 ms and restore 796.441 ms. Stage intervals can
overlap and are not additive. Full state checks passed on every move; after
pause, no active guest, migration, resource lease or capture budget remained,
and all six capture scopes had been collected. This is a host-setting
experiment, not a new default or a stable three-second bound.

The next checkpoint transport revision removed a duplicate tentative receiver
hash and moved source chunk hashing outside the frame serialization mutex.
Final disk verification, buffer ownership and publication authority remain
unchanged. Linux race suites passed for checkpoint storage (15.583 s) and node
runtime (50.960 s); the local checkpoint race suite also passed (8.230 s).
The resulting ctld hash was
`b7de79da121d4486b6d2e5fae38cb078062e930ea8cc04cf131e232d0bdc04c7`.

With the same `advise` host setting and unchanged manager, driver, runsc,
resources and workload, six moves took 3.330341, 3.270281, 3.368832, 4.481617,
3.374631 and 3.333023 seconds (median 3.350927; zero of six at or below three
seconds). This does not demonstrate an end-to-end latency improvement over
the preceding cohort. The fourth move's restore command took 1.993638 seconds;
the overall restore median was 702.708 ms. Median final repair was 282.543 ms,
RootFS cut 125.276 ms and regional publication 266.367 ms. All six grants and
repairs succeeded, complete state checks passed, and pause left zero active
guests, migrations, leases or capture budget, with all six capture scopes
collected. Both temporary nodes were then verified back at their original
`shmem_enabled=never` setting. Production was not changed.

An isolated follow-up compared the stock checkpoint `--direct` option (source
writes, distinct from the earlier restore-read experiment). Six interleaved
buffered/direct/direct/buffered/buffered/direct runs used independent runsc
roots and cgroups on a dedicated 2-GiB loop-backed XFS mount, encrypted regional
growing upload, 150 millicores, 512 MiB, a 100-ms CPU period, 128 MiB of random
memory and the original `shmem_enabled=never` setting. A private wrapper added
only the source checkpoint flag; the installed runsc inode was unchanged.
All six restored the full challenged memory hash, PID, file offset and tmpfs
state, and collected their unique regional capture prefixes.

| Isolated source I/O | Checkpoint median | Capture plus full sync median | Capture, sync and encrypted publication median |
| --- | --- | --- | --- |
| Buffered | 535.730 ms | 542.712 ms | 794.145 ms |
| Direct | 444.613 ms | 456.493 ms | 691.826 ms |

The direct publication totals were 691.826, 1008.034 and 680.429 ms. The second
sample's 176.012-ms final sync prevents a stable tail-latency claim. Restore
medians were effectively unchanged (918.941 versus 917.951 ms). These
component timings exclude actual cross-node placement, P2P receipt and writer
handover; they motivate a full-path comparison, not enabling a production
default. Probe runtimes, cgroups, mount and loop device were removed, and both
ctld HA probes passed afterward.

The subsequent full-path source-I/O comparison used the early-peer ctld
`b7de79da121d4486b6d2e5fae38cb078062e930ea8cc04cf131e232d0bdc04c7`,
unchanged manager/runsc, 150 millicores, 512 MiB, 128 MiB random memory, a 100-ms
CPU period and `shmem_enabled=never`. Both driver variants came from the same
build tree, with only the direct variant's test build overlay adding
`--direct` to the checkpoint command. The adapter race suite passed (2.668 s).
The full driver race suite exceeded its initial three-minute package budget
while building repeated 300-MiB RootFS fixtures; the timed-out package passed
with a larger budget in 387.073 s, and the remaining packages had passed.
No race report occurred in the timed-out attempt.

| Source checkpoint I/O | Six trigger-to-target times, seconds | Median | At or below 3 seconds |
| --- | --- | --- | --- |
| Direct, experimental overlay | 3.488125, 4.506994, 3.442739, 3.482087, 3.730311, 3.736231 | 3.609218 s | 0 / 6 |
| Buffered, control build | 3.605657, 3.713474, 3.588602, 3.518664, 3.450084, 4.638609 | 3.597129 s | 0 / 6 |

Checkpoint medians were 557.488 ms direct and 650.588 ms buffered, but the
complete migration did not improve. Final repair medians were 197.296 and
186.580 ms; restore medians were 1045.515 and 1041.287 ms. The cohorts are small
sequential comparisons and do not establish tail percentiles or attribute all
differences to the flag. All twelve moves preserved complete state, used both
early-peer acknowledgements without fallback and released source/staging
custody. Both fixtures were paused with zero active guests, migrations, leases
or capture budgets, and all twelve capture scopes were collected.
The direct driver hash was
`1837781737a325780815bdfb6ec334f7b8f17a7e72c7f4979a1a71e6e941dd20`;
the buffered control hash was
`6f06ea22037254c8f11b421ea7d7c04b5db77f780e244e64b42c1b94ad575776`.
Both hosts were then verified running the original driver
`9ff8e85a845e64609fae79ffb1b52e7967eb3046814813b14b97cbc686920321`.
The production source I/O default remains buffered.

Final peer repair now releases reuse frames before rechecking retained source
bytes, allowing the destination's complete local hash check to overlap source
validation. The node flushes buffered HTTP frames explicitly; otherwise tiny
reuse markers would remain buffered until the source scan finished. Reuse
protocol version 3 requires an explicit completion trailer after every source
check and also requires successful transport EOF. A missing trailer fails even
when all cached files are valid and the local reader ends cleanly. Late HTTP
errors, failed flushes, corrupt sources and incomplete streams retain custody
and cannot produce a successful receipt. Peers with incompatible repair wire
versions discard the optional cache and use the unchanged full-image protocol.
There is no checkpoint-manifest or RootFS-format change.

A first local experiment without a trailer failed the existing changed-source
regression: complete reuse frames could be misread as success on a clean local
EOF. The required trailer fixes that failure. Added tests exercise complete
cached-file verification followed by a late HTTP abort, flush failure, absent
completion, version rejection and an actual TLS-node fallback from a legacy
repair header. Final local checkpoint race tests passed in 7.055 s. Linux
checkpoint and node-runtime race suites passed in 16.347 s and 52.172 s. The
acceptance ctld hash was
`ad594257686b35a9def5cafaf188523b8f783047483a5a29e04272cdb989a752`;
the original driver, manager, procd and stock runsc remained in use.

Two fresh 150-millicore/512-MiB cohorts with 128 MiB random memory and a 100-ms
CPU period measured:

| Shared-memory THP | Six trigger-to-target times, seconds | Median | At or below 3 seconds |
| --- | --- | --- | --- |
| `never` | 3.699343, 3.663328, 3.598762, 3.692723, 7.146285, 3.634200 | 3.678026 s | 0 / 6 |
| `advise` | 3.145259, 3.167384, 3.433022, 3.408285, 3.246178, 3.351617 | 3.298898 s | 0 / 6 |

With `never`, final repair's median was 108.562 ms versus 186.580 ms in the
preceding buffered-control cohort. Publication, checkpoint and restore medians
were 160.220, 694.026 and 1092.811 ms. The fifth move's publication interval was
3767.576 ms; no target execution was authorized before it completed. A retained
object-store service log inspection found no entries in that interval, and the
store host had 80 GiB free. This does not identify the cause of that outlier.
With `advise`, repair, publication, checkpoint and restore medians were 172.356,
286.136, 507.924 and 796.427 ms. The final guest used 142,606,336 bytes of
shared-memory huge pages. Different overlapping-stage medians are not additive.

All twelve moves preserved the complete process/memory/file evidence, used both
early-peer acknowledgements without fallback and completed peer repair. Both
fixtures were fully verified and paused; active guest, migration, lease and
capture-budget counts were zero and all twelve capture scopes had been
collected. Both temporary hosts were verified back at `shmem_enabled=never`.
The shorter repair stage does not establish lower total migration latency or
a stable three-second bound. Production rollout remains stopped.

Final tentative chunk upload can now overlap RootFS sealing. After the growing
uploader has joined and the completed execution image is under stopped-source
custody, inventory inspection also stages missing chunks, including short file
tails, under the existing capture reservation. This worker has a 500-ms optional
deadline and must join before reconciliation custody is released. Failure keeps
existing budget charges and falls back to ordinary final publication. It does
not bind a RootFS cut or publish a restorable manifest early. Final publication
still rereads every planned chunk and publishes the manifest last, after the
exact durable RootFS cut has been obtained.

Tests cover short-tail upload without binding or manifest publication, changed
bytes after inspection, cancellation with retained budget charges, rejection
after publication binding, and actual overlap with RootFS sealing while a
canceled backend write is still exiting. The last test checks that cancellation
alone cannot release node custody. Local checkpoint race tests passed; Linux
checkpoint and node-runtime race suites passed in 16.853 s and 53.023 s. The
candidate ctld hash is
`9d0f099c6202731b9e3f1b4b442875101e00b7fd6368df11b9b48a80c099db1a`.
Publication timing now separates upload-scope opening, planning, object
publication, stopped-source recheck and journal commit, so regional-write tails
can be distinguished from those other stages.

The first six-move cohort with this candidate kept 150 millicores, 512 MiB,
128 MiB random memory, a 100-ms CPU period and `shmem_enabled=never`. Total
trigger-to-target times were 3.597392, 3.625329, 3.553882, 3.607454, 3.693179
and 3.549788 seconds: median 3.602423 seconds and zero of six at or below three
seconds. All six optional sealing uploads succeeded, with a median of
179.139 ms. The six initial RootFS cuts had a 175.480-ms median; one operation
also logged an 8.067-ms idempotent cut retry, which is not another migration
sample. Final publication had a 93.867-ms median, comprising upload opening
4.596 ms, planning 3.170 ms, objects 69.428 ms, source recheck 5.430 ms and
journal 9.582 ms. These component medians are not additive. Checkpoint,
restore and final peer repair medians were 585.457, 1089.311 and 145.815 ms.

Every move preserved the complete process, memory and file evidence and used
both early-peer acknowledgements without fallback. The fixture was fully
verified and paused; active guest, migration, lease and capture-budget counts
were zero and all six capture scopes were collected. Moving writes into the
seal interval shortens the later publication stage, but also lengthens the
seal interval. This cohort alone does not establish lower total latency.

A separate six-move `advise` cohort with the same candidate and unchanged
quotas measured 3.215906, 3.294467, 3.532142, 3.309415, 3.432484 and 4.254830
seconds: median 3.370949 seconds, zero of six at or below three seconds. All
six optional uploads succeeded. Sealing-upload, final-publication, checkpoint,
restore and peer-repair medians were 208.783, 86.659, 527.405, 788.037 and
181.539 ms. The sixth move included a 909.738-ms destination image-preparation
stage; its restore command took 779.099 ms. Its preparation transport was
`peer`, while only five of six moves logged successful final repair/prefetch.
Thus that move retransferred the image rather than promoting the early cache.
The logs alone do not prove why its optional prefetch failed. The implementation
can cancel an unfinished fill after the manager's 250-ms observer grace, making
that boundary a relevant recovery-path test rather than a confirmed cause.

All six moves preserved the full state evidence and used both peer grant
acknowledgements without disabling the early grant. That grant evidence does
not imply successful final cache reuse: five moves reused it, one retransferred. The final guest used 142,606,336 bytes of
shared-memory huge pages. It was verified and paused; active guest, migration,
lease and capture-budget counts were zero, all six capture scopes were
collected, and both hosts were restored to `never`. No stable three-second
bound or end-to-end improvement is established by these candidate cohorts.

Destination preparation now gives an exact, same-process prefetch worker up to
150 ms to finish before canceling and joining it. This is additional to the
manager's existing 250-ms observer grace, not a change to publication or restore
authority. A completed or absent worker adds no wait; an unrelated worker is
not observed. The final image still must pass published-manifest verification.
Tests cover productive completion without retransmission, exact worker matching,
observer cancellation and the existing stuck-fill cancellation/cleanup paths.
The Linux node-runtime race suite passed in 52.022 s for ctld
`969c606987e6cac72d532f58e3a104b95e179513fbdc3b61ac2083838e14d262`.

A fresh `never` cohort at the unchanged 150-millicore/512-MiB quota and 128-MiB
random-memory workload took 3.573934, 3.580143, 3.778573, 3.714979, 3.748320
and 3.703642 seconds: median 3.709310 seconds, zero of six at or below three
seconds. All six moves used final repair and `prefetch` preparation. Repair,
preparation, publication, checkpoint and restore medians were 113.884, 74.209,
90.921, 676.428 and 1083.781 ms. Complete state verification passed throughout.
These observations do not establish a reduction in total latency or prove that
the additional grace was exercised during this particular cohort.

The final-repair client also retries a pre-stream HTTP 503 up to three times,
with 5/10/15-ms backoffs, before using its existing fallback. The source can
briefly reject exclusive admission after retiring the shared publication reader
and before releasing publication custody. No cache mutation has begun at this
point. Permission failures, other HTTP statuses, transport errors and partial
or corrupt streams are not retried. The request context continues to bound
network operations; the 30-ms sum describes backoff only, not total RPC time.
Error bodies are closed without an extra potentially blocking drain.

An actual TLS node test holds source reconciliation across the first repair
request, then releases it. Repair succeeds with less than 32 KiB sent for an
already-retained full chunk, followed by normal published-image verification.
A persistent-busy test verifies that bounded repair failure discards the
optional cache and recovers through the regional publication path. Existing
late-stream, version-mismatch, corruption and authorization coverage remains
in the full node-runtime race suite, which passed in 53.167 s. The combined
acceptance ctld hash is
`0a0c921b0e295d419be8660d0070b4a2c0cb59aab6bb019e8e1fb906690c450e`.

The combined candidate's default `never` cohort took 3.532339, 3.502819,
3.493374, 3.402783, 3.538887 and 3.639924 seconds (median 3.517579). All six
moves used final repair and prefetched-image preparation. Repair, preparation,
publication, checkpoint and restore medians were 113.782, 74.071, 92.173,
666.736 and 987.131 ms. The separate `advise` cohort took 3.563687, 4.324232,
3.342873, 3.448943, 5.293101 and 4.411446 seconds (median 3.943959); all six
also reused the prefetched image. Its restore median was 796.943 ms, which
does not explain or eliminate the other tail intervals. These small cohorts
do not attribute their total differences solely to the recovery-path changes.

All eighteen moves across the grace-only and combined-candidate cohorts passed
complete state verification, used both early grant acknowledgements and were
verified/paused afterward. Each cohort ended with zero active guests, migrations,
leases and capture-budget bytes, and all capture scopes collected. Both hosts
were restored to `shmem_enabled=never`. Production CPU and hugepage defaults
remain unchanged; the tests do not establish a hard three-second SLO.

Regional migration 093 now records the exact peer grant in the same transaction
as the final staging acknowledgement. The existing staging worker first asks
the destination to retain that grant, then the source. Source preparation is
excluded from discovery and denied by both locked authorization and SQL guards
until both acknowledgements exist or an explicit fallback is committed. The
pending-staging index includes peer decisions after pool admission completes.
Receipts and fallback decisions cannot be reversed by retries or late replies.
Each optional node call has a 500-ms observer limit; a failed call records
fallback without claiming that the node accepted no work or releasing its
staging custody. Invalid acknowledgements remain errors. Legacy receipts
without endpoints, incompatible endpoint pairs, and operations without regional
capture-upload admission keep the existing path without extra peer calls.
Migration 093 has been applied to the isolated acceptance cluster after tests
in a separate database. The acceptance manager and both ctld A/B pairs now run
the streaming implementation; production remains unchanged. The acceptance
rollout verified no active guests, migrations or resource leases, updated each
standby before its primary, and confirmed four warm carriers per node.
The PostgreSQL authority/worker and existing staging race tests passed in
32.593 s without skips; the final indexed-schema peer tests passed in 11.896 s.
Coverage includes destination-before-source dispatch, concurrent acknowledgement
replay, failures with nonnil node results, late replies after fallback, expired
CPU eligibility, cancellation retaining staging release authority, immutable
SQL evidence, and source preparation remaining gated. Existing source-dispatch,
capture-upload and staging integration regressions also passed without skips
in 25.839 s. Coordinator race tests passed in 3.724 s, manager compilation passed,
and the final architecture checks passed in 0.025 s. The regional integration
tests use node doubles, with authenticated-channel behavior tested separately;
they do not establish a new migration SLO.

Linux race suites passed after this authority work: node runtime 45.083 s,
runtime-slot contracts 1.193 s, authenticated node channels 66.251 s, and
checkpoint storage 13.809 s; architecture checks passed in 0.022 s. Tests cover
immutable endpoints across restart/configuration changes, exact grant retries,
key/placement substitutions, mixed command payloads, unsupported peers, errors
discarding acknowledgement payloads, cancellation overtaking grants, forbidden
late grants after capture, and retaining cache custody until physical cleanup.
The fallback test additionally leaves partial peer data, removes the current
TLS identity, and verifies ordinary preparation removes that cache before
downloading and validating the committed regional image. The updated complete
node race suite passed in 45.191 s. Manager command compilation and the separate
Nomad driver suite also passed (driver package 24.202 s).


On the isolated amd64 Linux source tree, the complete checkpoint-store and node
runtime suites passed with the race detector (7.510 and 43.561 seconds), followed
by architecture checks (0.186 seconds) and the separate Nomad driver suite
(24.618 seconds). Follow-up checkpoint race tests passed in 7.308 seconds and
include version-2 peer round-trip,
lost chunk/publication replies, unchanged-chunk reuse, budget recovery, exact
source/cut rejection, regional encryption and partial garbage collection.
The manager migration coordinator tests passed with the race detector, and
`TestNomadMigrationImageGCRequiresPhysicalCustodyAndRetriesIntegration` executed
against a separate PostgreSQL test database and passed (1.006 seconds); it was
not skipped. The database test preserves the existing successful-publication
GC gate. The subsequent migration-092 tests additionally cover regional authorization
for collecting a capture that failed before final publication.

The growing-upload and version-2 planning changes passed the complete Linux
checkpoint-store and node-runtime race suites (11.858/45.762 seconds), the
architecture checks (0.176 seconds), and the separate Nomad driver suite
(25.739 seconds). Final test refinements and the opt-in real-runsc upload probe
passed checkpoint-store/gVisor race tests again (10.298/2.755 seconds). Cases
include preallocated pages overwritten after early upload, exact chunk reuse,
changed local plans, bounded parallel publication, canceled uploads releasing
custody, budget exhaustion and peer completion without regional publication.

The execution-image foundation is implemented in `pkg/gvisorcli` and
`pkg/runtimecheckpoint`. It includes stock runsc checkpoint/restore adapters,
bounded immutable image transfer using the regional object-store interface,
exact source/RootFS binding, and corruption/interruption tests. The privileged
Linux probes cover both three isolated runsc roots on one host and an A → B → A
round trip on two independent amd64 cloud instances. Both preserve process state
with DirectFS enabled and disabled. These runtime probes do not substitute for
manager/ctld integration or the complete Nomad/NBD/XFS acceptance run.

Procd has an internal generation-handover controller and an authenticated
`PUT /internal/v1/runtime/migration` contract. Only manager tokens bound to the
exact request digest, sandbox and team can prepare, restore or cancel a
handover. The digest also binds the preserved procd instance, lifecycle epoch
and action: a prepare token cannot authorize a restore. Generic system tokens,
gateway callers and wildcard permissions are rejected. This contract is not
part of public OpenAPI and is not routed as a user lifecycle action.

Preparation closes runtime readiness and drains admitted API mutations;
ordinary pause/resume/barrier controls cannot override the migration gate.
An incomplete drain stays gated until the exact operation retries or cancels.
Restoration changes the supervisor's generation in place while preserving live
process handles, attempt identities, journals and input receipts. Existing
attempts retain their original start generation. Failed session persistence
keeps readiness gated and must be retried under the same operation; activation
cannot bypass it. These local gates do not grant destination execution authority
or replace the host-side execution/RootFS consistency cut.

Source cancellation retains a process-local tombstone and the highest observed
migration lifecycle epoch. Cancellation may arrive before preparation without
first closing the source gate. The same cancellation can be replayed after a
lost reply, but a delayed prepare for that operation cannot close admission
again; older epochs and a different operation at the same epoch are rejected.
A later operation with a higher epoch can prepare the unchanged source. A
preparation-free cancellation or completed handover replay owns no API barrier
and cannot clear a separate lifecycle's barrier. Manager must still establish
regional cancellation authority before sending this private command: a procd
reply does not prove absence of node capture or target execution custody.

The regional claim transaction retains the exact canonical runtime assignment
and initial network-policy bytes on the one-shot carrier. This includes the
environment, webhook, ephemeral mounts and copied-session reset flag actually
sent at launch; subsequent template or sandbox-config edits do not rewrite it.
Both ordinary claims and restored migration destinations persist these inputs,
so the destination can later serve as a migration source. Hash, identity and
size checks bind them to the existing slot digests. The assignment cannot be
replaced, erased or backfilled after claim. Legacy digest-only claims remain
usable, but the system cannot invent their missing launch input from current
configuration.

An acknowledged network mutation updates the retained effective policy in the
same transaction as its existing slot digest and applied-token receipt. A
deferred database guard prevents policy bytes from committing without that
exact acknowledgement; pending desired policy is never migration input.
`GetRuntimeSlotClaimInputs` reads these bytes only on demand. They are excluded
from ordinary heartbeat/inventory projections and public status, and must not
be logged because launch configuration can contain secrets. Payload validation
triggers run on input/digest changes rather than hashing launch configuration
for every heartbeat. Reading retained inputs is not source execution authority:
the migration transaction must still recheck lifecycle, generation, writer,
placement and runtime eligibility before preparing or capturing a source.

Manager also runs a bounded source-execution worker for existing system-owned
reservations. It waits for both committed CPU receipts and both exclusive
staging receipts before preparing procd. The worker reads the original claim
assignment and effective policy, not current template or sandbox configuration;
legacy claims without those bytes cannot enter automatic preparation. The
evacuation worker creates reservations from regionally fenced draining nodes.

One pass commits the preparation command and its canonical source procd
address; a later pass delivers that exact command with a scoped token and
commits capture authority only after its matching acknowledgement. The address
is immutable alongside the preparation. Retry and cancellation keep this
original address even if later slot observations change. Already-prepared
legacy rows without an address cannot backfill it or join automatic dispatch.

The next pass rechecks the existing capture command under regional lifecycle
locks before sending it through the authenticated exact-boot node channel.
Capture dispatch does not grant a new writer, publish a generation or start a
destination. The driver retains its once-only checkpoint intent and uncertain
outcome fence. Lost dispatch replies retry the same command; an uncertain
capture never causes a second checkpoint or a fresh start. The independent
source-recovery worker seals completed captures and authorizes publication.
Capture receipts are observations rather than new regional commits, including
repeated completed receipts. The recovery worker briefly polls missing or exact
pending journal observations, including temporary node unavailability during a
concurrent RootFS seal, for up to two seconds at 50-ms intervals. Other errors
keep the ordinary retry backoff. The wait never dispatches a capture or
authorizes execution, and larger checkpoints retain periodic recovery.
CPU expiry excludes new preparation/capture authorization, while a previously
authorized capture retains its historical command. Cancellation excludes
further preparation and serializes against first capture authorization.
Termination and generation replacement prevent dispatch from a stale queue
snapshot; already-owned physical custody still requires its recovery protocol.

PostgreSQL race tests cover lost preparation/capture authorization replies,
node replies, manager recreation, concurrent replicas, retained launch inputs
and endpoints, invalid acknowledgements, CPU expiry, cancellation exclusion,
changed capture requests and termination after queue discovery. These tests use
controlled procd/node responses and do not prove the complete cross-host path.

An internal evacuation worker now discovers live, current-generation workloads
whose exact cluster/node/UID has a regional `draining` admission fence. A Nomad
scheduling flag alone is not sufficient. Warming, revoked, expired, terminated
and legacy input-less sources are excluded. The worker derives the next runtime
assignment from retained launch input and creates an automatic, noncancelable
migration transaction. It adds no public API or user-selected destination.

Reservation rechecks the exact source writer, generation and drain fence under
the existing transaction locks. A short cluster-scoped PostgreSQL transaction
try-lock serializes automatic source/destination pairing across manager
replicas; it is not a persistent leader or another capacity ledger. Removing or
changing the drain fence cannot race through that locked admission check. The
existing resource selector chooses compatible capacity on another node in the
same cluster, with the source's limits. No capacity leaves the source running
and creates no lifecycle or target lease. A lost commit response is recovered
through the committed lifecycle and its downstream workers, not a second move.

Each physical node participates in at most one automatically admitted migration
until both execution and staging custody finish. Destination selection skips all
warm carriers on occupied nodes. Aborted operations impose a five-minute retry
delay on both participating nodes, derived from existing lifecycle timestamps;
restarts do not reset it. Elapsed time never replaces missing staging-release,
source-finalization or destination-adoption receipts. These migration admission
limits leave ordinary claim capacity accounting unchanged. They prevent an
unsupported or quota-constrained node from repeatedly consuming fresh carriers.
The scanner uses the existing bounded coordinator and advances its cursor past
unavailable candidates so other nodes can continue.

Database race tests exercise eight competing managers, multiple workloads on
one source, occupied-destination exclusion, node-wide backoff and staging cleanup,
lost reservation replies, stale drain discovery, unavailable capacity, expired
writers and retained configuration. A controlled-node test runs the actual
regional discovery, CPU-preflight, staging and source-capture workers in order.
The cloud scale-in handler still refuses to terminate instances with active
leases; this worker does not turn a provider termination notification into
permission to destroy an active sandbox. Maintenance integration must retain
node fences, physical cleanup and routing-retirement proofs.

The regional reservation phase is implemented in sandboxstore. It creates an
automatic `migrate` lifecycle and selects a compatible warm carrier on a
different node in the same cluster. The target CPU, memory and PID limits come
from the exact source resource lease. Target capacity is reserved in the shared
resource ledger without attaching a physical claim or issuing another writer;
normal claims and ready-inventory projections exclude it. Concurrent retries
recover the same reservation. No capacity means the transaction rolls back
without advancing the source lifecycle epoch or disturbing source execution.

Before any preparation command is authorized, an exact abort can release this
never-attached lease and admission-fence its one-shot carrier for normal cleanup.
It preserves the ledger and lifecycle records, and cannot release a reservation
after the lifecycle advances beyond preparation intent. The new
`sandbox_runtime_migrations` table extends the existing lifecycle identity; it
does not replace the lifecycle phase or resource ledgers. This reservation is
not a source-checkpoint or target-execution authorization.

A separate internal preparation-cancellation worker handles a prepared source
whose original two-minute preparation/CPU window expired, whose sandbox became
terminal, or whose reserved target became unavailable. It rechecks that no
capture command was ever authorized, then persists the exact private cancel
command and original procd address before delivery. Cancellation and capture
serialize on the same regional operation and lifecycle locks; a database guard
forbids retaining both authorizations. An unanswered capture is therefore never
classified as an unused reservation or canceled through this lane.

Only a matching authenticated procd cancellation acknowledgement can commit
`aborted`, retire the unused destination carrier and release its unattached
resource lease. The source writer, RootFS head and visible runtime generation
remain unchanged. Both exclusive staging reservations then become eligible for
the existing independent release worker. Lost node or database replies retry the
same command; an expired CPU window, changed desired state or later slot address
cannot reconstruct it for another endpoint. Committed acknowledgements are
immutable and idempotent. This worker cannot recover a dead source by assuming
that a timeout means its gate, capture or writer is gone; node-loss reconciliation
still requires separate exact physical evidence.

The existing regional terminal worker also recovers expired, never-authorized
reservations. The preparation intent has a fixed two-minute deadline measured
from its PostgreSQL creation time; exact retries cannot extend it. Recovery
locks and rechecks the lifecycle before releasing capacity and retiring the
unused carrier. A manager restart or concurrent recovery passes cannot release
the same lease twice. Later phases are excluded even when their timestamps are
old: elapsed time does not prove that execution or writer custody is gone.
This recovery requires the existing terminal worker to be enabled; it does not
introduce a separate maintenance loop or a public migration setting.

Regional source authorization is also implemented. A committed `barriered`
phase stores the exact private procd prepare request before dispatch is allowed;
here the phase means ownership of preparation, not evidence that draining has
finished. Only the matching authenticated prepare response can advance to
`publishing` and persist the exact source capture request. That phase authorizes
capture, not publication success. Both requests are immutable recovery evidence.
Initial authorization rechecks the live source writer, hard TTL and reserved
destination. It also atomically retains the exact source-applied network policy
bytes and digest with the preparation command. The payload must identify the
same sandbox/team, remain within 64 KiB and hash to the source slot's effective
policy. Semantically equivalent but byte-different input cannot replace an
applied policy. Preparation retries reuse the same bytes; database guards reject
rewrites, erasure or backfilling after preparation. Capture and new source-fence
authorization reject policy drift. Legacy prepared operations without this
snapshot cannot manufacture one or execute a destination through the new lane. Concurrent retries and manager restarts recover the original
command, including its original node boot and control endpoint. Once a command
is stored, neither reservation expiry nor unused-target abort can discard it,
even if another recovery path changes the lifecycle phase. These store methods
do not issue a target writer or advance the visible runtime generation.

Source capture is wired through the existing authenticated node channel, ctld's
root-owned local control client and the driver's private Unix socket. Nodes
advertise an optional `migration_capture` capability; unsupported nodes cannot
receive the command. The request binds the source slot/allocation/node boot,
sandbox generation, assignment, resource lease, writer binding, procd instance
and lifecycle epoch. No host staging path comes from manager or the guest.
Upgrade the regional channel receiver before enabling the new node capability;
older receivers reject capabilities they do not understand.

The driver persists capture intent in its local state and ctld's exclusive Bolt
journal before calling runsc. ctld chooses a private staging directory outside
the Nomad allocation, and admits at most two unresolved image custodies per
node, shared by source captures and destination downloads.
Completed or uncertain captures retain admission until explicit handoff; retries
and restarts do not free it. A bounded five-minute driver worker performs the
checkpoint independently of an HTTP connection or periodic node-channel
reconnect. Matching retries observe that worker and cannot invoke checkpoint
twice. Image files and the image directory are synced before completion is
recorded. Driver state replacement also syncs its parent directory.

Expected checkpoint exit keeps the carrier in migration custody rather than
notifying Nomad of a crash. Ordinary stop, signal, driver GC and node cleanup
cannot discard this custody. Driver recovery consults the ctld journal, fences
remaining source execution, and retains images and mounts. An interrupted
intent becomes uncertain; filenames never establish successful capture. Lease
loss cancels an in-flight checkpoint and fences execution while retaining the
image. Authority loss also closes admission to a new capture atomically with
the custody check. Fencing waits for an exact stopped-runtime observation;
signal acceptance alone is insufficient. A captured image is only
execution-state evidence: it is not proof of a
matching RootFS cut, durable regional publication or destination authority.

The stock checkpoint command's successful save RPC can precede the sentry's
observable exit. The adapter now waits, for at most 30 seconds within its caller
deadline, for the exact source container to report `stopped` before returning
success. It neither kills the source to manufacture that observation nor accepts
a missing, paused or different container. Timeout retains the image as uncertain.
This keeps the independent ctld filesystem-seal check strict without misclassifying
the normal exit interval as resumed execution. A fresh full-stack attempt exposed
this interval after CPU preflight succeeded; its image was invalidated and has
not been replayed. The corrected adapter passed controlled delayed-exit/error
tests and real stock-runsc two-successive-move probes in both DirectFS modes.
After the schema 00088/journal 13 rollout, this retained invalidated attempt
converged automatically through the failed-capture protocol below. Both slots
became terminal, both resource leases and staging reservations were released,
and the sandbox became paused at generation 2 without a migration commit.
Deletion through the public API subsequently returned absence. No execution
image was repaired or replayed. This proves recovery of this failed attempt,
not successful execution continuity. Subsequent independent single-move
acceptance results and the earlier repeat-move defect are recorded above.

An uncertain capture before publication has a separate failure protocol
(schema 00088, node journal version 13). The source-recovery worker consumes the
authenticated uncertain observation and atomically excludes future publication,
fences the original writer, and retires the provably unattached destination
reservation. This does not claim that a destination ran or stopped. Existing
publication authority, even without its reply, excludes this failure branch.

The node independently requires the exact source capture and writer binding,
with no published RootFS cut, publication or source-fence authority. An
incomplete cut can enter this failure path only when its local owner is lost
and no synced sequence, result or detach/finalization exists. A still-owned or
synced cut retains migration custody. The node records exact regional failure
custody before asking RootFS to abandon the unsynced cut. RootFS session schema
12 retains its original cut identity and failure-writer operation durably;
ordinary freeze recovery cannot thaw or publish that abandoned branch. A
restart retries this handoff before physical slot cleanup. Runsc, mounts, networking and the resource cgroup must
all be absent; the external crash WAL and capture image remain retained until
PostgreSQL commits writer retirement. Final artifact removal checks the exact
terminal writer through regional authority. Neither a later stopped-container
observation nor a driver restart clears execution invalidation.

Only the final physical/artifact receipt permits the driver to expose an exited
carrier. The terminal reconciler then purges its allocation, verifies direct
client absence and obtains the node's allocation-GC acknowledgement. Source
lease release and lifecycle abort commit together, after the unused destination
carrier is also terminal. The reserved runtime generation is consumed even
though migration never committed; a later resume uses a new generation and the
last committed filesystem head. Terminating or hard-expired sandboxes continue
to deletion only after both staging-release receipts. Capture images never
become a fallback for failed execution.

Linux journal/driver tests cover partial cleanup, restart and lost replies,
incorrect identities, retained invalidation, finalization denied by writer
authority, allocation acknowledgements and journal downgrade rejection.
PostgreSQL tests cover competing publication/failure authorization, four
concurrent recovery replicas, transaction rollback, active/terminating/expired
disposition, generation consumption and staging-gated deletion. These component
tests do not by themselves establish successful automatic cross-host migration.

The driver now requests a RootFS cut after the completed memory capture has
been journaled. This private ctld operation checks the exact captured request,
source writer binding and stopped runsc instance before touching the filesystem.
It refuses an unexpectedly executing captured source; killing it later would
not make its newer filesystem match the older memory image. Ordinary node
lease/crash recovery uses the same per-slot admission as capture and only
fences execution while migration holds custody. Observing unexpected execution
durably invalidates the image, even when the capture/seal path refuses to kill
it. A later stopped observation cannot restore the validity of that cut.

The RootFS session records migration intent before freezing XFS, then records
the synced block-COW sequence before materializing immutable regional objects.
It keeps XFS frozen until the immutable cut is durably recorded, then thaws
XFS while retaining stopped execution and migration custody. Subsequent old
branch housekeeping writes are discarded during detach; they cannot replace
the captured generation. Ordinary attachment, retirement and release remain
rejected. Normal freeze recovery leaves an incomplete barrier intact. If materialization fails,
the same cut may retry after process restart only when its successful freeze
and WAL-sync sequence was already durable and the recovered WAL still matches.
An interrupted freeze with no confirmed sequence remains uncertain; a recovered
dirty tail alone cannot establish consistency with the memory image.
Slow immutable publication releases the session metadata lock so the exact
carrier can keep renewing its consumer lease. A per-session capture admission
prevents concurrent publication; renewal does not reopen filesystem writes.

Linux freeze retains an active superblock reference. Carrying that freeze
through a later NBD owner shutdown can strand the device even after unmount,
so thaw is part of completing the sealed cut. LinuxRuntime retains the exact
open directory FD from freeze until successful thaw, because a shutdown XFS
rejects new opens and getattr. The journal records thaw only after the syscall
succeeds; a failed thaw retries the already sealed result, never a new cut.
Session schema version 11 introduced this sealed, thawed migration state;
version 12 adds exact failed-cut abandonment custody. Both ctld A/B readers
must support version 12 before workloads use this path. Real primary-loss
tests now cover before freeze, after freeze before durable sequence, and after
the durable immutable cut before thaw. These exact-boundary tests do not cover
host destruction or every intermediate filesystem and device failure.

The completed filesystem descriptor uses regional immutable objects, not an
inline PostgreSQL dirty tail. ctld adds the cut and its digest to the exact
capture observation; a driver retry cannot erase or fabricate it. The cut's
request binds the memory-capture digest and original writer binding. Its presence
still does not commit a regional RootFS head, release source capacity or permit
target execution. The current session journal version is 12; an older ctld must not
take over these records. Coordinate both A/B instances before allowing capture.

Execution-image publication now uses the same encrypted regional object store
as RootFS. Before dispatch, the regional transaction stores an immutable
publication request joining the exact capture, filesystem descriptor, source
assignment, runtime compatibility and CPU eligibility digest. The materialized
RootFS cut is registered in the existing generation catalog and retained as the
lifecycle's prepared generation without changing the current head. ctld validates it
against both source journals and records upload intent before writing objects.
Only ctld derives the private image path. The optional `migration_publish`
node-channel capability cannot be used as claim, readiness or cleanup evidence.

A five-minute node publication worker survives transport cancellation and
channel rotation; ctld shutdown cancels it and retains the durable retry intent.
Successful upload records its immutable manifest receipt in Bolt before reply.
An exact retry reuses that receipt, while a different cut, CPU binding or image
is rejected. Manager commits the matching receipt to PostgreSQL under the same
lifecycle transaction. Neither receipt advances the visible runtime generation,
RootFS head or writer epoch, releases source custody, nor issues a target writer.
A changed writer/head rejects a new receipt. The image-size validation bounds
accepted uploads; it is not a hard limit on the disk used while runsc captures.

Destination image preparation now runs through the optional
`migration_image_prepare` node capability. Manager first persists a command
binding the published source image to the exact reserved carrier, node boot and
resource lease. ctld records download intent in the same exclusive slot journal
and derives the private destination path; no regional caller supplies a host
path. A bounded worker survives channel disconnects and is canceled by ctld
shutdown. Partial images retain custody across restart. An exact retry may
remove only its incomplete private download, after proving that the destination
has no runsc container or attached RootFS session.

The existing encrypted object store supplies the immutable manifest and chunks.
Every chunk is verified and the complete directory is synced before the node
journals and returns readiness. Cached readiness is revalidated against the
manifest and local file inventory, sizes and chunk digests; extra files,
symlinks, missing data and corruption fail closed instead of being repaired
under an already-completed receipt. This custody prevents generic carrier
cleanup, source recapture and image-directory reuse until an explicit handoff.
Migration custody uses slot-journal envelope version 2 so older ctld readers
reject it instead of ignoring the retained-image fields; ordinary registration
keeps its existing version. Destination restore custody advances to envelope
version 3: image-preparation-only readers must reject target execution history
instead of treating it as ordinary crash recovery. Upgrade both A/B instances
before enabling the corresponding capture, preparation or restore operations.
The shared two-entry migration admission limit includes unresolved images and
adoption receipts awaiting regional acknowledgement. It bounds custody count.
New source capture and destination download also require the node-wide,
kernel-enforced staging quota described below. This hard cap bounds aggregate
allocated image blocks and inodes, including incomplete and open-unlinked files.
The quota itself does not reserve capacity for an operation. The exclusive
staging admission protocol below supplies durable reservations on both nodes;
manager requires both receipts before authorizing source preparation or capture.
These reservations exclude competing staging writers but do not preallocate
physical disk blocks.

Manager commits only the matching destination receipt. Source-fence
authorization requires that receipt and rechecks destination admission; target
claim and writer issuance also require the stored image evidence. These checks
advance neither public runtime generation nor command readiness. Image
preparation is connected to a separate persisted restore authorization and the
driver execution branch described below. Its completed receipt now feeds the
internal procd handover and command-readiness commit through the automatic
recovery lanes described below. Ordinary start remains prohibited for a
migration destination.

Source fencing is now a separate durable handoff phase. Manager requires the
committed image receipt, atomically changes the original writer to retiring with
kind `migration`, and records the exact fence command. This revokes renewal;
a timeout cannot return the grant to normal consumption. The optional
`migration_fence` capability transports the command to the exact source boot.

ctld retains fence intent before deleting the already-stopped runsc instance.
Unexpected source execution invalidates the image rather than being killed to
make an old cut appear valid. ctld verifies container absence and the original
stable mount namespace, detaches the stable mount, and asks the RootFS session
to consume the exact handoff authorization. The session records intent before
thaw/unmount and reuses common physical teardown without building a new disk
checkpoint. It inspects mount and NBD absence before persisting a proof and
releasing the device reservation. Interrupted teardown retries the same intent
across restart; a completed proof cannot become ordinary crash abandonment or
permit generic artifact reclamation.

The regional transaction accepts only the matching physical proof. It retires
the old writer, installs the prepared RootFS generation as head, and leaves the
migration active in `committing`. Concurrent retries recover the same proof.
The writer epoch and visible runtime generation do not advance at this step.
Source image/WAL custody, network state and resource leases remain retained;
the reserved destination still has no writer or permission to execute.

The RootFS session layer now has an explicit source-artifact finalization
primitive for use after target adoption. It binds the exact source detach proof
and the regional finalization-command digest. Its runtime adapter verifies that
the source writer is regionally terminal before permitting deletion. The
session journals intent before removing the retained WAL and mount directories,
reuses ordinary terminal-artifact accounting, and synchronizes directory entry
removal before returning a stable absence proof. No new RootFS head is built or
published. Wrong bindings, live ownership and unreleased device reservations
are rejected, and partial deletion can finish after restart.

RootFS session schema version 10 introduced this finalization intent and proof;
older readers must reject it. Both ctld A/B instances need matching readers.
Ordinary release, crash cleanup and generic journal forgetting still cannot
consume migration custody. A separate exact finalization-forget operation lets
ctld remove the compact session only after its own final slot proof is durable.
The internal optional `migration_finalize` node-channel command now connects
this primitive to source image removal and the existing network/cgroup cleanup.
It binds the exact source fence, target adoption receipt, original resource
lease and canonical cleanup operation. Ctld journals irreversible intent before
deletion, then records the RootFS proof, synchronizes source-image removal, and
uses the ordinary slot cleanup journal as the sole physical-cleanup authority.
Journal envelope version 6 preserves these bindings and the allocation-GC
retention barrier across restart. Version 5 records remain readable and are
retained until acknowledgement; new writes require version 6 readers on both
ctld A/B instances. Generic
cleanup cannot initiate this transition, and conflicting retries are rejected.

Only after the complete slot proof is durable does ctld forget the compact
RootFS session. A separate confirmation prevents retention pruning from losing
that retry handle when session deletion fails. Invalidated source execution
keeps historical evidence readable but cannot reuse it to authorize cleanup.
The root-only local interface projects a completed finalization receipt from
that same journal. It returns no receipt while session forgetting is pending,
and rejects invalidated execution. Driver Stop/Close verifies the exact capture,
writer, container and claim identities before marking the old carrier exited.
Recovery also accepts a complete ctld receipt when Nomad retains its original
warm handle after bundle deletion. It binds the immutable slot, allocation,
node, control endpoint and container, and validates any surviving claim or
capture metadata. This produces only an exited handle for pending Nomad GC:
it creates no bundle, control listener, registration, heartbeat or writer.
It then removes only its control socket and bundle; it does not repeat physical
runtime cleanup. Restart recovery consults ctld before ordinary crash handling,
including a local state file that predates capture. Capture history remains
and forbids reuse of that carrier. No user-facing migration action is exposed.

The regional finalization transaction now derives its command from the stored
source fence, committed generation, restored target and target-adoption receipt.
It verifies the original source slot, binding, resource lease and terminal
writer before durably authorizing deletion. Schema migration 00070 retains the
immutable command digest and composite physical receipt in the existing
migration row. Concurrent retries return the same command and proof.

TTL expiry or desired termination does not prevent this historical cleanup;
neither authorization nor receipt persistence changes public runtime routing,
the RootFS head or either resource lease. The lifecycle stays `committing`.

The regional completion transaction now joins that stored node receipt with
the existing exact allocation-missing observation. Only an orphaned source with
both proofs can release its original resource lease and become terminal; the
same PostgreSQL transaction commits the migration lifecycle. It reuses the
ordinary runtime-slot lease-release transition, leaves the target lease and
writer intact, and never republishes the RootFS head or changes routing/TTL.
Concurrent completion retries retain the original terminal digest and release
timestamp. Schema migration 00071 guards lifecycle commitment, and ordinary
`FinalizeRuntimeSlot` rejects migration predecessors to prevent partial release.
The existing enabled terminal worker now recognizes retired migration source
writers. It waits for committed target adoption, dispatches the authenticated
finalization command and persists its complete receipt before purging the exact
Nomad allocation. It requires purge acknowledgement and direct-client absence,
then records the existing orphan observation. It sends `migration_source_gc`
over the authenticated node channel, binding the finalization-command digest,
node-cleanup proof and direct-client allocation-absence digest. Ctld validates
its complete retained receipt and journals this acknowledgement before normal
proof retention can expire. Driver receipt reads cannot acknowledge GC.
Only after the acknowledgement returns does the worker invoke atomic completion.
Failures retain the source lease and durable queue entry. A stored node receipt
is reused after restart or lost purge responses, so ctld need not repeat cleanup
while allocation reconciliation finishes. A lost GC acknowledgement is retried,
including after the node journal has already expired; that retry creates no
cleanup proof or runtime authority. If ctld is unavailable, the source remains
queued with its lease until acknowledgement succeeds. This retention-only
acknowledgement may use the existing single authenticated successor boot for
the same durable node UID, preserving the old slot and boot in its journal
binding. Multiple successor boots fail closed. Unsupported node transports cannot
fall back to ordinary crash cleanup. This path does not initiate migration or
choose a destination; the earlier automatic evacuation phases remain unwired.

The allocation observation proves direct-client directory absence, not erasure
of every historical Nomad task handle. In the Nomad v1.11.1 client source used
by the driver module,
allocation destruction removes directories before deleting its state database
bucket, and logs a bucket-deletion failure without failing GC. Ctld now handles
late recovery after local proof pruning through a read-only regional-receipt
lookup. The existing authenticated node authority endpoint checks the source
slot's cluster, Nomad node and durable node UID before reading its immutable
migration marker and committed finalization receipt. The client checks the
bounded response and complete proof; ctld checks its own placement and returns
the historical receipt through the existing root-only driver RPC. It never
recreates a local journal, registers a carrier, renews a writer or releases a
lease. The receipt remains readable after source terminal release and expiry.

Regional fallback is used only when the local slot journal is absent. Pending,
invalidated or corrupt local custody cannot be bypassed by regional history.
Missing regional evidence does not count as cleanup, and unavailable authority
does not produce a completed driver handle. A retained old handle is checked
against the receipt's exact allocation, slot, container and available claim
metadata by the same driver terminal-recovery path.

Destination claim and writer assignment are now implemented as internal regional
transactions. They require the stored image receipt and committed source-fence
proof, then attach the exact reserved carrier and resource lease without
reserving capacity again. The predecessor enters quiescing custody and retains
its original claim, writer history and resource lease. An immutable database
marker permits only that fenced predecessor to coexist with its successor;
it does not declare the old carrier physically cleaned or allow it to execute
again. Sandbox queries continue resolving the committed allocation, preventing a
prepared target from becoming publicly routable before generation handover.

The target writer uses the existing filesystem epoch CAS and atomic slot binding.
Its node boot, allocation, claim, prepared RootFS generation, runtime generation
and fixed consume deadline must all match. Concurrent retries recover the same
grant and cannot increment its epoch or extend its deadline. The ordinary runtime
start method rejects migration destinations: possession of the new storage writer
must never substitute a fresh entrypoint for restoration of the committed image.

Restore authorization now joins the exact prepared-image receipt, committed
source physical-fence proof and tokenless destination Stage binding in
PostgreSQL. The image and full filesystem descriptor must match the fenced
source; the new writer epoch, allocation, boot, claim, resource lease and target
assignment must match the destination. Authorization is immutable and does not
advance public runtime generation. A migration-specific starting digest allows
only that command through the existing starting endpoint after writer
consumption. The current filesystem epoch, writer lease, node heartbeat and
fixed pre-command-ready claim deadline must still be live. A stale starting
retry cannot restore execution authority.

The driver verifies ctld's retained image before attaching storage, then reuses
the normal RootFS, network, resource-cgroup and OCI-create path. It calls stock
`runsc restore` instead of `start`. ctld journals `intent`, `restoring` and
`restored` against the immutable command. Entering `restoring` requires the exact
live RootFS session and created container; `restored` additionally requires that
container to be running. Concurrent and completed claim retries do not call
restore again. This local running observation is not proof of procd generation
handover or regional command readiness.

Before destination adoption, an interrupted restore, lost completion response,
driver restart or authority loss enters `uncertain` custody and fences target
execution. Driver recovery
consults ctld even when its own metadata is stale. Fencing continues while a
restore call is in flight, so a late response cannot revive execution after an
earlier stopped observation. Uncertain custody cannot return to intent or
restoring. Generic stop, signal, destroy and crash cleanup retain the image and
target filesystem instead of discarding possible post-restore writes. There is
no fresh-entrypoint fallback. Uncertain migration reconciliation and source
cleanup are still required; driver loss does not provide transparent recovery.

For an authorized, uncommitted restore, manager now records an immutable failure
decision when the target claim expires or quiesces, or the sandbox terminates
or reaches its hard deadline. Schema 00083 commits that decision and the target
claim fence together. It excludes later restore, handover, command readiness and
adoption while retaining the original restore command. A committed generation
cannot be converted into this failure path. Concurrent manager retries reuse the
same decision; elapsed time alone does not release either resource lease.

A separate worker delivers the committed decision over the authenticated node
channel. Ctld journals it before deleting the exact destination container and
records a stop receipt only after observing container absence. Journal envelope
version 9 prevents older readers from silently ignoring this execution fence,
including records with staging reservations. Restore observations and adoption
are rejected after the fence; restart recovery retains it even before a local
restore observation exists. A driver with stale warm metadata must also honor
that custody. Upgrade the node binaries before relying on that driver recovery
behavior. Stop receipts are idempotent and remain immutable in PostgreSQL.

This receipt is execution-stop evidence only. It does not prove absence of the
RootFS writer, dirty tail, image, allocation, network or resource cgroup, and
does not authorize image replay or capacity release. The two-sided cleanup
protocol below requires separate physical and regional terminal evidence.

Schema 00084 permits the existing source-finalization protocol to consume either
an adoption receipt or an exact failure-stop receipt. The two outcomes are
mutually exclusive; failed cleanup never fabricates adoption. The source cut
and publication remain bound to the original source fence. Ctld then uses the
same RootFS finalization, image removal, network and cgroup cleanup, and durable
receipt as successful-source cleanup. Source allocation purge and direct-client
absence still precede source lease release. For this failure path, source
release leaves the lifecycle committing and preserves the target writer, lease,
staging and public generation. Source staging may be released independently
after its physical finalization receipt. Destination cleanup and the final
failed lifecycle transition are still required.

Schema 00085 adds destination physical cleanup after the irreversible stop.
Manager atomically records an exact cleanup command and cancels an unconsumed
writer or fences a consumed writer into crash abandonment. The target uses a
distinct retirement operation, because the source already owns the migration's
retirement ID. Ctld journal envelope 10 records this command before reusing the
existing physical writer, mount, network and cgroup cleanup. Only the matching
node proof permits regional writer retirement. The committed source filesystem
cut remains unchanged; target writes are not published. Concurrent retries,
response loss and database rollback retain the same command. Image, external
crash journal, staging, allocation and resource-lease custody remain until the
subsequent finalization protocol; writer retirement alone cannot complete the
lifecycle or authorize image replay.

Schema 00086 uses that committed cleanup receipt as the authority for target
artifact finalization. Ctld records finalization intent in journal envelope 11,
independently verifies the exact writer's terminal state, reclaims the external
RootFS dirty tail and compact session, and removes the execution image. Its
durable receipt survives a lost reply or restart. The region then permits target
staging release while retaining the resource lease and allocation custody.
The driver can expose an exited failed-target handle only after this exact
receipt, including when Nomad still holds the original warm metadata. Neither
an execution-stop receipt nor writer retirement alone can satisfy that gate.
Allocation purge, physical-absence acknowledgement and the final failed
lifecycle transition are separate gates.

Schema 00087 connects those gates through the existing terminal-slot reconciler.
Failed targets cannot enter generic writer cleanup. A validated committed target
finalization receipt permits allocation purge; direct-client absence is recorded
before ctld acknowledges retention release. Journal envelope 12 preserves that
exact acknowledgement, rejects changed retries and permits ordinary retention
expiry. The acknowledgement grants no execution or physical-cleanup authority.

The regional completion transaction requires the source and target physical
finalization receipts, source allocation disappearance and released lease, and
the target allocation disappearance and node acknowledgement. It atomically
releases target capacity, records `migration_failed` and aborts the migration
lifecycle. Deferred database guards prevent a partial terminal transition.
The failed target generation is permanently consumed. An active sandbox becomes
paused at the last committed filesystem cut; an expired or terminating sandbox
continues deletion. A later resume uses a new generation and new processes;
failed migration never replays a possibly executed image or reports preserved
memory as successfully migrated. Deletion still waits for both staging-release
receipts before releasing retained storage references. Fully completed failure
no longer occupies both nodes indefinitely, but the existing five-minute abort
backoff still applies.

The node claim response now carries ctld's exact completed restore receipt.
An ordinary active response, uncertain custody or a receipt for another image
cannot authorize handover. Manager persists that receipt and the exact internal
procd `restore` command before dispatch; the command retains the source procd
instance UUID, lifecycle epoch and immutable assignment, changing only its
runtime generation. A matching authenticated procd response is persisted before
command readiness may advance. This acknowledges the in-memory generation
handover without creating a new procd or session supervisor.

The existing command-ready path now carries the immutable restore digest for
migration targets. Driver verifies completed ctld custody and the original procd
instance before forwarding the ordinary authenticated command-probe proof.
PostgreSQL rechecks the exact target writer, node heartbeat, claim deadline,
filesystem epoch, expected allocation address and procd handover receipt. It
atomically marks the target active and publishes its allocation and runtime
generation and persists the exact destination-adoption command in the same
transaction. Failures roll back all three changes; concurrent retries preserve
the original readiness timestamp, proof and adoption command. TTL and compute billing values are
unchanged. Historical source-generation commands cannot authorize another
restore after this commit.

The lifecycle remains `committing` and both physical resource leases stay held
until migration-aware cleanup completes. The committed allocation is now the
runtime-slot lookup result; this is not evidence of complete ingress, egress or
long-lived connection handover.

The authenticated command-ready response carries the destination-adoption
command back to the driver. It binds the target node incarnation, claim,
restored generation, original procd instance, restore digest and command probe.
Driver asks ctld over its root-only local socket to adopt that exact runtime.
Ctld first verifies the live writer and running container, journals irreversible
intent, removes only its private destination image directory, synchronizes the
parent directory and journals an absence proof. The live RootFS, writer and
resource cgroup stay intact. Adoption records now use journal envelope version
7, which also protects the regional receipt-retention barrier from older
readers. Unacknowledged version 4 adoption records remain readable. A record
that also owns a later source finalization retains all version 6 cleanup and
allocation-GC checks inside the version 7 envelope. Upgrade both A/B instances
before creating version 7 records.

After this proof, driver permits ordinary signal, stop and destroy behavior;
ctld permits the existing terminal cleanup protocol. Adoption history remains
until regional acknowledgement and ordinary terminal-proof retention both
permit pruning, and forbids download or restore replay. An unacknowledged
receipt continues to consume migration custody admission after image removal;
regional acknowledgement releases that admission without releasing the live
runtime's resources.
The adopted runtime can become the source of a later migration while retaining
its prior adoption history. A crash between intent, image removal and receipt
is completed idempotently. Driver restart reconciles ctld custody even with a
stale warm handle, then uses existing driver-restart fencing; it does not resume
the process transparently. Lease loss racing adoption, including a lost adoption
response, still fences execution and moves to ordinary cleanup.

The command-ready node response carries the exact adoption request and receipt.
The regional node channel now commits this receipt to PostgreSQL before
acknowledging command readiness to its caller. The response must match the
authenticated target node, boot, allocation, slot and socket, as well as the
exact command-ready proof. PostgreSQL then requires the immutable adoption
command issued by generation publication; an unsolicited or premature receipt
cannot make source cleanup eligible. Missing store support or a database error
returns failure rather than silently discarding the receipt. Exact retries,
including a lost database commit acknowledgement, record the same historical
fact without granting execution or releasing either lease. It remains recordable
after TTL expiry or a desired-state change. Once committed, the existing terminal
worker can automatically proceed with source finalization.

Ctld also delivers adoption receipts independently of the driver's response.
On startup and every ten seconds, a dedicated background lane scans at most
128 journal entries and processes at most 16 candidates within a twenty-second
pass budget. It uses the existing authenticated node-to-region transport and
the private `PUT .../migration-adoption-receipt` action. The region verifies
node ownership and the historical allocation and boot, then applies the same
PostgreSQL commit gate. A successor boot of the same durable node may report
its retained historical receipt; this grants no execution authority.

For a completed restore without local adoption intent, the lane reads the
private `GET .../migration-adoption-command` action. PostgreSQL returns only the
immutable command committed with generation publication; a pending or rolled-back
publication returns no authority. The region authenticates the owning node and
checks the exact target slot, allocation and historical boot. Ctld compares the
command with its completed restore, then uses the existing per-slot adopter,
which requires the original live writer and running process before recording
new intent. An uncertain restore, changed binding, stopped process or missing
writer cannot acquire intent through this path. Once intent is durable, image
removal can finish after a crash without authorizing execution again.

Before stop, signal, close or a subsequent capture, the driver can import a
completed ctld adoption receipt that the original response failed to deliver.
It validates the local restore and any existing procd/readiness metadata, persists
the exact receipt, and preserves phase and lease-loss fences. Incomplete custody
cannot unlock these controls, and recovery never calls runsc Start or Restore.
Driver restart continues to use the existing conservative execution-fencing path.

The driver also retains an already-observed runsc exit while destination
adoption custody is unresolved. It retries retrieval of the exact completed
ctld receipt once per second between bounded RPC attempts, then reports the
original exit result without issuing another runsc Wait, Start or Restore.
Transient receipt-read failure cannot lose the exit or bypass migration custody.
Cancellation stops this recovery waiter; source checkpoint exits remain owned
by the migration finalization protocol. The waiter never acquires the driver's
cleanup lock, because Stop holds that lock while waiting for exit completion.

A lost regional acknowledgement leaves the receipt pending, retained and
retryable across restart. Only an acknowledgement matching the completed receipt
permits normal terminal retention to prune it. Unacknowledged receipts remain in
the existing two-entry migration admission budget, so an unavailable region
cannot create an unbounded delivery backlog. The successful command-ready response
path and this background path may race safely on the same immutable PostgreSQL
receipt. No additional state store or public migration action is used.

The earlier source preparation and capture sequence still requires its automatic
dispatcher and preflight gates. The authenticated channel tests cover database failure before and
after commit, concurrent exact retries, and changed target or command-proof
identities. PostgreSQL integration tests separately enforce the generation gate,
command retrieval before and after publication or rollback, and retention of
both resource leases. Node tests exercise the actual TLS client and regional
handler with a test authority store, lost commands and acknowledgements, journal
reopen, interrupted local intent, bounded scanning, admission backpressure and
the retention barrier. Driver tests exercise receipt recovery before controls
and another capture, reject changed or incomplete custody, and preserve a
concurrent lease fence. These do not substitute for two-node production acceptance.

The manager now runs an independent migration-transfer coordinator whenever
its node authority is enabled. Its queue is a bounded, indexed scan of the
existing lifecycle/migration rows, starting only after an exact publication
command has been committed. It does not create reservations, approve preflight,
or checkpoint a process. Each pass reads at most eight operations and advances
one physical step per operation: publish the source image and commit its receipt,
prepare the reserved target image and commit its receipt, then authorize and
physically fence the source and commit that proof. New commands must be stored
before dispatch. Missing or invalid node evidence never permits the next step.

Each operation has a two-minute attempt budget and each pass a five-minute budget.
Committed progress immediately advances the next pass and broadcasts a wakeup
to the other migration lanes in the same manager. These notifications contain
no authority; every lane still rereads PostgreSQL. Idle lanes reconcile every
second to recover missed notifications and commits from other replicas. Failed
passes retain a one-second retry delay even while other lanes make progress.
Continuous successful work yields for ten milliseconds after eight passes.
A cursor traverses unresolved work
without letting the first failed batch starve later operations; cancellation
preserves the unvisited suffix. This loop is separate from terminal cleanup, so
an image transfer cannot occupy that worker. Every manager replica can run it;
the existing database CAS and node journals serialize effects, and each replica
uses its own authenticated node channels. A replica without the relevant node
stream fails that attempt and retries on a later pass.

Optional peer transport is configured with `nomad_runtime.migration_peer_address`
on ctld, using a concrete private IP and fixed TCP port, for example
`10.0.1.10:19443`. It requires the existing migration staging quota. The primary
ctld owns the listener; standby instances must not bind it. Both nodes need the
configuration and private connectivity. An empty address leaves regional image
download in use.

Each daemon generates a private TLS identity. The destination's public
certificate digest is durably retained with staging reservation and returned
over the existing authenticated manager channel. The region binds that digest
into the source publication command. The source's public endpoint certificate
is included in its immutable publication receipt. The target validates that
exact trust anchor, and the source serves bytes only to the exact destination
certificate authorized for that operation. Neither exchange contains a bearer
token or private key. Redirects, DNS endpoints, public addresses and ambient
HTTP proxies are not used. The listener bounds concurrent connections and
request headers. The source independently limits each stream to two minutes;
cancellation or regional cleanup expires its socket write deadline so a
non-reading destination cannot retain source custody indefinitely. The target
also uses the authenticated migration operation's context for its request.

The peer stream carries the existing canonical manifest followed by its file
chunks. The target verifies the regional manifest digest and binding, checks
staging capacity before creating files, validates each chunk and fsyncs the
completed image before journaling preparation. Source custody remains locked
during streaming so cleanup cannot remove the files. A partial or corrupt peer
image is discarded before falling back to the exact regional reference; bytes
from the two sources are never combined. A daemon restart changes its key;
existing staging receipts retain their original key and safely fall back.

The stream contains the runsc execution image only. It does not contain the
block-COW RootFS or unchanged RootFS blocks: the publication binding carries
the exact RootFS generation and descriptor, and the destination reattaches that
regional generation through the existing writer/claim path. The bytes that can
still dominate this transfer are guest memory and other runsc execution state.

Regional publication uploads up to four chunks concurrently with at most four
8 MiB chunk buffers, then publishes the manifest only after all chunks succeed.
The manifest format and immutable retry checks are unchanged.

This transport currently starts after regional publication completes. It
eliminates the regional image download but does not remove the durable upload
from the critical path. Source fencing, RootFS durability, restore authorization
and command-ready publication are unchanged. Unit and transport checks are not
an end-to-end latency SLO; measure trigger-to-first-successful-user-command on
two real nodes for each memory size, cache state and dirty-filesystem workload.

A September 21, 2026 transport probe used two temporary `ecs.c7.xlarge`
instances in the same `us-east-1a` private network, each with a 100 GiB ESSD
PL0 system disk. Random image data traveled through `WritePeerImage` and
`ReceivePeerImage` over pinned TLS 1.3 with an authorized client certificate.
The timer began before the HTTP request and ended after digest verification,
file and directory fsync, and stream termination. Transfers were serial:

| Image bytes | Samples | Observed transfer and fsync seconds |
| --- | --- | --- |
| 128 MiB | 5 | 0.390, 0.834, 1.018, 1.024, 1.018 |
| 256 MiB | 2 | 1.386, 2.042 |
| 512 MiB | 1 | 3.884 |

This probe used a memory object store to construct the reference before the
timer and a small authenticated HTTP handler around the checkpoint transport.
It did not run the ctld listener, migration coordinator, runsc checkpoint or
restore, RootFS handoff, regional object upload, or command-ready publication.
Source files were already cached after publication; target directories were
new, but neither node's caches were explicitly dropped. The samples cannot
establish a percentile SLO or isolate network throughput from disk throughput.
In particular, 512 MiB exceeded three seconds in this transfer stage alone.
The two test instances and their temporary network resources were released
after the probe. End-to-end trigger-to-command-ready under three seconds remains
unverified.

The checkpoint adapter also passes stock runsc's
`--exclude-committed-zero-pages` alongside `--compression=none`. This preserves
zero-filled memory implicitly in the existing runsc format. It matters after
runtime usage sampling marks pages known-committed: the default runsc save path
does not rescan those pages for zeros. Scanning costs CPU and does not reduce
random or otherwise nonzero memory, so it is not a general memory-size limit.

A subsequent two-host probe on September 22, 2026 (Asia/Shanghai) used the same
instance type and disk class and the pinned stock `release-20260914.0`
distribution. A guest allocated and populated 128 MiB, then either zeroed it or
retained random contents. After a `runsc events --stats` observation, the
following single samples compared the existing arguments with zero-page
exclusion. Each real execution image was transferred using the pinned TLS peer
stream and restored on the other host:

| Workload | Zero-page exclusion | Image MiB | Checkpoint seconds | Transfer + fsync seconds | Restore seconds |
| --- | --- | --- | --- | --- | --- |
| 128 MiB zero | Off | 130.77 | 0.095 | 0.344 | 0.057 |
| 128 MiB zero | On | 2.56 | 0.093 | 0.020 | 0.033 |
| 128 MiB random | Off | 130.81 | 0.099 | 0.345 | 0.059 |
| 128 MiB random | On | 130.43 | 0.101 | 0.384 | 0.058 |

Before usage sampling, both zero-page cases were already about 2.6 MiB. All
eight cross-host cases retained the process token, PID, open-unlinked-file
offset, tmpfs sentinel, CPU flags and full workload memory digest after restore.
The source was deleted before transfer, and the destination made further
progress and checkpointed again. Tests can reproduce the workload with
`SANDBOX0_CHECKPOINT_MEMORY_MIB=128`,
`SANDBOX0_CHECKPOINT_MEMORY_PATTERN=zero` or `random`, and
`SANDBOX0_CHECKPOINT_OBSERVE_USAGE=1` in the explicitly enabled cross-host probe.

These are separate stage timers, not a trigger-to-ready measurement. The
reference was published into a memory store outside the transfer timer; the
probe used a small TLS handler rather than the ctld listener. RootFS was a
prepared read-only test bundle, and neither regional object upload, Nomad
placement, manager handoff, target creation nor authenticated command-ready
publication was timed. Source caches were warm and caches were not dropped on
either host. One sample per case does not establish a percentile or a
three-second full-migration SLO.

A lost node response or database acknowledgement retries the same immutable
command. Each new pass rereads PostgreSQL, so a committed receipt survives manager
restart without another in-memory queue or operation ledger. Once physical
source fencing commits, the operation leaves this transfer queue in the existing
`committing` lifecycle phase. Neither carrier is released, no destination writer
is issued, and the public runtime generation remains unchanged. The existing claim planner now has an internal `RestoreMigration` entry point.
It reuses deterministic writer-token derivation, network preparation, stage
construction and authenticated claim delivery, but acquires only the reserved
migration target and issues its writer through the migration-specific PostgreSQL
gates. The returned slot, resource lease and endpoint must match the committed
image destination. Migration claim identity comes from the immutable assignment;
writer grant identity and token remain stable across lost responses. The
migration target uses its database-owned claim TTL rather than an ordinary
claim caller's configured TTL.

Before node delivery, the planner persists a tokenless restore command and
requires its exact image and stage in the claim. An ordinary active reply,
missing receipt, changed digest or uncertain restore is an error; none can
trigger a fresh entrypoint. The planner returns immediately after a complete
restore receipt. It does not run the ordinary command probe, publish command
readiness, or count migration work as ordinary startup-SLO observations. Procd
generation handover must happen before those later steps.

Planner tests reuse the complete ordinary network/writer path and inject writer
issue response loss, restore response loss, unsupported authorities, changed
targets/resources/policies and invalid restore replies. They verify exact retry
identities, a single writer epoch, no bearer token in persisted restore authority,
and no early procd probe or command-ready call. Ordinary claim regression tests
cover the shared implementation.

The manager now installs a separate destination recovery lane alongside its
configured claim planner, using the same writer-token key and authenticated node
hub. It reuses the transfer scheduler's bounded batch, per-attempt/pass budgets,
cursor fairness and cancellation behavior. Its indexed PostgreSQL queue requires
a committed physical source fence, the immutable source policy snapshot, an
uncommitted public generation and no stored restore receipt. Reading a candidate
does not refresh TTL or admission. The migration-specific planner still rechecks
target attachment, writer and restore authority before execution.

After a complete restore response, the lane verifies the exact image and policy,
then records it through the existing procd handover authorization transaction.
A missing, uncertain or mismatched restore cannot reach that transaction. A lost
restore response retries the same node command; a lost handover-commit response
is recovered by the next database scan without restoring again. The operation
leaves this queue only after restore evidence and the exact procd handover command
are durable. Both carrier leases remain held and public routing stays on the
source. This lane cannot treat command creation as handover completion; the
separate handover lane below owns delivery and readiness.

PostgreSQL-backed destination-worker tests use a controlled restorer to exercise
response loss, commit failure, exact policy delivery, queue exclusion before
physical fencing and rejection of incomplete restore evidence. They separately
verify retained leases and the absence of handover acknowledgement or generation
publication. They do not replace the planner's execution tests or real two-node
network and storage acceptance.

The manager also installs an independent handover recovery lane with the same
bounded scheduler and authenticated node hub. It scans committed procd commands
whose destination generation is not yet public. Each candidate read rechecks
source-generation lifecycle ownership, hard TTL, the exact target writer, claim
lease and node heartbeat. It does not extend any lease. The target procd address
is derived from the persisted applied network token, never the current public
source route or a caller-supplied address.

The first step signs a short-lived manager token scoped to the exact command,
team and sandbox, sends the internal procd restore handover, validates the
original process UUID and target generation, and commits its acknowledgement.
The next pass runs the existing authenticated command probe against that same
address. A replacement procd instance cannot satisfy it. The exact response
body digest and restored launch identity form the existing command-ready proof,
which travels over the authenticated node channel. The node and regional
readiness transactions retain responsibility for generation CAS and destination
adoption; the worker validates the exact adoption receipt in the response.

Committed procd receipts suppress repeat handover delivery. A lost readiness
response after generation commit removes the operation from this queue; existing
ctld adoption retrieval and receipt delivery retain recovery responsibility.
Both resource leases stay held until the independent source finalization path
receives physical cleanup evidence. The lane exposes no user API, migration
switch, or destination selection. Startup requires the scoped token signer and
handover client alongside the normal claim planner.

PostgreSQL-backed handover tests use controlled procd and node implementations
to exercise lost replies, failures before and after receipt/readiness commits,
manager recreation, concurrent replicas, changed procd identity, and expired
TTL/writer/heartbeat/claim authority. The node test double calls the real
readiness and adoption database transactions but supplies controlled physical
evidence. Separate signer tests verify Ed25519 tokens and exact permission
bindings. These tests do not prove live node, storage or network convergence.

PostgreSQL-backed coordinator tests inject lost node responses and failures
before and after each receipt commit, construct a fresh manager coordinator
between retries, and exercise concurrent replicas. They verify command-before-
dispatch ordering, target-image-before-fence ordering, unchanged public generation,
and retention of both resource leases. Node execution in those tests is controlled;
it does not prove real storage/network handover or cross-host acceptance.

Manager also runs a bounded source-recovery worker for already authorized
captures that do not yet have a publication command. The optional
`migration_recover` capability is separate from `migration_capture`: it can read
existing ctld custody and finish the RootFS cut, but cannot initiate or replay
runsc checkpoint. It does not depend on the driver control socket and never
falls back to the initial capture command. Unsupported nodes retain their
pending work. As with other optional capabilities, upgrade the regional channel
receiver before enabling it on nodes.

The worker reads the immutable preparation, capture and source CPU evidence
from the existing lifecycle row. An elapsed CPU-preflight window does not erase
an already authorized capture. Missing custody, an unfinished intent or an
uncertain capture cannot authorize publication. For a completed memory capture
without its filesystem cut, ctld retries the exact writer seal under its existing
per-slot reconciliation. Unexpected source execution invalidates the image; it
does not kill a running source to make an older image appear consistent. After
sealing, ctld rereads and validates durable custody before responding.

Only a completed, consistent cut reaches the existing publication transaction,
which rechecks writer and lifecycle authority before retaining the immutable
command. CPU requirements come from the original guest launch profile, including
its retained lineage after previous moves. Manager recreation, concurrent
replicas and lost replies reuse that command. A committed publication removes
the operation from this queue and hands it to the existing transfer worker;
source recovery itself never uploads an image or advances the public generation.

PostgreSQL integration tests cover lost node replies, failures before and after
publication authorization, manager recreation, concurrent replicas, expired
preflight history and writer changes during recovery. Node tests reopen the
durable journal with no driver control connection, verify exact seal retry and
reject resumed source execution. Separate mTLS tests reject unsupported recovery
capabilities and changed node boot/UID without dispatching capture. The database
tests use controlled physical evidence and do not establish cross-host acceptance.

Gateway and network convergence, complete failure reconciliation and full
cross-host acceptance remain required. The regional evacuation and execution
workers are connected, but their presence does not prove these remaining gates. The publication contract carries
CPU evidence derived from the retained launch profile. Regional transactions
now require both fresh authenticated preflight receipts before first preparation
or capture authorization; a host observation alone cannot satisfy those gates.
Manager now discovers draining-node workloads, reserves compatible capacity
and dispatches eligible captures through its internal workers.
Live migration must not be advertised as available until the complete path and
its acceptance gates below pass. Existing pause/resume remains filesystem-only.

The first full-stack acceptance setup uses two isolated Linux workers with
Nomad 1.11.3, stock runsc `release-20260817.0`, ctld A/B, physical cgroup limits,
project-quota-enforced XFS staging, block-COW RootFS and encrypted objects in a
regional S3-compatible store. A digest-pinned Python template was imported and
a sandbox created through the public cluster gateway. Draining its source node
caused manager to reserve capacity on the other node without a user migration
request. This setup and reservation are not evidence of a completed migration.

That acceptance attempt exposed an eligibility bug: native CPU observation
enumerated `0,1,2,3`, while the resource lease used `0-3`. CPU containment now
recognizes adjacent entries as covering the same range, while still rejecting
gaps. Missing launch evidence is never repaired from a later host observation;
the initial sandbox remained ineligible and a new launch is required after the
fix. Direct public manager delegation also now generates a signed operation
identity independently of optional audit delivery, so standalone claims do not
depend on enabling an audit backend.

Deleting a migration before any source preparation/capture authorization now
atomically releases its unattached destination reservation and retires that
carrier. Maintenance can also recover an older aborted lifecycle with such an
unused active lease, preserving its original abort timestamp and reason. Every
release rechecks the absence of both durable execution commands and any target
claim/writer attachment. An aborted phase alone cannot release prepared or
captured work. The isolated cluster verified recovery of the first attempt's
remaining target lease; prepared/captured termination remains a separate
execution-proof acceptance gate.

A second sandbox with retained launch evidence passed CPU preflight and both
staging reservations, captured its running Python REPL, sealed the real
block-COW/NBD/XFS RootFS and published its encrypted execution image. Destination
image transfer also completed. Source fencing then rejected a mount identity
comparison: the mounted RootFS inode differs from the underlying warm carrier
directory inode. The corrected check uses a nonrecursive detached parent mount
view to inspect the underlying directory without detaching the live RootFS.
An exact consumer mount namespace and path remain required. The existing
captured operation was retained for retry. A subsequent ctld handoff exposed
a frozen-XFS superblock reference after NBD owner exit. The isolated test host
required an explicitly recorded emergency thaw to clear that intermediate
failed cleanup; this round is assisted debugging evidence, not unattended
migration acceptance. With physical absence proven, the target restored the
stock-runsc image, but procd handover at its new IP timed out. Target writer and
claim lease expiry then fenced execution. Public generation did not advance,
and the image was not replayed. A separate HTTP listener then proved that the
temporary cloud setup reached the peer's host address but could not reach even
its host-owned CNI gateway address. Adding an isolated IPIP transport with
explicit return routing made both checks pass. This diagnoses an environment
routing defect. A subsequent ordinary sandbox was created on B through the
gateway on A, and its Python REPL executed two commands through that same
gateway. This proves ordinary cross-node guest routing, not restored process
continuity. A fresh, unassisted migration and complete failure cleanup remain
required.

Deletion of an earlier aborted operation also exposed a migration-history
foreign-key retention issue. Schema migration 00082 separates the immutable
source writer ID from its live storage-retention reference. Sandbox deletion
releases that reference in the same transaction as RootFS cleanup only after
both carriers have terminal physical proofs, both resource leases are released,
the source writer is terminal, and any staging reservations have matching
release receipts. The migration must be either completed or canceled before
capture with the required preparation-cancellation receipt. Migration history,
commands, receipts and the original writer ID remain available; they cannot
authorize another execution. A pending or merely aborted captured migration
still blocks deletion. Failed deletion rolls back storage-reference release.
The isolated full-stack environment upgraded from schema 81 to 82 and then
automatically completed deletion of the first, unprepared aborted sandbox:
its public GET returned 404, the source writer row was removed, and the immutable
migration history remained. The second, restored-but-uncommitted operation
retained its writer reference and lifecycle custody.

The environment subsequently upgraded to schema 83 and the failure-stop ctld.
Manager automatically classified the second operation as failed and committed
the target's exact container-absence receipt. Both slots remained quiescing,
both resource leases remained active, public generation stayed at 1, and source
finalization and storage release remained absent. No image was replayed. This
is real node-channel stop evidence for the previously assisted attempt, not a
successful migration or completed cleanup. Isolated PostgreSQL tests cover lost
stop replies, worker recreation, immutable receipts, transaction rollback and
exclusion of forward execution; node tests cover journal reopening and retention
of images, staging and RootFS custody.

After upgrading both node runtimes and Nomad drivers and then manager to schema
84, this same failed operation automatically completed source physical cleanup,
allocation purge and source capacity release. Its source slot became terminal,
the source resource lease was released and source staging release was committed.
The target slot remained quiescing with its lease active and staging retained;
the lifecycle remained committing, public generation stayed at 1 and storage
retention remained attached. No manual source cleanup or execution replay was
used in this cleanup phase. The earlier assisted restore still prevents treating
the overall attempt as unattended migration acceptance.

The schema 85 rollout subsequently completed target physical cleanup for this
same operation through the authenticated node channel. The target writer became
retired and the cleanup receipt was committed automatically. Source state stayed
terminal/released; target state stayed quiescing with an active lease, retained
staging and storage references. The lifecycle remained committing and public
generation remained 1. This validates the physical cleanup phase, not target
artifact finalization, complete failure recovery or successful live migration.

With schema 86 and the corresponding ctld/driver rollout, the same target
automatically committed artifact finalization and released staging. Both sides'
staging receipts were committed, while the target lease remained active, the
lifecycle remained committing and the old public generation remained 1. The full
root runtime, RootFS session and node-channel regression suites passed; targeted
driver source/failure finalization tests passed with the race detector. A broader
driver migration run exceeded its four-minute test budget while building fixture
RootFS data and is not counted as a passing full-suite result.

After upgrading ctld and driver on both hosts and manager to schema 87, the
same failed operation automatically completed target allocation purge, physical
absence acknowledgement and terminal release. Both slots became terminal and
both leases released. The migration was aborted, the failed generation 2 was
consumed without a successful-generation commit, and the already-terminating
sandbox was deleted with both staging receipts and storage-reference release.
This phase used no manual runtime cleanup or execution-image replay. Database
failure/staging/evacuation/termination regressions, node/runtime/channel suites,
architecture checks and targeted driver finalization tests passed with the race
detector. A fresh unattended success path is still required; recovering the
previously assisted attempt establishes failure convergence only.

Termination requests preserve an already dispatched migration's lifecycle phase
while committing the sandbox's terminating intent. Generic claim cleanup waits
for migration cancellation or completion before selecting a physical slot.
Preparation cancellation can then acknowledge the exact source gate and release
the unused destination reservation. Captured or restored operations still need
the separate two-sided failure cleanup protocol; preserving custody is not a
claim that that protocol or full termination acceptance is complete.

## Node staging quota

Regional execution-image reclamation is separate from node staging. Schema
00089 records an immutable GC binding and completion receipt on the migration
row. Manager authorizes that binding only after publication, source physical
and allocation retirement, source lease release, both staging releases, and
either successful destination adoption/commit or completed failure cleanup.
These retained receipts survive sandbox deletion. Object age, an absent manifest
or an aborted lifecycle alone cannot authorize reclamation.

The collector derives one exact prefix from the validated publication binding.
It validates the whole returned page before deletion, rejects unexpected names
or escaped keys, and removes at most 64 objects per pass. It restarts listing
at the beginning after deletion and records completion only on an empty list.
Context cancellation reaches provider list/delete calls through both prefix and
encryption wrappers. Lost deletion or completion replies remain retryable;
another migration's chunks are never shared under this prefix. No new user API
or bucket-wide age rule is introduced.

The isolated cluster's schema 89 rollout automatically reclaimed five eligible
published images, including both successful moves and failed target restores.
Unit tests cover partial deletion, page boundaries, bad listings, cancellation
and namespace isolation; PostgreSQL tests cover retained physical/staging gates,
concurrent authorization, completion retries and unchanged target capacity.
This does not recover a still-ambiguous publication or authorize deleting its
objects. Such custody must first reach an explicit terminal recovery protocol.

`pkg/migrationstaging` verifies an operator-provisioned XFS project quota before
ctld admits new execution-image writes. This is node infrastructure, not a user
migration setting. The pool is `migration-images` beside
`nomad_runtime.runtime_slot_journal_path`; with the default journal it is
`/var/lib/sandbox0/ctld/nomad/migration-images`. Source and destination images
share its hard byte and inode limits across slots and ctld A/B primary changes.
Keep the journal outside this project so exhausting image space cannot consume
its quota. Provision enough physical disk for this pool, bounded RootFS dirty
tails, caches, journals and filesystem metadata; a project quota is a ceiling,
not a reservation of free physical space.

The guard requires Linux `quotactl_fd` support (Linux 5.14 or newer), XFS with
project accounting **and enforcement**, a private root-owned directory, its exact
nonzero project ID and limits, and inherited project membership. Startup checks
existing descendants too: uncharged legacy files, symlinks, other filesystems,
special files and noninheriting directories are rejected. Realtime allocation
is unsupported. ctld never formats storage, sets quotas, reassigns existing
images or deletes custody evidence to make this check pass.

Provision this on an empty, fenced node before either ctld instance opens the
pool. First mount its data filesystem with `prjquota` through the host's normal
storage provisioning. Do not format or remount an active node as part of enabling
migration. For example, on an already provisioned quota-enabled XFS filesystem,
with an unused project ID and a **new** staging directory:

```sh
    staging_root=/var/lib/sandbox0/ctld/nomad/migration-images
    staging_mount=$(findmnt -n -o TARGET -T /var/lib/sandbox0/ctld/nomad)
    test ! -e "$staging_root" || exit 1
    install -d -m 0700 -o root -g root "$staging_root"
    xfs_quota -x -c "project -s -p $staging_root 1395654657" "$staging_mount"
    xfs_quota -x -c "limit -p bhard=8g ihard=4096 1395654657" "$staging_mount"
```

The sample paths must be canonical and contain no symlinks or whitespace. Set
matching values in the common ctld configuration for both instances:

```yaml
    nomad_runtime:
        migration_staging_project_id: 1395654657
        migration_staging_bytes: 8589934592
        migration_staging_inodes: 4096
```

These are illustrative capacity values, not a supported sandbox-size guarantee.
Byte limits must be 4096-byte aligned, between 1 MiB and 1 PiB; inode limits must
be between 16 and 16384. All three values zero disables new migration image
writes. Partial or invalid configuration fails static validation. A configured
but unavailable quota logs an error and disables new migration writes while
ordinary runtime service continues. Once provisioning is repaired, a normal
ctld primary restart reopens the guard. Do not change project membership, quota
limits, filesystem mounts or directory identity while migration custody exists;
these are trusted host-administration boundaries, not guest permissions.

Each new capture/download checks the pinned directory identity and exact kernel
limits. A full pool rejects admission. For destination downloads, the immutable
manifest is authenticated before any directory creation or chunk fetch. The
node then checks the complete image footprint against remaining quota bytes,
quota inodes and filesystem-available bytes. It rounds file data to 4 KiB and
includes empty-file inodes, distinct parent directories and the image root,
with one block of headroom per directory. The manifest rejects trees exceeding
2048 total files/directories, matching the source inventory walk bound. A
failed quantity check leaves the existing destination intent retryable without
creating a partial directory. Prepared-image verification and restore reads do
not request fresh allocation.

This footprint is an admission requirement, not a guarantee of filesystem
metadata overhead or a reservation against concurrent writers. A successful
observation alone cannot replace both regionally committed source/target
staging receipts before preparation. XFS enforces the limit during writing and reports
`ENOSPC` for project-quota exhaustion; the check does not promise that all of a
future image fits. Legacy captures without an exclusive reservation can still
compete for quota. Failed or uncertain source capture retains its existing custody and
must not be retried as fresh execution or silently thawed. Recording an existing
capture outcome and reading recovery evidence do not require fresh admission.
An exact incomplete destination can be removed only after proving absence of
both its runsc container and RootFS session; admission is rechecked afterward.
Already verified image reads do not require free quota. Open-unlinked files
remain charged, and XFS may defer reclamation after their final close; a retry
must observe kernel accounting instead of resetting an application counter.

The opt-in probe owns a disposable loop-mounted XFS filesystem on an isolated
Linux host with root, `xfsprogs` and loop mount support:

```sh
    SANDBOX0_RUN_PRIVILEGED_STAGING_QUOTA=1 go test -race ./pkg/migrationstaging -count=1 -v
```

It tests shared image limits while the enclosing filesystem still has space,
open-unlinked charging, eventual reclamation, inode exhaustion, primary reopen,
and rejection of changed limits or disabled enforcement. It also checks known
image quantities against partially occupied byte/inode budgets. Transfer tests
verify rejection before chunk reads or directory creation, invalid-manifest
rejection before admission, cancellation and exact retry after capacity returns.
This test does not prove full migration capacity admission or two-node
execution handover.

### Exclusive node admission

ctld now persists exclusive migration-staging admission in the existing slot
journal. The optional `migration_staging_reserve` and
`migration_staging_release` mTLS channel commands bind the exact source capture
identity, both node/allocation/boot placements, destination resource-lease digest,
and a bounded byte/inode admission floor. They neither prepare procd nor invoke
runsc, and do not accept a host staging path. Reserve and release have different
sealed command identities and response types; a reserve receipt cannot satisfy
a release command. Unsupported nodes reject dispatch. Upgrade the regional
receiver before enabling these capabilities on nodes.

The journal first records an exclusive intent, then checks the enforced XFS
pool and available byte/inode quantities, and finally records readiness. Quota
failure or interrupted admission retains the intent. First capture/download
intent and reservation exclusion share one Bolt transaction, so another migration
cannot pass between the scan and the write. A reservation also excludes legacy
image writers without reservations; conversely, an already-retained legacy image
prevents a new exclusive reservation. Existing captures are never backfilled with
a reservation to manufacture pre-capture evidence. Ordinary sandbox execution
does not consume this staging admission. The separate two-custody count bound
also applies before reservation readiness, including image-free adoption
receipts still awaiting regional acknowledgement.

Exact retries after readiness verify the pinned pool but do not charge the same
minimum capacity again. Reopening the journal preserves exclusivity, and wall
clock expiry never releases it. Release binds the original request and requires
either no image custody or an existing source-finalization/destination-adoption
image-absence proof. It serializes with active capture/download reconciliation.
A released request cannot be revived by a late reserve or capture command. A
subsequent migration may acquire a new reservation after release, while previous
adoption history remains intact. Cancellation can arrive before the initial
reserve: ctld records an exact released tombstone instead of leaving the late
request free to acquire untracked custody. Only a higher lifecycle epoch of the
same live sandbox can replace a released reservation, preventing an older
reserve/release from overwriting newer cancellation evidence. A delayed release
from a lower epoch is acknowledged without modifying the newer reservation;
this allows recovery of a lost release reply after the next migration begins.
Completed physical cleanup of an unused carrier also proves that a late reserve
cannot acquire it. Cleanup intent alone does not establish that fact. Version 8
journal envelopes require readers that understand this custody; older A/B
binaries must not ignore it.

This is durable exclusive **admission**, not physical block preallocation. The
minimum quantities do not predict or cap a future checkpoint's size; the XFS
project quota still supplies the hard node limit. Shared-filesystem consumers,
metadata costs and a larger-than-admitted execution image can still cause a
write failure. No successful reservation permits replay of an uncertain capture.

Manager now runs separate bounded staging-admission and staging-release workers
on the authenticated node hub. The existing regional migration row retains one
immutable source-shaped request, from which both node commands are derived,
and each node's receipt. The node with the lexically lower durable UID is always
acquired first, regardless of migration direction; its committed receipt is
required before dispatch to the second node. This prevents opposite-direction
migrations from holding opposite pools while waiting for each other.

Initial budgets are system-selected: the source's hard memory lease plus 64 MiB
of serialization headroom, rounded to 4 KiB, and 2048 image file/directory inodes.
These are admission floors, not supported-image-size guarantees. The original
budget is immutable and survives retries or manager upgrades. First dispatch
and receipt commits recheck exact live placements, both CPU receipts, writer
authority and the original two-minute reservation/preflight windows. Retries do
not extend them. Both staging receipts are required for first procd preparation
and first capture authorization, enforced by Go transactions and database
guards. Recovery of an already committed execution command retains its previous
authority. Legacy already-prepared operations without staging evidence cannot
backfill it to authorize a new capture.

The release worker persists each release intent before dispatch and retains its
exact acknowledgement. A never-prepared, aborted lifecycle can release both
nodes, including when one reserve command or reply was lost. A prepared lifecycle
can use this unused-reservation path only after the source acknowledges the
retained cancellation command and the regional transaction aborts the lifecycle;
no capture may have been authorized. Once capture is authorized, destination
release requires its committed adoption image-absence proof; source release
requires its separate finalization proof. These historical cleanup
commands do not depend on current CPU freshness, public generation, hard TTL or
current sandbox desired state. One receipt does not release the other side.
Node-loss recovery remains part of the broader incomplete failure reconciliation.
Neither worker can prepare procd, checkpoint execution, create a migration
intent or select a destination. There is no public migration endpoint, SDK
option or user-selected node.

Race tests exercise concurrent reservation attempts, interrupted quota admission,
journal reopen, lost replies, legacy-writer exclusion, changed boot/resources,
late commands after release and release during an active download. Controlled
runtime tests take a reserved source through physical finalization and a reserved
destination through restore/adoption and a subsequent migration. mTLS tests cover
optional capability negotiation, distinct actions and node failures. They do
not replace the required cross-host Nomad/gVisor/storage acceptance run.
PostgreSQL tests additionally exercise both acquisition orders, missing receipts,
immutable budgets, concurrent replicas, lost reserve/release replies and failures
before and after receipt commits, with recreated manager workers between retries.
Their node/physical proofs are controlled fixtures, not cross-host evidence.

## Observable behavior

A successful migration preserves sandbox identity and the running workload's
processes, memory, guest PIDs, supported open files, and runtime tmpfs contents.
It changes the physical allocation and advances the runtime generation. The
guest pauses during the consistency cut and resumes on the selected node.
The current implementation uses stop-and-copy: the source stops before state
transfer and remains stopped until the destination resumes. It does not yet
implement iterative memory pre-copy or promise zero downtime.
Migration must never silently restart the entrypoint to make a failed restore
look successful. User pause/resume, RootFS snapshots and forks retain their
existing, distinct semantics.

The initial correctness target permits external connections to reset. Requests
already sent to external services are not automatically replayed. Existing
connections in ctld's host-side egress proxy and gateway streaming sessions are
outside the gVisor execution image. Preserving those connections requires a
separate network continuity design; restoring guest TCP state alone is
insufficient. Workload execution state must still be preserved even when an
application subsequently handles a connection error.

Migration is initially confined to compatible nodes within one cluster and
region. Source and destination use an identical runsc execution shape and an
explicit CPU-feature compatibility check. These are system eligibility checks,
not user controls. There is no fixed downtime claim: memory size, transfer
bandwidth, dirty RootFS data and restore work affect interruption time.

## Authority and consistency

PostgreSQL owns migration intent, phase, source and target incarnation,
execution authorization, and the committed image reference. Reuse the existing
sandbox lifecycle transaction and resource/writer ledgers. A local journal or
an object-store manifest must never become a second lifecycle authority.

The immutable image binding joins:

- team, sandbox, and migration operation identity;
- the digest of the existing durable source RootFS StageRequest, which binds
  node UID, boot ID, allocation, runtime generation, writer grant and epoch;
- runtime compatibility, source assignment and exposed CPU features;
- the exact RootFS generation and its complete descriptor at the execution cut.

`runtimecheckpoint.Bind` derives these fields from existing RootFS and runtime
contracts. Assignment digests use the `sha256:` prefix in the image manifest;
the underlying assignment revision remains the existing hexadecimal digest.
The regional publisher must compare this binding to its locked transaction.
Valid JSON and checksums do not grant permission to migrate or restore.

Stopping only incoming procd requests is insufficient: existing processes,
mapped files and background threads can still write. Source execution must be
stopped before capturing its matching filesystem generation, and must remain
stopped while that cut is published. The filesystem barrier must also cover
gofer/host-side writes. Existing running-fork freeze/checkpoint/thaw cannot be
used unchanged because it deliberately lets the source continue execution.

Execution images include memory and may contain credentials. Use the same
configured application-encrypted regional store as RootFS. Objects are scoped
by the final binding digest in version 1, or the exact capture scope in the
version-2 staging foundation described above, split into fixed bounded chunks, and created
conditionally. A manifest is published only after all chunks. Collision reads
verify bytes; downloads verify the committed manifest digest, exact binding,
file geometry and every chunk. Host image directories are private and must
never be mounted into the guest. Image size, concurrent transfers, staging disk
and abandoned-object collection all require bounded budgets.

## Transaction sequence

1. **Reserve.** Manager discovers an eligible source on a draining node and
   locks the sandbox lifecycle. It reserves target slot/resource capacity
   without issuing a second RootFS writer. Quota and compute metering continue
   to represent one user sandbox; temporary migration overhead is system cost.
   No compatible capacity means waiting while the source keeps running.
2. **Prepare.** Apply exact destination network policy and prefetch immutable
   RootFS data. Verify runtime version, CPU features and storage eligibility
   before disturbing the source. Serialize user lifecycle and network mutations
   with migration through existing transaction ownership.
3. **Capture.** Persist an exact migration custody intent in the ctld journal
   before invoking checkpoint. The driver and terminal reconciler must recognize
   the expected source exit, rather than classifying it as crash abandonment.
   Invoke stock `runsc checkpoint` without `--leave-running`, then seal the
   matching RootFS and publish the bounded execution image. An interrupted
   command or a directory's existence is not completion evidence.
4. **Fence.** Commit the complete checkpoint reference. Prove source execution
   and writer access revoked through the existing physical cleanup protocol.
   Do not release source capacity based on a checkpoint exit code, heartbeat
   timeout or Nomad record deletion alone.
5. **Authorize restore.** Atomically select the destination as sole execution
   owner, advance the writer epoch and authorize its exact runtime generation.
   Target commands are bound to node UID, boot ID, allocation and operation.
   Unknown outcomes retry or query that exact target; they cannot start a
   different target without fencing the first one.
6. **Restore and rebind.** Attach the exact checkpoint RootFS, download and
   verify the execution image, create the target runtime and call restore.
   Rebind the preserved procd process to its new runtime generation without
   calling ordinary activation or restarting supervised session attempts.
   Verify the expected source procd instance identity survived the restore.
7. **Commit.** Publish the new runtime routing only after authenticated
   command-ready verification. Complete source physical cleanup, release its
   lease, and remove image references only when recovery no longer needs them.

The compatibility digest currently covers runtime architecture and immutable
runsc settings, but not the exposed CPU feature set. Migration must add a
separate verified feature check rather than assuming the existing digest proves
cross-host restore compatibility. Memory, CPU quota and cgroup values remain
resource-lease inputs; they do not become warm-slot compatibility keys.

`gvisorcli.CPUProfile` provides a read-only observation primitive. It obtains
the current host's complete known feature names from stock `runsc cpu-features`,
including names not printed in `/proc/cpuinfo`. It requires a native ELF runsc
executable, checks that its file identity, size and modification time remain
stable during observation, and records its reported version. These metadata
checks are not executable attestation. The shared `MigrationCPUProfile` contract
requires canonical, sorted feature names. Its compatibility check requires a
target feature superset and the same runsc version and architecture. On amd64
it additionally compares gVisor's cache-line interpretation and an exact digest
of the native XSAVE layout and XCR0, conservatively rejecting different saved
floating-point state layouts. ARM64 uses the pinned runtime's fixed FPSIMD
representation and has no x86 layout fields. Runtime-version upgrades must
revalidate these architecture assumptions.
The pinned upstream references are the
[stock feature command](https://github.com/google/gvisor/blob/release-20260817.0/runsc/cmd/cpu_features.go),
[amd64 compatibility check](https://github.com/google/gvisor/blob/release-20260817.0/pkg/cpuid/cpuid_amd64.go)
and [ARM64 feature state](https://github.com/google/gvisor/blob/release-20260817.0/pkg/cpuid/cpuid_arm64.go).

This measurement is not a report of the CPU state exposed to an existing
guest. Before automatic capture is enabled, source launch evidence must be
persisted and bound to the exact runtime and node boot, and source and target
preflight must verify that the evidence still covers the runtime's eligible
CPU set. Existing guests without launch evidence must not become migration
eligible merely because a later host observation succeeds. Stock runsc's own
restore validation remains mandatory. The CPU observation does not grant
execution authority or change ordinary claim behavior.

`MigrationCPULaunch` now defines the compact historical binding needed by that
recorder. It binds the exact node boot, allocation and control endpoint,
sandbox generation, launch attempt, and existing writer/resource/assignment
digests to the observed CPU profile, CPU set and runtime bundle SHA-256 digest.
The binding also retains the immutable launch resource lease and verifies its
digest and placement before using its CPU set. This snapshot grants no capacity
and contains no writer bearer token or workload environment. `BindMigrationCPULaunch` checks an ordinary
regional claim, owns its feature-list copy, and rejects restore claims: a restored
guest's CPU state must retain its source-image history. This helper binds supplied
evidence; it does not establish that a launch observation actually occurred.

The production driver records ordinary launch evidence around its actual
`create/start` calls. Before advertising a new warm carrier it prepares one
shared, bounded CPU cache for the driver process. Preparation runs stock
runsc observations across the observer's eligible CPUs, requires homogeneous
profiles, hashes the complete runtime bundle, and retains native per-CPU
capability fingerprints. Migration currently qualifies only stock
`release-20260914.0`; an unqualified release can serve ordinary claims but
cannot acquire migration launch evidence through this cache. Preflight also
rejects older retained launch evidence before preparing or stopping the source.
Historical evidence stays readable for failure recovery. This release contains
the upstream [epoll restore repair](https://github.com/google/gvisor/pull/14249).
Qualification is an exact release gate, not a promise that arbitrary newer
versions or custom builds have passed migration acceptance.


Install the complete official archive: `runsc` and its adjacent `gvisor-bin/`
directory containing `checkpointgofer`, `gvisor-sentry-prewarmer`,
`gvisor_sentry`, `runsc-fd-parking` and `runsc-metric-server`. Installing only
`runsc` fails under this release's strict companion policy. The migration
executable digest binds the ordered names and content hashes of all six files;
node-local absolute paths are excluded. Missing, changed or symlink-resolved
companions and sidecar/release environment overrides exclude migration rather
than silently falling back to main-file identity. Source and destination must
have identical bundles. Routine claims do not acquire migration evidence if
this qualification fails.

Immediately before create and after successful start, the driver verifies
the original boot, eligible CPU set, native capability fingerprints and
executable identity. These checks do not execute runsc or hash its entire
bundle again. A bounded set of nonblocking inotify monitors, one per bundle
member and shared across carriers, invalidates the cache on writes, metadata
changes, rename, deletion, lost monitoring or queue overflow. This closes the same-clock-tick gap in metadata-only validation;
it assumes trusted, locally installed runtime binaries and is not protection
against a hostile host, mmap writes or remote filesystem changes. See the
[inotify limitations](https://man7.org/linux/man-pages/man7/inotify.7.html).
Replacement closes the old monitor and conservatively invalidates outstanding
witnesses. Shutdown closes the cache; it never accumulates a watch per carrier.

Migration preflight and capture/restore boundary checks now reuse this same
qualified warm evidence. Every check still freshly scans native capabilities
on every eligible CPU and revalidates the boot and all executable monitors.
A changed, closed or incomplete retained snapshot rejects migration; it is not
replaced by a fresh profile during the operation. Adapters with no prepared
snapshot retain the full stock-runsc observation and bundle-hashing path.
The original source launch binding and stock restore validation remain required.

On the isolated four-vCPU hosts, three component samples measured one bundle
hash at 0.254–0.260 seconds and CPU coverage at 0.035–0.038 seconds. A full
preflight hashes twice. Begin/complete warm revalidation took 0.0008–0.0012
seconds. These measurements explain an optimization opportunity; they do not
establish the complete migration latency.

Only successful start followed by successful verification can persist a launch
record with the active claim. The witness is one-use and expires after two
minutes. Each fast verification has its own 100 ms context budget, separate
from create/start deadlines. Failure leaves an ordinary claim operational
without migration evidence; it never falls back to a new slow observation on
the claim path. Lost-response retries reuse the persisted record. Restore
claims skip the ordinary recorder. After a successful restore and matching
before/after target observations, `BindMigrationCPURestore` retains the original
guest profile separately from the target host's observed profile. The driver
persists this evidence with the restored claim before advertising readiness.

The driver reports the added claim work as `cpu_launch_verify_us`. It is part
of end-to-end startup latency, even though the expensive observation happened
during warm-up. A local CPU probe does not establish the 200 ms startup target.

The source preflight adapter rejects absent or mismatched historical evidence
before measuring the current host. It then uses the resource lease's CPU set
and requires the same profile as at launch. A new source feature superset does
not repair changed history, and a larger fresh observation cannot widen the
recorded launch coverage. The destination adapter measures its own leased CPU
set and requires a superset of the inherited guest profile, subject to the
existing exact runtime and state-layout checks. Features present only on an
intermediate host never become requirements for the next destination. Both adapters hash the configured executable before and
after measurement and require its exact launch digest; a version string alone
cannot substitute for artifact identity. Local CPU numbers are never compared
across nodes. Errors and cancellation return no partial observation.

Launch recording, the local preflight adapters and authenticated node delivery
are implemented. The optional `migration_cpu_preflight` node capability routes
through the regional mTLS channel, ctld and the driver's root-only control
socket. Each request binds both placements, boot identities, resource leases
and the migration operation. The live source reads only its durable launch
record; the unclaimed destination receives that same record through the region.
Neither side can manufacture missing source history. Busy, fenced or changed
carriers reject the observation, including changes detected after sampling.
The separate response type cannot report command readiness, start a workload
or grant capture authority. The operation has its own two-minute deadline.

Nomad/plugin restart discards the process-local CPU warm cache. Recovery now
warms that cache for an unclaimed carrier before reopening its control socket
and regional heartbeat. This permits a later create/start to record its actual
launch evidence. Recovery never backfills evidence for an already running guest.
Warm-up failure preserves ordinary claims while keeping migration ineligible.
A real two-node attempt exposed this missing recovery warm-up: CPU preflight
rejected the evidence-less source, the reservation expired without preparation
or capture, and the source retained its process and generation. The corrected
path is covered through Nomad handle/database persistence and subsequent claim,
including failed warm-up and repeated recovery.

Regional retention and pre-capture freshness gates are implemented in the
existing `sandbox_runtime_migrations` row. Source authorization first records
the exact request and a PostgreSQL timestamp. Source and destination receipts
are immutable; concurrent retries neither replace the result nor extend its
two-minute validity window. The source launch attempt must also match the
regional active claim. The destination request is derived from that retained
source receipt, not from a caller-selected launch record. Every first receipt
rechecks live source/writer and destination/resource custody.

Manager now runs a bounded CPU-preflight worker on its authenticated node hub.
Reservation atomically retains the original migration assignment alongside its
digest; the database rejects mutation, erasure and late backfill. Recovery reads
that immutable input rather than reconstructing it from current sandbox config.
Legacy digest-only reservations are excluded from this worker and remain eligible
for existing unused-reservation expiry cleanup. No user API or migration option
is added.

Each pass advances one source or target receipt per operation. The source
command is committed before dispatch; the target command is derived only after
committing source history. Restart and concurrent manager replicas reread the
same PostgreSQL rows. A lost response retries the same read-only probe, while a
committed receipt suppresses subsequent dispatch for that side. Every dispatch
passes the existing transactional authorization again; stale work scans cannot
override expired writers, heartbeat failures, changed node identity or an
aborted reservation. Scanning excludes elapsed reservation/preflight windows,
already prepared operations and completed target receipts. It never renews
those windows, freezes procd, captures execution or creates new reservations.

PostgreSQL integration tests inject source and target response loss, receipt
commit failures before and after commit, manager recreation and concurrent
replicas. They verify invalid-receipt rejection, expiry and abort handling,
original-assignment retention, unchanged public generation and absence of
preparation/capture commands. Their node replies are controlled fixtures;
separate mTLS channel tests cover authentication, and actual cross-node CPU
eligibility remains part of full migration acceptance.

Initial preparation and capture authorization require both receipts, current
matching placements and an unexpired window. A database trigger also rejects
late first receipts and new execution authority after expiry. Exact recovery
of an already committed prepare/capture command retains its original authority;
expiry never silently releases capacity after a possibly delivered command.
Publication retains the full source launch evidence, derives its CPU digest from
the profile that authorized capture, and rejects a different caller-supplied
digest. Upload and recovery may outlast the preflight window without changing
that historical binding.

The driver remeasures the source before recording first capture intent and again
immediately before checkpoint after durable custody is confirmed. Each check
requires the recorded launch lease, unchanged runsc bundle digest and source
CPU profile, and rechecks local identity/admission after observation. Rejection
before intent leaves the source untouched; rejection after intent retains
uncertain custody and never replays checkpoint.

The destination requires source launch evidence in the immutable publication.
It remeasures its leased CPU set before restore intent, immediately before
restore, and after runsc returns before reporting completion. Any mismatch uses
the existing uncertain-restore fencing path, with no fallback to Start. Older
metadata-only publications remain readable but cannot authorize execution.
These observations are point-in-time checks, not a host hotplug lock or a proof
of continuous CPU stability during execution. Stock runsc validation remains
required. A restored guest's lineage binds the immediate source launch digest
and exact restore request digest to the inherited guest profile. It never embeds
a recursively growing history. Local source checks validate the link against
the durable restore command; regional receipt commits independently resolve the
current carrier's previous completed migration and validate the same link.
Omitting lineage, widening or narrowing guest exposure, or substituting another
restore is rejected. Older restored claims without this evidence remain
ineligible; observing the current host cannot backfill them.

The current host must still match its own restore-time observation before it
can become a source. For A → B → C, B may have extra features while C need only
support the original guest profile from A. A profile change during B's restore,
even to another compatible superset, fails the restore rather than manufacturing
a stable historical observation. Lost-response retries reuse the persisted
record; recovery never reruns restore to obtain missing history.
The qualified stock release saves and loads the guest's feature set, then checks
host compatibility before loading the rest of the kernel; see its
[kernel checkpoint path](https://github.com/google/gvisor/blob/release-20260817.0/pkg/sentry/kernel/kernel.go#L803)
and [static amd64 CPUID serialization](https://github.com/google/gvisor/blob/release-20260817.0/pkg/cpuid/cpuid_amd64.go#L51).

Automatic source orchestration is connected; complete node-loss recovery
remains required. Prepared sources whose capture eligibility expired can use
the cancellation worker when no capture command was authorized. CPU evidence
alone cannot authorize capture or retroactively certify old guests. These
CPU tests exercise authenticated
routing, optional capability rejection, identity and coverage checks, feature
changes, cancellation, concurrent cache use, executable
changes and digest custody. They do not prove a real migrated guest's historical
CPU exposure.

`gvisorcli.CPUCoverage` adds explicit per-CPU coverage. Its caller supplies the
CPU set from the intended runtime resource boundary. On a dedicated locked
thread it pins each measurement and its runsc subprocess to one requested CPU,
checks the applied affinity before and after sampling, and requires identical
profiles across the entire set. It restores and verifies the original thread
affinity before returning. If restoration fails, the goroutine exits without
unlocking its thread so Go retires that thread instead of reusing its modified
affinity. Any unavailable CPU, heterogeneous profile, cancellation or affinity
failure discards the whole observation. The operation has a two-minute upper
context deadline and rejects CPU sets beyond the supported kernel mask rather
than silently truncating them. Linux's
[affinity contract](https://man7.org/linux/man-pages/man2/sched_setaffinity.2.html)
and Go's [locked-thread lifecycle](https://pkg.go.dev/runtime#LockOSThread)
define these boundaries.

`MigrationCPUObservation` binds the measured CPU set separately from its
profile; `Covers` rejects reuse after eligible CPUs widen beyond that set.
Node-local CPU numbers do not become cross-node compatibility keys. Coverage
is a point-in-time observation, not a guarantee against later CPU hotplug,
microcode, cgroup or executable changes. Production migration still needs
complete automatic evacuation acceptance and restore-time reconciliation. Ordinary claims use the
bounded native cache verification described above, not fresh runsc commands.

The opt-in `TestStockRunscCPUProfile` probe uses
`SANDBOX0_CPU_PROFILE_RUNSC` to select a native stock binary. It observes CPU
state without starting a sandbox. It passed against official ARM64 runsc
`release-20260817.0` in an isolated Linux container, with 21 known features and
identical repeated profile digests. Protocol tests cover malformed evidence,
missing target features, changed runtime versions, cache lines and XSAVE
layouts. An amd64 cross-build is not native amd64 migration acceptance; neither
this probe nor the one-host checkpoint test replaces the two-node acceptance
suite below.
The companion `TestStockRunscCPUCoverage` probe passed against the same ARM64
binary across all four CPUs available in the isolated Linux container. A real
fork/exec test verifies each child has the selected single-CPU affinity and
that the caller's affinity is preserved. Fault tests cover missing CPUs,
heterogeneous profiles, ignored pinning, mid-measurement affinity changes,
cancellation, and failed or narrowed affinity restoration.

`TestStockRunscCPULaunchWarmCache` also passed against that official ARM64
binary in the isolated four-CPU Linux environment. It exercises warm-up,
native revalidation and executable hashing without launching a workload.
The cache tests prohibit further subprocess execution after warm-up, verify
concurrent witnesses and one-use completion, and reject stale boot, coverage,
hardware, expired or canceled evidence. File-monitor tests cover in-place
writes with restored timestamps, atomic replacement, rename, unlink, metadata
changes and closure. Driver tests traverse warm registration and actual claim
state persistence, including optional-recording failure, create/start failure,
lost-response retry and restored-claim exclusion. These checks do not establish
cross-host execution or network continuity.

On 2026-09-20, these CPU probes also passed natively on an isolated four-vCPU
amd64 ECS host (Alibaba Cloud Linux 4, kernel `6.6.102-7.alnx4.x86_64`, Go
`1.25.5`). The host used official stock runsc `release-20260817.0`, verified
against the deployment-pinned SHA-256
`048b89aada69dc3333422e139d6e9d02f8ab06bda52398060e0fbdacca00074c`.
The complete `pkg/runtimeslot`, `pkg/gvisorcli` and `pkg/nomadruntime` race
checks passed. The full `manager/pkg/runtimeslotnode` race suite passed after
its concurrent-cleanup test was changed to wait for both pending callers
instead of assuming they joined within a fixed 10 ms delay. Driver CPU
launch/preflight checks also passed, including the
private socket response, unchanged durable state, warm-target exclusion and
source fencing or history changes during observation. Architecture checks
passed against a clean source snapshot without historical untracked services.

On that same host, `TestPrivilegedExecutionCheckpoint` and
`TestPrivilegedProcdSessionCheckpoint` both passed with DirectFS enabled and
disabled. They used native stock runsc without the earlier container-host
cgroup workaround. These remain two-runtime-root, one-host primitive checks:
they prove restored process/session state, not cross-node disk ownership,
network continuity, migration downtime or production claim latency. The
automatic orchestration now has regional worker coverage; the full two-node
acceptance gates remain required.

The regional CPU custody tests use an isolated PostgreSQL 15.18 database on
that temporary host. They exercise missing/partial receipts, exact launch
identity, immutable deadlines, concurrent retries, source and destination
replacement, and late receipts rejected by both the store and SQL guard.
The target-carrier lock refreshes the reservation projection before comparing
CPU evidence, including when the control endpoint changed after the earlier
read. Expiry tests preserve recovery of previously committed commands and
publication while rejecting new preparation/capture authority. These database
tests use controlled node receipts; the native CPU/runtime probes above provide
separate runtime evidence rather than an end-to-end automatic migration claim.

Execution-boundary fault tests also passed under the race detector on that
host. They reject missing launch history, changed leases, executable or CPU
changes before capture intent and after journaling, and admission/history
changes during observation. Destination tests inject incompatibility before
intent, before restore, and after restore returns; each preserves uncertain
custody without entrypoint fallback or replay. Publication tests bind the full
launch evidence, including executable and resource lease, to the receipt. The
regional migration integration suite passes with this evidence retained across
publication and expiry. These are controlled fault tests, not two-host execution
or a measurement of migration downtime.

Repeated-migration custody tests on the same isolated PostgreSQL host complete
A → B, including source physical cleanup, then reserve C, retain both CPU
receipts, authorize a second capture, and publish its CPU binding. B's controlled
host profile has an extra feature; C supports only the original guest profile.
The store rejects missing or altered lineage and refuses publication using B's
host-profile digest. Driver tests independently persist the inherited evidence
through the driver restore, claim and adoption path with a controlled runner
and capture it again,
reject compatible host-profile changes during restore, and preserve exact retry
behavior. These tests use controlled node observations.

The native amd64 `TestPrivilegedExecutionCheckpoint` probe was then extended
and passed against stock `release-20260817.0` with DirectFS both enabled and
disabled. It performs two successive checkpoint/object-transfer/restore cycles
across three isolated runtime roots on one host, deleting each predecessor
before its successor starts. The original memory token, PID, open-unlinked-file
offset and tmpfs sentinel survive both cycles. The guest rereads `/proc/cpuinfo`
on every iteration; the digest of its feature flags remains unchanged. This is
evidence of repeated process-state restoration on that host, not heterogeneous
CPU migration, two-node filesystem ownership, external connection continuity,
or a migration downtime measurement.

## Failure and recovery rules

| Failure point | Required behavior |
| --- | --- |
| Before source capture | Keep source running; release only proved-unused target reservations. |
| Checkpoint command result unknown | Inspect exact node custody and execution state; never infer success from image filenames. |
| Regional upload or commit fails | Retain source cut and retry the same immutable publication; do not resume either side speculatively. |
| Source dies before a complete durable image | Apply the existing crash policy; do not claim successful process recovery from a filesystem checkpoint. |
| Source fencing unproved | Destination execution remains forbidden. |
| Target restore result unknown | Query/reconcile the same target; keep its execution and writer custody fenced against competing restores. |
| Target has executed | Never roll back to the old source image merely because a command-ready response or route update was lost. |
| Manager or ctld restarts | Recover from PostgreSQL intent and exact durable node journal; retry the same operation and incarnation. |
| User deletion or hard TTL wins | Serialize termination and clean both reservations; migration cannot resurrect a deleted sandbox. |

Writer fencing prevents divergent disk publication. It does not prevent a
second process copy from issuing external effects. Sole execution ownership
must therefore be established before target restore starts, independently of
routing visibility and independently of writer lease renewal.

Node maintenance must not evict the source until evacuation is committed and
cleanup is proved. Cloud lifecycle hooks retain their existing protection and
bounded heartbeat behavior. Migration is a planned movement capability, not
recovery of volatile memory from an already failed host.

## Verification

The focused local foundation checks are:

```sh
    go test -race ./pkg/gvisorcli ./pkg/runtimecheckpoint ./pkg/runtimecontrol ./pkg/procdapi ./manager/procd/pkg/runtimecontroller ./manager/procd/pkg/session ./manager/procd/pkg/http
```

Source custody and its private driver/ctld integration are Linux tests:

```sh
    go test ./pkg/runtimeslot ./pkg/nomadruntime ./pkg/rootfssession
    go test -race ./manager/pkg/runtimeslotnode -run '^TestMigration' -count=1
    (cd nomad-driver-sandbox0 && go test ./internal/driver -run '^TestMigration' -count=1)
```

These cover durable intent before checkpoint, exact retries, partial image
retention, driver recovery, lease loss, connection loss, and concurrent calls
through the private driver socket. Node tests reopen the Bolt journal, reject
ordinary cleanup and terminal-custody revival, and check the unresolved-capture
limit. Channel tests use real mTLS streams and reject unsupported capabilities
and another boot identity. Driver tests use a controlled runsc test double;
the stock-runsc probes below provide separate runtime evidence. Neither set
alone proves the complete migration lifecycle.

Filesystem-cut tests exercise the real block-COW WAL and immutable object
builder with a controlled filesystem runtime. They cover retained freeze
intent, interrupted freeze, object-publication failure, owner restart, changed
WAL rejection and exact descriptor replay. Private Unix RPC tests reject
unconfirmed capture, changed source identity and still-running sources; recovery
tests prove that source fencing cannot invoke ordinary RootFS crash cleanup or
revive an invalidated image. These tests do not substitute for the real
NBD/XFS/OverlayFS and two-node acceptance gates below.

Image-publication tests use the real checkpoint object store and reopen the Bolt
journal. They cover upload failure, immutable retry identity, retained source
custody, transport cancellation and daemon shutdown. PostgreSQL integration
tests reject unapproved publication, changed filesystem/writer/CPU bindings and
conflicting receipts; concurrent retries preserve the same durable receipt
without granting destination execution. These are component and transaction
checks, not evidence of a completed node-to-node migration.

Source-fence tests also cover interrupted teardown, owner restart before and
after physical proof, NBD ownership rejection, retained cut identity and denial
of ordinary reclamation. Node tests verify that deletion cannot begin for an
invalidated capture and that source fencing retains carrier custody. Regional
tests verify renewal revocation, proof-bound head publication, concurrent
recovery, immutable evidence and unchanged destination execution authority.

Source-finalization node tests reopen the real Bolt journal after RootFS,
cgroup and session-forgetting failures. They verify exact response replay,
no duplicate reclamation, retention until session forgetting is confirmed,
source-image absence, invalidated-evidence preservation and rejection of older
journal envelopes. The node-channel suite exercises the finalization capability
over real mTLS, including unsupported agents and changed boot identity. Host
network, cgroup and RootFS finalization operations are controlled test doubles
in this suite; the separate RootFS tests exercise real WAL and journal cleanup.
These checks do not prove Nomad allocation purge or regional resource release.

PostgreSQL source-finalization tests require committed target adoption, reject
replacement commands and incomplete physical evidence, and retain immutable
receipts under concurrent retries. They also verify cleanup after hard TTL and
desired termination while both resource leases remain active and the lifecycle
remains `committing`. The source fixture uses the same canonical runsc container
identity as the driver and source-fence protocol.

Regional completion tests require both the stored node receipt and allocation
absence. They inject lifecycle-commit failure after the source release update
and verify that PostgreSQL rolls back both the lease and slot state. Concurrent
retries release only the source once, while preserving target resources, public
routing, RootFS head and TTL even when desired termination has already won.
These transaction tests do not execute a real Nomad allocation purge.

Terminal-worker tests connect the node controller, reconciliation loop and real
mTLS node channel with controlled allocation and database boundaries. They
verify receipt-before-purge ordering, pending adoption, unsupported transports,
incomplete evidence, changed source identity, allocation still present and
failed transaction commits. Lost purge responses and restarted workers reuse
the committed node receipt; unavailable or malformed GC acknowledgements retain
the source for retry. Node tests reopen Bolt after acknowledgement, keep receipts
beyond their time limit before GC, reject changed bindings, and safely replay a
lost acknowledgement after pruning without recreating evidence. These tests exercise
the coordinated path, but their Nomad controller is a test double and does not
replace the two-node acceptance run.

The source-finalization private RPC tests verify that driver-visible completion
waits for compact-session deletion, and rejects another slot or invalidated
evidence. Driver tests cover Stop/Close after completion, pending or malformed
proofs, changed source identity, capture still in flight, stale state recovery
and late authority loss. A real Nomad Bolt state round trip covers repeated
recovery from the original warm handle after bundle removal, with no regional
registration or control endpoint recreated. Negative tests reject changed
immutable identities, partial claim metadata and incomplete cleanup proofs.
The driver must never repeat runsc deletion or unmount
after ctld has supplied its complete receipt.

Regional-receipt tests read the source proof after its lease and lifecycle
commit, reject destination-slot lookup, and preserve the original proof after
expiry. Authority tests reject foreign node identities, malformed or oversized
responses and mutation methods. Linux node tests prune the real Bolt record and
retrieve the complete receipt through both the root-only RPC and regional TLS
client/handler, without recreating custody; the regional store in that transport
test is controlled. Pending or invalidated local records never invoke fallback.


Restore tests cover immutable regional authorization, consumed-writer and
current-epoch checks, expired heartbeat/claim rejection, concurrent starting
retries and unchanged public routing. Linux ctld tests exercise the private RPC,
real Bolt journal reopening, exact container/session checks, image corruption,
and retention of images and target dirty state during ambiguous recovery.
Driver tests exercise the complete claim branch with a runsc test double:
create followed by exactly one restore, concurrent retries, lost responses,
stale local recovery, authority loss during restoration and no `start` fallback.
These tests do not establish real cross-host restore compatibility or completed
end-to-end migration.

Handover tests additionally cover the real private claim socket's restore
receipt, original procd identity checks, exact digest forwarding, immutable
regional procd acknowledgement, atomic readiness/routing rollback, concurrent
commit retries, writer expiry, hard TTL and termination. They verify that the
public generation advances once while source capacity, TTL and billing values
remain unchanged. The automatic coordinator and real cross-host network path
are not exercised by these component tests.

Source RootFS finalization tests exercise real branch accounting and Bolt
journals with a controlled host runtime. They cover intent, WAL-removal,
artifact-removal and proof restart boundaries, immutable retries, incorrect
writer/fence/path rejection, retained device ownership, terminal-authority
denial, and explicit compact-record forgetting. The shared RootFS suite also
checks ordinary retirement after the reclamation helper was reused. These do
not replace real NBD/XFS or complete source-slot cleanup acceptance.

Destination-adoption tests cover the private ctld RPC, durable intent recovery
before and after image deletion, symlink-safe removal, retained restore history,
ordinary stop/cleanup after receipt, later source custody, stale driver state,
lease loss during adoption and lost responses. Regional tests inject adoption
persistence failure to verify atomic routing rollback and accept only exact
cleanup receipts, including after termination, without releasing capacity.

Regional reservation tests require an isolated PostgreSQL database:

```sh
    INTEGRATION_DATABASE_URL="$ISOLATED_TEST_DATABASE_URL" go test -race ./manager/pkg/sandboxstore -run '^TestNomadMigration' -count=1
```

These tests exercise real transactions, source identity preservation, exact
concurrent retries, insufficient-capacity rollback, normal-claim exclusion,
capacity/ready-inventory projections, never-authorized reservation aborts and
restart/concurrent expiry recovery. Metering regression tests verify that
migration transitions retain one continuous user runtime interval; temporary
destination capacity does not create another billed sandbox.
Source-authorization tests additionally cover exact prepare acknowledgement,
changed source identities, hard phase boundaries, immutable dispatch evidence,
restart recovery and concurrent preparation/capture retries. A source command
does not attach target resources or create a second writer.
The test suite recreates its manager schema; never point it at a shared or
production database. A missing database causes skips, not successful validation.

The isolated runtime primitive probe must be compiled as a static Linux binary:

```sh
    CGO_ENABLED=0 go test -c -o /tmp/sandbox0-gvisor-checkpoint.test ./pkg/gvisorcli
    SANDBOX0_RUN_PRIVILEGED_CHECKPOINT=1 /tmp/sandbox0-gvisor-checkpoint.test -test.run '^TestPrivilegedExecutionCheckpoint$' -test.v
```

Run it only on the authorized Linux test host with stock runsc on PATH. It
creates its own rootfs and private runtime roots and never consumes production
NBD devices, cgroups or ctld resources. It covers DirectFS and gofer modes,
preserved Go memory and PID, tmpfs, an open unlinked file descriptor, immutable
image transfer, and destruction of the source before target execution.

`TestPrivilegedCrossHostCheckpoint` adds an explicitly staged two-host probe.
Compile the same static test binary and copy it to two isolated Linux hosts with
the identical stock runsc binary. Choose a new private absolute directory under
an existing test-owned parent, at the same path on both hosts. Run the source
phase on A:

```sh
    SANDBOX0_RUN_CROSS_HOST_CHECKPOINT=1 \
        SANDBOX0_CHECKPOINT_PHASE=source \
        SANDBOX0_CHECKPOINT_DIRECTORY=/var/lib/sandbox0-test/crosshost-true \
        SANDBOX0_CHECKPOINT_DIRECTFS=true \
        /tmp/sandbox0-gvisor-checkpoint.test -test.run '^TestPrivilegedCrossHostCheckpoint$' -test.v
```

After that invocation succeeds, transfer its `bundle`, `rootfs`, `evidence`,
`image-1` and `record.json` to the same directory on B. Preserve private modes,
verify the transferred archive's digest, and do not transfer the `runtime-*`
directories. Invoke the same command on B with `SANDBOX0_CHECKPOINT_PHASE=target`.
After success, transfer B's `image-2`, `evidence` and `record.json` back to A and
invoke `SANDBOX0_CHECKPOINT_PHASE=return` there. Repeat with a different new
directory and `SANDBOX0_CHECKPOINT_DIRECTFS=false`. Failed phases retain their
files for inspection; do not reuse an incomplete source directory as a new run.

On 2026-09-20 UTC this round trip passed on two separate Alibaba Cloud Linux 4
instances, each with four amd64 vCPUs, kernel `6.6.102-7.alnx4.x86_64` and stock
runsc `release-20260817.0`. The test verifies distinct source/target boot IDs,
return to the original boot and identical runsc SHA-256. In both filesystem
modes, the in-memory token, PID, unlinked file descriptor offset, tmpfs contents
and guest CPU-feature digest survived both moves; the counter advanced from
2 to 4 to 6 without restarting the entrypoint. Each exporting runtime was
deleted before transport, and each phase cleaned its isolated runtime/netns.
The transport archives were SHA-256 verified before extraction.

This result establishes stock-runsc execution-state portability across those
two compatible hosts. The probe uses a copied readonly rootfs and test-owned
transport, not the regional lifecycle, encrypted object store, mutable
block-COW/NBD/XFS mounts or gateway network. The recorded checkpoint/restore
durations measure individual runsc adapter calls for this small workload; they
do not measure migration downtime or establish a startup or migration SLO.

The additional procd probe runs the real session supervisor, its pipe child
process and authenticated HTTP handover inside the checkpointed guest:

```sh
    CGO_ENABLED=0 go test -c -o /tmp/sandbox0-procd-migration.test ./manager/procd/pkg/http
    SANDBOX0_RUN_PRIVILEGED_CHECKPOINT=1 /tmp/sandbox0-procd-migration.test -test.run '^TestPrivilegedProcdSessionCheckpoint$' -test.v
```

This probe uses stock runsc's isolated loopback network mode and independent
copied readonly rootfs paths. It checks the preserved procd instance, child PID,
attempt ID, a child-memory-only token, generation advancement, and input
deduplication followed by new input to the same process. The procd probe also
uses strict OCI restore validation and different source/target OCI environment
values; the captured process retains its original environment while the exact
authenticated handover advances its in-memory generation. Both probes passed
with stock `release-20260817.0` on a local ARM64 Linux Docker VM in DirectFS and
gofer modes. The Docker diagnostic wrapper used `--ignore-cgroups=true` because
the nested root cgroup has internal processes; this evidence does not validate
production resource leases, x86 compatibility, block-COW RootFS or cross-node
network continuity. The authorized remote test host was unreachable during
those earlier checks; the later full-stack two-node results are recorded at the top of this document.

Shipping the complete feature additionally requires:

- PostgreSQL integration tests for lifecycle races, target reservation,
  writer/execute fencing, repeated requests, manager recovery and deletion.
- Driver and ctld tests proving checkpoint exit cannot trigger crash cleanup,
  standby promotion retains custody, and a restore retry cannot execute twice.
- Procd tests proving generation rebinding preserves live commands, REPL state,
  supervised attempts and their journals without emitting a second creation.
- Real two-node Nomad/runsc tests with block-COW/NBD/XFS/OverlayFS, persistent
  open files, shared mmap, tmpfs, child processes, active REPL and network policy.
- Injected interruption at every transaction boundary, including source and
  destination loss, expired authority, unavailable PostgreSQL/object storage,
  and loss of the response after target execution begins.
- Verification that public OpenAPI, generated SDKs, CLI and template schemas
  expose no migration trigger or destination control.
- End-to-end interruption and first-command measurements across representative
  memory sizes, dirty write rates, warm/cold caches and concurrent migrations;
  verification of staging/transfer limits and image garbage collection.

Upstream references: [gVisor checkpoint/restore](https://gvisor.dev/docs/user_guide/checkpoint_restore/),
[pinned stock checkpoint command](https://github.com/google/gvisor/blob/release-20260817.0/runsc/cmd/checkpoint.go),
and [pinned stock restore command](https://github.com/google/gvisor/blob/release-20260817.0/runsc/cmd/restore.go).
Validate these behaviors against the exact deployed runsc release, not only
upstream HEAD.

### Explicit runtime-upgrade maintenance

The existing audited runtime rollout retains an exact sandbox generation and
owns filesystem pause/resume. Automatic evacuation excludes its
`audited-runtime-rollout:` drain fences in both discovery and the locked
reservation check. It must not race that maintenance owner by changing the
retained runtime generation. Ordinary node drain fences continue to trigger
system-owned execution-state evacuation when both placements are eligible.
