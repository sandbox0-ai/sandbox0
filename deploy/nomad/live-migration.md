# System-owned live migration

This document specifies the internal execution-state migration path. Migration
is a system lifecycle operation, selected by manager during planned node
evacuation. It is not a sandbox API, SDK method, CLI command, template option,
or user-selected scheduling constraint. A user cannot request migration,
select its destination, or download an execution image.

## Implementation status

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

Each operation has a two-minute attempt budget, each pass a five-minute budget,
and passes wait one second before rescanning. A cursor traverses unresolved work
without letting the first failed batch starve later operations; cancellation
preserves the unvisited suffix. This loop is separate from terminal cleanup, so
an image transfer cannot occupy that worker. Every manager replica can run it;
the existing database CAS and node journals serialize effects, and each replica
uses its own authenticated node channels. A replica without the relevant node
stream fails that attempt and retries on a later pass.

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
by the binding digest, split into fixed bounded chunks, and created
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
