# RootFS automatic growth and live commits

Implementation and acceptance record for the automatic-growth rollout. This
feature is not deployed until the gates below have evidence attached.

## Required behavior

- New image imports expose a large, sparse logical device while formatting a
  smaller initial XFS filesystem. Empty address space creates no data objects.
- The node grows mounted XFS before free space is exhausted, without replacing
  NBD, stopping runsc, or changing guest process identity.
- Live block cuts publish incrementally to regional object storage. Uploads
  hold regional custody before PUT, and publication is authenticated against
  the exact active writer. Old snapshots remain immutable.
- Only a regionally acknowledged cut may replace the local branch base and
  reclaim its covered WAL records. Writes after the cut survive, including
  overwrites of blocks already included in the cut.
- Local branch, reader, WAL path, sequence, and dirty-budget accounting switch
  together. A crash between regional publication and local installation must
  recover either complete version. Ambiguous acknowledgements are retried with
  the same operation and descriptor, never treated as permission to delete.
- WAL and cache occupancy remain bounded independently of accumulated RootFS
  size. Slow or unavailable storage applies backpressure without retiring the
  live writer. Thaw/flush must not deadlock behind blocked normal writes.
- Pause, memory checkpoints, fork, snapshot, restore, rebase, migration, writer
  fencing, storage inventory/GC, and live ctld transfer retain their existing
  ownership and durability guarantees.

## Work remaining

1. Complete full regional lifecycle/fencing/GC acceptance with installed live
   bases. Component running-fork/cold-restore gates and actual Nomad growth,
   live ctld transfer and retained-memory pause/resume gates below passed.
2. Qualify complete node loss and replacement-node admission. Abrupt
   process-owner fencing and a separately provisioned machine's fresh-cache
   remote recovery passed below; provider loss observation and actual Nomad
   replacement admission remain separate gates.
3. Extend injected S3 request-outage acceptance to provider disruption and
   larger Nomad workloads. The shared-node two-writer contention gate passed;
   node behavior remains disabled by default.
4. Verify exact production release/checkpoint compatibility, including retained
   stock runsc bundles and procd digests; fixture release IDs are synthetic.
5. Finish acceptance, retain sanitized evidence and remove temporary resources.
6. Merge passing runtime, contracts/SDK descriptions, CLI, website and operator
   documentation together. Deploy the compatible manager before enabling node
   behavior, roll workers with memory preservation and verify a regional canary.

## Acceptance gates

- A process keeps its PID and memory marker through multiple XFS growth and
  WAL rotation cycles, while reads/writes and existing open files continue.
- Accumulated data exceeds the original filesystem and configured WAL budget;
  host disk stays within its fixed cache/WAL plus documented transient budget.
- Unique writes, repeated overwrites, zeros, and deleted/recreated files remain
  correct after commits, snapshot/fork, pause/resume and fresh-node restore.
- Faults before/after PUT, regional commit, local journal persistence, pointer
  switch and unlink preserve data/custody and converge on exact retries.
- Storage outage and node-wide pressure block safely, recover without process
  restart, and do not consume protected retirement space.
- Lifecycle/fencing races cannot publish from a stale writer or delete a WAL
  still needed for a cut, restore, or ownership transfer.
- The temporary host, storage namespace, database and access credentials are
  removed; production rollout receipts and canary evidence prove deployment.

## Import geometry and rollout contract

This design separates guest capacity from compute-node disk capacity. A large
sparse NBD address space does not allocate an equally large host file. Mounted
XFS grows within that address space, while acknowledged live publication moves
data into remote object storage and permits covered local WAL records to be
reclaimed. A local sparse image alone would still consume local disk as data
accumulates and would not provide this property.

The compute node still needs its configured cache, dirty-tail, journal and
transient-operation budgets. Memory checkpoint images have a separate enforced
staging quota; they are not included in a RootFS cache/WAL bound. Remote storage
capacity, the committed device ceiling and upload throughput remain limits.
Online growth preserves the running guest, but is not a promise of zero I/O
latency or uninterrupted write progress during storage outages.

Guest fsync makes the local WAL durable; it does not by itself acknowledge a
regional live publication. Complete loss of that node's WAL can recover only
the last regionally committed cut, not its unpublished suffix. Planned drain
must settle publication and retain the exact memory checkpoint before removing
the worker. Abrupt-node-loss acceptance must check this boundary explicitly.

`rootfs_importer.sparse_device_size_bytes` is a regional image-import setting.
Zero retains the previous fixed-size contract during rollout. The intended
enabled value is `70368744177664` (64 TiB); supported ceilings are aligned values
from 1 TiB through 64 TiB. A template's `ephemeralStorage` supplies the initial
XFS size, still defaulting to 8 GiB and accepting 300 MiB through 1 TiB.

Sparse imports explicitly bind `initial_filesystem_size_bytes` and
`logical_size_bytes` in their deterministic operation and version-4 ready
attestation. Version 4 can combine either supported data-layout policy and
either supported mapping-group policy. Only the initial-size image is formatted
and scanned; the larger address space creates no zero data objects or local
image allocation. Existing zero-valued fields remain omitted from canonical
legacy JSON. Migration 120 adds the indexed geometry and transactional fences:
both capacities are immutable, and a Ready result must match its import.

Image lookup matches both capacities. Exact-digest lookup for a captured
template matches its original initial capacity, with the artifact digest
retaining its committed device geometry and other policies. Paused resume does
not select a replacement image. Enabling a new import setting does not change
existing artifacts, generations, or live devices.

Upgrade every regional manager and node before enabling the setting. An older
manager cannot decode the version-4 attestation, and an older node cannot recover
a WAL header carrying a nonzero `sequence_base`. Disablement stops new sparse
imports; it cannot reinterpret existing sparse artifacts as fixed-size imports.
Migration rollback refuses to erase geometry while sparse artifacts or import
operations exist. Runtime integration, lifecycle/fault acceptance and production
rollout remain required before activation.

## Verified foundation evidence

On an isolated Ubuntu 24.04 host with Linux 6.8, the privileged NBD/XFS gate
exposed a 64-TiB sparse device, grew mounted XFS from 300 MiB to 1,200 MiB in
two online growth operations, and rotated its WAL 37 times. One Python process
retained its PID, heap marker and open file while writing 600 patterned 1-MiB
chunks. Sampled bytes in every chunk matched; peak WAL payload was 18,341,888
bytes under the configured 192-MiB writer limit.

This is the block-device/filesystem primitive gate. Its object source is an
in-memory fixture and its process runs on the host; it does not establish runsc,
regional publication, restart recovery or deployment acceptance. The separate
full-product gates above remain mandatory.

The strengthened gate checked all bytes of the completed 600-MiB file and then
mounted the last live increment on a fresh writable branch without the original
WAL. XFS log replay recovered 593 complete 1-MiB chunks, all byte-correct. Those
593 chunks belong to that earlier cut; the later writes remained in the original
WAL suffix. The run again performed two growth operations and 37 rotations,
with 17,911,808 peak WAL payload bytes. This proves cut consistency, not remote
durability of writes after the cut or full-product node-loss recovery.

Linux PostgreSQL import acceptance passed all four combinations of empty/
file-relative data layout and empty/contiguous mapping grouping. It verified
both capacities survive lease reconstruction, exact commit retries, mismatched
initial-size rejection, captured-template digest lookup, metadata immutability,
SQL terminal fencing independent of Go validation, and guarded migration
rollback. Existing import/format/geometry regression also passed on that host.

## Real S3 lifecycle and installed-base retention

The regional store gates
`TestRootFSLiveCommitInstalledBaseRetentionS3Integration` and
`TestRootFSLiveCommitSnapshotForkRestoreGCS3Integration` passed with PostgreSQL,
real RustFS S3 and the race detector, both without encryption and with the
AES-GCM/RSA immutable-object wrapper. Each backend mode also reran the existing
progress/replay and terminal-publication regression gates. The four-test suites
passed in 23.102 and 22.981 seconds respectively.

The progress gate exercises lost/replayed publication and installation receipts,
GC of obsolete live generations while the installed read base remains pinned,
bounded retained receipts and rejection of forged writer/descriptor updates.
The lifecycle gate installs a live read base, publishes a terminal generation
that inherits it plus a new suffix block, and commits `MarkRuntimePaused` in
the same transaction. It then creates a snapshot, forks the filesystem and
restores the snapshot into a separate filesystem. Five inventory passes and
real object GC retain every referenced head; fresh readers verify both the
inherited live block and terminal suffix for the source, fork and restored copy.

Objects use a unique fixture prefix, including all catalog and custody reads,
and are removed by the fixture cleanup. These are regional transaction/storage
gates with synthetic physical-retirement evidence. Actual NBD/runsc/Nomad
and retained-memory evidence is recorded separately below; the public manager
API and deployment canary remain required.

## Regional live publication and remaining node integration

The writer's Stage, binding digest and initial generation remain immutable.
Live publication tracks a separate regional committed generation and node-installed
base generation; updating the Stage's initial generation would invalidate writer
authentication and retained lifecycle proofs.

Migration 121 and the authenticated `/live-commit`, `/live-commit/installed`
and `/live-commit/status`
writer endpoints implement this regional cursor. A positive cut sequence identifies
each live generation; sequence zero preserves the existing single terminal
generation per filesystem/writer epoch. New cuts require the exact consumed,
unexpired node binding, previous committed generation, installed read base, and
uploaded mapping-root custody. Receipt replay validates the complete request digest.
Both remote and installed generations have real retention FKs. New remote commits
compact prior successful live upload receipts even during repeated cuts before
installation; the latest complete request digest remains retryable. Canonical
grant/sequence operation IDs reject obsolete upload attempts after compaction.
A local installation acknowledgement releases the old installed pin;
checksum-verified inventory allows obsolete generations to retire.

The filesystem lifecycle head remains at the immutable launch generation while
the writer is active. Final lifecycle publication retains that launch parent and
adds an explicit dependency on the installed live read base until inventory
completes. Crash abandonment promotes the last remote commit only after the existing
exact runtime, lifecycle and physical terminal-proof checks. This preserves writer
authentication and lifecycle proof identity without discarding remote durable cuts.

This regional path passed isolated Linux PostgreSQL tests with in-memory immutable
objects, plus HTTPS client/handler authentication tests. It does not yet establish
full-product automatic growth, remote object-service or gVisor acceptance. The
node's full-product automatic-growth and lifecycle recovery
acceptance remains required before enabling sparse imports or deploying this
feature.

## Durable node live-commit transaction

The session manager now exposes an explicit `CommitLive` operation through a
separate authority interface. The authenticated node adapter and bounded scheduler
are wired behind `nomad_runtime.live_commits`, which defaults to false. Session
journal version 13 records the deterministic grant/
sequence operation, exact flushed WAL cut, materialized publication payload,
regional receipt, selected branch path/sequence, installed base and one obsolete
branch. The launch Stage and generation ID remain unchanged.

Before PUT, persist the flushed cut. Upload retries use `CheckpointAtCut`, which
reads the original prefix even if later writes overwrite those blocks. Persist
the complete publication before asking the region to accept it. A lost reply is
resolved by the authenticated status observation; an accepted operation is never
rebuilt or re-uploaded after its upload receipt has been compacted.
Active status observations include the immutable committed descriptor's SHA-256
digest. Matching the operation and generation IDs alone cannot authorize WAL
release if the locally journaled publication payload differs.

After acceptance, the Bolt transaction selects the materialized read base and
suffix WAL before switching the same NBD Branch. Ambiguous local acknowledgements
retain both files. Recovery reads the durable selection and verifies every copied
suffix record against the complete old WAL before resuming append admission.
Pending writes wait through this transition rather than receiving a temporary
I/O error that could shut down XFS. The region receives the installed-base ack
only after durable selection and in-memory installation; lifecycle publication
and planned live handoff reject unsettled installations.

WAL rotation copies at most 8 MiB of post-cut payload while holding the branch
lock. If that suffix or its normal node-budget duplication does not fit, retain
the acknowledged remote cursor and full selected WAL, then take a newer cut.
An empty successor consumes no payload budget and can reclaim a preloaded WAL
even after limits were lowered. Protected retirement reserve remains unavailable
to normal writes and nonempty rotation copies. Obsolete WAL cleanup uses exact
parent/sequence paths, not directory globs.

Uploads hold neither the parent lock nor a filesystem freeze. Storage progress
is merged in the Bolt write transaction so concurrent consumer renewal is not
overwritten by an older upload snapshot. Branch pressure is observed before a
Bolt writer transaction to avoid the branch-lock/journal-lock inversion during
installation. Unplanned physical teardown can retain an unresolved receipt;
artifact deletion still requires the existing exact regional terminal proof.

Isolated Linux race tests verify upload failure, lost publication and installation
replies, original-cut retry, continued suffix writes, concurrent consumer renewal,
normal-budget recutting, durable successor reopen and guarded terminal cleanup.
The complete session test suite also passed on that host. These tests use real
Bolt journals with fake host runtime and in-memory immutable objects; actual
runsc, remote object-storage and deployment gates remain open.

## Authenticated background commit scheduling

The node's one-second reconciliation loop admits up to four concurrent live
commits. A writer becomes eligible at 64 MiB of WAL payload, a quarter of a
smaller configured writer limit, 30 seconds since its last local installation,
or immediately for admission pressure or an unresolved operation. Consumer
renewal timestamps do not postpone commits. Round-robin admission prevents
continuously dirty writers from starving other eligible writers; failed attempts
use bounded one-to-30-second retry backoff. Node shutdown and planned live update
join these workers through the existing control/reconciliation wait group.

Enabled writers bypass legacy pressure-triggered planned retirement. They keep
their process and mounted filesystem while a new cut reclaims the WAL; an
unavailable object store applies backpressure. The legacy path remains available
when live commits are disabled. Pauses, running forks and migration RootFS cuts
settle existing pending intent without starting a new cut. Before a live owner
transfer, control workers join and every pending receipt is settled before the
session manifest is captured. A failed settlement preserves the journal and
aborts the transfer.

Isolated Linux race tests verify bounded worker concurrency, fair admission,
retry backoff, disablement, absence of pressure retirement for enabled writers,
installed-time scheduling, exact payload/parent mapping through the authority
adapter, and settlement without capturing post-cut writes. Full-product
remote-service acceptance remains required before rollout.

## Session-owned automatic XFS growth

`nomad_runtime.auto_grow` requires `live_commits` and defaults to false.
The one-second control loop schedules a separate two-worker lane. Workers
inspect idle candidates through each snapshot rather than spending one tick
per two idle filesystems; active kernel growth never occupies the upload lane.
Available space at or below the greater of 20 percent and 64 MiB triggers
doubling, capped at the immutable logical device size. At the ceiling, ordinary
filesystem capacity limits apply. A finite inspection interval does not guarantee
that arbitrary write bursts cannot exhaust free space before growth completes.

The manager validates the durable writer binding, ready custody and exact live
device before retaining a growth permit. That permit rejects freeze/capture,
release and live owner transfer while allowing background cuts and installation.
Neither inspection nor the kernel grow ioctl holds the parent mutex. A growth
progress worker retries pending commits and pressure reclamation under the
primary session lifetime, even after planned-update cancellation stops control
workers. It shares the manager's four-upload admission bound with the scheduled
commit lane. A storage outage keeps the same backend and may delay the planned
update; a timeout aborts transfer without dropping its source journal or fence.
Unexpected primary shutdown closes growing branches to wake blocked kernel
writes before joining growth. This is a shutdown path, not a planned rollout or
a substitute for memory-preserving sandbox drain.

The complete Linux race suites for `pkg/rootfssession`, `pkg/nomadruntime`,
`ctld/cmd/ctld`, `pkg/config` and `deploy/nomad/ctld` passed after this integration.
Focused tests cover exact binding, growth/commit overlap, rejection of captures
and transfer during growth, reclaim after control cancellation, unexpected-close
admission wakeup, shared upload bounds, worker separation, idle-owner sweeps,
per-parent retry state and disabled configuration.

The isolated Linux race gate `TestPrivilegedSessionAutoGrowthAndLiveCommitsKeepWriterRunning`
passed with a real 64 TiB NBD device, XFS and the production Bolt session journal.
One Python process retained its PID, heap marker and open file while writing
600 patterned 1 MiB chunks. Two growth operations increased XFS from 300 MiB to
1,200 MiB; 19 foreground commits and the pressure-reclaim path kept the selected
WAL below its 32 MiB budget (observed peak 33,468,416 payload bytes). All chunks
were verified after writing. Rotation suffixes that exceed the 8 MiB copy limit
are treated as a retry with a newer cut, preserving the acknowledged remote
cursor and complete old WAL. The fixture retired its mounted session in normal
order and released its exclusive NBD device. These objects and authority receipts
are still in-memory doubles; this evidence does not replace remote-service,
runsc, lifecycle or production acceptance.

The isolated fixture now includes a real RustFS 1.0.0 container pinned to
`sha256:8cc9801755448b71a786705ce76692c77e14936cccd87cf2fc31842e58f4d1ff`,
bound only to loopback, with separate generated test credentials. The real
PostgreSQL/RustFS `TestRootFSNodeLegacyUploadRetentionS3Integration` and
`TestRootFSNodeUploadCustodyS3Integration` race gates passed. Legacy checkpoint
inventory/GC reduced 11 objects to 3 and 1,114 bytes to 241 while preserving
contents; tracked node uploads retained the expected live generation and released
completed upload holds. These are storage-regression gates, not the combined
authenticated live-cut/runsc acceptance gate. The fixture container, data,
database and credentials remain temporary resources to remove after acceptance.

## Combined authenticated guest and object-service acceptance

`TestPrivilegedRemoteAutogrowPreservesRunscWriterAndRegionalCustody` passed
under the Linux race detector in 99.525 seconds. It uses the pinned official
`release-20260914.0` runsc bundle, a real 64 TiB NBD device and mounted XFS,
production Bolt session journals, the production node commit/growth scheduling
methods, PostgreSQL migrations, real loopback RustFS S3 objects, and the regional
writer and lifecycle handlers behind verified client-certificate authentication.
The source image contains a locally compiled static executable; its OCI identity
is explicit fixture provenance, so this gate does not test registry importing.

One gVisor guest process wrote and fsynced 600 patterned 1 MiB chunks through one
open file, retaining its PID, fresh-entry token, heap marker, inode and file offset.
XFS grew from 314,572,800 to 1,258,291,200 bytes; peak node WAL payload was
40,845,312 bytes, below the 32 MiB writer budget plus 8 MiB charged rotation
duplication allowance. Every completed chunk was checked byte for byte.

The fixture deliberately lost one successful regional publication reply and
one successful installed-base reply. Exact retries converged to installed cut
151,334 while retaining the immutable launch head. After guest exit, normal
retirement published the final generation, verified regional retirement, reclaimed
local artifacts and released NBD. Successful test cleanup removed its bucket's
objects. Empty buckets and the fixture host/services remain temporary resources.

This is a combined component acceptance gate, not a running Nomad cluster or
production rollout. Storage-outage recovery, fresh-node recovery, lifecycle/
fencing/GC races, memory-preserving transfer and deployment gates remain open.
Run it only on an isolated root-owned fixture with an exclusive unused NBD device:

```sh
# Also supply private fixture S3 credentials and the dedicated test database URL.
SANDBOX0_ROOTFS_AUTOGROW_REMOTE=1 \
SANDBOX0_TEST_RUNSC=/path/to/pinned/gvisor/runsc \
SANDBOX0_PRIVILEGED_NBD_DEVICE=/dev/nbd63 \
SANDBOX0_RUSTFS_ENDPOINT=http://127.0.0.1:19000 \
go test -race ./pkg/nomadruntime \
  -run '^TestPrivilegedRemoteAutogrowPreservesRunscWriterAndRegionalCustody$' \
  -v -count=1 -timeout=10m
```

The test refuses to drop schemas unless the database name starts with
`s0_rootfs_autogrow`. Its destructive fixture migration and object cleanup must
never run against a production database or shared bucket.

## Running-fork freeze progress under WAL pressure

A running-fork freeze retains exclusive checkpoint custody but releases the
parent mutex while the kernel drains dirty filesystem writes. A private owner
permit allows the exact authenticated writer to commit and reclaim during that
drain. The permit is not persisted: a recovered freeze intent cannot authorize
new live cuts, and recovery must thaw it before resuming normal ownership.

After the kernel freeze returns, new cuts are excluded. The capture joins any
in-flight commit, settles its publication/installation journal, and reloads the
current installed base before taking the WAL checkpoint. Recovery scans do not
thaw a barrier still owned by an active capture. RPC cancellation retains the
owner's progress guardian until the kernel drain returns; failed settlement
thaws and clears the fork request while preserving the pending live journal.
Unexpected owner shutdown wakes blocked branch writes before joining freeze
work. Planned retirement and live handoff continue to reject active captures.

Linux race regressions cover full-budget metadata writes, both lost receipts,
capture of the rotated base and suffix, cancellation, owner shutdown, failed
settlement retry and rejection of recovered-intent authorization. Focused tests
passed in 9.757 seconds. Full session, Nomad runtime, ctld, configuration and
deployment tests passed (session 133.284 seconds; Nomad runtime 193.629 seconds).

The combined real guest/S3 gate also passed with
`SANDBOX0_ROOTFS_AUTOGROW_FREEZE=1` in 100.829 seconds. It stopped and joined the
ordinary scheduler, waited for the running guest to fill its WAL, then captured
a local running-fork boundary using the freeze-owned reclaim path. The same
guest subsequently completed all 600 MiB; every byte was verified before final
authenticated retirement. XFS reached 1,258,291,200 bytes and peak sampled WAL
payload was 41,680,896 bytes, below the 40 MiB transient allowance. Both lost
receipts were resolved, and installed sequence reached 153,359.

This local boundary gate does not create or publish a regional fork target.
Full fork/snapshot transaction, memory checkpoint, migration, outage and
production gates remain required before deployment.

## Injected S3 request-outage acceptance

The combined Linux race gate passed with
`SANDBOX0_ROOTFS_AUTOGROW_OUTAGE=1` in 105.752 seconds. All reads/writes still use
the real RustFS S3 backend; a loopback HTTP proxy returns S3-shaped `503`
responses during the injected interruption. This simulates failed storage
requests without stopping the isolated backend container or claiming a real
provider outage.

After the guest had written at least 80 MiB, every object-service request failed
until at least one rejection and normal WAL admission pressure were observed.
The test held that pressure for five additional seconds, checked that the
regional writer remained consumed, then restored transport. The same guest
retained its PID/token, heap marker, open-file inode and offset and completed all
600 MiB. Every byte was checked; XFS reached 1,258,291,200 bytes. Peak sampled WAL
payload was 41,750,528 bytes, below the 40 MiB transient allowance. Both injected
lost receipts converged, installed sequence reached 154,349, and normal final
retirement and object cleanup succeeded.

Longer failures, provider-level disruption, multiple contending writers,
node-loss recovery and full regional lifecycle/deployment gates remain open.

## Fresh session, cache and device recovery

The combined Linux race gate passed with
`SANDBOX0_ROOTFS_AUTOGROW_COLD_RESTORE=1` in 125.681 seconds. After normal
authenticated retirement of the original writer, it closed the session manager
and created a new Bolt journal, branch directory, empty read cache and separately
reserved `/dev/nbd62`. The region issued and consumed a new writer grant at epoch
2 against the final committed generation. The new session fetched that
generation from real RustFS through the authenticated regional descriptor.

A fresh stock-runsc guest read all 629,145,600 file bytes and checked every byte,
including EOF. Mounted XFS retained its 1,258,291,200-byte size. The second writer
then completed normal authenticated retirement and released its device. The
original guest retained its PID/token, heap marker, inode and open offset during
growth; its sampled peak WAL payload was 39,579,648 bytes, within the configured
40 MiB transient allowance. Both lost acknowledgements converged.

Set `SANDBOX0_PRIVILEGED_NBD_RESTORE_DEVICE` explicitly to an unused device
different from `SANDBOX0_PRIVILEGED_NBD_DEVICE` when enabling this gate. This is
fresh-session/cache/device recovery on the same isolated host after normal
retirement. It does not establish crash abandonment, replacement-node recovery,
full Nomad resume or memory-checkpoint continuity. Those gates remain open.

## Regional running-fork publication and independent reads

The combined Linux race gate passed with
`SANDBOX0_ROOTFS_AUTOGROW_FREEZE=1` and
`SANDBOX0_ROOTFS_AUTOGROW_REGIONAL_FORK=1` in 112.073 seconds. The source used
real regional capacity/slot/claim transitions and the normal running-fork request
transaction to reserve a paused target. Under WAL pressure, the runtime captured
and published the fork through authenticated regional handlers. An exact request
retry observed the committed target. This exercises the regional transaction,
but the slot readiness evidence is fixture-supplied; it does not establish a
Nomad/procd handshake.

After the original guest completed all 600 MiB and retired normally, an empty
read cache and independently reserved NBD device mounted the remote target.
Every byte of its 164,626,432-byte file matched the earlier source prefix. The
target remained at that earlier boundary despite later source live commits and
terminal publication. The original guest retained its PID/token, heap marker,
inode and open offset. XFS reached 1,258,291,200 bytes; peak sampled source WAL
payload was 33,554,432 bytes. Both lost receipts converged and installed sequence
reached 147,086. Both devices and fixture objects were cleaned up normally.

The verification branch is disposable and only permits XFS replay locally; it
does not publish a new child writer. Set `SANDBOX0_PRIVILEGED_NBD_RESTORE_DEVICE`
to a separately reserved unused device when enabling this mode. Full Nomad
fork/resume, memory capture, crash recovery and deployment gates remain open.
The workload uses repeating byte patterns, so high-entropy storage throughput
and multiple contending writers still need separate acceptance.

## High-entropy writes, mutations and bounded local artifacts

The combined Linux race gate passed with `SANDBOX0_ROOTFS_AUTOGROW_UNIQUE=1`,
regional fork and cold restore in 306.161 seconds. A deterministic high-entropy
stream includes a distinct address marker in every 4 KiB block. The guest first
wrote 600 MiB, then overwrote 78 earlier 1 MiB chunks with new contents, zeroed
55 chunks and deleted/recreated a 64 KiB file with different contents. Both the
original mount and a fresh stock-runsc reader verified the final bytes. The
independent regional fork retained its earlier 157 MiB prefix after all source
mutations. Peak sampled source WAL payload was 33,787,904 bytes and installed
sequence reached 187,900. XFS retained its 1,200 MiB size through cold recovery.

The strengthened gate also passed with injected object-request failures and
`SANDBOX0_ROOTFS_AUTOGROW_OUTAGE_HOLD=30s` in 337.796 seconds. After observing
failed requests and normal WAL admission pressure, it held transport failure for
30 more seconds (16 rejected requests), then restored access to real RustFS.
The same guest completed the high-entropy writes and mutations; regional fork,
remote cold verification and both normal retirements passed. Both deliberately
lost acknowledgements converged. Peak sampled WAL payload was 41,844,736 bytes
and installed sequence reached 188,548.

Allocated filesystem blocks of source-side branch files, disk cache and Bolt
state were sampled once per second and after verification. Their observed peak
was 42,225,664 bytes, within a fixed 134,217,728-byte acceptance bound. This
counts disposable compute artifacts, excluding mounted virtual RootFS contents
and the separate RustFS object-service data. The test object service runs on the
same disposable host, so whole-host `df` would mix worker and durable-storage
usage. This is sampled evidence for one writer, not a fleet-wide occupancy bound
or a guarantee about arbitrary write rates.

Set `SANDBOX0_ROOTFS_AUTOGROW_COLD_RESTORE=1`,
`SANDBOX0_ROOTFS_AUTOGROW_FREEZE=1` and
`SANDBOX0_ROOTFS_AUTOGROW_REGIONAL_FORK=1` alongside the unique-write mode to run
this gate; it requires both explicitly reserved unused devices. Use a 20-minute
Go test timeout. Optional `SANDBOX0_ROOTFS_AUTOGROW_OUTAGE_HOLD` accepts durations
from five seconds to two minutes. Provider-level outages, node-wide contention,
crash abandonment, full Nomad lifecycle and production rollout remain open.

## Shared-node two-writer contention gate

`TestPrivilegedRemoteAutogrowTwoWritersShareNodeBudget` extends the combined
component fixture with two real stock-runsc guests sharing one session manager,
aggregate dirty-tail budget, cache and commit/growth scheduler. Each writes
600 MiB of high-entropy data and performs the overwrite/zero/delete/recreate
workload. The individual writer limit is 32 MiB; the shared node limit is
40 MiB including an 8 MiB protected retirement reserve, less than the sum of
the individual limits. The shared disk cache remains 32 MiB.

After both writers pass 80 MiB, the proxy fails every real S3 request. The gate
requires both writers to hit admission pressure, holds failure for another
minute, verifies both regional grants remain consumed and then restores
transport. Both original PID/token/heap/inode/open-offset proofs, every final
file byte and installed live receipts must remain valid. Sampled aggregate WAL
payload must stay within 40 MiB, and allocated worker WAL/cache/journal blocks
within 128 MiB. The two sources renew their real regional leases throughout.

Enable `SANDBOX0_ROOTFS_AUTOGROW_CONTENTION=1`,
`SANDBOX0_ROOTFS_AUTOGROW_REMOTE=1` and
`SANDBOX0_ROOTFS_AUTOGROW_OUTAGE=1`; leave regional-fork mode disabled. Supply
the same private isolated PostgreSQL/S3/runsc inputs as the combined gate and
both separately reserved NBD paths. Run only this test under the race detector
with a 20-minute timeout. The gate passed under the race detector in 365.74
seconds. Both original guests retained PID 1, heap markers, tokens and their
open-file inode/offsets; each completed 600 MiB plus 78 overwrite and 55 zero
chunks and a deleted/recreated file. All final contents were verified. After
both writers blocked, the one-minute request-outage hold rejected 77 requests.
Neither consumed regional grant was retired by pressure.

Installed sequences reached 189,358 and 189,354. Both XFS filesystems reached
1,258,291,200 bytes, up from 300 MiB. Peak aggregate WAL payload was 33,550,336
bytes, below the 40 MiB node limit; normal pressure stayed within 32 MiB and
left the protected reserve available. Across 270 samples, allocated worker
WAL/cache/journal blocks peaked at 52,260,864 bytes, below 128 MiB, despite
1,258,291,200 accumulated guest data bytes. Both normal retirements and
exclusive-device cleanup succeeded. The fixture writes a durable remote test
log and explicit exit-code receipt, independently of SSH output delivery.

This qualifies shared component admission with real NBD/XFS/runsc, regional
PostgreSQL, mTLS and RustFS. It does not qualify multiple actual Nomad claims,
a provider outage or production fleet density; those boundaries remain explicit.

## Abrupt process-owner loss and remote-cut recovery

The isolated runner kills the actual Go storage-owner process with SIGKILL;
source defers and planned retirement cannot run. It verifies a root-owned
fixture, exact test executable, package working directory and mount namespace,
and uses a Linux pidfd to avoid signaling a reused numeric PID. PostgreSQL and
the real S3-compatible object service remain available independently of that
process. A durable remote log and kill/result receipts survive the SSH session.

Before killing the owner, the guest writes and verifies the high-entropy
600 MiB workload, including 78 overwritten chunks, 55 zero chunks and the
deleted/recreated file. The source syncs XFS and completes a remote live cut,
then stops publication and fsyncs a separate file and its directory only into
the local suffix. Regional committed/installed progress must remain unchanged
after that suffix write.

Recovery reopens the existing database and session journal, follows the
production startup freeze/fork/rebase/release reconciliation order, expires
only the dedicated fixture writer's lease after proving process death, and
uses the production crash-fence path with actual Linux mount/NBD observations.
The last remote cut becomes the lifecycle head; the old grant is retired and
cannot renew. After five inventory passes and generation GC, a fresh cache,
session and separately reserved NBD device restore all 600 MiB from S3. The
unpublished suffix file must be absent. The restored writer retires normally.

The complete fresh-source run passed under the race detector, without runner
or recovery retries. The source reached remote sequence 189,335 before SIGKILL;
its expected nonzero Go exit was recorded explicitly. Recovery passed in
25.53 seconds, writer epoch advanced to 2, and restored XFS geometry was
1,258,291,200 bytes. Both recovery and runner exited zero. The source's allocated
WAL/cache/journal peak was 41,705,472 bytes, below 128 MiB. Both NBD devices
were released and the fixture's mount-free state and bucket contents removed.
An earlier retained-scene retry also passed; startup reconciliation and the
required task key are fixture obligations, not bypasses of production fencing.

Reproduce with the infra repository's
`ali/nomad/tests/live_node/owner_loss_rootfs_run.py`. Run only on the disposable
host with both isolated markers, private RustFS credentials, a PostgreSQL root
superuser and exclusive NBD 62/63. `--resume DATABASE` handles a source retained
before SIGKILL; `--recover DATABASE` retries a failed recovery only with a
previous durable kill receipt. Successful recovery removes fixture objects
and mount-free temporary state. Failed attempts retain their evidence.

This proves abrupt process-owner fencing and cold remote recovery on the same
host, including the distinction between local fsync and remote custody. It
does not prove physical-machine loss, provider observation, replacement-node
admission or process-memory recovery after an unplanned crash.

## Separate-machine remote recovery

`TestPrivilegedRemoteAutogrowReplacementMachineRestoresPublishedCut` passed
under the race detector on a newly provisioned second Ubuntu 24.04 machine.
The source run wrote and verified the high-entropy 600 MiB workload, completed
remote sequence 189,350, then fsynced the unpublished suffix. Its storage owner
was killed with pidfd-bound SIGKILL; the existing recovery path completed NBD
crash fencing, inventory and GC. Export mode removed the source's mounted local
RootFS, WAL and cache while retaining the remote cut for the second machine.

The replacement had a different machine boot ID and authenticated writer node
UID. Only tooling, public Go dependencies, a token-free source receipt and
private backend connection material were transferred. Neither source RootFS/
WAL/cache nor PostgreSQL/object-store data were copied. Private loopback SSH
forwards connected to the original fixture's PostgreSQL and S3 services.
The replacement used a new session/cache/device, consumed its epoch-2 grant,
restored XFS geometry of 1,258,291,200 bytes and verified all 629,145,600 data
bytes in a fresh stock-runsc guest. The local-only suffix was absent. Recovery
passed in 21.59 seconds; Go and the durable runner receipt both reported zero.
The replacement retired normally and removed its mount-free state and fixture
bucket contents. This gate did not sample peak replacement disk occupancy;
bounded disk evidence comes from the growth and contention gates above.

Reproduce with `SANDBOX0_ROOTFS_AUTOGROW_OWNER_LOSS_EXPORT=1` on the source's
`owner_loss_rootfs_run.py`, then the infra fixture's
`replacement_rootfs_bootstrap.sh` and `replacement_rootfs_run.py` on an exact
new disposable machine. Do not use a disk snapshot or copy session directories,
databases or buckets. Keep private backend keys/environment files mode 0600 and
pin SSH host keys. Save sanitized receipts before deleting both machines and
their operation-owned access key. This proves separate-machine RootFS recovery
after a fenced owner crash; it does not prove complete provider machine-loss
observation, actual Nomad replacement admission or crashed process-memory
recovery. The authority/certificate and OCI provenance remain fixture-specific.

## Actual Nomad, procd and live-owner transfer acceptance

The isolated systemd live-node fixture now has an explicit
`S0_NOMAD_LIVE_REAL_ROOTFS=1` mode. Its production runtime-slot, RootFS writer,
lifecycle and node-channel handlers use a fresh dedicated PostgreSQL database
and a real RustFS S3 bucket. Import publication records real object custody and
the embedded procd digest; OCI provenance and the certificate/node bindings
remain fixture-specific.

The gate passed after a real Nomad allocation, driver claim, authenticated procd
readiness probe and regional command-ready commit. The same guest wrote and
verified 600 MiB of random data through one retained file descriptor. XFS grew
from its 300 MiB initial image to 1,191,182,336 bytes of statfs data capacity on
the 64 TiB logical device. Three production Worker ownership-transfer cycles
preserved the allocation, zero restart count, Nomad/Sentry PIDs, guest PID,
heap, tmpfs, mmap and unlinked file offset. The first transfer also survived
the injected driver-publication failure and completed its exact journal.

Authenticated PostgreSQL observations before and after the cycles confirmed
the original grant remained consumed, the original slot active and the
filesystem's immutable initial generation unchanged. Installed sequence
advanced from 157,890 before handoff to 159,102 after the third cycle. Across
353 one-second samples, peak allocated worker WAL/read-cache/journal blocks
were 54,788,096 bytes, below the fixture's 128 MiB assertion despite 600 MiB of
guest data. This excludes the durable object backend, the temporary import
image and the virtual mounted filesystem. It is sampled one-writer evidence.

The fixture runner and provisioning/cleanup requirements are documented in
the infra repository's `ali/nomad/tests/live_node/README.md`. Services and guests
stopped through normal fixture teardown. This qualifies real node integration
and live ownership transfer; public manager lifecycle, memory checkpoint/resume,
replacement-node crash recovery and production release provenance remain open.

## Expanded RootFS memory-checkpoint acceptance

The optional isolated Nomad memory fixture uses the complete production
node-authority component, allocation observation, checkpoint coordination and
checkpoint claim planner. It retains canonical assignment and network-policy
inputs from the actual source claim, executes the canonical immutable stock
runsc bundle for native CPU evidence, and requests `memory: true` for both pause
and resume after growth and three live-owner transfers.

The workload starts once through authenticated procd context creation. A
restored allocation must retain the same running context, original guest PID,
heap and tmpfs markers, mmap and unlinked-file offset. Its original open growth
file is reverified against all 600 MiB of hashes retained in guest memory after
the regional restore receipt; a pre-checkpoint verification count is insufficient.
Stock gVisor deliberately kills `runsc exec`-injected processes on restore
([upstream explanation](https://github.com/google/gvisor/pull/11478)), so raw
exec injection cannot qualify this product process-continuity gate.

The restored writer has a new live cursor starting at zero. Acceptance waits
for its own first installed live cut instead of treating the source writer's
receipts as evidence of continued publication. The regional authority process
must also exit successfully. Checkpoint staging uses a separately enforced
512 MiB/4096-inode XFS project quota on a disposable 2 GiB sparse backing image;
it is excluded from, and does not replace, the fixed RootFS WAL/cache budget.
The complete gate passed on the isolated host. The original procd-managed guest
retained PID 10, its heap/tmpfs markers, mmap, unlinked FD and original open
growth file through the three transfers and a fresh-allocation memory restore.
After the regional restore receipt its counter advanced from 805 to 809 and
its full-file verification count from 2 to 3, proving a new complete 600 MiB
verification using retained in-memory hashes. The same authenticated procd
context remained running. The new writer installed sequence 1,024 after a
17.46-second wait; both committed and installed receipts agreed. XFS retained
1,191,182,336 bytes of statfs data capacity on its 64 TiB logical device.

The 322 one-second worker-artifact samples peaked at 54,706,176 allocated bytes,
below 128 MiB. This excludes the separate enforced checkpoint staging quota,
remote storage backend and temporary import image. The regional authority
finished successfully, and fixture services and guests stopped normally.
This qualifies expanded RootFS with real retained-memory pause/resume and
continued live publication; public manager API, abrupt node-loss recovery,
node-wide contention and production release provenance remain separate gates.

## Migration filesystem drain and live-WAL custody

A stopped Sentry may leave buffered XFS writes larger than the retirement
reserve. A Linux regression reproduced a freeze failure with a full 4 KiB
normal WAL, a 4 KiB reserve and two additional buffered metadata writes: the
second drain write returned a dirty-tail capacity error. Enabling retirement
admission before the kernel freeze prevented ordinary pressure/reclaim from
making progress.

Live-commit sessions now retain normal blocking admission during the migration
freeze. The exact original owner holds an in-memory drain permit and releases
the parent journal lock while its progress guardian publishes and installs
immutable bases. After freeze returns, it stops new cuts, joins active work,
settles receipts and reloads the updated base before syncing the immutable
migration boundary. Only then does it enter retirement admission. Sessions
without live publication retain their existing bounded reserve behavior.

The permit requires the exact migration operation and an unsealed journal;
owner recovery cannot reconstruct it from a persisted intent. Cancellation or
failed settlement leaves migration custody without a synced sequence, and
ordinary recovery cannot thaw or promote that cut. Owner shutdown wakes blocked
branch writes before joining the non-cancellable freeze. Regression coverage
checks installed-base bytes plus the WAL suffix, both lost receipts,
cancellation and owner shutdown. The focused migration/fork/growth Linux race
suite passed in 21.934 seconds; the full session race suite passed in 235.296
seconds. Real memory capture and replacement-node migration remain separate
deployment gates.

## Migration peer cancellation

The acceptance regression also reproduced a migration-peer cancellation stall:
an expired HTTP write deadline interrupted a non-reading destination, but
HTTP's write-error path then entered TLS `close_notify` with a fresh five-second
deadline. A blocked peer reader retained source publication custody beyond the
cleanup's one-second budget. The captured Linux goroutine stack confirmed the
wait in `crypto/tls.Conn.closeNotify`, with the publication worker waiting for
its readers.

The dedicated peer listener now retains the underlying socket in request
context. Abandoning a transfer closes that socket before expiring the HTTP
deadline, so TLS shutdown cannot prolong canceled stream custody. The durable
publication record stays intact. The migration peer/publication race suite
passed ten repetitions in 33.452 seconds; its fixtures use the same connection
context hook as the real listener.
The complete Nomad runtime race suite passed in 204.184 seconds after this
fix, and the complete driver race suite passed in 55.187 seconds.

## Historical-schema regression checks

Current artifact lookup requires migration 120's initial-filesystem geometry
column. Historical storage tests at schemas 108, 109 and 110 now seed their
original SQL shape explicitly, including the initial memory-fork filesystem,
before running the intended upgrade or rollback guard. Production readers do
not fall back to an unmigrated schema. These three PostgreSQL race tests and
live-commit receipt replay passed in 22.891 seconds. Rotation recovery now also
asserts that a rejected incomplete candidate keeps its exact truncated size;
the focused block/session rotation and live-commit race tests passed.

## Public capacity description synchronization

The public OpenAPI field keeps its existing name and shape. Its description now
identifies initial XFS capacity, conditional automatic growth and retained
geometry. Go, JavaScript and Python SDK copies were updated and regenerated with
their existing pinned generators. Review showed only the generated capacity
comments changed. Generated Go code compiled; JavaScript and website typechecks,
Python compilation and quota checks, and website docs metadata/generation
verification passed.

SDK/runtime/CLI README files, public template and self-hosted configuration docs,
website template labels and the infrastructure rollout plan describe the same
contract. Website MDX registries, Markdown aliases and `llms.txt` are generated
from the runtime docs at build time and are intentionally untracked. The feature
remains opt-in and has not been merged or deployed by this acceptance record.

## Live publication ordering

1. Persist a deterministic pending operation, source descriptor and flushed
   WAL cut before publication. Background cuts do not freeze XFS: blocked normal
   writes can otherwise prevent a freeze from completing and deadlock reclaim.
2. Journal each immutable object under regional upload custody before PUT and
   mark upload success before generation publication. Publish only from the
   exact consumed, unexpired writer and node/boot binding, serialized with
   lifecycle operations and fenced by the previous committed head.
3. Atomically insert the immutable child generation, transfer its object custody,
   advance the separate live progress cursor, and retain an idempotent operation
   receipt. Keep the lifecycle head and launch binding unchanged. A lost response
   retries the same cut and payload.
4. Keep the grant's installed-base generation pinned for GC while the node still
   reads it. The committed head alone is insufficient: a previous base's root
   mapping may no longer be reachable from the new descriptor even though the
   running NBD branch still needs it. Update this pin only after durable local
   installation, with an idempotent regional installation acknowledgement.
5. Persist the regional receipt locally; install the new reader and suffix WAL
   through the session journal's durable transaction, retaining both complete
   versions on ambiguous local errors. Keep NBD attached to the same Branch.
6. If duplicated suffix capacity does not fit normal node headroom, retain the
   acknowledged head and old complete WAL and take a newer cut. At admission
   pressure, normal writes are already blocked, so a later acknowledged cut can
   have an empty suffix and rotate without borrowing retirement reserve. The
   pending journal must distinguish published head from installed base through
   these retries; lifecycle cuts must include every post-install WAL write and
   keep their inherited installed base pinned until inventory completes.
7. Growth must retain exact mount/session ownership without preventing the
   commit/rotation path from reclaiming WAL if kernel growth writes encounter
   pressure. A parent mutex held across a blocking growth ioctl must not also
   be required by the reclaim operation that can unblock it.
8. Coordinate fork, memory checkpoints, pause, migration and live handoff with
   pending publication/installation. Preserve original writer proofs while using
   the current installed descriptor and WAL for disk contents. Serialize capture
   publication with installation so the regional inherited-base pin matches the
   base used by the cut; retain the immutable launch parent for lifecycle proofs.

Regional receipts, node journal recovery, bounded scheduling and growth custody
are implemented. Full-product service/lifecycle acceptance and deployment remain
required before enabling either node switch or sparse imports.
