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

1. Validate the wired authenticated scheduler with real remote object storage
   and regional services in the full Nomad deployment, including lifecycle/
   fencing races. The combined component gate below passed.
2. Exercise growth and live reclaim through lifecycle barriers and owner loss;
   the combined scheduled-growth guest workload below passed.
3. Extend the injected S3 request-outage gate below to longer interruptions and
   node-wide contention; the new node configuration is still disabled by default.
4. Verify all lifecycle operations, GC and live ctld transfer with installed
   live bases and interrupted node operations.
5. Deploy the regenerated contracts/SDK descriptions and synchronized runtime,
   CLI, website and operator docs alongside the compatible runtime release.
6. Use the isolated remote fixture for real NBD/XFS/runsc and PostgreSQL/
   object-storage acceptance. Retain sanitized evidence and remove resources.
7. Merge passing changes, deploy manager before enabling node behavior, then
   roll workers with memory-preserving drain and verify production continuity.

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
retained its PID, heap marker and open file while writing 600 distinct 1-MiB
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
600 distinct 1 MiB chunks. Two growth operations increased XFS from 300 MiB to
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

One gVisor guest process wrote and fsynced 600 distinct 1 MiB chunks through one
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
