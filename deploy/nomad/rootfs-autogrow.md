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

1. Complete acceptance of separate initial XFS size and sparse device geometry
   through imports and their immutable artifact identity.
2. Add mounted-XFS inspection and bounded early growth under session ownership.
3. Add crash-safe WAL suffix rotation and accurate node budget reclamation.
4. Add authenticated, idempotent live generation publication and recovery,
   coordinated with lifecycle operations and regional storage custody.
5. Schedule bounded live commits/growth and wake blocked writers after reclaim.
6. Regenerate affected public contracts/SDKs and update runtime, CLI, website,
   and operator docs to describe initial size, growth ceiling, and durability.
7. Create an isolated remote fixture and run real NBD/XFS/runsc and PostgreSQL/
   object-storage acceptance. Retain sanitized evidence and remove resources.
8. Merge passing changes, deploy manager before enabling node behavior, then
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

Migration 121 and the authenticated `/live-commit` and `/live-commit/installed`
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
full-product automatic growth, node journal recovery, remote object-service or
gVisor acceptance. The node scheduler and durable rotation transaction remain to
be integrated before enabling sparse imports or deploying this feature.

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

Regional commits and installation receipts are implemented; node journal recovery,
scheduling and lifecycle coordination are still integration requirements.
