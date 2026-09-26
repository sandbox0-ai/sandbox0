# RootFS storage retention and metering

This document records the repair and its validation. It does not authorize
production deployment, object deletion, invoice adjustments or refunds.

## Root causes

Checkpoint publication creates an immutable generation and advances the filesystem
head. Previously, only whole-filesystem collection released its object references.
Unbound fork-origin filesystems could also retain an unrelated former head. Storage
accounting deduplicated objects across every generation, so objects unique to old
versions continued to contribute bytes even without a named snapshot.

Two additional historical FKs were storage roots: immutable memory-fork retry
records, and terminal runtime slots' original launch generations. Their historical
identities remain useful after their disk custody ends; keeping those identities
does not require keeping obsolete disk objects.

A materialized descriptor reads its complete mapping tree directly, without loading
generation ancestors. Incremental builders report newly published objects, however,
not every inherited tree/data object. Deleting ancestors merely because their IDs
are not heads would destroy unchanged data. Parent IDs must also remain immutable
for publication and restore identities.

Previously, storage observations enumerated teams from existing filesystems. The
last filesystem's deletion removed the team from that scan, while PostgreSQL's
storage projection retained its positive capacity. The next observation after
recreation integrated that old capacity across the empty interval. A stale state
alone does not establish that a payment was taken during the empty interval; the
window, projection, export, import and settlement must be examined separately.

## Authority and collection

Migration 109 maintains distinct per-team object reference counts and absolute
capacity transitions in the transaction that adds/removes references or acknowledges
an upload. Removing the last uploaded reference produces an explicit zero transition.
The tenant retention boundary is metadata-reference removal; failed S3 deletion or
temporary upload/publication journals can leave platform-owned physical bytes until
the existing deletion worker succeeds. Base-artifact storage remains independently
retained and is excluded from writable tenant inventory.

The metering worker locks the same team usage row as capacity mutations, replays
at most 1,000 transitions in revision order, and acknowledges them in the same
transaction as the metering outbox. It never samples past an undrained transition.
Accounts are selected in bounded batches with oldest-processed-first ordering.
Quiescent zero accounts stop producing repeated observations. Their small authority
rows remain so later recreation starts from zero and continues the revision order.

Migration 110 replaces permanent ancestry FKs with temporary read-dependency FKs.
The immutable parent ID remains as provenance. A persisted page queue traverses
authenticated mapping ranges; each step registers a bounded page's actual object
references, including inherited objects. Base objects retain their platform owner.
Until the traversal completes, the ancestor dependency FK remains. Once complete,
it is removed atomically with the completion marker. Inventory uses a shared
generation lock and an advisory worker lock, so immutable readers can proceed
while network I/O runs. A step checks a two-second work budget between pages and
has a thirty-second deadline. Invalid/unavailable mappings fail closed and rotate
behind other pending roots. It first takes compatible filesystem custody, following
the same lock order as checkpoint publication. Persisted partial queues continue
even if a newer head is published; active writes do not discard traversal progress.

Generation GC is bounded and reentrant. Retention roots include filesystem heads,
snapshots, rollbacks, runtime-slot sources, live writers, active lifecycle expected/
prepared/target generations, running forks/captures, runtime checkpoint references
and live fork custody, and bounded materialization journals. Incomplete child
dependencies remain real FKs. Removal cascades object references and reuses the
durable object deletion queue. Shared packs are retained while any reference needs
any of their ranges; this change does not compact live packs.

Sandbox deletion releases an unbound origin's former head while keeping its origin
row and IDs. Child heads, snapshot heads and incomplete dependencies still protect
their own versions. Explicit snapshots and effective fork/runtime custody keep their
existing retention promises.

Migration 111 separates immutable memory-fork `generation_id` from its live
`generation_ref`. Release is one-way, after the fork is committed and that child's
image reference ends; other owners and heads independently retain the generation.
Migration 112 similarly retains terminal slots' exact launch identity while
releasing `source_generation_ref` only with physical terminal proof, a released
resource lease and a retired/canceled writer. Cleanup retries retain their launch,
writer, allocation and proof identities. Background batches also reconcile old
deleted-origin heads, requiring durable owner-deletion evidence and no binding,
live writer or running fork/capture.

Migration 113 closes a separately discovered node path: large dirty tails,
execution-memory cuts and offline rebase materialize directly to S3, outside
manager batches. The
internal authenticated writer endpoint reserves every complete object before PUT,
then acknowledges it. Publication of retirement, running fork/template capture,
memory checkpoint, migration or rebase transfers that operation's holds into generation
references in the same transaction. A deletion fence is checked after acquiring
catalog custody, so a concurrent retirement cannot delete a newly reserved object.
The node bounds each reserve/PUT/ack sequence to thirty seconds. Unpublished holds
remain retryable while the writer is live; its existing terminal CAS records a
terminal boundary. After a two-minute quiet interval, bounded GC releases abandoned
objects, including successful PUTs with lost acknowledgements. Upload holds are
platform custody and are not tenant capacity. Published journal metadata has a
bounded terminal replay window.

Offline rebase uses its existing region-selected worker node and lifecycle identity
instead of a live sandbox writer grant. Its upload custody ends only after the exact
node cleanup acknowledgement, including rejected operations during sandbox deletion.
Rollback pins retain the old and new disk cuts independently of upload journals.
Reusing a platform base object's existing key does not create tenant capacity.

Legacy direct-upload generations with no catalog references remain protected by
`storage_inventory_required`, including after their last owner is deleted. An exact
base descriptor can reuse platform custody; an equal mapping digest with a different
object locator still requires inventory. Inventory verifies complete object content
addresses and plaintext sizes before adoption. It
adopts at most four missing objects per step, checks a two-second budget between
objects, and persists temporary custody with its page queue. The next worker can
resume that page without repeating already committed verification. References then
replace temporary custody page by page. No old generation is collected while its
unknown uploads still require inventory. Newly adopted capacity begins at the known
reconciliation time; the repair does not infer historical raw-upload charges.
Invalid, unavailable or noncanonical legacy objects remain protected for review.

## Existing-state reconciliation

An orphan positive legacy projection has no trustworthy historical deletion time.
Reconciliation records its previous projection in
`manager.rootfs_storage_reconciliation_audit` and establishes zero at the known
reconciliation boundary, without emitting an invented historical window. Existing
usage windows, charges and settlements are unchanged. A later legacy bootstrap that
resurrects a positive projection for an authoritative zero account is handled likewise.

Read-only checks (run with the matching regional database; do not infer historical
removal time from `sandbox.deleted_at`, since snapshots/forks may outlive it):

```sql
SELECT state.subject_id, state.size_bytes, state.observed_at,
       usage.storage_bytes AS current_bytes, usage.changed_at, usage.revision
FROM metering.storage_projection_state state
LEFT JOIN manager.rootfs_storage_usage usage ON usage.team_id=state.subject_id
WHERE state.subject_type='rootfs' AND state.size_bytes>0
  AND COALESCE(usage.storage_bytes,0)=0;

SELECT team_id, COUNT(*) AS pending, MIN(changed_at) AS oldest
FROM manager.rootfs_storage_transitions GROUP BY team_id;

SELECT team_id,reconciled_at,previous_state,reason
FROM manager.rootfs_storage_reconciliation_audit ORDER BY reconciled_at;

SELECT COUNT(*) AS pending_inventories,
       MIN(COALESCE(inventory_attempted_at,created_at)) AS oldest_attempt
FROM manager.rootfs_generations
WHERE NOT inventory_complete AND durability_state='s3_materialized';

SELECT generation_id,filesystem_id,created_at,inventory_attempted_at
FROM manager.rootfs_generations WHERE storage_inventory_required
ORDER BY COALESCE(inventory_attempted_at,created_at),generation_id LIMIT 1000;

SELECT grant_id,operation_id,state,terminal_at,updated_at
FROM manager.rootfs_node_uploads
WHERE state='uploading' ORDER BY terminal_at NULLS FIRST,updated_at LIMIT 1000;

SELECT generation_id,COUNT(*) AS verified_pending_objects
FROM manager.rootfs_inventory_object_custody GROUP BY generation_id;

SELECT slot_id,source_generation_id,source_generation_ref,terminal_at
FROM manager.runtime_slots
WHERE state='terminal' AND source_generation_ref IS NOT NULL
ORDER BY terminal_at,slot_id LIMIT 1000;

SELECT operation_id,target_sandbox_id,generation_id,generation_ref,storage_released_at
FROM manager.sandbox_runtime_checkpoint_forks
ORDER BY created_at,operation_id LIMIT 1000;
```

Historical review must join actual regional window IDs to Cloud immutable usage
imports, effective prices and settlement entries. Use reliable reference-removal
receipts/logs or backups to establish the empty interval. If the exact boundary is
unknown, preserve the audit and report the uncertainty; do not fabricate an earlier
zero timestamp. Any financial correction requires separately reviewed compensating
entries through the billing system, not mutation of historical ledger rows. This
implementation does not perform corrections or refunds. A legacy team that has
already recreated storage before migration also needs historical review: today's
positive inventory alone cannot establish whether an earlier empty interval existed.

## Rollout and recovery

1. Back up PostgreSQL and retain object-store recovery/versioning capability. First
   validate migrations and the workload in an isolated environment. Initial
   backfills and index creation touch existing inventory; measure their duration
   on a representative copy and schedule a maintenance window. The bounded
   controller does not make these schema migrations zero-downtime DDL.
2. Stop legacy RootFS maintenance/metering writers before applying migrations 109–113.
   Their polling-only observations must not advance a projection past an undrained
   zero transition. Keep them stopped through replacement of every manager replica;
   an old publication retry also expects an ancestor that reclamation may remove.
3. Apply migrations and replace every ctld/node publisher with the custody-aware
   build before enabling inventory or deletion. Confirm all old raw builders have
   stopped and allow their bounded in-flight requests to drain. An old publisher
   must never race deletion of a globally content-addressed object. Start the new
   metering path, and inspect orphan audits and
   pending transitions. Mutation triggers capture changes even while maintenance is
   temporarily disabled; processing later uses their committed boundaries.
4. Inventory retained versions and protected legacy uploads before retiring history.
   Inspect complete/incomplete markers, byte totals, independent S3 reads and
   deletion retries in a small canary.
   An operator-driven job using the existing team-scoped store methods can limit
   the initial cohort; the normal controller processes all teams. Keep other
   maintenance instances disabled during that cohort. Expand after evidence
   shows retained snapshot/fork/runtime contents and billing windows are correct.
5. Watch deletion queue pending/due/claimed/dead-lettered counts and maintenance
   failures. An unavailable/corrupt mapping must leave ancestry intact. Resolve the
   source error and let the persisted queue retry. Failed object deletion keeps its
   durable retry state; never bypass catalog/journal registration before upload.

Recovery should preserve the capacity journal and reconciliation audit. Do not run
schema down migrations after reclamation: restoring an ancestry FK cannot recreate
removed parent identities or S3 bytes. A binary rollback must keep legacy RootFS
maintenance writers disabled and preserve a compatible transition processor. Recover
incorrectly removed content from coordinated PostgreSQL/object backups only after
identifying the affected immutable roots; do not republish guessed object identities.
Migrations 109, 111, 112 and 113 reject schema rollback to preserve financial/history
authority; migration 110 also rejects restoring an ancestry FK after parents are
collected. After reclamation, roll forward with compatible code. An old binary that
requires the removed ancestor rows is not a safe rollback target.

## Validation evidence (2026-09-27)

Local isolated services: PostgreSQL 17.6, RustFS 1.0.0, Go 1.25.5,
ClickHouse 26.10.1.753 development build. No production data or invoices were used.
Linux validation uses a separate Ubuntu 24.04/Linux 6.8 host and PostgreSQL 16.15.
Its disposable database moved to a dedicated 2 GiB tmpfs for test throughput,
with fsync and synchronous commit enabled. Worker/transaction recovery is covered;
this is not a host power-loss durability rehearsal.

- The previous filesystem-only GC path fails the real S3 regression: after 24
  writer checkpoints, 25 generations remain instead of one.
- With inventory/GC: 24 overwrites converge from 49 S3 objects to 4; 240
  overwrites converge from 481 to 4. The final encrypted macOS 240-version trace
  decreased 310,025 ciphertext bytes to 2,338 and 72,860 tenant bytes to 334.
  Its 24-version trace decreased 31,447 ciphertext bytes to 2,339 and 7,258 tenant
  bytes to 335. Small compressed
  payload/locator sizes can vary between fixture runs. The retained data includes
  the current map/data, a shared pack containing an unchanged block, and the base map.
  Tenant accounting keeps its existing compressed plaintext-object size contract;
  envelope overhead and independently owned base objects explain the physical
  byte difference. History count does not change the retained object count here.
- Independent S3 reads preserve overwritten/current content, inherited blocks,
  named snapshots, fork/restore copies, and evolved forks after origin deletion.
- A one-page inventory step resumes on another worker, including after the head
  advances. Injected deletion failure
  leaves valid content readable; a subsequent worker drains the queue after retry.
- The original implementation's real PostgreSQL deletion/recreation trace emits
  3,047,424 byte-hours instead of the correct 8,192; its last-filesystem scan also
  leaves a 4,096-byte projection despite zero filesystems. These are fail-before
  results, not evidence of a production payment during the gap.
- The repaired 31-day trace with two retained hours and an empty interval emits exactly 8,192
  byte-hours for two retained 4,096-byte hours. Real outbox delivery and ClickHouse
  export preserve the same two windows. Duplicate/late observations, transaction
  rollback, locked accounts, 1,002 capacity transitions, idle zero accounts,
  late legacy bootstrap, migration-108 backfill and migration-110 live-memory-fork
  backfill cases pass. Rejected down migrations preserve capacity history.
- The actual regional HTTP export, Cloud HTTP client, PostgreSQL import/rating and
  settlement preserve those 8,192 byte-hours. With the artificial test price of
  two micros per byte-hour, settlement records 16,384 micros; repeated import and
  settlement add zero. This is test pricing and a shadow account, not a payment
  provider charge or a change to a customer's financial records.
- Concurrent publication with two inventory/GC workers passed ten consecutive
  encrypted S3 runs: twelve checkpoints per run and four acknowledged snapshots
  remain readable. Three subsequent runs also created two forks during collection;
  all acknowledged roots remained readable. Pending-deletion registration refuses an old object identity;
  after physical S3 deletion/queue acknowledgment it can be safely reuploaded.
- The raw node S3 path initially failed on an unregistered mapping. Five
  overwrites now reconcile and collect 11 objects to three, retaining current
  contents. A real migration-112 database containing seventeen raw-upload objects
  upgrades successfully; a four-object partial custody step survives a worker
  restart, and last-owner deletion eventually leaves zero tenant bytes. Reserved
  object reuse survives collection before PUT. Offline rebase publication/retry,
  rollback reads and rejected-upload cleanup also pass with real S3. Lost-acknowledgement uploads remain
  protected while live and are physically deleted after terminal custody ends.
- Real PostgreSQL pause/resume, writer fencing and runtime-slot transitions retain
  independently readable S3 contents after GC. The test uses fixture node receipts;
  it does not by itself prove a live Nomad/gVisor allocation. A NULL terminal receipt
  originally bypassed a SQL length comparison; a fail-before/pass-after regression
  now proves that missing physical evidence cannot release disk custody.
- A separate real RustFS 10,000-generation packing workload publishes only three
  objects, with a final Linux disk increase of eight files and 2,380,375 bytes.
  Shared pack granularity, retained snapshots and unfinished jobs can retain bytes;
  this repair does not compact partially live packs.
- The explicit 1,000-deep plus 1,000-way fork scale gate passes with one shared
  generation. Related rootfsblock/outbox/maintenance race suites, architecture
  checks and Linux-targeted repository lint pass. Skipped tests are not service
  validation evidence.

Static privileged stock-runsc checkpoint, procd filesystem resume, memory pause/
resume, memory fork and failed-restore retry gates pass on isolated Linux.
Separate race-enabled gates on an exclusively owned NBD device pass encrypted
XFS demand reads, copy-up/WAL reattachment, busy-consumer retirement retries,
dirty-tail headroom and XFS shutdown/thaw. The real bind-mount identity gate also
passes. Two old fixture cases initially requested unsupported 1 MiB data ranges;
they now use Format2's 64 KiB ranges while retaining real 1 MiB encryption frames
and all byte-content/reattachment assertions.
The full final macOS sandboxstore race suite completes all 398 top-level names,
with 608 pass records and no failures or skips. Linux's four database-isolated
sandboxstore shards complete the same 398 names, with 611 pass records including
four package completions. Other Linux manager packages produce 1,723 pass records;
related block/session/runtime/object/metering/architecture packages produce 1,898.
The final NULL-proof tightening also passes a fresh Linux runtime-slot and
pause/resume suite. Real PostgreSQL/S3 durable import publication replay passes
separately. Closed Cloud package tests produce 1,319 pass records; the complete
storage export/import/settlement test passes separately.

Pass records include subtests and package completions, not just distinct test
functions. Ordinary Linux suites skip helper entry points and opt-in gates;
required privileged/real-service gates run separately. These skips are not counted
as validation evidence. The optional sixteen-million-extent 1 TiB mapping model
and 24-hour runtime-slot soak were not run. They are not claims about production
scale or endurance.

The disposable Linux host and its temporary SSH key were deleted after logs were
downloaded and checksum-verified. Local test services stopped and their disposable
data was removed. Source changes and diagnostic evidence remain in the workspace;
production infrastructure, invoices and balances were not modified.

### Reproduction commands

Use isolated databases and a real disposable S3-compatible bucket. Set
`INTEGRATION_DATABASE_URL`, `SANDBOX0_RUSTFS_ENDPOINT`, `SANDBOX0_RUSTFS_BUCKET`,
`SANDBOX0_RUSTFS_ACCESS_KEY`, `SANDBOX0_RUSTFS_SECRET_KEY` and optionally
`SANDBOX0_RUSTFS_DATA_DIR`. The sandboxstore fixtures recreate their manager schema,
so do not share that database with another concurrent sandboxstore test process.

```sh
SANDBOX0_RETENTION_ENCRYPTED=1 ROOTFS_FORK_SCALE_TEST=1 \
  go test -race -count=1 -timeout=20m ./manager/pkg/sandboxstore
go test -race -count=1 ./pkg/rootfsblock ./pkg/objectstore ./pkg/metering/...
go test -race -count=1 ./manager/pkg/rootfsmaintenance ./manager/pkg/rootfsmaterializer ./tests/architecture/...
go test -buildvcs=false -race -cover -count=1 -timeout=30m ./manager/...
GOFLAGS=-buildvcs=false golangci-lint run ./...
```

On a disposable root-owned Linux host, enable the documented NBD/XFS gates with
`SANDBOX0_PRIVILEGED_NBD_DEVICE` set to a verified unused device excluded from every
ctld pool, plus `SANDBOX0_RUN_XFS_THAW_TEST=1`; run `go test -race -count=1 -run
'^TestPrivileged' ./pkg/rootfssession`. Enable `SANDBOX0_RUN_MOUNT_IDENTITY_TEST=1`
for `TestPrivilegedStableMountUnderlyingIdentity` in nomadruntime. The final run
uses `/dev/nbd0` exclusively in the disposable host. Set
`SANDBOX0_ROOTFS_IMPORT_DATABASE_URL` for the real RustFS durable importer replay.
Stock-runsc/procd guest tests require a static test binary (`CGO_ENABLED=0 go test
-c`) and `SANDBOX0_RUN_PRIVILEGED_CHECKPOINT=1`, as their fixture instructions state.

For a slow test machine, sandboxstore may be sharded by top-level test name:
compile it once with `go test -c -buildvcs=false -race -cover`, enumerate
`-test.list '^Test'`, and partition the complete unique name list. Each process
must use a different disposable PostgreSQL database, and each name must run exactly
once with all its subtests. The final Linux run used four database-isolated shards
covering all 398 top-level names. Run other schema-recreating manager packages
serially in another database. Run the RustFS physical-disk packing test after all
S3 workloads finish, because its disk-size measurement cannot overlap them.

For the complete billing chain, set `SANDBOX0_CLICKHOUSE_INTEGRATION_DSN` and
`SANDBOX0_STORAGE_WINDOW_EVIDENCE_PATH` to a local artifact path, run
`TestRootFSStorageDeletionRecreationGapIntegration` in sandboxstore, then run
`TestStorageGapExportImportSettlementIntegration` in the closed Cloud repository's
`./billing-worker/tests` with its isolated `S0_CLOUD_TEST_DATABASE_URL`. The latter
consumes the actual producer's exported windows. Never substitute production data.

The tests cover the retention and financial authority paths, not production load
capacity, previously orphaned raw objects whose generation metadata was already
deleted before this repair, Aliyun OSS outages/versioning or a complete live regional Nomad deployment.
Inventory/deletion throughput and backlog age must be watched during rollout;
an input rate exceeding a bounded worker's capacity requires more service capacity,
not bypassing dependency or upload fences.
