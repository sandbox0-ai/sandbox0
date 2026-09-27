# Shared objects during legacy inventory

A legacy direct publisher uploaded content-addressed objects without regional
object references. Migration 113 retained the generation until its mapping
inventory completed. That generation hold did not protect a shared object
registered by another writer: retiring the registered owner could remove its
last known catalog reference and delete an object still reachable from an
unknown legacy graph. Inventory would then encounter NoSuchKey; the same
sequence could also break a retained legacy reader.

Migration 115 adds durable object retirement holds and an indexed unknown-graph
predicate. While any generation requires legacy inventory, retiring catalog
objects keep a hold, and the final physical-delete path defers existing queue
entries without consuming retry attempts. Content addresses can be shared
across filesystems and tenants, so this temporary gate is regional. Metering
continues to charge actual generation references, not deferred GC holds.

After all unknown graphs are authenticated, each deletion pass revisits at most
its batch limit of held objects. Real references continue to protect them;
unreferenced objects enter the existing persistent deletion queue. Lost PUT
acknowledgements remain eligible for idempotent deletion. Worker restarts and
queue retries do not discard this custody. A transaction advisory gate serializes
new unknown generation publication with the final external DELETE; modern
publishers still require their ordinary reserve-before-PUT protocol. Physical
calls use the context-aware store with a 30-second deadline.

Existing unclaimed queue entries can be canceled during legacy adoption only
after full-object encryption authentication and complete content-address
verification. An active deletion claim is never stolen. Mapping read and
adoption failures include generation, object key and stage for diagnosis.

Run the encrypted S3/PostgreSQL regressions with an isolated endpoint:

```sh
INTEGRATION_DATABASE_URL=postgres://... \
SANDBOX0_RUSTFS_ENDPOINT=http://... \
SANDBOX0_RUSTFS_ACCESS_KEY=... SANDBOX0_RUSTFS_SECRET_KEY=... \
SANDBOX0_RETENTION_ENCRYPTED=1 \
go test -race -count=3 ./manager/pkg/sandboxstore -run '^TestRootFSLegacy'
```

The shared-owner test fails before the fix with a real S3 NoSuchKey and passes
with retained content intact. After inventory, worker restart and final owner
deletion, temporary holds disappear and only the platform base mapping remains.
Additional tests cover an inherited queue, a live claim, and publication waiting
through the actual S3 DELETE. The existing overwrite, snapshot/fork/restore,
node upload and rebase suites remain required; skipped tests are not evidence.

## Rollout and recovery

Finish upgrading or fencing pre-custody publishers with the existing maintenance
cutover procedure. Do not resume legacy raw uploads concurrently with deletion:
those publishers cannot reserve objects before PUT. Install the migration before
starting the compatible manager; let bounded inventory and GC converge. Monitor
`storage_inventory_required`, pending inventory pages, temporary hold count/bytes,
delete queue age/retries and exact generation/object error context. The gate
trades temporary platform storage for data safety and cannot close while an
unrecoverable legacy graph remains. Do not clear the flag to force progress.

If inventory already encounters a missing object, first verify the full mapping
reference and all retention roots. Where object versioning retains a trusted
copy, authenticate its encryption, complete content address and mapping range,
then restore exactly that immutable content via the normal conditional publisher
only after the protection is installed. This is object recovery, not a RootFS
head change or billing-history correction. Never fabricate bytes, timestamps,
ledger adjustments or user-visible snapshots. If no authentic copy survives,
retain protection and report the missing graph rather than treating it as an
empty filesystem. Repairing a graph does not prove every legacy graph is intact.

Migration 115 intentionally requires compatible forward recovery. Do not drop
pending holds or run an older collector as a rollback. Preserve the worker image,
checkpoint retention and billing state; this change requires no public API change.
