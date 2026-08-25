# Legacy ACK sandbox migration

`tools/legacy-ack-migration` converts the final ACK manager schema (version 19)
to the Nomad block-COW product graph. The old and new runtimes may use the same
regional PostgreSQL and object store. The tool therefore keeps the full frozen
source catalog in `legacy_ack_migration.source_catalogs`, outside the manager
schema and without foreign keys to it.

This procedure is an irreversible production cutover. Keep gateways closed and
the old manager stopped from the beginning of the capture through target
catalog commit. Use absolute, owner-only (`0600`), single-link files for every
PostgreSQL DSN and manager configuration. Reports contain only counts and
digests; they never contain sandbox configuration or storage credentials.

## Ordered transition

1. Pause every live sandbox. Stop all ACK manager replicas and any writer that
   can mutate manager tables. Drain active lifecycle transactions, RootFS object
   deletions, and deletion-webhook deliveries.
2. Run `inventory` and `validate` against schema version 19. Preserve the
   owner-only validation report.

   A legacy manager could recreate an exact
   `source_filesystem_id = filesystem_id` edge after additive migration 00016
   cleaned it. Validation records the number of these historical same-filesystem
   restore edges and canonicalizes only those edges to no source lineage in the
   target graph. The captured source catalog and its retirement digest retain
   the original row. Any multi-filesystem cycle still fails validation.
3. Run `capture` with one immutable session ID and the target Nomad cluster ID.
   Capture revalidates the complete graph and stores the full catalog in
   PostgreSQL. Preserve its `source_catalog_digest`.
4. Run `retire` with that exact digest in
   `--confirm-source-catalog-digest`. Retirement takes exclusive `NOWAIT`
   locks, rereads the source catalog, compares the digest, rejects non-empty
   deletion queues, and atomically drops only the legacy `manager` schema while
   marking the independent capture retired. An exact retry returns the original
   retirement marker.
5. Start the new manager with public traffic still closed. Its normal migrations
   create a fresh Nomad-only manager schema. Keep the durable OCI RootFS importer
   running.
6. Run `prepare` with the new manager configuration. It creates exact migration
   build operations and reuses the normal Base-artifact importer. Repeat until
   `pending_base_imports` is zero.
7. Run `build` as root on a dedicated Linux host with XFS tools, loop/mount
   support, sufficient staging space, the pinned `procd`, and access to both the
   legacy and target object-store configurations. Every immutable object is
   journaled before PUT, uses conditional create, and converges through a fenced
   ready CAS.
8. Run `commit`. It atomically installs paused sandbox, filesystem, generation,
   snapshot, and binding rows only after every required build and Base artifact
   is ready. Exact retries return the same commit digest.
9. Enable Nomad carriers, resume representative sandboxes, verify files,
   snapshots, process execution, network policy, metering, and public routing,
   then complete traffic cutover.
10. After the acceptance record is durable, drop the temporary
    `legacy_ack_migration` schema, remove the migration binary and secret files,
    delete the ACK cluster and obsolete ECS resources, and audit DNS, security
    groups, credentials, object prefixes, and deployment policies for residue.

The `source-dsn-file` and `target-dsn-file` may point to the same regional
PostgreSQL endpoint. `source-manager-config-file` defaults to the target manager
configuration for `build`; specify the frozen ACK manager configuration when
legacy RootFS objects use different credentials or encryption keys. Never pass
DSNs, storage keys, registry credentials, or encryption passphrases as command
line arguments.
