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

1. While the old runtime is still serving traffic, run `preflight`. This
   read-only mode accepts only active or paused sandboxes, validates every
   schema, graph, platform, template, and resource-policy invariant, and emits
   `freeze_ready: false` plus the non-paused count when a final pause barrier is
   still required. An active ACK sandbox may not have its first durable RootFS
   binding until pause; preflight reports that explicitly as a deferred binding
   while still validating its template and resources. Strict validation and
   capture never permit a missing binding. Preflight never captures state and
   does not weaken later commands.
2. Before closing ingress, run `pause-access` through a loopback
   `kubectl port-forward` to the old manager. Supply its data-plane signing key
   through an owner-only file. This read-only check signs a fresh team-bound
   token and verifies manager ownership access for every live sandbox without
   dispatching a pause. Then close public and SSH ingress and run `pause` with
   the same inputs. The tool calls the normal manager lifecycle API
   with a fresh team-bound token for every active sandbox; it never edits
   desired state directly. It rejects any change to the live sandbox set and
   waits for every sandbox to be paused, lifecycle transactions to finish, and
   RootFS deletion and deletion-webhook queues to drain. Preserve its report.
   Then stop all ACK manager replicas and remaining writers.
3. Run `inventory` and strict `validate` against schema version 19. Preserve the
   owner-only validation report.

   A legacy manager could recreate an exact
   `source_filesystem_id = filesystem_id` edge after additive migration 00016
   cleaned it. Validation records the number of these historical same-filesystem
   restore edges and canonicalizes only those edges to no source lineage in the
   target graph. The captured source catalog and its retirement digest retain
   the original row. Any multi-filesystem cycle still fails validation.
4. Run `capture` with one immutable session ID and the target Nomad cluster ID.
   Capture revalidates the complete graph and stores the full catalog in
   PostgreSQL. Preserve its `source_catalog_digest`.
5. Run `retire` with that exact digest in
   `--confirm-source-catalog-digest`. Retirement takes exclusive `NOWAIT`
   locks, rereads the source catalog, compares the digest, rejects non-empty
   deletion queues, and atomically drops only the legacy `manager` schema while
   marking the independent capture retired. An exact retry returns the original
   retirement marker.
6. Start the new manager with public traffic still closed. Its normal migrations
   create a fresh Nomad-only manager schema. Keep the durable OCI RootFS importer
   running.
7. Run `prepare` with the new manager configuration. It creates exact migration
   build operations and reuses the normal Base-artifact importer. Repeat until
   `pending_base_imports` is zero.
8. Run `build` as root on a dedicated Linux host with XFS tools, loop/mount
   support, sufficient staging space, the pinned `procd`, and access to both the
   legacy and target object-store configurations. Every immutable object is
   journaled before PUT, uses conditional create, and converges through a fenced
   ready CAS.
9. Run `commit`. It atomically installs paused sandbox, filesystem, generation,
   snapshot, and binding rows only after every required build and Base artifact
   is ready. Exact retries return the same commit digest.
10. Enable Nomad carriers, resume representative sandboxes, verify files,
   snapshots, process execution, network policy, metering, and public routing,
   then complete traffic cutover.
11. After the acceptance record is durable, drop the temporary
    `legacy_ack_migration` schema, remove the migration binary and secret files,
    delete the ACK cluster and obsolete ECS resources, and audit DNS, security
    groups, credentials, object prefixes, and deployment policies for residue.

The `source-dsn-file` and `target-dsn-file` may point to the same regional
PostgreSQL endpoint. `source-manager-config-file` defaults to the target manager
configuration for `build`; specify the frozen ACK manager configuration when
legacy RootFS objects use different credentials or encryption keys. Never pass
DSNs, storage keys, registry credentials, or encryption passphrases as command
line arguments.
