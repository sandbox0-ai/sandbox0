# Node-provided procd

procd is a platform executable, separate from user RootFS generations. A cold
runtime assignment can select a newer executable without rewriting user data,
reimporting an image, or changing the resource-neutral carrier pool.

## Executable identity and isolation

`runtime_procd` in the manager configuration selects a SHA-256 digest and protocol.
The manager copies this pair into the immutable runtime assignment for create,
resume, and snapshot-derived create. Its assignment revision is fenced by the
existing PostgreSQL reservation and the driver's persisted claim. Retrying a
claim with a different executable is a conflicting assignment, never an implicit
upgrade. Changing the manager policy affects new runtime generations only.

Driver 0.3.0 or newer resolves the exact executable under
`/var/lib/sandbox0-procd/sha256/<hex>/procd` (configurable using
`procd_artifact_dir`). Every path component must be root-owned, without symlinks
or group/world write access. The cache is a sibling of service-owned
`/var/lib/sandbox0`, so service users cannot replace a cache ancestor. The executable is a non-writable regular file,
verified against the assigned digest before RootFS attachment and runsc launch.
The OCI bundle binds only that file read-only at `/procd`, with `nosuid,nodev`.
Neither the host directory nor host credentials are exposed to the guest. procd
still executes inside stock gVisor and the sandbox resource lease.

The manager rejects mounted-executable configuration with an older driver class.
A missing or corrupt node artifact fails the claim; there is no fallback to the
embedded binary. Operators must install the selected artifact on every eligible
worker and include it in the authenticated bootstrap release for new workers
before publishing that selection. This deployment ordering is a prerequisite,
not an assertion that driver availability implies artifact availability.

The installer publishes by digest without replacing an existing inode. Keep all
versions referenced by live allocations, saved memory images, pending operations,
or rollback policy. Release-directory cleanup cannot remove the separate cache.
Cache pruning must only remove versions proven unreferenced under a maintenance
fence; the installer deliberately does not guess which live versions are safe to
remove.

## Existing and new RootFS artifacts

An existing RootFS may still contain an old `/procd`. The runtime mount shadows
it without modifying that committed artifact, snapshot, or writable generation.

New imports use `pkg/procdartifact/rootfs-placeholder`, a small fixed script that
exits with an explicit error if executed without the platform mount. Its digest
is independent of all procd builds. The existing immutable artifact attestation
continues to bind the actual imported `/procd` bytes and protocol; no database
hashes or historical attestations are rewritten. `procd_digest` in that RootFS
attestation identifies the placeholder, while `assignment.procd.digest` identifies
the real executable. This preserves legacy schema and import-worker fencing while
removing the daemon from the durable filesystem. A manager cannot select the
placeholder itself as a runtime executable, or use placeholder imports without
`runtime_procd`.

New image imports use the placeholder requirements. Snapshot and captured-template
lookups retain the existing exact artifact identity and protocol checks. Merely
upgrading procd does not change the placeholder import identity. The protocol in
`runtime_procd` must match `rootfs_importer.procd_protocol`; changing a protocol is
an explicit compatibility migration, not a binary-only rollout.

## Rollout and rollback

1. Build ctld using `sh scripts/build-ctld.sh OUTPUT [PREBUILT_PROCD]`.
   The build embeds procd for the same target through a temporary Go overlay,
   without modifying the source checkout. Ordinary unbundled `go build` outputs
   refuse startup. The immutable release also contains the fixed placeholder.
   ctld stages its executable before HA readiness; both slots can safely add
   versions concurrently. `ctld --procd-digest` reports the embedded identity.
2. Upgrade the driver/catalog together through the existing fenced rollout. Stage
   the new executable on workers using the release's authenticated digest:

    ```sh
    /opt/sandbox0/releases/COMMIT/bin/ctld --install-procd \
        --expected-procd-digest sha256:VERIFIED_HEX
    ```

3. Configure new RootFS imports to use the release's `share/sandbox0/procd-placeholder`
   and its digest. Configure `runtime_procd.digest` with the real executable digest
   and `runtime_procd.protocol` with the compatible protocol. Prepare required
   placeholder artifacts before switching the claim policy. Aliyun's audited
   import-alignment operator performs this configuration transition after the
   worker rollout; elastic bootstrap installs the release's executable in the
   same cache before admission.
4. Cold resume selects the new executable. Running sandboxes keep their current
   process and mount until an authorized pause/resume or replacement.
5. Roll back the executable selector to a retained compatible digest. Ensure new
   workers' bootstrap artifacts match that selector too. Do not downgrade to a
   driver that ignores mounted assignments while placeholder RootFS artifacts
   remain in use.

A process-memory checkpoint or live migration must preserve the original
assignment's exact executable digest and protocol on the destination. It must
never apply the current cold-start policy to a restored process image. This
change does not implement or enable live migration; integrations must carry and
validate the existing assignment rather than reconstruct its executable from
mutable manager configuration.

## Session recovery and readiness

procd serves HTTP before runtime activation. Until activation succeeds, readiness
and the authenticated command-ready probe remain unavailable. Listening on a TCP
port is not proof of command readiness. Lifecycle controls and local webhook
publication also wait for activation, so they cannot race session owner binding.

Ownership/reset checks and session control-state loading still happen before
command readiness. Sessions requiring runtime reconciliation still recover their
journals synchronously. A terminal stopped session with no unfinished attempt
keeps its persisted cursor without opening the event journal or rewriting/fsyncing
unchanged state. The first journal operation performs the ordinary validated
recovery exactly once; the existing background retention sweep also opens deferred
journals, so retention is not silently disabled. Errors are returned by the
accessing operation and logged by the retention sweep. Closing an untouched
journal does not trigger historical reads.

Session control state accepts the original unversioned format and format 1.
Unknown versions fail activation before command readiness. Journal format checks
remain unchanged. Binary rollback is allowed only while the selected executable
supports the persisted state and journal formats; destructive format migrations
need their own explicit rollback boundary.

Validation must distinguish local unit/race checks from real Nomad/gVisor testing.
The host artifact avoids remote executable reads, but neither that change nor
journal deferral alone establishes an end-to-end startup latency guarantee.
