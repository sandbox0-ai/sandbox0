# Sandbox0 Nomad gVisor Driver (experimental)

This task driver is part of an isolated Nomad + gVisor architecture PoC. It is
not a replacement for the production Kubernetes runtime path.

The driver creates a generic warm Nomad allocation without a gVisor container.
After manager/ctld have attached a RootFS generation and applied network policy,
an authorized caller binds that generation to a private OCI root mount, writes
the generic OCI spec, and invokes stock `runsc create` and `runsc start`. The
claim is one-shot and is rejected after the first attempt.

## Status

Implemented:

- Nomad task-driver plugin lifecycle and HCL schemas
- stock `runsc` CLI adapter with `overlay2=none`
- generic writable OCI bundle generation
- generic warm allocation with no image-specific runtime state
- local Unix control socket with `GET /status` and `PUT /claim`
- warm default-deny and claim-time L3/L4 network policy in the Nomad netns
- block-map RootFS attach through NBD, XFS, and host OverlayFS
- PostgreSQL writer consume, renewal, planned seal, and terminal publication
- node-scoped `nomad-rootfs-sessiond` ownership of writer renewals, NBD, XFS,
  OverlayFS, and terminal reconciliation over a root-only Unix socket
- tokenless durable Stage and exact runsc/stable-mount consumer journal
- independent Nomad node-allocation catalog reconciliation, including purged
  allocation fencing when the task-driver misses `DestroyTask`
- per-session unpublished dirty-tail capacity with request-atomic `ENOSPC`
  backpressure that survives daemon restart
- crash-safe running forks: durable XFS freeze intent, immutable branch
  checkpoint, thaw-before-S3 publication, exact node retry cache, and an
  atomic PostgreSQL target-filesystem transaction that keeps the source writer
  active
- policy-digest verification against the immutable RootFS handoff
- RootFS bind, start, stop, delete, signal, and cleanup paths
- one-shot claim and basic recovery semantics
- on-disk task state for driver crash recovery
- fail-closed plugin/session-daemon crash cleanup, regional crash abandonment,
  and fallback to the last durable generation
- unit tests with a fake runsc runtime
- PostgreSQL-backed regional warm-slot authority and authenticated v1 node API
  for register, readiness, heartbeat, runsc starting, and command readiness
- synchronous task-driver registration after the control socket, default-deny
  netns policy, root mount, runsc compatibility, and RootFS session-daemon
  health proofs are ready
- exact allocation/node-boot re-registration and bounded regional heartbeats
  after a task-driver restart; a warm slot is poisoned if its authority lease is
  lost, while an already claimed writer remains fenced by the independent
  RootFS session daemon
- exact claim operation/claim ID persistence and an idempotent regional
  `starting` transition after RootFS writer consumption, network application,
  and root bind but before `runsc create`; the proof binds launch attempt,
  runsc container ID, RootFS binding, and network-incarnation digest
- a real authenticated and runtime-gated procd command probe with a unique
  per-process instance ID, plus root-only `PUT /command-ready` forwarding that
  validates and hashes the complete probe proof before the idempotent regional
  `starting -> active` transition

Not implemented:

- manager/ctld claim integration with the regional slot registry
- manager/ctld orchestration of the procd probe and driver `command-ready` call
- production remote-block service and cross-node device ownership
- full network-policy incarnation-token persistence
- procd first-command-ready accounting
- guest stdout/stderr console forwarding
- full cgroup and Nomad stats integration
- production Nomad deployment and upgrade automation
- production manager-to-node running-fork orchestration and privileged
  multi-node XFS/NBD/runsc validation

## Writer authority PoC

`cmd/nomad-writer-authority` runs the PostgreSQL/mTLS writer-grant authority
used by this experiment. It can issue an initial block-COW grant from a Stage
JSON file, consume/renew grants over mTLS, and publish a locally sealed
generation as the next PostgreSQL head. It also exposes the two-phase regional
fence used to crash-abandon an unsealed writer without advancing the durable
head. Run it with `--help` for the current flags.

The authority also accepts an authenticated `fork-running` checkpoint from the
exact consumed source grant. The target sandbox must already exist, be paused,
belong to the source team, and have no RootFS binding. PostgreSQL creates the
target filesystem, checkpoint generation, binding, and idempotency record in
one transaction without changing the source head or lease.

Serve mode also owns the regional composite-tail backlog policy and S3
materializer. `--composite-backlog-bytes` is a shared PostgreSQL descriptor
budget (1 GiB by default), while `--object-bucket` and the object credential
environment variables are required. Concurrent publishers serialize on the
singleton policy row. Once the budget is full, publication returns HTTP 507
with `Retry-After` and keeps the exact local retire intent retryable; writer
lease renewal continues. The materializer scans oldest-first, writes immutable
content-addressed objects, and releases PostgreSQL backlog only after an exact
locator-version CAS. An S3 outage therefore cannot grow region tail data past
the configured bound.

The same mTLS listener exposes `/internal/v1/runtime-slots/{slot_id}` and its
`ready`, `heartbeat`, `starting`, and `command-ready` PUT transitions.
`--runtime-slot-heartbeat-ttl` is server policy; node requests cannot choose
their own liveness duration or node UID. The PostgreSQL registry remains
authoritative after a Nomad allocation is purged. With `runtime_slot_enabled`,
the task driver synchronously registers an exact physical allocation, reports
its three readiness proofs, and starts heartbeats before returning the slot to
Nomad. Recovery must reproduce the same allocation, node boot, netns inode,
control endpoint, and compatibility digest. No production manager claim
controller consumes the registry yet. When a trusted caller supplies the exact
regional operation and claim IDs in `PUT /claim`, the driver reports `starting`
before invoking runsc and retries an ambiguous response with the same proof.
Procd exposes `PUT /api/v1/runtime/command-ready-probe` behind its normal
authentication, runtime-ready, and lifecycle-barrier middleware. A trusted
caller submits the exact response and process identity to the driver's
root-only `PUT /command-ready`; the driver then reports regional active with a
canonical digest. Production manager/ctld orchestration of those calls remains
to be implemented.

The network policy implementation is intentionally minimal: production ctld
owns policy compilation, TPROXY, applied-token persistence, and L7 handling.

## Build and test

```sh
go test ./...
go vet ./...
go build -o nomad-driver-sandbox0 .
go build -o nomad-rootfs-sessiond ./cmd/nomad-rootfs-sessiond
go build -o nomad-rootfs-sessionctl ./cmd/nomad-rootfs-sessionctl
```

The module is intentionally separate from the repository root so the Nomad
dependency is not imposed on every Sandbox0 service.

## Example client configuration

```hcl
plugin "sandbox0-gvisor" {
  config {
    runsc_path         = "/usr/local/bin/runsc"
    runsc_root         = "/var/run/sandbox0/runsc"
    control_dir        = "/var/run/sandbox0/nomad-slots"
    allowed_rootfs_dir = "/var/lib/sandbox0/rootfs"
    platform           = "systrap"
    overlay2           = "none"
    file_access        = "shared"
    directfs           = true
    network_policy_enabled = true
    rootfs_enabled             = true
    rootfs_sessiond_socket     = "/run/sandbox0/rootfs-sessiond.sock"
    rootfs_mount_root          = "/run/sandbox0/rootfs"
    rootfs_consumer_mount_root = "/opt/nomad"
    rootfs_authority_url              = "https://regional-authority.internal:9443"
    rootfs_authority_ca_file          = "/etc/sandbox0/pki/ca.pem"
    rootfs_authority_client_cert_file = "/etc/sandbox0/pki/node.pem"
    rootfs_authority_client_key_file  = "/etc/sandbox0/pki/node-key.pem"
    rootfs_authority_token_file       = "/run/credentials/sandbox0-node-token"
    runtime_slot_enabled              = true
    runtime_slot_cluster_id           = "cluster-uuid"
    runtime_slot_node_boot_id_file    = "/proc/sys/kernel/random/boot_id"
  }
}
```

See `example/warm-slot.nomad` for a warm allocation. A development smoke task
may set `wait_for_claim = false` and provide an allowed RootFS directory only
when the client explicitly sets `dev_smoke_enabled = true`; the production path
always waits for manager authorization.

Regional runtime slots additionally require an argument-free `/procd` command,
a Nomad allocation network namespace, and the node-scoped RootFS session daemon.
The task driver reuses the writer-authority mTLS endpoint and credentials for the
runtime-slot API; it never accepts a caller-selected heartbeat TTL or node UID.

## Node boot prerequisites

The experimental RootFS runtime needs the NBD and bridge modules before Nomad
starts. Persist both the module list and the NBD pool size instead of relying on
one-time `modprobe` commands:

```text
# /etc/modules-load.d/sandbox0.conf
nbd
bridge
br_netfilter

# /etc/modprobe.d/sandbox0-nbd.conf
options nbd nbds_max=64 max_part=0
```

The Nomad unit must start after `systemd-modules-load.service`, and node
readiness must verify every configured `rootfs_nbd_devices` path. PostgreSQL,
the object store, and the mTLS writer authority must also be reachable before a
RootFS claim is issued. A failed attach after grant consumption is
crash-abandoned; it is never returned to the warm pool or published as a
successful pause.

Production RootFS mode runs `nomad-rootfs-sessiond` as a separate root system
service before Nomad. Pass the full Nomad node UUID through `--nomad-node-id`
and a token with `node:read,namespace:read-job` through
`--nomad-token-file`. The daemon reads object credentials from
`SANDBOX0_ROOTFS_OBJECT_ACCESS_KEY` and
`SANDBOX0_ROOTFS_OBJECT_SECRET_KEY`; do not place them in Nomad plugin HCL.
The driver fingerprint remains unhealthy until the daemon socket, durable
journal, and Nomad allocation catalog are all readable.

`--max-dirty-tail-bytes` bounds the logical 4 KiB payload represented by one
session's local branch WAL. Repeated overwrites count because they consume WAL
until publication. Once exhausted, the daemon rejects an entire NBD write or
write-zeroes request with `ENOSPC`; already completed writes remain readable
and flushable, and planned retirement can still publish them. The default is
10 GiB. Size the branch volume for node concurrency and set the explicit value
in `rootfs-sessiond.env`; this node-local bound does not replace the separate
regional PostgreSQL composite-tail backlog quota.

## Running fork control

The root-owned node control utility exercises the complete
freeze/checkpoint/thaw/object-publication/regional-transaction path:

```sh
nomad-rootfs-sessionctl \
  --socket /run/sandbox0/rootfs-sessiond.sock \
  --stage-file /run/sandbox0/handoffs/source.json \
  --operation-id fork-operation-1 \
  --source-sandbox-id source-sandbox \
  --target-sandbox-id paused-target-sandbox \
  --target-generation-id target-generation-1
```

The Stage file may contain either a `StageRequest` or a `{"stage": ...}`
envelope. The raw one-time writer token is stripped before RPC. The daemon
keeps at most one unresolved running-fork checkpoint per source session; an
exact retry reuses the same proof, including after regional response loss,
while a different operation fails closed until the current operation succeeds
or the source session terminates. This utility is an administrative PoC
entrypoint, not the final manager/ctld control-plane API.
