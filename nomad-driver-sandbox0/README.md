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
- local Unix control socket with `/status` and `/claim`
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
- policy-digest verification against the immutable RootFS handoff
- RootFS bind, start, stop, delete, signal, and cleanup paths
- one-shot claim and basic recovery semantics
- on-disk task state for driver crash recovery
- fail-closed plugin/session-daemon crash cleanup, regional crash abandonment,
  and fallback to the last durable generation
- unit tests with a fake runsc runtime

Not implemented:

- manager/ctld integration and slot registry
- production remote-block service and cross-node device ownership
- full network-policy incarnation-token persistence
- procd first-command-ready accounting
- guest stdout/stderr console forwarding
- full cgroup and Nomad stats integration
- production Nomad deployment and upgrade automation

## Writer authority PoC

`cmd/nomad-writer-authority` runs the PostgreSQL/mTLS writer-grant authority
used by this experiment. It can issue an initial block-COW grant from a Stage
JSON file, consume/renew grants over mTLS, and publish a locally sealed
generation as the next PostgreSQL head. It also exposes the two-phase regional
fence used to crash-abandon an unsealed writer without advancing the durable
head. Run it with `--help` for the current flags.

The network policy implementation is intentionally minimal: production ctld
owns policy compilation, TPROXY, applied-token persistence, and L7 handling.

## Build and test

```sh
go test ./...
go vet ./...
go build -o nomad-driver-sandbox0 .
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
    rootfs_enabled             = true
    rootfs_sessiond_socket     = "/run/sandbox0/rootfs-sessiond.sock"
    rootfs_mount_root          = "/run/sandbox0/rootfs"
    rootfs_consumer_mount_root = "/opt/nomad"
  }
}
```

See `example/warm-slot.nomad` for a warm allocation. A development smoke task
may set `wait_for_claim = false` and provide an allowed RootFS directory only
when the client explicitly sets `dev_smoke_enabled = true`; the production path
always waits for manager authorization.

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
