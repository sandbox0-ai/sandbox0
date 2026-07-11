# ctld node-local high availability

ctld uses one active process and one passive process on every sandbox node. The
pair protects existing FUSE portals from a single ctld process or Pod failure.
It does not protect against a node reboot, kernel failure, or simultaneous loss
of both processes.

## Invariants

- Exactly one process holds the node-local primary lock and reads FUSE requests.
- A portal is HA-ready only after a standby holds a cloned channel for the same
  kernel FUSE connection and the corresponding recovery manifest.
- The primary is the only process that binds the CSI socket, serves the node
  control port, heartbeats volume ownership, or opens a writable S0FS engine.
- Promotion starts only after the previous primary has been fenced by release
  of the kernel-backed primary lock.
- A returning process joins as a standby. Role changes never perform automatic
  failback.
- Graceful shutdown detaches the primary FUSE channels without unmounting live
  portals. The standby channels keep the kernel connections alive.

## Process layout

Two independent DaemonSets, `ctld-a` and `ctld-b`, provide the stable slots.
Slot A carries the CSI node-driver registrar sidecar; the registrar always
connects to the node-local CSI socket served by whichever slot is primary. The
slots share `/var/lib/sandbox0/ctld`, `/var/lib/kubelet`, and the containerd host
mounts, but have independent Pod lifecycles.

Each slot attempts a non-blocking exclusive lock on
`/var/lib/sandbox0/ctld/ha/primary.lock`. The winner increments the persisted
epoch and starts active services. A synchronized standby waits for the lock;
acquiring it is the fencing boundary for promotion.

## Portal handoff

For every published portal, the primary creates a second `/dev/fuse` channel
with `FUSE_DEV_IOC_CLONE`. It transfers that file descriptor, the original
FUSE INIT request, and the portal recovery manifest to the standby over a
node-local Unix packet socket using `SCM_RIGHTS`. The standby keeps the channel
open without reading it.

An idle cloned channel does not consume requests: the kernel moves a request
from the shared pending queue to a channel's processing queue only when that
channel is read. If the primary exits, requests already read into its processing
queue may fail and the caller must retry them. The kernel connection remains
mounted because the standby channel is still open. After promotion the standby
restores the portal session and starts consuming pending and new requests from
its channel.

## Recovery state

Portal manifests are written under `/var/lib/sandbox0/ctld/ha/portals` and
contain the pod identity, target path, backend binding, and FUSE negotiation
input. Runtime state uses backend-specific recovery:

- S0FS reopens its node-local WAL only after promotion. Kernel inode IDs remain
  stable because S0FS persists them, and a separate handle state preserves
  open and open-unlinked inode references.
- Rootfs-backed portals persist their inode-to-path and handle journal.
  Open-unlinked files move into a hidden orphan directory until the restored
  final handle is released.
- S3 portals persist inode allocation, local-only directory entries, and open
  handles in an atomic metadata snapshot. Uncommitted writable-handle data is
  fsynced to a separate sequential recovery file before each write is
  acknowledged, avoiding full-buffer rewrites. A recovery-state write failure
  removes the primary from readiness.

The volume registry uses a stable node-level ctld owner identity so a promoted
standby can immediately continue the existing logical mount instead of waiting
for the previous Pod heartbeat to expire.

## Rollout

Both slot DaemonSets use `maxSurge: 1` and `maxUnavailable: 0`. A replacement
Pod is ready only after it has synchronized all current portals, allowing
Kubernetes to delete the old slot without removing the last standby. The
primary accepts multiple synchronized receivers only during these overlapping
rollouts; steady state remains one primary and one standby. If multiple
receivers observe primary loss, the flock and epoch admit one winner while the
others discard their old clones and resynchronize from the winner.

The first upgrade from the historical single-process ctld requires a controlled
sandbox drain. The old process cannot transfer existing FUSE connections because
it predates this protocol. Subsequent upgrades use normal standby-first role
transfer and do not require sandbox recreation.
