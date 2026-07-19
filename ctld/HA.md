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
  control port, exposes the kubelet plugin-registration socket, heartbeats
  volume ownership, opens a writable S0FS engine, or runs the network runtime.
- Promotion starts only after the previous primary has been fenced by release
  of the kernel-backed primary lock.
- A returning process joins as a standby. Role changes never perform automatic
  failback.
- Graceful shutdown detaches the primary FUSE channels without unmounting live
  portals. The standby channels keep the kernel connections alive.

## Process layout

Two independent DaemonSets, `ctld-a` and `ctld-b`, provide the stable slots.
Both slots run the same single ctld container. The elected primary owns both the
node-local CSI socket and the kubelet plugin-registration socket; the standby
owns neither. The slots share `/var/lib/sandbox0/ctld`, `/var/lib/kubelet`, and
the containerd host mounts, but have independent Pod lifecycles.

Each slot attempts a non-blocking exclusive lock on
`/var/lib/sandbox0/ctld/ha/primary.lock`. The winner increments the persisted
epoch and starts active services. A synchronized standby waits for the lock;
acquiring it is the fencing boundary for promotion.

The primary creates the CSI socket before advertising its registration socket.
It becomes ready only after kubelet validates the CSI endpoint and reports a
successful registration, and after the network runtime completes its first
redirect-rule synchronization. On graceful shutdown it stops the network
runtime and removes the registration socket before releasing the primary
lease. A fatal network-runtime error is treated as a primary service failure,
so the process shuts down and releases the same kernel-backed lease used for
ctld promotion. After a crash, the promoted process removes the stale socket
after acquiring the lock, registers again, and starts a fresh network runtime.
Existing mounts continue through the cloned FUSE channels, while new publish
operations resume through the promoted primary and may need to retry during the
handoff.

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
  open and open-unlinked inode references. File-handle opens and closes append
  constant-size recovery events that are visible to the same-node standby
  before the FUSE operation returns. Periodic compaction and graceful handoff
  replace the legacy snapshot atomically; handoff also syncs the final
  snapshot. Directory handles are stateless because directory requests carry
  the inode they operate on. During a rolling upgrade, a primary falls back to
  snapshot updates until every connected standby advertises journal replay
  support.
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

Both slot DaemonSets use `maxSurge: 0` and `maxUnavailable: 1` because their
host-network listeners prevent a replacement from overlapping its same-slot
predecessor on a node. The operator rolls `ctld-b` completely and waits for it
to become current and ready before changing `ctld-a`. This keeps the unchanged
peer available while the other slot is replaced in place.

A replacement Pod becomes ready only after it has synchronized all current
portals as the standby, or, if it becomes primary, after its services have
started and at least one standby is synchronized. If the replaced slot was
primary, the unchanged peer promotes through the flock and epoch fencing
boundary; the returning replacement joins as standby without causing automatic
failback.

A sandbox node is advertised as data-plane ready only when both distinct HA
slots are ready and the node's `CSINode` object contains `volume.sandbox0.ai`.
This prevents scheduling onto a synchronized standby before the active process
has completed kubelet registration.
