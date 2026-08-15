# ctld node-local high availability

ctld runs as two independent DaemonSets, `ctld-a` and `ctld-b`, on every
sandbox node. A kernel-backed `flock` elects one primary per node. The primary
owns rootfs checkpoint/restore and the node-local network runtime; the standby
waits for promotion.

## Invariants

- Exactly one process holds the node-local primary lock.
- Promotion starts only after the previous primary releases, or the kernel
  releases, that lock.
- The elected process increments a persisted epoch before starting primary
  services.
- A returning process joins as a standby. Role changes do not automatically
  fail back.
- A fatal primary-service error shuts down the process and releases the same
  lock used for promotion.

## Process layout

Both slots share `/var/lib/sandbox0/ctld` and the containerd host mounts, but
have independent Pod lifecycles. Each slot attempts a non-blocking exclusive
lock on `/var/lib/sandbox0/ctld/ha/primary.lock`. The winner increments the
persisted epoch, starts rootfs and network services, and reports ready after the
network runtime completes its initial synchronization.

The standby does not serve rootfs or network operations. When it acquires the
lock after a primary exit, it reloads durable state from PostgreSQL and object
storage and starts a fresh node-local runtime. In-flight requests can fail
during handoff and must be retried; persisted sandbox rootfs state remains
recoverable because the node-local cache is not the source of truth.

## Rollout

Upgrade one slot at a time. Wait until every node again reports exactly one
primary before upgrading the other slot. Scrape both DaemonSets and group HA
checks by the `node` label:

- `ctld_ha_primary` identifies the elected peer; alert when
  `sum by (node) (ctld_ha_primary) != 1` remains true.
- `ctld_ha_role`, `ctld_ha_epoch`, and `ctld_ha_role_transitions_total` show
  election and failover history.
- `ctld_ha_state_duration_seconds` shows how long a peer has held its role.
- `ctld_ha_lock_info` exposes the shared lock device and inode; both peers on a
  node should report the same identity.
