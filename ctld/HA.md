# ctld node-local high availability

Each dedicated Nomad client node runs `sandbox0-ctld@a.service` and
`sandbox0-ctld@b.service` directly in the host mount and network namespaces.
Both instances share `/var/lib/sandbox0/ctld`; a kernel-backed `flock` on
`ha/primary.lock` elects the only instance allowed to own NBD devices, Bolt
state, slot control sockets, network state, and per-lease cgroups.

## Invariants

- Exactly one instance holds the node-local primary lock.
- Promotion starts only after the previous primary releases the lock, or the
  kernel releases it when that process exits.
- The winner durably increments the shared epoch before starting privileged
  services.
- A standby is ready only after it observes a committed epoch in the shared
  state directory.
- A fatal primary-service error terminates the instance and releases the same
  lock used for promotion.
- An incomplete privileged shutdown retains the lock and fails closed rather
  than allowing overlapping ownership.

The primary lock and epoch file are the complete HA coordination state. Runtime
and RootFS truth remain in PostgreSQL, encrypted object storage, and the shared
node journal; no process-local cache is authoritative.

## Rollout

Use `deploy/nomad/ctld/rollout-node.sh`. It restarts slot B and then slot A,
waiting for each instance to become primary-ready or synchronized-standby-ready
before touching its peer. Roll nodes one at a time for changes to runtime
compatibility, RootFS format, NBD capacity, or network policy format.

Scrape each slot's dedicated HA metrics endpoint and alert unless every node
has one primary and one synchronized peer. The relevant metrics are
`ctld_ha_primary`, `ctld_ha_role`, `ctld_ha_epoch`, `ctld_ha_synchronized`,
`ctld_ha_state_duration_seconds`, `ctld_ha_role_transitions_total`, and
`ctld_ha_lock_info`.
