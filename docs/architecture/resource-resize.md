# Resource changes through compute replacement

The update API accepts a standalone `config.resources.memory` change. Memory
uses the same minimum, platform maximum and CPU derivation as claim, resume and
fork. This operation preserves sandbox identity and durable RootFS files and
restarts processes. It is not a live cgroup mutation. Ephemeral mounts and
in-memory terminal state follow the existing filesystem pause semantics.

`manager.sandbox_resource_resizes` owns the desired operation independently of
HTTP requests and manager replicas. Admission locks the sandbox, rejects an
active lifecycle or pending network update, and records its current runtime
generation and requested memory/CPU. Equal pending targets reuse that operation;
equal applied limits do not restart compute. Different pending targets conflict.

An originally running sandbox follows these steps:

1. `pausing`: keep the existing config and immutable resource lease. Admit the
   resize's exact planned filesystem pause, publish the RootFS head and wait for
   terminal runtime-slot custody. Publication alone is insufficient: the source
   runsc, mount, writer, network and resource cgroup must be cleaned up before
   the old capacity is released.
2. `resuming`: commit the new next-start resource override and discard retained
   memory references. Claim a fresh slot through ordinary capacity and team
   quota admission, enforce its lease through the existing OCI/cgroup path and
   await command readiness. Resource-neutral carriers can be reused at any size.
3. `applied`: verify the new runtime generation, exact active binding and lease
   metering values. HTTP success for a running resize requires this boundary.

An originally paused sandbox advances directly from `pausing` to `applied` once
physical custody is terminal. Its next start uses the new config. It never
starts compute as a side effect of a resource update.

The lifecycle fence permits only the resize's pause/resume while its intent is
pending. Cold access requests can help finish an admitted resume after the new
config is committed. Memory restore is rejected during resize and old memory
references are removed, so automatic access cannot revive the old process image.
Deletion, hard expiry and crash recovery supersede the resize; the reconciler
cancels its intent when those authorities change its owner. Node side effects
still use their original exact, durable lifecycle and writer identities.

Requests wait for at most 20 seconds before reporting `503` for pending work.
A controller discovers pending rows at startup and every 30 seconds and retries
with backoff. Capacity or quota shortage leaves the files in a paused sandbox;
it does not edit the source lease, silently fall back to old resources or lose
the operation when a request disconnects. If a reply is lost after commit, the
next request observes an applied limit and does nothing.

Metering retains the initial committed claim size and resource snapshots at
committed pause/resume boundaries. A projector that runs after several resizes
closes each old compute window at its old size and starts the next one at the
new size. Configuration changes while paused do not consume runtime capacity
or rewrite historical usage. Resource snapshots of committed lifecycles cannot
be modified.

Deploy the manager migration and controller together. Existing ctld and Nomad
drivers need no protocol change: they already support filesystem retirement,
exact physical cleanup, size-independent warm slots and cold resource admission.
Schema rollback is deliberately blocked until resource operations and metering
history are explicitly reconciled. During a rolling manager upgrade, older
managers remain fail-closed for resource writes; admitted operations are durable
and the new controller resumes them.
