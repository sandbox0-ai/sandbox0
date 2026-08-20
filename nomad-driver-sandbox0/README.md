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
- ctld-journaled warm default-deny and claim-time production network policy,
  compiled and redirected through the normal node TPROXY/ipset path
- block-map RootFS attach through NBD, XFS, and host OverlayFS
- PostgreSQL writer consume, renewal, planned seal, and terminal publication
- node-scoped `nomad-rootfs-sessiond` ownership of writer renewals, NBD, XFS,
  OverlayFS, and terminal reconciliation over a root-only Unix socket
- tokenless durable Stage and exact runsc/stable-mount consumer journal
- independent Nomad node-allocation catalog reconciliation, including purged
  allocation fencing when the task-driver misses `DestroyTask`
- per-session and aggregate node unpublished dirty-tail fail-stop boundaries,
  restart accounting, and shared retirement headroom
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
- manager PostgreSQL primary fencing: new connections select only a read-write
  server, pooled connections are periodically revalidated after a role change,
  and `/readyz` fails closed while no writer is reachable
- synchronous task-driver registration only after the control socket, ctld
  warm-policy acknowledgement, root mount, runsc compatibility, and RootFS
  session-daemon health proofs are ready
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
- a canonical claimed-slot cleanup request/proof contract, a manager-side
  transport adapter, and root-owned session-daemon cleanup of the exact runsc
  container, stable mount, RootFS writer, and network chain without invoking
  the task-driver plugin
- durable separation between local crash fences completed by the session
  daemon and externally managed fences completed by the regional reconciler;
  external retries retain their compact proof record while large branch and
  mount artifacts are reclaimed after regional retirement
- symlink-resolved allow-listing of the persisted Nomad network namespace path
  and inode-incarnation verification before node cleanup enters that namespace
- durable node-local registration of the deterministic runsc ID, stable mount,
  network namespace incarnation, and network chain before regional readiness
- plugin-independent cleanup of warm/grantless slots, with the exact cleanup
  request and absence proof persisted before destructive work and response
  respectively; byte-stable proof replay survives a session-daemon restart
- bounded node cleanup-proof retention (24 hours) and delayed compact external
  RootFS-fence retention (48 hours); Bolt page-reuse tests cover two 10,000-slot
  proof churn cycles without proportional second-cycle file growth
- a manager-side Nomad allocation controller that validates immutable server
  catalog identity, sends idempotent stop, invokes synchronous GC on the exact
  client, and derives absence from that client's allocation directory rather
  than from eventual server-record deletion; server and client endpoints use
  mTLS, exact SPIFFE URI SANs, rotating ACL tokens, and a trusted endpoint map
- a node-initiated runtime-slot command channel that binds mTLS-authenticated
  cluster, Nomad node, and node UID plus the reported boot ID, replaces
  same-agent stale streams without overlapping commands, keeps a different
  standby agent from preempting a live owner,
  and carries exact network prepare, claim, command-ready, and
  plugin-independent cleanup to root-owned local executors; request digests
  bind every target and payload,
  responses are strictly correlated, up to 64 independent slot operations run
  concurrently per node without serializing the warm path, and no raw writer
  token is persisted by the regional hub
- an explicitly enabled, non-overlapping regional terminal worker that scans
  bounded PostgreSQL batches and assembles the writer fence, outbound node
  cleanup, direct Nomad stop/client GC, physical-absence observation, and
  terminal transaction; each pass and delay are bounded and per-slot failures
  are reported without stopping later passes

Not implemented:

- service wiring of the manager claim planner package
- manager/ctld orchestration of the procd probe and driver `command-ready` call
- PostgreSQL-terminal acknowledgement-driven cleanup-proof compaction and
  reconciliation of active node journal registrations whose regional register
  response was ambiguous; current compaction uses a bounded local TTL
- production remote-block service and cross-node device ownership
- procd first-command-ready accounting
- guest stdout/stderr console forwarding
- full cgroup and Nomad stats integration
- production Nomad deployment and upgrade automation
- production manager-to-node running-fork orchestration and privileged
  multi-node XFS/NBD/runsc validation
- deployment wiring and privileged race validation for the direct Nomad
  allocation controller, including Nomad server-GC concurrent with client GC
- manager claim-loop wiring for the node channel and migration of its current
  sessiond local executor into the final ctld-owned runtime

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
control endpoint, and compatibility digest. Manager-side claim planning and
terminal reconciliation packages now consume the registry, but they are not
yet wired into the deployed manager service. When a trusted caller supplies the
exact regional operation and claim IDs in `PUT /claim`, the driver reports `starting`
before invoking runsc and retries an ambiguous response with the same proof.
Procd exposes `PUT /api/v1/runtime/command-ready-probe` behind its normal
authentication, runtime-ready, and lifecycle-barrier middleware. A trusted
caller submits the exact response and process identity to the driver's
root-only `PUT /command-ready`; the driver then reports regional active with a
canonical digest. Production manager orchestration of the claim and
command-ready calls, plus migration of the remaining node executor into ctld,
remains to be implemented.

Ctld now owns runtime-slot warm default-deny, strict v1 policy validation,
production L4/L7 compilation, TPROXY/ipset application, durable physical
incarnation/epoch state, exact claim-token replay, and synchronized terminal
absence. The remaining network gates are privileged multi-node validation and
the final deletion of the non-runtime compatibility implementation.

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

## PostgreSQL high availability

Nomad-backed manager instances enable the shared database pool's primary-only
policy. Configure `database_url` with either a managed writer endpoint or an
ordered multi-host PostgreSQL URL, for example:

```text
postgres://manager@pg-a:5432,pg-b:5432/sandbox0?sslmode=verify-full
```

Every new connection must report `transaction_read_only=off`. Existing pooled
connections are revalidated before checkout at most one second after their
last successful role check, so a demoted primary is discarded and connection
fallback can reach the promoted server. Connection attempts are capped at
three seconds per host unless the DSN specifies a shorter timeout. Readiness
also acquires and pings through this policy and returns 503 while no primary is
available.

The pool deliberately does not replay arbitrary SQL transactions. Lifecycle
controllers retry their durable operation IDs, and database uniqueness/CAS
rules decide whether an ambiguous commit already happened. The opt-in local
promotion test creates a streaming primary/standby pair, kills the primary,
promotes the standby, replays one exact operation, and verifies a single row:

```sh
SANDBOX0_RUN_PG_HA_TESTS=1 go test ./pkg/dbpool \
  -run TestPostgresPrimaryPromotionPreservesExactOperationReplay -count=1 -v
```

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
    rootfs_consumer_netns_root = "/var/run/netns"
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
a task named `slot`, a Nomad bridge network with the `procd` allocation port
fixed to `49983`, and the node-scoped RootFS session daemon.
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
service before Nomad. Pass the regional data-plane cluster ID through
`--cluster-id`, the full Nomad node UUID through `--nomad-node-id`, and a token
with `node:read,namespace:read-job` through
`--nomad-token-file`. Keep `--runtime-slot-journal` on node-local durable
storage (the default is `/var/lib/sandbox0/runtime-slots.db`). The daemon reads
object credentials from
`SANDBOX0_ROOTFS_OBJECT_ACCESS_KEY` and
`SANDBOX0_ROOTFS_OBJECT_SECRET_KEY`; do not place them in Nomad plugin HCL.
The driver fingerprint remains unhealthy until the daemon socket, durable
journal, and Nomad allocation catalog are all readable.

Set `--consumer-netns-root` to Nomad's root-owned persistent namespace
directory (`/var/run/netns` by default). Consumer registration resolves both
the configured root and namespace path, persists only the canonical path, and
binds its device/inode identity. Configure
`--runtime-slot-ctld-network-socket` with the host-visible mode-`0600`,
root-owned socket served by the elected ctld HA primary. Sessiond passes only
the validated path relative to `--consumer-netns-root`; ctld resolves it below
its read-only host-netns mount, rechecks the device/inode incarnation, and
derives the allocation IPv4 address from that namespace.

The daemon exposes private runtime-slot registration and
`PUT /v1/runtime-slots/cleanup` only through its mode-`0600` Unix socket.
Before regional readiness, the task driver registers the deterministic
physical identities. Sessiond durably journals them, asks ctld to inspect the
same namespace through its read-only host mount, and waits until the warm
default-deny policy is present in the normal node redirect set. Regional
readiness is not reported before that acknowledgement. Sessiond can therefore
later clean a warm slot without the plugin or a RootFS writer session. Cleanup
persists the exact request before touching runsc, mounts, or network state and
persists its absence proof before replying.
Completed proofs expire after 24 hours and compact external RootFS fences after
48 hours. These TTLs bound local state but are not a PostgreSQL terminal
acknowledgement protocol; unresolved active registrations are not yet compacted
automatically.

The Unix API remains a node execution primitive and is never dialed directly
by the region. With `--runtime-slot-node-channel`, sessiond resolves the
authority hostname and establishes one outbound WebSocket over mTLS to every
exact address in the current manager membership set. The original HTTPS host
is retained for the HTTP authority and TLS DNS verification; resolved Pod IPs
are used only as pinned TCP destinations. Duplicate or reordered DNS answers
are canonicalized, membership additions and removals are reconciled every
second, resolution failures retain the last known exact set, and an empty set
never falls back to a load-balanced virtual IP. The regional hub
derives cluster, Nomad node, and node UID from authentication, checks the
advertised boot ID, and routes only commands whose cluster, node, allocation,
slot, UID, boot, and local control endpoint match the canonical request. The
agent then invokes the mode-`0600`, root-owned task socket or the
plugin-independent cleaner locally. Network preparation is advertised only
when the ctld client is configured. Ctld requires the pre-existing exact warm
registration, then durably transitions it to the region-authenticated claim
operation and strict v1 policy. The journal binds namespace incarnation,
allocation IP, monotonic network epoch, logical sandbox/team identity, and the
deterministic physical token before merging the slot into the same policy
compiler and node-level TPROXY/ipset reconciliation used by Kubernetes
sandboxes. It replies only after a successful redirect sync; cleanup similarly
waits for synchronized absence. The shared host journal lets the standby ctld
replay the same token and desired set after promotion, and retained terminal
records are pruned periodically rather than only at process startup.
It requires `--runtime-slot-node-uid`, an exact
`--runtime-slot-channel-peer-uri-san`, and an allow-root supplied by
`--runtime-slot-control-root`; ambient proxies are disabled and certificates,
boot ID, and projected bearer token are reloaded on reconnect, with a bounded
five-minute connection age so rotated credentials are eventually enforced.
`--authority-url` must therefore use a resolvable headless-Service or private
DNS hostname whose complete answer is the reachable manager replica set, and
the server certificate must contain that hostname as a DNS SAN. An IP literal
is rejected. A ClusterIP or load-balancer DNS name violates the exact-membership
contract; the operator deliberately does not publish node authority on that
Service.
The authority's `--allowed-clients` entry for a channel identity must use
`commonName:nodeUID:podUID:clusterID:nodeID`; legacy three-field entries remain
valid for writer and slot APIs but cannot establish a node channel.
The regional hub is transient by design: PostgreSQL owns retries and the node
journal owns cleanup proof. Runtime-slot policy compilation and application
are now ctld-owned; the outbound node agent and remaining runsc/mount cleanup
executor still live in sessiond and must move into the final ctld-owned
runtime before legacy removal. The task driver no longer installs its PoC
namespace-local iptables policy for regional runtime slots. That legacy path
remains only for non-runtime-slot compatibility and must be deleted at final
cutover. The
10,000-slot Bolt test proves local page reuse only; it does not constitute the
required privileged, multi-node, end-to-end 24-hour soak.

The regional terminal controller also needs trusted HTTPS endpoints for each
Nomad server cluster and exact client node. Its ACL policy must permit
namespace `read-job`, `submit-job`, and `read-fs`: server identity is checked
before stop, while direct client `fs/stat` and allocation GC distinguish
physical client state from an eventually deleted server record. Both endpoint
classes require mTLS, a target-bound SPIFFE URI SAN, and a rotating token file;
ambient proxies and redirects are rejected.

In addition to the authority's normal required flags, `serve` runs this
terminal path only when explicitly enabled:

```sh
nomad-writer-authority \
  --mode=serve \
  --runtime-slot-terminal-reconciler \
  --runtime-slot-nomad-endpoints-file=/etc/sandbox0/nomad-endpoints.json \
  --runtime-slot-reconcile-interval=1s \
  --runtime-slot-reconcile-timeout=2m \
  --runtime-slot-reconcile-limit=100
```

The strict versioned catalog format is shown in
`example/nomad-endpoints.example.json`. It has one server endpoint per cluster
and one exact client endpoint per node. Catalog size, endpoint count, request
timeout, JSON fields, duplicates, and orphan client entries are bounded or
rejected at startup. Credential contents remain reloadable per request. This
wiring is still deployment foundation: it does not replace privileged
server-GC/client-destroy race validation or move the local executor into ctld.

`--max-dirty-tail-bytes` bounds the logical 4 KiB payload represented by one
session's local branch WAL. Repeated overwrites count because they consume WAL
until publication. A production session daemon does not send a normal-limit
failure through NBD: the first request that would cross the limit is left
pending, and subsequent writes wait behind it. The daemon persists a
deterministic pressure-pending marker in Bolt, asks the authenticated regional
writer authority to create an exact automatic planned pause, and only then
promotes the local marker to planned retirement. The node reconciler fences
the allocation without depending on the task-driver process and unblocks the
pending request as retirement I/O. A sessiond restart recovers the pending
marker and retries the same regional operation instead of crash-abandoning the
dirty branch.

The lower-level branch API still returns `ENOSPC` when no pressure observer is
installed, and retirement can still reach the absolute reserve boundary.
Linux's NBD client maps every nonzero remote errno to block-layer `EIO`, so
that boundary can shut down XFS. It remains an emergency fail-stop guard, not
a guest-visible quota mechanism. The default per-session limit is 10 GiB.

`--max-node-dirty-tail-bytes` independently bounds the aggregate logical
payload across active sessions, terminal journals awaiting regional
acknowledgement, interrupted work, and all three offline-rebase branches. The
default is 40 GiB. Admission is atomic across concurrent NBD requests on
different sessions. A node-limit crossing uses the same blocked-write and
exact planned-pause path as a per-session crossing. Startup scans every durable
`.wal` below `--branch-root`
before serving requests; a limit lowered below recovered usage does not block
retirement, but normal writes remain blocked until acknowledged artifacts are
deleted. Closing a branch does not release capacity—only deletion after the
regional terminal proof does. Account for fixed record framing, filesystem
headroom, and the Linux NBD error behavior when sizing the branch volume. These
node-local limits do not replace the separate regional PostgreSQL
composite-tail backlog quota.

`--dirty-tail-retirement-reserve-bytes` protects one shared 64 MiB headroom
pool inside the node cap. It is not multiplied by the number of attached
branches, so 10,000 idle owners do not consume 640 GiB of logical admission.
Normal writes cannot consume the pool. After the runtime is fenced, the
session manager switches its branch to retirement mode before XFS sync/unmount,
allowing shutdown I/O to use the shared node headroom and expanding that
branch's per-session limit by the same amount. A privileged NBD/XFS/OverlayFS
test verifies clean retirement when fencing begins before the hard boundary.
Unrelated retirements are serialized until regional acknowledgement reclaims
the active group's local journals. The three branches of one offline rebase
share one group and may retire together.
The pressure path retains the writer lease while local sealing, S3 publication,
and regional acknowledgement retry. The reserve cannot repair an XFS instance
after Linux NBD has already surfaced a hard-cap response as `EIO`.

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
