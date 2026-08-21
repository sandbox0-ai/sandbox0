# Sandbox0 Nomad gVisor Driver (experimental)

This task driver is the Nomad + gVisor runtime selected for the production
cutover. The repository still retains the predecessor Kubernetes runtime until
the remaining acceptance gates pass; there must be no dual runtime after the
final deletion step.

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
- node-scoped `ctld` ownership of writer renewals, NBD, XFS,
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
- fail-closed plugin/ctld runtime crash cleanup, regional crash abandonment,
  and fallback to the last durable generation
- unit tests with a fake runsc runtime
- PostgreSQL-backed regional warm-slot authority and authenticated v1 node API
  for register, readiness, heartbeat, runsc starting, and command readiness
- manager PostgreSQL primary fencing: new connections select only a read-write
  server, pooled connections are periodically revalidated after a role change,
  and `/readyz` fails closed while no writer is reachable
- an opt-in OCI golden corpus that runs identical filesystem and isolation
  semantics through stock runsc DirectFS, stock runsc Gofer, and runc
- real RustFS request-cost, outage recovery, and independent-node RootFS-head
  migration tests; the receiving node uses a new writer epoch and only the
  published immutable generation, never the retiring node's local journal
- synchronous task-driver registration only after the control socket, ctld
  warm-policy acknowledgement, root mount, runsc compatibility, and RootFS
  ctld runtime health proofs are ready
- exact allocation/node-boot re-registration and bounded regional heartbeats
  after a task-driver restart; a warm slot is poisoned if its authority lease is
  lost, while an already claimed writer remains fenced by the independent
  ctld Nomad runtime
- exact claim operation/claim ID persistence and an idempotent regional
  `starting` transition after RootFS writer consumption, network application,
  and root bind but before `runsc create`; the proof binds launch attempt,
  runsc container ID, RootFS binding, and network-incarnation digest
- a real authenticated and runtime-gated procd command probe with a unique
  per-process instance ID, plus root-only `PUT /command-ready` forwarding that
  validates and hashes the complete probe proof before the idempotent regional
  `starting -> active` transition
- a canonical claimed-slot cleanup request/proof contract, a manager-side
  transport adapter, and root-owned ctld runtime cleanup of the exact runsc
  container, stable mount, RootFS writer, and network chain without invoking
  the task-driver plugin
- durable separation between local crash fences completed by ctld and
  externally managed fences completed by the regional reconciler;
  external retries retain their compact proof record while large branch and
  mount artifacts are reclaimed after regional retirement
- symlink-resolved allow-listing of the persisted Nomad network namespace path
  and inode-incarnation verification before node cleanup enters that namespace
- durable node-local registration of the deterministic runsc ID, stable mount,
  network namespace incarnation, and network chain before regional readiness
- plugin-independent cleanup of warm/grantless slots, with the exact cleanup
  request and absence proof persisted before destructive work and response
  respectively; byte-stable proof replay survives a ctld runtime restart
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

Remaining cutover gates:

- the public regional-gateway-to-procd serial and concurrency SLO reports
- normal completion of the real-clock 10,000/24-hour PostgreSQL/RustFS and
  Bolt journal soaks
- final deletion of the superseded Kubernetes runtime, schema compatibility,
  configuration, tests, documentation, and redundant dependencies

Guest console forwarding and complete cgroup statistics remain separate
product work; neither is allowed to create a second RootFS or lifecycle truth.

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
lease renewal continues. The materializer scans oldest-first and groups only
the same tenant and RootFS format into shared 64 MiB data and mapping packs.
It fixes exact ordered batch membership in PostgreSQL before the first PUT,
journals every immutable object before upload, and publishes every locator in
the batch atomically. A crash or another manager replica therefore resumes the
same pack boundary instead of creating one data and mapping object per tiny
sandbox lifecycle. `--materializer-min-pack-bytes` defaults to 32 MiB;
`--materializer-max-delay` defaults to five minutes, with one age-forced lane
flush per pass, so low traffic is bounded by time without defeating batching.
Stale changed-locator uploads and terminal journals are reconciled through the
durable object deletion queue; uploading batches fence Sandbox deletion until
publication finishes. An S3 outage cannot grow region tail data past the
configured PostgreSQL bound or turn a pre-PUT journal into an untracked orphan.

The same mTLS listener exposes `/internal/v1/runtime-slots/{slot_id}` and its
`ready`, `heartbeat`, `starting`, and `command-ready` PUT transitions.
`--runtime-slot-heartbeat-ttl` is server policy; node requests cannot choose
their own liveness duration or node UID. The PostgreSQL registry remains
authoritative after a Nomad allocation is purged. With `runtime_slot_enabled`,
the task driver synchronously registers an exact physical allocation, reports
its three readiness proofs, and starts heartbeats before returning the slot to
Nomad. Recovery must reproduce the same allocation, node boot, netns inode,
control endpoint, and compatibility digest. When the Nomad backend is enabled,
the deployed manager wires claim planning and terminal reconciliation to this
registry and routes node operations over the authenticated node channel. The
driver reports `starting` before invoking runsc and retries an ambiguous
response with the same proof. Procd exposes
`PUT /api/v1/runtime/command-ready-probe` behind its normal authentication,
runtime-ready, and lifecycle-barrier middleware. Manager executes that command,
submits the exact response and process identity over the node channel, and the
driver reports regional active with a canonical digest. The root-owned
executor now runs inside the elected ctld HA primary rather than the plugin.

Ctld now owns runtime-slot warm default-deny, strict v1 policy validation,
production L4/L7 compilation, TPROXY/ipset application, durable physical
incarnation/epoch state, exact claim-token replay, and synchronized terminal
absence. Nomad host mode consumes only the durable runtime-slot registry; it
does not initialize Kubernetes Pod, Service, Endpoint, CRI, or containerd
sources.

## Build and test

```sh
go test ./...
go vet ./...
go build -o ../bin/nomad-driver-sandbox0 .
go build -o ../bin/nomad-rootfs-sessionctl ./cmd/nomad-rootfs-sessionctl
cd ..
go build -o bin/ctld ./ctld/cmd/ctld
```

The module is intentionally separate from the repository root so the Nomad
dependency is not imposed on every Sandbox0 service.

### Runtime compatibility and isolation corpus

The privileged corpus executes the same statically linked Go payload through
stock runsc DirectFS, stock runsc Gofer, and runc. Supply an extracted OCI
rootfs containing `/bin/sh`, `/proc`, `/dev`, `/sys`, and `/tmp`:

```sh
cd nomad-driver-sandbox0
CGO_ENABLED=0 \
SANDBOX0_RUN_PRIVILEGED_RUNTIME_CORPUS=1 \
SANDBOX0_RUNTIME_CORPUS_ROOTFS=/tmp/s0-runtime-corpus-rootfs \
go test ./internal/driver \
  -run TestPrivilegedRuntimeGoldenCorpus -count=1 -v -timeout=3m
```

The required common result covers open/read/write/stat/readdir/fsync,
mmap/msync, xattrs, hardlinks, symlinks, rename with a retained directory file
descriptor, sparse files, truncate, inotify, arbitrary chown, SCM_RIGHTS, and
OverlayFS whiteouts. The isolation checks deny host-root traversal through
`/proc`, host Unix-socket and FIFO access, mount and device creation without
the corresponding capabilities, non-loopback networking, and host-path
leakage through mountinfo. The static payload requirement avoids silently
testing a host dynamic loader that is absent from the guest image.

This corpus requires sparse-file behavior and verifies physical sparsity in
the host upperdir. It does not claim `FALLOC_FL_PUNCH_HOLE` parity: stock runsc
may return `EOPNOTSUPP` for that operation even when ordinary sparse-file
semantics pass.

### RustFS migration and request-cost matrix

With an isolated RustFS test bucket endpoint, run the real S3 client tests from
the repository root:

```sh
SANDBOX0_RUSTFS_ENDPOINT=http://127.0.0.1:19000 \
SANDBOX0_RUSTFS_ACCESS_KEY=sandbox0test \
SANDBOX0_RUSTFS_SECRET_KEY=sandbox0testsecret \
go test ./pkg/rootfssession \
  -run 'TestRustFS(DirtyTailPressureRetiresAfterObjectStoreRecovery|RootFSMigratesBetweenIndependentNodes)' \
  -count=1 -v -timeout=5m
```

The cost test excludes one-time fixture upload, then enforces at most two GETs
for cold attach, zero requests for a hot re-attach, at most two GETs and zero
PUT/metadata requests for online writes, at most four HTTP 503 attempts for one
bounded outage operation, at most four GETs and four PUTs for recovery, and at
most three GETs for a final uncached read. HEAD and LIST are forbidden from the
RootFS data path. The migration test forces node A's dirty head into immutable
S3 objects, reclaims node A's acknowledged branch, and verifies that an
independent node B with writer epoch 2 reads the exact filesystem bytes from
the published head. This validates cross-node storage/control semantics; a
privileged multi-host NBD/XFS/runsc deployment test remains a production gate.

### Regional ingress-to-procd SLO acceptance

The Nomad manager emits one trusted timer from the signed regional-gateway
ingress timestamp through authenticated procd command readiness. The public
claim response carries that exact value as
`Server-Timing: sandbox0-command-ready;dur=<milliseconds>` and reports `met` or
`missed` in `Sandbox0-Command-Ready-SLO`; these fields are response metadata and
are not part of the OpenAPI payload. Prometheus exports the terminal sample as
`manager_runtime_slot_claim_end_to_end_duration_seconds`. A separate bounded
`manager_runtime_slot_claim_phase_duration_seconds` histogram attributes
request validation, regional ingress-to-planner work, RootFS metadata, slot
acquire, network prepare, writer issue/bind, node claim, procd probe, and
command-ready commit without operation, sandbox, or slot labels.

Run the serial gate against the public regional URL with a prewarmed compatible
pool and hot RootFS working set:

```sh
SANDBOX0_API_TOKEN=... go run ./tools/runtime-slot-slo \
  --url https://region.example.com/api/v1/sandboxes \
  --template default \
  --batches 1000 \
  --concurrency 1 \
  --p50-target 500ms \
  --hard-limit 1s \
  --cleanup-timeout 2m \
  --cleanup-poll 100ms \
  --output serial-1000.json
```

The concurrency-8 gate uses at least 100 synchronized batches:

```sh
SANDBOX0_API_TOKEN=... go run ./tools/runtime-slot-slo \
  --url https://region.example.com/api/v1/sandboxes \
  --template default \
  --batches 100 \
  --concurrency 8 \
  --batch-settle 5s \
  --cleanup-timeout 2m \
  --cleanup-poll 100ms \
  --output concurrency-8.json
```

The harness performs no hidden claim retries, requires the trusted timing and
SLO headers on every `201`, and disables ambient HTTP proxies. Outside each
measured interval it sends public DELETE and polls public GET until `404`;
acceptance therefore requires the asynchronous terminal worker and physical
slot cleanup to converge within the configured timeout rather than merely
accepting deletion intent. Report version 2 records the cleanup distribution
separately. Any claim, cleanup-convergence, or successful command-ready sample
above one second fails the gate. The command-ready p50 must be at or below 500
ms and p99 at or below one second. Cold S3, unclean replay, Nomad refill,
full-cold-node, and 1/8/32 concurrency results must be recorded as separate
labeled reports rather than mixed into the hot distribution.

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
    runsc_root         = "/run/sandbox0/runsc"
    control_dir        = "/run/sandbox0/nomad-slots"
    allowed_rootfs_dir = "/var/lib/sandbox0/rootfs"
    platform           = "systrap"
    overlay2           = "none"
    file_access        = "shared"
    directfs           = true
    network_policy_enabled = true
    rootfs_enabled             = true
    rootfs_node_socket         = "/run/sandbox0/ctld-nomad-runtime.sock"
    rootfs_mount_root          = "/run/sandbox0/rootfs"
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
fixed to `49983`, and the node-scoped ctld Nomad runtime.
The task driver reuses the writer-authority mTLS endpoint and credentials for the
runtime-slot API; it never accepts a caller-selected heartbeat TTL or node UID.

## Nomad node deployment

Production nodes run ctld A/B as root systemd services directly in the host
mount and network namespaces. Both instances share `/var/lib/sandbox0/ctld`,
but only the flock-elected primary opens Bolt, NBD, RootFS, network, and Unix
socket resources. Running ctld in a Kubernetes Pod or adding a systemd
filesystem namespace is invalid because the RootFS bind mount must be visible
in the exact mount namespace used by the Nomad task driver.

Use `deploy/nomad/ctld/install-node.sh` and its pinned examples. The installer
loads the NBD, XFS, OverlayFS, bridge, and TPROXY modules; configures a bounded
NBD pool and required sysctls; installs ctld, runsc, and the driver; and adds a
hard `sandbox0-ctld.target` dependency to Nomad. The B-then-A rollout script
waits for role-aware HA readiness between restarts. PostgreSQL, object storage,
manager mTLS authority, and the local Nomad HTTPS API must be reachable before
the plugin can advertise a RootFS-capable warm slot.

Ctld configuration owns object credentials, node-side writer/channel
credentials, NBD paths, dirty-tail budgets, node identity, and Nomad catalog
credentials. The plugin keeps the ctld node socket, stable-mount compatibility
guard, storage-cap compatibility proof, and the regional slot lifecycle mTLS
credentials used for registration and heartbeat. Its fingerprint remains
unhealthy until the root-owned mode-`0600` ctld socket, durable journals,
network registry, and Nomad allocation catalog are healthy.

Consumer registration resolves `/opt/nomad` and `/var/run/netns`, persists
their canonical path and device/inode identity, and rejects a driver in a
different mount namespace. Ctld passes only a validated path relative to the
configured netns root to its network registry, which rechecks incarnation and
derives the allocation IPv4 address from the namespace.

The RootFS session journal keeps synchronous bbolt durability at every
reserved, device-bound, mounted, and ready crash boundary. The per-session
lock preserves their order, while independent sessions group-commit at most 64
updates within a bounded 1 ms window. This avoids a node-wide fdatasync queue
on concurrent claims without enabling `NoSync`, weakening restart recovery, or
combining two states of the same session into one transaction.

The ctld node socket exposes private runtime-slot registration and
`PUT /v1/runtime-slots/cleanup` only through its mode-`0600` Unix socket.
Before regional readiness, the task driver registers the deterministic
physical identities. Ctld durably journals them, inspects the same host
namespace, and waits until the warm default-deny policy is present in the
normal node redirect set. Regional readiness is not reported before that
acknowledgement. Ctld can therefore clean a warm slot without the plugin or a
RootFS writer session. Cleanup persists the exact request before touching
runsc, mounts, or network state and persists its absence proof before replying.
Completed proofs expire after 24 hours and compact external RootFS fences after
48 hours. These TTLs bound local state while PostgreSQL terminal state remains
the authority for retries.

The Unix API remains a node execution primitive and is never dialed directly
by the region. When `nomad_runtime.enabled` is set, ctld resolves the
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
deterministic physical token before merging the slot into the production
policy compiler and node-level TPROXY/ipset reconciliation. It replies only
after a successful redirect sync; cleanup similarly
waits for synchronized absence. The shared host journal lets the standby ctld
replay the same token and desired set after promotion, and retained terminal
records are pruned periodically rather than only at process startup.
It requires `nomad_runtime.node_uid`, an exact
`nomad_runtime.authority_peer_uri_san`, and a canonical
`nomad_runtime.control_root`; ambient proxies are disabled and certificates,
boot ID, and projected bearer token are reloaded on reconnect, with a bounded
five-minute connection age so rotated credentials are eventually enforced.
`nomad_runtime.authority_url` must therefore use a resolvable headless-Service or private
DNS hostname whose complete answer is the reachable manager replica set, and
the server certificate must contain that hostname as a DNS SAN. An IP literal
is rejected. A ClusterIP or load-balancer DNS name violates the exact-membership
contract; the operator deliberately does not publish node authority on that
Service.
The authority's `--allowed-clients` entry for a channel identity must use
`commonName:nodeUID:podUID:clusterID:nodeID`; legacy three-field entries remain
valid for writer and slot APIs but cannot establish a node channel.
The regional hub is transient by design: PostgreSQL owns retries and the node
journal owns cleanup proof. Runtime-slot policy compilation and application,
the outbound node agent, runsc/mount cleanup, and RootFS physical ownership
are ctld-owned. The task driver no longer installs its PoC
namespace-local iptables policy for regional runtime slots. That legacy path
remains only for non-runtime-slot compatibility and must be deleted at final
cutover. The 10,000-slot Bolt test proves local page reuse only; it does not
constitute the required privileged, multi-node, end-to-end 24-hour soak. The
opt-in `TestRuntimeSlotJournalTwentyFourHourSoak` companion distributes 10,000
exact terminal cleanup proofs across a real 24-hour clock, prunes them through
the production journal, reopens Bolt at one-third of the run, emits fsynced
JSONL evidence outside the repository, and requires final size to stay within
one host page of its warm size. It must pass together with the real
PostgreSQL/RustFS `tools/rootfs-materializer-soak` gate; neither short smoke
mode nor the accelerated 10,000-record unit test may be reported as 24-hour
evidence. The full `go test` invocation must set `-timeout 25h` because Go's
default test timeout is too short for this gate.

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
rejected at startup. Credential contents remain reloadable per request.

`--max-dirty-tail-bytes` bounds the logical 4 KiB payload represented by one
session's local branch WAL. Repeated overwrites count because they consume WAL
until publication. A production ctld runtime does not send a normal-limit
failure through NBD: the first request that would cross the limit is left
pending, and subsequent writes wait behind it. The ctld runtime persists a
deterministic pressure-pending marker in Bolt, asks the authenticated regional
writer authority to create an exact automatic planned pause, and only then
promotes the local marker to planned retirement. The node reconciler fences
the allocation without depending on the task-driver process and unblocks the
pending request as retirement I/O. A ctld primary restart recovers the pending
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

The root-owned node control utility exercises the complete ctld
freeze/checkpoint/thaw/object-publication/regional-transaction path:

```sh
nomad-rootfs-sessionctl \
  --socket /run/sandbox0/ctld-nomad-runtime.sock \
  --stage-file /run/sandbox0/handoffs/source.json \
  --operation-id fork-operation-1 \
  --source-sandbox-id source-sandbox \
  --target-sandbox-id paused-target-sandbox \
  --target-generation-id target-generation-1
```

The Stage file may contain either a `StageRequest` or a `{"stage": ...}`
envelope. The raw one-time writer token is stripped before RPC. Ctld
keeps at most one unresolved running-fork checkpoint per source session; an
exact retry reuses the same proof, including after regional response loss,
while a different operation fails closed until the current operation succeeds
or the source session terminates. This utility is an administrative diagnostic;
normal fork requests use the manager/ctld control plane.
