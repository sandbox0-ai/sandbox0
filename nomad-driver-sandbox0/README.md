# Sandbox0 Nomad gVisor Driver

`nomad-driver-sandbox0` is the production Sandbox0 task driver for dedicated
Nomad client nodes and stock gVisor `runsc`. It accepts only regional
runtime-slot claims backed by ctld-owned RootFS, network, and resource leases.
There is no alternate local lifecycle, local RootFS, or driver-owned network
policy mode.

## Runtime Contract

A warm carrier is a generic Nomad allocation with a network namespace and
control socket but no guest runtime. After manager commits an exact claim:

1. PostgreSQL atomically leases node CPU/memory and one ready carrier;
2. ctld creates the cgroup-v2 lease, attaches the block-COW RootFS, and applies
   the network policy;
3. the driver writes the exact lease, static assignment, and existing network
   namespace into an OCI bundle;
4. the driver invokes stock `runsc create` and `runsc start`; and
5. manager publishes the sandbox generation only after an authenticated procd
   first-command proof.

The carrier is single-use. After stop, pause, crash, or deletion, only a fresh
Nomad allocation can replace it. Configure the warm job with
`restart { attempts = 0 }`.

Nomad allocation resources reserve only carrier/driver overhead. They are
never sandbox limits or metering truth. CPU quota/weight/cpuset, memory, and
PIDs come only from the claim's PostgreSQL resource lease.

The driver advertises `DisableLogCollection`: carriers do not connect their
stdout/stderr to Nomad FIFOs, so Nomad must not launch a per-carrier `logmon`.
Guest command output continues through procd's API, and runtime diagnostics
continue through the driver/ctld logs. `nomad alloc logs` is not a guest-output
API. Apply this change through normal carrier replacement when upgrading a node;
changing capabilities does not retroactively reclaim existing logmon processes.

### Light-load admission

ctld can opt a dedicated node into higher-density admission without changing
any sandbox's CPU quota, memory maximum, cpuset, PIDs limit, or metering lease.
The physical capacity remains `resource_cpu_millicores` / `resource_memory_bytes`.
Optional `admission_cpu_millicores` / `admission_memory_bytes` bound the sum of
claimed sandbox limits. Omitted or zero values retain physical-capacity
admission. CPU admission is bounded to 16 times physical capacity and memory
admission to twice physical capacity; these are validation ceilings, not
recommended operating ratios or proven density.

For example, a 14-core / 56GiB sandbox budget can explicitly advertise 35000
millicores and 70GiB for light-load admission. That changes scheduling only:
every individual request must still fit the physical node, and PostgreSQL
serializes concurrent claims against the admission budget. Capacity cannot be
resized within a node boot, including reconnects. Resource reservations remain
charged until the existing physical cgroup absence proof releases the lease.

Overcommitted nodes require a predelegated parent cgroup with `cpu.max` equal
to the physical CPU budget using a 100000-microsecond period, `memory.max`
equal to the physical memory budget, and `memory.swap.max=0`. ctld rejects an
unbounded parent rather than silently enabling overcommit. The memory guard
rejects new cgroups when current usage reaches 90% of that budget or a request
exceeds remaining physical headroom. Exact retries of existing cgroups still
work. These checks do not guarantee simultaneous peak usage can fit: the parent
hard limit is the final boundary if running guests grow after admission.

The node-pool autoscaler's per-node resource values must match these admission
budgets. Carrier count, NBD device count, address space, quotas, and actual
memory pressure are separate bounds. Increasing admission alone does not prove
500 resident sandboxes or improve a 100-way startup burst.

The canonical `example/warm-slot.nomad` accepts `standard_slots` (default 6)
and `privileged_slots` (default 2). Existing `warm-0` through `warm-7` names and
classes remain stable when increasing these bounds. Extra carriers require
the target node's `sandbox0_standard_carriers` or
`sandbox0_privileged_carriers` metadata to include their ordinal. For example,
`standard_slots=500` plus `sandbox0_standard_carriers=500` permits 500 standard
carriers on an explicitly configured node; unconfigured nodes retain their
original six. Metadata is an operator-owned capacity profile, not a workload or
benchmark selector. Provision enough NBD devices, private addresses, and host
overhead first, and use `nomad job plan` to verify placement. Decreasing counts
or node metadata requires draining the affected carriers.

## Ownership

| Owner | State |
| --- | --- |
| Regional PostgreSQL | Sandbox lifecycle, runtime generations, node capacity, slots, resource leases, RootFS heads/writer epochs, and terminal transactions |
| Manager replicas | Claim planning, mTLS node authority, procd readiness, planned lifecycle operations, and plugin-independent terminal reconciliation |
| Nomad | Dedicated node and one-shot carrier placement |
| ctld A/B | Node capacity advertisement, dynamic cgroups, NBD/XFS/OverlayFS, exact writer, network policy, node journal, and physical cleanup proof |
| Task driver | One claimed OCI/runsc process lifetime and Nomad task protocol |
| procd | Guest command, context, file, service, and event APIs |

The task driver is not terminal authority. If it disappears, manager and the
ctld primary can still stop the exact allocation, fence the writer, clean
runsc/mount/network/cgroup state, and commit terminal absence.

## Compatibility Classes

The driver and manager use `pkg/runtimeslot.RuntimeCompatibility`. A class
contains immutable execution inputs only:

- architecture;
- driver and runsc versions;
- gVisor platform;
- overlay, file-access, and DirectFS modes;
- fixed `/procd` command/port and static control mode; and
- security class.

CPU and memory do not belong in the digest. The checked-in outer class catalog
format is version `3`; each compatibility entry is version `2`. A cluster may
publish both `standard` and `privileged` classes. The template
`mainContainer.securityClass` selects one exact class; omitting it selects
`standard`.

`privileged` grants Linux capabilities only inside the gVisor guest kernel; it
does not bypass runsc or expose host devices. Template `ephemeralMounts` become
size-bounded guest tmpfs mounts and are intentionally absent from every
durable RootFS generation.

## RootFS And Failure Safety

ctld owns the RootFS writer and node-local journal. Planned pause seals the
branch and atomically publishes a new PostgreSQL head before the allocation is
terminal. An unplanned crash is not a successful pause: cleanup crash-abandons
uncommitted dirty tail and recovery uses the last durable generation.

Running fork uses an exact writer-bound XFS freeze/checkpoint/thaw operation.
Paused rebase uses isolated old/source/target branches, file-aware conflict
checks, immutable publication, PostgreSQL CAS, and bounded rollback retention.

Per-session and aggregate dirty-tail limits apply backpressure before local
disk becomes an unbounded durability buffer. Regional composite-tail backlog
is separately bounded and materialized into shared S3 packs. Capacity and
local branch files remain charged until regional acknowledgement and physical
terminal proof.

Node reboot recovery is bound to durable node UID plus boot incarnation. A new
boot may clean an old boot only for the same authenticated node UID and only
after independently proving old runsc, mounts, network state, writer, and
lease cgroup absent.

Driver task handles use version 2 and persist the normalized command and security
class inside the opaque driver state. Nomad's public task handle does not retain
its private driver configuration across client database persistence. Recovery
rejects version 1/unknown handles, missing configuration, and identity or local
state conflicts; it never fills missing security inputs from defaults. Plan
upgrades through the normal one-shot carrier replacement lifecycle, not seamless
recovery of old handles. Regional fencing and replacement proofs remain required;
this state-format change does not preserve processes across a node reboot.

## Build And Test

The driver is a separate Go module so Nomad dependencies do not enter every
Sandbox0 service.

```sh
cd nomad-driver-sandbox0
go test -buildvcs=false ./...
go vet ./...
go build -buildvcs=false -o ../bin/nomad-driver-sandbox0 .
go build -buildvcs=false -o ../bin/nomad-rootfs-sessionctl \
  ./cmd/nomad-rootfs-sessionctl
```

Build ctld from the repository root:

```sh
go build -buildvcs=false -o bin/ctld ./ctld/cmd/ctld
```

## Runtime Compatibility Corpus

The opt-in privileged corpus executes the same static payload through stock
runsc DirectFS, stock runsc Gofer, and runc. DirectFS remains the production
lane; Gofer and runc are compatibility controls.

```sh
cd nomad-driver-sandbox0
CGO_ENABLED=0 \
SANDBOX0_RUN_PRIVILEGED_RUNTIME_CORPUS=1 \
SANDBOX0_RUNTIME_CORPUS_ROOTFS=/tmp/s0-runtime-corpus-rootfs \
go test ./internal/driver \
  -run TestPrivilegedRuntimeGoldenCorpus -count=1 -v -timeout=3m
```

The corpus covers file operations, sparse files, xattrs, links, rename,
inotify, ownership, SCM_RIGHTS, OverlayFS whiteouts, and host-isolation
negative cases. It does not claim `FALLOC_FL_PUNCH_HOLE` parity when stock
runsc returns `EOPNOTSUPP`.

## RustFS Request-Cost And Migration Matrix

```sh
SANDBOX0_RUSTFS_ENDPOINT=http://127.0.0.1:19000 \
SANDBOX0_RUSTFS_ACCESS_KEY=sandbox0test \
SANDBOX0_RUSTFS_SECRET_KEY=sandbox0testsecret \
go test ./pkg/rootfssession \
  -run 'TestRustFS(DirtyTailPressureRetiresAfterObjectStoreRecovery|RootFSMigratesBetweenIndependentNodes)' \
  -count=1 -v -timeout=5m
```

These tests enforce bounded GET/PUT behavior, forbid HEAD/LIST on the RootFS
data path, inject an S3 outage, and prove a second independent node reads the
published immutable generation with a new writer epoch. They do not replace a
privileged multi-host production gate.

## PostgreSQL High Availability

Use a managed writer endpoint or ordered multi-host URL:

```text
postgres://manager@pg-a:5432,pg-b:5432/sandbox0?sslmode=verify-full
```

Manager admits only read-write PostgreSQL connections and periodically
revalidates pooled connections after role changes. Readiness fails closed while
no primary is reachable. Lifecycle code retries durable operation IDs; it does
not replay arbitrary SQL after an ambiguous commit.

The opt-in promotion test is:

```sh
SANDBOX0_RUN_PG_HA_TESTS=1 go test ./pkg/dbpool \
  -run TestPostgresPrimaryPromotionPreservesExactOperationReplay -count=1 -v
```

## Node Deployment

Use the direct deployment assets:

- `../deploy/nomad/README.md` for the complete production contract;
- `../deploy/nomad/ctld` for ctld A/B, cgroup, NBD, kernel, systemd, and Nomad
  plugin installation; and
- `example/` for runtime classes, trusted Nomad endpoints, and the carrier job.

The driver HCL must match ctld and the runtime-class digest. It references the
mode-0600 ctld Unix socket and manager node-authority credentials. It does not
copy ctld's mount roots, dirty-tail budgets, object credentials, or node
capacity; those are returned and attested by ctld's versioned health contract.

Production clients belong to Nomad node pool `sandbox0` and advertise
`sandbox0_dedicated=true`. General jobs must never target that pool.

## Terminal Reconciliation

Manager's `node_authority.terminal` worker owns the full terminal path:

1. durable writer fence;
2. outbound exact-node cleanup through the authenticated ctld channel;
3. exact Nomad server stop and client allocation GC;
4. physical absence observation; and
5. atomic PostgreSQL slot/resource/writer terminal transaction.

Trusted Nomad endpoints require mTLS, exact target-bound SPIFFE URI SANs,
rotating ACL token files, bounded requests, no redirects, and no ambient
proxies. Server-record deletion alone is not physical absence; the exact
client allocation directory is checked.

ctld persists cleanup requests and byte-stable absence proofs before replying.
Accelerated 10,000-record/two-cycle Bolt churn tests verify bounded page reuse.
A 24-hour soak may be run as an additional diagnostic, but it is not required
for cutover and an interrupted soak must never be reported as a pass.

## Regional Ingress-To-Procd SLO

Build one fixed harness artifact:

```sh
go build -buildvcs=false -trimpath -o /tmp/runtime-slot-slo \
    ./tools/runtime-slot-slo
sha256sum /tmp/runtime-slot-slo
```

Serial gate:

```sh
SANDBOX0_API_TOKEN=... /tmp/runtime-slot-slo \
    --url https://region.example.com/api/v1/sandboxes \
    --template default \
    --batches 1000 \
    --concurrency 1 \
    --p50-target 500ms \
    --hard-limit 1s \
    --request-timeout 10s \
    --cleanup-timeout 2m \
    --cleanup-poll 100ms \
    --output serial-1000.json
```

Synchronized production-width gate:

```sh
SANDBOX0_API_TOKEN=... /tmp/runtime-slot-slo \
    --url https://region.example.com/api/v1/sandboxes \
    --template default \
    --batches 100 \
    --concurrency 8 \
    --request-timeout 10s \
    --batch-settle 5s \
    --cleanup-timeout 2m \
    --cleanup-poll 100ms \
    --output concurrency-8.json
```

The harness requires signed `Server-Timing` and SLO headers, performs no hidden
claim retries, rejects redirects/proxies, deletes every successful claim, and
waits for public `404` terminal convergence. The command-ready p50 target is
500 ms; command-ready and public round-trip p99 hard limits are one second.

Production width requires eight ready carriers, eight genuinely dedicated CPU
cores, sufficient non-oversubscribed memory, at least eight usable ctld NBD
devices, quota headroom, and replacement capacity. A narrower machine may run
only its truthful width and must not label that report as concurrency eight.
Cold S3, refill, unclean recovery, and full-cold-node reports remain separate
from the hot distribution.
Test logical RootFS size and actual stored data volume independently, including
mapping depth and file count; do not use a large empty sparse filesystem as the
sole large-RootFS case. No tenant RootFS contents may be prewarmed. The supported
size matrix must pass the same startup boundary without relaxing the hard limit.

## Running Fork Diagnostic

Normal running forks use manager and the authenticated ctld node channel. The
root-owned diagnostic can exercise the node boundary directly:

```sh
nomad-rootfs-sessionctl \
  --socket /run/sandbox0/ctld-nomad-runtime.sock \
  --stage-file /run/sandbox0/handoffs/source.json \
  --operation-id fork-operation-1 \
  --source-sandbox-id source-sandbox \
  --target-sandbox-id paused-target-sandbox \
  --target-generation-id target-generation-1
```

The raw one-time writer token is stripped before the RPC. Exact retries reuse
the same proof; a different unresolved operation fails closed.
