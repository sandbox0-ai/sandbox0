# Nomad production deployment contract

Sandbox0 uses Nomad for sandbox compute and stock gVisor `runsc` for isolation.
Run regional and data-plane control services as host services or Nomad service
jobs. Run ctld A/B directly on every dedicated Nomad client host so ctld, the
task driver, NBD mounts, network namespaces, cgroups, and runsc observe the same
host namespaces.

- [`control/`](control/) contains the direct systemd boundary for
  regional-gateway, optional scheduler, cluster-gateway, manager, and
  ssh-gateway.
- [`host/`](host/) contains the Nomad agent wrapper and the periodic exact
  identity renewal units used by disposable workers.
- [`ctld/`](ctld/) installs ctld A/B, runsc, the task driver, cgroup delegation,
  NBD devices, and Nomad client plugin configuration.
- [`nomad-driver-sandbox0/example/`](../../nomad-driver-sandbox0/example/)
  contains the runtime-class catalog, endpoint catalog, and warm-slot job.

## Immutable deployment inputs

Pin one build of every service, `nomad-driver-sandbox0`, `procd`, and stock
`runsc`. Record and keep consistent:

- region ID, cluster ID, Nomad server cluster, and exact client node IDs;
- durable node UID, current node boot ID, and node certificate identity;
- driver and runsc versions, platform, DirectFS, filesystem mode, and security
  class; and
- RootFS artifact OS and architecture.

Runtime-class catalog version `3` contains immutable carrier compatibility only.
CPU, memory, PIDs, quota/weight, and cpuset do not belong in the catalog. Each
cluster may publish one `standard` and one `privileged` class; template
`mainContainer.securityClass` selects between them. Zero or multiple matches
for the same cluster and security class fail closed.

Nomad schedules dedicated Sandbox0 nodes and resource-neutral warm carriers. Node recovery and lifecycle inventory use
bounded, paginated allocation summaries with exact node checks. Avoid the
node-specific full allocation endpoint in operational tooling: it embeds the
complete Job in every allocation and can exhaust control-plane memory at high
carrier counts. Catalog truncation or pagination errors must block reclamation;
completed/stopping allocations remain present until physical cleanup is proven.

With terminal reconciliation enabled, manager also checks failed allocations in
the default `sandbox0-warm-slots` job family, including its bounded shards.
Startup can fail before regional registration, leaving no PostgreSQL slot for
the terminal worker to discover. After terminal work, at most once every 30
seconds, manager visits one configured Nomad cluster and up to eight failed
carriers, with a five-second request budget. It revalidates each exact allocation
before requesting replacement of that exact failed allocation. Retries recheck
whether it already has a replacement. This
scheduling repair does not release regional leases, garbage-collect client
state, or discard node journals; physical retirement remains a separate proof.

ctld also reconciles registrations left only in its durable journal. Every ten
seconds, one bounded pass scans at most 256 records and processes up to eight
candidates older than two minutes, within a twenty-second budget. An existing
regional slot stays with its normal lifecycle coordinator. For an absent slot,
PostgreSQL first persists a registration-abort fence; database triggers exclude
late registration even from an older manager binary during a rolling update.
Only that exact authenticated fence permits the existing grantless physical
cleanup. ctld preserves an unacknowledged cleanup proof until the region confirms
it. These fences are retained after cleanup and cannot be removed by migration
rollback. They do not create resource leases or declare a running guest terminal.

Runtime-slot authority credentials must resolve to the exact cluster ID, Nomad
node ID, and durable node UID. Registration rejects another placement before
writing PostgreSQL. A successor boot of the same authenticated node may finish
an older journal incarnation; its exact network and mount identity checks remain
required.

Manager atomically leases exact CPU and memory from ctld-reported node capacity;
ctld creates `/sys/fs/cgroup/sandbox0/<lease>` and the driver writes that lease
into the OCI spec. Carrier allocation resources are overhead, not sandbox
limits. Use the Nomad `sandbox0` node pool with node metadata
`sandbox0_dedicated=true`, and never schedule general workloads there.

## Fixed and elastic worker pools

The current production topology has one fixed worker and an independently
bounded elastic pool with minimum zero and maximum 299. The fixed worker keeps
the ordinary warm claim path available; it is not a stopped standby. Elastic
workers are fresh ECS instances created by the provider only when manager's
PostgreSQL-backed pressure controller raises desired capacity.
Operators may configure a narrower `min_elastic_nodes..max_elastic_nodes`
range inside `0..299`, including `1..1` for one retained worker or `0..0` to
disable new elastic capacity. The provider fleet's outer boundary stays
unchanged. Density-profile trials must account for the configured fleet bound
before enrollment is enabled; a smaller node subnet does not itself limit
cloud scale-out.
If the fixed worker loses its live carrier set, the same controller temporarily
requests one elastic worker even without user pressure; it scales that
replacement back to zero only after the fixed baseline has recovered and the
normal scale-in stabilization window has elapsed.

### Capacity policy and cost controls

Do not confuse guest capacity, carrier inventory, and readiness. A carrier is
an allocation/netns/control channel without a running guest. It still consumes
host and Nomad overhead. CPU/memory leases, NBD devices, IP addresses, compatible
security classes, and carrier inventory independently constrain admission.

`warm_slots_per_node` is the new-worker admission readiness threshold.
`elastic_slots_per_node` describes the provisioned carrier capacity of one new
elastic worker; when omitted it falls back to the legacy readiness value.
Without adaptive inventory, the fixed worker contributes its **observed** usable
carriers without being capped by either setting. With `carrier_pool.enabled`,
the cloud scaler may credit the exact-boot, provisioned carrier ceiling while
the carrier controller is healthy. This is replenishable inventory, not a
claim-ready slot: claims still require an authenticated ready allocation and
an atomic CPU/memory lease. The corresponding job catalog, node profile, NBD
pool and network must exist first.

The fixed worker contributes its observed admission CPU/memory budget separately
from the elastic worker shape. A density-profile fixed worker must not cause a
purchase merely because its leases exceed the smaller elastic budget. Physical
per-request validation and ctld pressure protection remain unchanged.

Unsatisfied demand also retains its indivisible CPU/memory/slot shape. When no
live, unfenced node can fit a request that is within the configured fresh-worker
budget, a placement-progress floor requests one worker beyond the ready elastic
set. An already-enrolling worker covers that floor, so the same fragmented
request cannot repeatedly purchase workers while enrollment is in progress.
This is bounded progress, not optimal bin-packing or live workload migration.

The controller uses leased workload CPU/memory and slots, unsatisfied claim
pressure, and explicit `headroom_*`; it does not use guest CPU utilization as
permission to overcommit. Retiring workloads are excluded from new workload
demand but all unreleased leases still protect their node from removal, even
after heartbeat expiry or a successor boot. A successfully acquired operation
is excluded from pressure even if its earlier failed-attempt row has not expired.

Start with a narrow elastic bound and a ten-minute `scale_in_stabilization`,
then tune from measurements of create bursts, worker readiness, and idle cost:

| Setting | Suggested initial policy | Purpose |
| --- | --- | --- |
| `scale_in_stabilization` | `10m` | Require sustained low demand, rather than a full idle hour |
| `max_scale_in_step` / `scale_in_cooldown` | `1` / `1m` | Bound each shrink batch; keep the established quiet window between batches |
| `max_scale_out_step` / `max_pending_nodes` | `2` / `2` | Bound purchases and include cloud-requested workers not yet enrolled |
| `scale_out_warmup` | `5m` | Do not reverse a purchase when failed-claim pressure expires during boot |
| `demand_ttl` | `5m` | Bounded pressure signal; this is not a durable waiting queue |
| `headroom_*` | Workload-dependent | Reserve capacity for arrivals during measured node preparation time |

An enrolling worker blocks scale-in while healthy admitted workers do not yet
cover the provider's current desired count. Historical enrollment records cannot
block an otherwise fully ready pool forever; their identity and cleanup
obligations are retained independently. A live in-progress drain blocks another
scale-in pass.
Running sandboxes are not migrated to make an aggregate packing calculation
come true: busy elastic nodes remain protected even after an operator lowers
the purchase ceiling. Retained historical leases never authorize purchases above
that ceiling. The lifecycle transaction remains the final authority
against claim/drain races. A default minimum of zero is cost-oriented, not a
guarantee of instant capacity for bursts or fixed-node failures. Compute quota
rejections are distinct from node shortage and must not trigger purchases.

Decision logs include the unconstrained required node count, configured target,
actually applied count, and a capacity-limit indicator. `max_elastic_nodes: 0`
must be observable as disabled growth, even with `enabled: true`. Inspect
`scale_out_pending_budget`, `scale_in_waiting_for_warmup`,
`scale_in_waiting_for_enrollment`, and `scale_in_waiting_for_drain` before
changing limits to bypass a stalled lifecycle.

### Adaptive carrier inventory and acceptance boundary

The opt-in regional carrier controller adjusts the existing Nomad system-job
family's per-node membership. It separates two time scales:

1. A node-local ready buffer, bounded by claim burst rate times carrier refill
   latency and by real free resources, per compatibility/security class.
2. A regional compute reserve, bounded by net new workload demand during the
   much longer ECS/bootstrap/admission interval. In-flight nodes count once.

The initial policy uses a combined ceiling of 256 carriers (240 standard, 16
privileged), an idle target of 16, standard low watermark 8, and a privileged
ready reserve of 2. Surplus above 32 must persist for two minutes before shrink.
The eight enrollment anchors and every busy or cleanup-owned group are retained,
including high ordinals without retaining their unused lower-ordinal prefix.
No extra idle buffer is added when CPU is fully leased or less than 64 MiB is
free. These inventory hints never increase the guest admission budget.

Scale-out and scale-in lifecycle cleanup is driven by durable PostgreSQL
actions, not solely by the provider's current lifecycle-action queue. Each
nonterminal action has a bounded recovery deadline and a fenced owner/epoch.
If a successful provider enumeration no longer sees the action and every child
ECS instance is absent from the exact scaling group, manager records both
absence timestamps, waits the bounded grace period, retires only expired and
unclaimed warm carriers with a proof digest, rejects remaining lease or
non-warm-allocation custody, removes the route, purges Nomad identity, revokes
the node, and writes a terminal convergence receipt. This closes the former
window where an expired provider token could strand a durable cleanup action.

Resize intent is persisted in PostgreSQL, bound to node UID/boot and serialized
with claim capacity locks. Only allocations outside the retained set are fenced
during a resize; existing retained carriers continue serving. After a successful
Nomad plan application and stopped-removal proof, newly registered carriers in
allowed groups can join the exact allocation allowlist individually. A slow
sibling does not hide already verified ready capacity. The resize remains
pending until all groups converge; partial admission does not renew its
provisioning-credit deadline or bypass resource leases and node drain fences. Nomad updates use
`EnforceIndex` compare-and-swap and monotonic shard revisions. Removed carriers
must stop before their old ready rows are permanently retired; the normal
terminal reconciler still owns physical cleanup and resource-release proofs.
New groups must be running in Nomad and registered ready before refill completes.
A pending refill loses cloud-capacity credit after two minutes; controller
heartbeat expiry also falls back to observed capacity. Failed repair therefore
cannot suppress genuine cloud scale-out indefinitely. Before a scale-out write,
manager re-reads the capacity snapshot and provider desired capacity, then defers
the purchase when any decision input changed. A pressureless fixed-node
replacement is also debounced for two reconcile intervals, so a normal carrier
refill cannot immediately purchase an unnecessary elastic worker.

Claims with available team quota wait for capacity for up to 30 seconds by
default (or half `node_authority.claim.claim_ttl`, whichever is smaller). Set
`node_authority.claim.capacity_wait_timeout` to a positive duration no greater
than half the claim TTL. Client cancellation or an earlier request deadline
ends the wait. Startup runs after acquisition. Shared gateway proxies give exact create, resume,
and fork routes at least 45 seconds (30 seconds waiting plus startup time), while
preserving earlier caller deadlines. External proxies and clients must also cover
both phases. Quota rejection remains
immediate; exhausting the capacity wait returns the existing capacity-unavailable
response. Unfinished claim reservations follow the existing expiry/recovery
protocol; cancellation does not prematurely free an uncertain resource lease.

Each manager bounds queued requests with `capacity_wait_max_pending` (default
1024) and `capacity_wait_max_pending_per_team` (default 256), both under
`node_authority.claim`. It rotates retry opportunities between teams with at
most eight concurrent database retries. These are local scheduling bounds;
PostgreSQL retains regional quota, operation identity, and resource authority.
Internal retries reuse the same operation and resource request. Capacity pressure
wakes the carrier controller immediately; the periodic reconcile remains a
fallback. Compatible pending demand raises spare inventory above the normal
low watermark, distributed across physical request fit and unleased resources.

For known scheduled workloads, configure `carrier_pool.prewarm_windows`.
Choose the start early enough to cover observed node enrollment and carrier
readiness time. Windows require the node pool autoscaler and authenticated
enrollment. They feed the existing demand ledger, so the same placement and
cloud node ceilings apply. For example:

```yaml
    carrier_pool:
        prewarm_windows:
            - name: one-time-agent-batch
              start: "2026-10-01T09:50:00Z"
              end: "2026-10-01T10:10:00Z"
              slots: 100
              cpu_millicores: 1000
              memory_bytes: 1073741824
            - name: recurring-agent-batch
              cron: "50 12 * * FRI"
              duration: 30m
              slots: 100
              cpu_millicores: 1000
              memory_bytes: 1073741824
```

Choose either `start`/`end` for one occurrence or `cron`/`duration` for a
recurring window; mixing the forms is rejected. Cron uses five fields
(minute, hour, day of month, month, day of week), always in UTC, with standard
lists, ranges, and steps. Seconds, timezone overrides, and `@` descriptors are
not supported. Cron duration must be between one minute and 24 hours.
The controller evaluates current windows during its existing reconcile loop;
there is no separate scheduling service or catch-up queue. A restart within an
active window resumes renewal. Missed windows are skipped, and overlapping
occurrences of one plan renew the same demand without multiplying its quantity.


Each entry describes spare capacity for that many requests of the given shape;
it does not grant a team quota or reserve specific nodes. At most 32 windows
are accepted, each lasting at most 24 hours with at most 1024 slots. Pressure
expires within 30 seconds after renewal stops, then ordinary scale-in and
carrier shrink stabilization apply. No benchmark account or schedule is built
into the controller. Record actual command-ready capacity before starting a
scheduled batch; a desired count or successful Nomad job update is insufficient.

Compatibility-specific demand and ready inventory prevent spare standard
carriers from hiding a privileged shortage. This is bounded placement progress,
not an optimal packing algorithm or live workload migration. Node metadata
defines a physical ceiling and is not lowered to perform live shrink.

Migration from a static profile requires a durable drain of all affected
allocations. Install the adaptive job family and resource/network profile with
cloud purchases and adaptive reconciliation disabled, then enable a bounded
canary. Runtime rollouts preserve existing memberships and revision metadata.
The reference physical profile admits 14 CPU / 56 GiB, uses 288 fixed-node NBD
devices (320 kernel devices), and gives elastic workers a `/23` allocation subnet.
This ceiling does not promise 256 simultaneous guests: requested CPU/memory,
security class, IP and device availability still constrain the actual count.

Acceptance must cover 200 carriers with resources for only 30 guests, 30
carriers with resources for 200, mixed security classes, failed claim followed
by successful retry, full-node requests, expired capacity heartbeat, slow
enrollment beyond demand TTL, partial scale-in, and preserved business data.
Use regional ingress-to-first-command timings, separate carrier miss from
RootFS cache miss, and measure idle overhead as well as successful startup.

The separation follows [Agones ready buffers](https://agones.dev/site/docs/reference/fleetautoscaler/),
[ECS request-based capacity, warmup and bounded steps](https://docs.aws.amazon.com/AmazonECS/latest/developerguide/managed-scaling-behavior.html),
and [Karpenter disruption controls](https://karpenter.sh/docs/concepts/disruption/).
Only the control principles apply: Sandbox0 must not adopt eviction-based
consolidation for live persistent sandboxes or reintroduce Kubernetes runtime
dependencies.

All workers are disposable. PostgreSQL owns leases, fences, enrollment,
metering projection state, and lifecycle decisions. S3 owns RootFS and volume
data. `/var/lib/sandbox0`, `/opt/nomad`, local NBD state, branches, downloads,
and materialization files are reconstructible caches and runtime state. A node
must never be the only owner of sandbox data.

An elastic node is admitted in three stages:

1. Cloud-init starts a post-cloud-final unit. It uses Alibaba IMDSv2 to obtain
   a signed instance identity, receives a one-time manager challenge, and is
   checked against the exact ESS group, account, image, instance type, source
   address, and region. ECS signatures may omit their certificate; verification
   resolves the single signer against the pinned official Alibaba certificate
   and still verifies the exact document and challenge.
2. Manager atomically reserves the configured node subnet (default `/26`), prepares its routes, returns one
   content-addressed runtime bundle, a short Nomad certificate, and a scoped
   introduction JWT. The client registers with
   `sandbox0_admitted=false`; no warm carrier can land on it.
3. Manager binds certificates to the resulting Nomad node ID and durable node
   UID. The node installs ctld A/B and its exact rendered config. Only after a
   live ctld capacity heartbeat does the node present
   `sandbox0_admitted=true`, and manager enables Nomad scheduling. The ESS
   scale-out lifecycle action continues only after all configured warm carriers are
   ready. A PostgreSQL `warming` fence still excludes those carriers from the
   claim transaction until ESS has accepted `CONTINUE`, so Nomad readiness
   alone cannot expose a half-admitted node.

Scale-out enrollment has a durable 20-minute deadline by default. On timeout,
manager first blocks late bootstrap retries, then removes allocation routes,
stops and purges warm allocations, revokes the node identity, releases its
subnet, and completes the whole ESS action with `ABANDON`. A node with an
unexpected active sandbox lease is protected and fails closed instead.

Aliyun permits only twenty heartbeat extensions per lifecycle action. Manager
reserves renewal attempts atomically in PostgreSQL, independently of readiness
polling, and retains the count across replicas and restarts. Renewal timing
covers the enrollment deadline while reserving attempts for cleanup. Ambiguous
provider calls consume an attempt; the controller does not replay them freely.
Pending hooks are renewed before independent readiness and protection work.
Protection changes apply only to ESS `InService` or `Protected` instances;
waiting instances remain under their lifecycle hook. One failed action does
not prevent other pending actions from renewing or completing.

Exact node certificates are short-lived. `sandbox0-node-bootstrap.timer`
renews them before expiry. Nomad temporarily marks the node ineligible for new
carrier allocations; certificate reload and the B-then-A ctld rollout preserve
existing carriers and sandbox claims.

Build the control-owned node config template from a reviewed source directory:

```sh
sudo deploy/nomad/control/build-node-runtime-template.sh \
  --source /etc/sandbox0/node-runtime-template-source \
  --output /etc/sandbox0/node-enrollment/runtime-config-template.tar.gz
```

Identity-bearing `ctld.env.tmpl` and `10-sandbox0.conflist.tmpl` files must use
the exact Go template fields enforced by the builder. The archive may contain
only the documented `/etc/sandbox0`, Nomad driver, and CNI config paths. It is
never assembled by copying a live worker.

Set `SANDBOX0_NOMAD_ADDRESS=https://{{.PrivateIP}}:4646` in `ctld.env.tmpl`.
The bootstrapper checks this exact node address; loopback and control-server
addresses are not valid replacements.

## Manager authority files

Provision these root-owned inputs on the current control host (and identically
on every manager replica when control-plane HA is added):

| Directory | Required files |
| --- | --- |
| `/etc/sandbox0/node-authority/tls` | `tls.crt`, `tls.key`, `client-ca.crt` |
| `/etc/sandbox0/node-authority/claim` | `runtime-classes.json`, `writer-token.key` |
| `/etc/sandbox0/node-authority/control` | `nomad-endpoints.json` and all credential files referenced by it |
| `/etc/sandbox0/node-enrollment` | CA signing keys, `runtime-config-template.tar.gz`, and atomically updated `runtime-artifact.json` |

`writer-token.key` is exactly 32 random bytes and remains stable across retries
and rollouts. The endpoint catalog has one trusted HTTPS server endpoint per
Nomad cluster and one exact HTTPS client endpoint per node. Redirects and
ambient proxies are rejected. Manager uses direct file paths; see
`control/manager.yaml.example`.

The authority URL on each node must resolve only to current manager replica
addresses and present the configured DNS and SPIFFE SANs. Keep PostgreSQL,
RootFS object storage, manager authority, and Nomad TLS endpoints private and
mutually authenticated.

## Bring-up and acceptance

1. Provision regional PostgreSQL through its writer endpoint and an
   access-controlled RootFS S3 bucket.
2. Install regional-gateway, optional scheduler, cluster-gateway, manager, and
   ssh-gateway from `control/`. The current topology intentionally uses one
   control host; a later HA topology must provision identical authority files.
3. Install the fixed dedicated node with `ctld/install-node.sh`; verify one
   primary ctld process and one synchronized local ctld peer before enabling
   the Nomad client. This A/B process pair is not a stopped ECS standby node.
   Elastic nodes execute the signed enrollment flow automatically.
4. Submit `nomad-driver-sandbox0/example/warm-slot.nomad` with
   `-var='datacenter=<region-id-with-hyphens-replaced-by-underscores>'`. The
   default eight carriers fit in shard zero. Larger pools require every
   nonempty `warm_shard` from 0 through 17 with identical standard/privileged
   counts; each job is bounded to 32 groups. Migrate an existing unsharded pool
   only after fencing claims and draining every affected node. Keep
   `restart { attempts = 0 }`: a consumed slot gets a fresh allocation and
   network namespace, never a task restart in the same allocation. Keep the
   task groups on `cni/sandbox0`; Nomad's built-in `bridge` network does not
   use the node's rendered allocation-CIDR configuration.
   Use the stock CNI `ptp` data plane in `ctld/10-sandbox0.conflist.tmpl`.
   Bridge-backed carrier interfaces are rejected before claim: the bridge
   netfilter path can lose UDP TPROXY delivery and bypass a deny policy.
   Point-to-point veth links retain each allocation's IPAM and firewall
   contracts without a shared guest Layer 2 segment. Migrating an existing
   bridge requires node fencing, zero active allocations and physical removal
   of every old bridge port before replacing the CNI configuration. Claimed
   legacy namespaces remain inspectable for exact drain and cleanup.
5. Confirm PostgreSQL has live node capacity, resource-neutral ready slots,
   connected node channels, default-deny networking, and replacement slots.
6. Run `tools/runtime-slot-slo` through the public regional endpoint as
   documented in `nomad-driver-sandbox0/README.md`.

Production acceptance requires at least eight carriers, eight truthful dedicated
CPU cores, enough non-oversubscribed memory, serial 1000, synchronized
concurrency, multi-node failure injection, and security gates. A narrower local
host may validate only its real width and must never be reported as an eight-way
run. After reboot, a successor boot may perform plugin-independent cleanup only
for the same authenticated durable node UID and only after proving old runsc,
mount, network, writer, and lease-cgroup state absent.

## Platform executable upgrades

See [node-provided procd](procd-runtime.md) for digest-pinned runtime mounts,
legacy RootFS compatibility, phased rollout, and session recovery readiness.
