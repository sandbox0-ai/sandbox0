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

Nomad schedules dedicated Sandbox0 nodes and resource-neutral warm carriers.
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
If the fixed worker loses its live carrier set, the same controller temporarily
requests one elastic worker even without user pressure; it scales that
replacement back to zero only after the fixed baseline has recovered and the
normal scale-in stabilization window has elapsed.

All workers are disposable. PostgreSQL owns leases, fences, enrollment,
metering projection state, and lifecycle decisions. S3 owns RootFS and volume
data. `/var/lib/sandbox0`, `/opt/nomad`, local NBD state, branches, downloads,
and materialization files are reconstructible caches and runtime state. A node
must never be the only owner of sandbox data.

An elastic node is admitted in three stages:

1. Cloud-init starts a post-cloud-final unit. It uses Alibaba IMDSv2 to obtain
   a signed instance identity, receives a one-time manager challenge, and is
   checked against the exact ESS group, account, image, instance type, source
   address, and region.
2. Manager atomically reserves a `/26`, prepares its routes, returns one
   content-addressed runtime bundle, a short Nomad certificate, and a scoped
   introduction JWT. The client registers with
   `sandbox0_admitted=false`; no warm carrier can land on it.
3. Manager binds certificates to the resulting Nomad node ID and durable node
   UID. The node installs ctld A/B and its exact rendered config. Only after a
   live ctld capacity heartbeat does the node present
   `sandbox0_admitted=true`, and manager enables Nomad scheduling. The ESS
   scale-out lifecycle action continues only after all eight warm carriers are
   ready. A PostgreSQL `warming` fence still excludes those carriers from the
   claim transaction until ESS has accepted `CONTINUE`, so Nomad readiness
   alone cannot expose a half-admitted node.

Scale-out enrollment has a durable 20-minute deadline by default. On timeout,
manager first blocks late bootstrap retries, then removes allocation routes,
stops and purges warm allocations, revokes the node identity, releases its
`/26`, and completes the whole ESS action with `ABANDON`. A node with an
unexpected active sandbox lease is protected and fails closed instead.

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
   `-var='datacenter=<region-id-with-hyphens-replaced-by-underscores>'`. Keep
   `restart { attempts = 0 }`: a consumed slot gets a fresh allocation and
   network namespace, never a task restart in the same allocation.
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
