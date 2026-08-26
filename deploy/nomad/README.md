# Nomad production deployment contract

Sandbox0 uses Nomad for sandbox compute and stock gVisor `runsc` for isolation.
Run regional and data-plane control services as host services or Nomad service
jobs. Run ctld A/B directly on every dedicated Nomad client host so ctld, the
task driver, NBD mounts, network namespaces, cgroups, and runsc observe the same
host namespaces.

- [`control/`](control/) contains the direct systemd boundary for
  regional-gateway, optional scheduler, cluster-gateway, manager, and
  ssh-gateway.
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

## Manager authority files

Provision these root-owned inputs identically on every manager replica:

| Directory | Required files |
| --- | --- |
| `/etc/sandbox0/node-authority/tls` | `tls.crt`, `tls.key`, `client-ca.crt` |
| `/etc/sandbox0/node-authority/claim` | `runtime-classes.json`, `writer-token.key` |
| `/etc/sandbox0/node-authority/control` | `nomad-endpoints.json` and all credential files referenced by it |

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
   ssh-gateway from `control/`. Run manager active-active.
3. Install each dedicated node with `ctld/install-node.sh`; verify one primary
   and one synchronized standby before enabling the Nomad client.
4. Submit `nomad-driver-sandbox0/example/warm-slot.nomad`. Keep `restart {
   attempts = 0 }`: a consumed slot gets a fresh allocation and network
   namespace, never a task restart in the same allocation.
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
