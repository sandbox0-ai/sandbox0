# Nomad production deployment contract

This directory contains the production deployment boundary for Nomad-backed
sandbox compute. Regional services and manager may still run under the
Sandbox0 operator, but sandbox workloads are Nomad allocations and ctld runs
directly on every Nomad client host. Do not deploy ctld in a Kubernetes Pod:
the elected ctld primary, the Nomad task driver, NBD mounts, network
namespaces, and runsc must observe the same host namespaces.

## Immutable deployment inputs

Pin and record one build of `manager`, `regional-gateway`, `scheduler`,
`cluster-gateway`, `ctld`, `nomad-driver-sandbox0`, `procd`, and stock `runsc`.
The following identities must agree across every generated file:

- region ID and cluster ID;
- Nomad server cluster and exact client node IDs;
- durable node UID and current node boot ID;
- node certificate common name and its manager authority mapping;
- runsc version, driver version, immutable runtime settings, and security
  class; and
- RootFS artifact OS/architecture.

The exact compatibility object belongs in
`nomad-driver-sandbox0/example/runtime-classes.example.json`. Catalog version
`3` contains only immutable carrier compatibility: architecture, driver/runsc
versions, gVisor platform, filesystem settings, DirectFS, runtime mode, and
security class. CPU, memory, PIDs, CPU quota/weight, and cpuset are forbidden
from this catalog. Until the public API exposes an explicit class selector,
configure exactly one class for each requested cluster; zero or multiple
matches fail closed.

Nomad schedules only dedicated Sandbox0 nodes and low-overhead, resource-neutral
warm carriers. Manager atomically leases exact CPU and memory from the live
capacity reported by ctld, then ctld creates `/sys/fs/cgroup/sandbox0/<lease>`
and the driver writes that lease into OCI. Neither component derives sandbox
limits from the carrier allocation; per-lease swap is disabled so it cannot
bypass PostgreSQL memory accounting. Register these clients in the Nomad
`sandbox0` node pool and set node metadata `sandbox0_dedicated=true`; the warm
job requires both. Do not register general workloads against this node pool.
After a node reboot, only plugin-independent cleanup may be routed through one
authenticated successor boot for the same durable node UID. Ctld must replay
the old slot journal and prove the old runsc, mount, network, writer, and lease
cgroup absent before manager releases that old lease; ambiguous successors
fail closed.

## Manager authority secrets

For an operator-managed manager, create three Secrets before applying a Nomad
backend configuration:

| Secret | Required keys |
| --- | --- |
| node authority TLS | `tls.crt`, `tls.key`, `client-ca.crt` |
| claim authority | `runtime-classes.json`, `writer-token.key` |
| terminal Nomad control | `nomad-endpoints.json` and every credential file named by that catalog |

`writer-token.key` must contain exactly 32 random bytes and remain stable
across retries, manager replicas, and rollouts. The terminal catalog example
uses the operator's fixed
`/etc/sandbox0/node-authority/control` mount. Keep every referenced credential
under that directory so one projected Secret is the complete, rotatable input.
The catalog needs one trusted HTTPS server endpoint per Nomad cluster and one
exact HTTPS client endpoint per node; redirects and ambient proxies are not
accepted.

A data-plane manager configuration has this shape (replace every placeholder):

```yaml
services:
  manager:
    enabled: true
    replicas: 2
    config:
      sandboxRuntimeBackend: nomad
      nodeAuthority:
        enabled: true
        listenHost: 0.0.0.0
        tlsSecretName: manager-node-authority-tls
        identities:
          - commonName: replace-with-node-certificate-cn
            clusterId: replace-with-cluster-id
            nodeId: replace-with-nomad-node-id
            nodeUid: replace-with-durable-node-uid
            podUid: replace-with-node-agent-token-identity
        runtimeSlotHeartbeatTtl: 30s
        claim:
          secretName: manager-nomad-claim
          claimTtl: 15s
          slo: 1s
        terminal:
          enabled: true
          controlSecretName: manager-nomad-control
          interval: 1s
          passTimeout: 2m
          scanLimit: 100
```

The authority URL configured on each node must resolve to the exact current
manager replica addresses and present the configured DNS and SPIFFE SANs. A
ClusterIP or load-balancer address is not an exact membership source. Keep
PostgreSQL, RootFS object storage, manager authority, and Nomad TLS endpoints
private and mutually authenticated.

## Node and warm-pool sequence

1. Configure PostgreSQL through its primary/writer endpoint and provision the
   RootFS object bucket with least-privilege credentials.
2. Create the authority Secrets and deploy regional-gateway, optional
   scheduler, cluster-gateway, and manager with the Nomad backend enabled.
3. Install ctld A/B, runsc, and the task driver with
   `ctld/install-node.sh`. Verify both role-aware ctld health states before
   enabling Nomad on the node. The installer provisions a cgroup-v2 subtree at
   `/sys/fs/cgroup/sandbox0`, enables `cpu`, `cpuset`, `memory`, and `pids` for
   child leases, and fails startup if those controllers are unavailable or the
   resource root itself contains processes.
4. Submit `nomad-driver-sandbox0/example/warm-slot.nomad`. Its count of eight
   is the minimum production acceptance width. Configure at least that many
   ctld NBD devices and enough replacement headroom. The example reserves only
   50 MHz and 64 MiB of Nomad carrier overhead per slot; these are not sandbox
   limits. For the production eight-way acceptance gate, ctld must truthfully
   report at least eight dedicated CPU cores and sufficient sandbox memory
   after host overhead. Never falsify Nomad node capacity, ctld capacity, or
   cpusets, and never oversubscribe memory for an SLO report. Keep the supplied
   `restart { attempts = 0 }` policy: one-shot slot termination must create a
   fresh allocation and network namespace, never restart the driver inside the
   consumed allocation.
5. Confirm PostgreSQL shows healthy resource-neutral class slots and live node
   capacity, every node channel is
   connected, warm default-deny is applied, and Nomad replacement allocations
   reach ready after one batch is deleted.
6. Verify the acceptance team's active and rate quotas, then run the fixed
   `tools/runtime-slot-slo` binary through the public regional URL exactly as
   documented in `nomad-driver-sandbox0/README.md`.

Serial 1000, synchronized 8-by-100, both reboot-resumable active-time 24-hour
bounded-growth gates,
privileged multi-node validation, and all failure-injection gates must pass
before cutover. The cutover is deletion-based: physically remove the
superseded Kubernetes sandbox runtime code, configuration, tests, documents,
schema compatibility, and dependencies. Disabling the old route while leaving
its state machine in the repository is not completion.
