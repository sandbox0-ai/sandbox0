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
- runsc version, driver version, runtime settings, and Nomad CPU/memory shape;
  and
- RootFS artifact OS/architecture.

The exact compatibility object belongs in
`nomad-driver-sandbox0/example/runtime-profiles.example.json`. Manager hashes
that object, and the driver independently hashes the actual task configuration.
A mismatch intentionally leaves the pool unavailable instead of scheduling a
near-compatible slot.

## Manager authority secrets

For an operator-managed manager, create three Secrets before applying a Nomad
backend configuration:

| Secret | Required keys |
| --- | --- |
| node authority TLS | `tls.crt`, `tls.key`, `client-ca.crt` |
| claim authority | `runtime-profiles.json`, `writer-token.key` |
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
   enabling Nomad on the node.
4. Submit `nomad-driver-sandbox0/example/warm-slot.nomad`. Its count of eight
   is the minimum production acceptance width. Configure at least that many
   ctld NBD devices and enough replacement headroom.
5. Confirm PostgreSQL shows healthy exact-profile slots, every node channel is
   connected, warm default-deny is applied, and Nomad replacement allocations
   reach ready after one batch is deleted.
6. Verify the acceptance team's active and rate quotas, then run the fixed
   `tools/runtime-slot-slo` binary through the public regional URL exactly as
   documented in `nomad-driver-sandbox0/README.md`.

Serial 1000, synchronized 8-by-100, both real 24-hour bounded-growth gates,
privileged multi-node validation, and all failure-injection gates must pass
before cutover. The cutover is deletion-based: physically remove the
superseded Kubernetes sandbox runtime code, configuration, tests, documents,
schema compatibility, and dependencies. Disabling the old route while leaving
its state machine in the repository is not completion.
