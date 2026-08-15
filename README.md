<p align="center">
  <img src="https://sandbox0.ai/sandbox0.png" alt="Sandbox0 logo" width="140" />
</p>

<p align="center">
  <a href="https://sandbox0.ai/docs"><img src="https://img.shields.io/badge/docs-sandbox0.ai-0f172a?style=for-the-badge" alt="Docs" /></a>
  <a href="https://sandbox0.ai/docs/self-hosted"><img src="https://img.shields.io/badge/self--hosted-supported-0b6bcb?style=for-the-badge" alt="Self-hosted" /></a>
  <a href="./LICENSE"><img src="https://img.shields.io/badge/license-Apache_2.0-1f8f5f?style=for-the-badge" alt="License" /></a>
</p>

# Sandbox0

**Persistent, encrypted, Kubernetes-native sandboxes for long-running AI agents.**

Sandbox0 is an open-source runtime for platforms that need to run untrusted code without treating every workspace as a disposable container. The runtime pod is replaceable; the sandbox identity and writable rootfs checkpoints are durable.

Sandbox0 Cloud uses `https://api.sandbox0.ai` for sandboxes, templates, credentials, and team-scoped API keys.

> Sandbox0 is under active development. Prefer the SDKs and `s0` CLI over hardcoded HTTP paths, and check the docs before depending on beta surfaces.

## Why Sandbox0

| Differentiator | What it means |
| --- | --- |
| **Storage and compute are separated** | Persisted rootfs checkpoints are application-encrypted and stored in horizontally scalable S3-compatible object storage. Sandbox worker nodes keep disposable caches, not the durable source of truth. |
| **The sandbox lifetime is your policy** | `ttl` and `hard_ttl` both default to `0` (disabled), so the API imposes no fixed execution window. Keep a runtime running, or pause idle compute and resume the same sandbox identity later. |
| **Choose `runc` or `gVisor`** | Sandbox Pods use Kubernetes `RuntimeClass`. Run trusted or compatibility-sensitive workloads with `runc`, or untrusted multi-tenant workloads with `gVisor`/`runsc`; Sandbox0 supports rootfs checkpoint and restore on both runtime families. |
| **Fast starts, warm or cold** | Ready template pools make common claims a metadata handoff instead of a Pod boot. When a pool is empty, the cold path avoids a per-Pod init copy by mounting `procd` from a small OCI artifact and can work with cloud node autoscaling. |

## Quickstart

Install the `s0` CLI.

```bash
curl -fsSL https://raw.githubusercontent.com/sandbox0-ai/s0/main/scripts/install.sh | bash
```

Windows PowerShell:

```powershell
irm https://raw.githubusercontent.com/sandbox0-ai/s0/main/scripts/install.ps1 | iex
```

Sign in, then create a team-scoped API key for SDK or automation usage.

```bash
s0 auth login

# If no team is selected yet:
# s0 team list
# s0 team create --name my-team --home-region <region-id>
# s0 team use <team-id>

export SANDBOX0_TOKEN="$(s0 apikey create --name sdk-quickstart --role developer --expires-in 30d --raw)"
```

For Sandbox0 Cloud, SDKs default to `https://api.sandbox0.ai`. Set `SANDBOX0_BASE_URL` only when connecting to a self-hosted or private deployment.

Install an SDK. The Python SDK requires Python 3.9 or later, the TypeScript SDK requires Node.js 18 or later, and the Go SDK requires Go 1.25 or later.

```bash
# Python
pip install sandbox0

# TypeScript
npm install sandbox0

# Go
go get github.com/sandbox0-ai/sdk-go
```

Claim a sandbox, keep state in a REPL context, and run an isolated command.

```python
import os

from sandbox0 import Client
from sandbox0.apispec.models.sandbox_config import SandboxConfig

client = Client(
    token=os.environ["SANDBOX0_TOKEN"],
    base_url=os.environ.get("SANDBOX0_BASE_URL", "https://api.sandbox0.ai"),
)

with client.sandboxes.open(
    "default",
    config=SandboxConfig(ttl=300, hard_ttl=3600),
) as sandbox:
    sandbox.run("python", "x = 41")
    second = sandbox.run("python", "print(x + 1)")
    print(second.output_raw, end="")

    result = sandbox.cmd("/bin/sh -c 'pwd && ls -la'")
    print(result.output_raw, end="")
```

More examples:

- [Sandbox lifecycle and execution](https://sandbox0.ai/docs/sandbox)
- [Templates and warm pools](https://sandbox0.ai/docs/template)
- [Rootfs snapshots, restore, and fork](https://sandbox0.ai/docs/sandbox/snapshot-restore)
- [Network policy](https://sandbox0.ai/docs/sandbox/network)
- [Credentials and egress auth](https://sandbox0.ai/docs/credential)
- [GitHub CI integration](https://sandbox0.ai/docs/integrations/github-ci)
- [OpenAI Agents SDK integration](https://sandbox0.ai/docs/integrations/openai-agents)
- [Vercel Eve integration](https://sandbox0.ai/docs/integrations/vercel-eve)

## Storage, Encryption, And Long-Running Sandboxes

```mermaid
flowchart LR
    api["Sandbox0 API"] --> pod["Replaceable sandbox Pod<br/>runc or gVisor"]
    pod --> procd["procd<br/>cmd, REPL, files, services"]
    pod <--> ctld["ctld<br/>rootfs + network runtime"]
    ctld --> rootfs["Writable rootfs checkpoint"]
    rootfs --> encryption["Application-layer<br/>object encryption"]
    encryption --> object[("S3-compatible<br/>object storage")]
```

| State | Where it lives | Survives pause/resume? | Use it for |
| --- | --- | --- | --- |
| Running process, memory, sockets | Runtime pod | No | Active tool calls, REPLs, dev servers, agent gateways |
| Writable root filesystem | Rootfs checkpoint tied to one sandbox identity | Yes, after checkpoint | Same-sandbox file continuity across idle pauses |
| Named rootfs snapshot | Snapshot of initialized rootfs state | Claimable by new sandboxes | Prepared repos, dependency installs, benchmark seeds, fan-out |
| Metering, quota, and policy state | Control-plane storage | Yes | Usage truth, policy audit, quota, showback, and export |

By default, Sandbox0 applies envelope encryption to persisted rootfs checkpoint objects before writing them to S3-compatible storage. This is service-side rather than end-to-end encryption: `manager` and the active `ctld` hold the installation key and can decrypt objects while serving sandbox operations. Self-hosted deployments control this behavior through rootfs object-storage configuration.

On pause, `ctld` captures the writable containerd rootfs, uploads the encrypted checkpoint, and releases the runtime Pod. Resume creates a new Pod for the same sandbox identity and restores that rootfs. Running processes, memory, and sockets are intentionally not checkpointed.

This model lets storage capacity scale independently of compute nodes and lets long-running agents survive idle periods, node replacement, and runtime restarts. Set `ttl` to pause idle compute, set `hard_ttl` when you need a cleanup deadline, or leave either value at `0` to disable that expiration path.

## Why Kubernetes + gVisor Instead Of A Firecracker-First Stack

[Firecracker](https://github.com/firecracker-microvm/firecracker) is excellent microVM technology. Its KVM boundary is a strong choice when hardware virtualization is the primary requirement. Sandbox0 deliberately optimizes for a different systems boundary: multi-tenant isolation that fits directly into managed Kubernetes and preserves containerd-native rootfs lifecycle operations.

| Goal | Sandbox0's Kubernetes + gVisor approach |
| --- | --- |
| **Use the cloud's managed control plane** | Major clouds already operate Kubernetes as a managed service: [Amazon EKS](https://docs.aws.amazon.com/eks/latest/userguide/what-is-eks.html), [Google GKE](https://cloud.google.com/kubernetes-engine/docs/concepts/kubernetes-engine-overview), [Azure AKS](https://learn.microsoft.com/en-us/azure/aks/core-aks-concepts), and [Alibaba Cloud ACK](https://www.alibabacloud.com/help/en/ack/product-overview/product-introduction). Sandbox0 can use their cluster lifecycle, version upgrades, node pools, networking integrations, and horizontal node autoscaling instead of building a separate microVM control plane. |
| **Add a multi-tenant isolation layer** | [gVisor](https://gvisor.dev/docs/architecture_guide/intro/) handles guest system calls in a per-sandbox application kernel, reducing direct exposure to the host Linux kernel while retaining the Pod resource model. |
| **Keep deployment portable** | GKE exposes gVisor as [GKE Sandbox](https://cloud.google.com/kubernetes-engine/docs/concepts/sandbox-pods). On other Kubernetes clusters, `runsc` integrates with [Kubernetes and containerd](https://gvisor.dev/docs/user_guide/quick_start/kubernetes/) through a `RuntimeClass`. Sandbox0 can also use the cluster's standard `runc` runtime. |
| **Scale with ordinary Kubernetes primitives** | Sandbox workloads remain Pods, so the scheduler, quotas, node pools, rolling upgrades, observability, and cluster autoscaler stay on the normal operational path. |
| **Persist and restore the writable rootfs** | Sandbox0's `gvisor-rootfs` containerd handler uses shared rootfs access so `ctld` can checkpoint writable layers to encrypted S3-compatible storage. Pause releases compute; resume restores the rootfs into a replacement Pod. |

This is a tradeoff, not a claim that gVisor has a stronger isolation boundary than a Firecracker microVM. Sandbox0 chooses gVisor when teams want a substantial isolation layer without giving up managed Kubernetes operations, elastic Pod scheduling, or containerd-backed rootfs pause/resume.

## Isolation, Network, And Credentials

Sandbox0 also provides the expected policy surfaces for untrusted code: selectable runtime isolation, default-deny egress rules, protocol controls, credential projection at the network boundary, Git-over-SSH credential proxying, and authenticated public services with CORS, rate limits, timeouts, and path policy.

Sandboxing reduces blast radius and gives policy a real enforcement point. It does not make prompt injection disappear. Isolation strength depends on your deployment choices, runtime class, CNI, storage, credential policy, and network defaults.

## Self-Hosted Architecture

```mermaid
flowchart TB
    client["Client, SDK, CLI, or agent platform"] --> cgw["cluster-gateway"]
    cgw --> mgr["manager<br/>lifecycle + rootfs state"]
    cgw --> pod["sandbox pod with procd"]
    mgr --> pod
    mgr --> ctld["ctld HA pair (node-local)<br/>rootfs + network runtime"]
    pod --> ctld
    mgr --> pg[("PostgreSQL")]
    mgr --> s3[("S3-compatible storage")]
    ctld --> s3
```

Sandbox0 separates region-scoped control-plane services from cluster-scoped data-plane services. In single-cluster mode, `cluster-gateway` can act as the entrypoint. In multi-cluster mode, `regional-gateway` and `scheduler` select and route to one of the data-plane clusters in the same region.

`manager` owns sandbox lifecycle and rootfs metadata. Each sandbox node runs the `ctld-a` and `ctld-b` HA pair; the elected primary owns rootfs persistence and the network policy runtime, while the standby takes over those responsibilities after promotion.

| Layer | Components | Responsibility |
| --- | --- | --- |
| Control plane | Optional `regional-gateway`, optional `scheduler` | Tenant/API key management, cluster selection, internal routing, template distribution |
| Data plane | `cluster-gateway`, `manager`, `ctld-a` / `ctld-b` | Sandbox lifecycle, rootfs checkpoints, process/file APIs, and network enforcement |
| In-pod runtime | `procd` | PID 1 inside each sandbox pod, process abstraction, and file I/O |
| Storage | PostgreSQL, ClickHouse, and S3-compatible object storage | Transactional state and metering producer state/outbox in PostgreSQL, asynchronous metering query projection in ClickHouse, and encrypted rootfs objects in S3 |

Self-hosting is operator-first:

1. Install `infra-operator`.
2. Apply a `Sandbox0Infra` resource.
3. Let the operator reconcile gateways, manager, storage, networking, and supporting services.

Start here: <https://sandbox0.ai/docs/self-hosted>

## Repository Boundary

This repository contains the core Sandbox0 control plane, data plane, API contract, Kubernetes operator, runtime components, and docs.

Open-source `sandbox0` owns runtime primitives, metering, usage truth, API spec, and deployable components. Billing, pricing, invoices, payments, and closed cloud workflows belong outside this repository.

Related repositories:

- CLI: <https://github.com/sandbox0-ai/s0>
- Go SDK: <https://github.com/sandbox0-ai/sdk-go>
- JavaScript/TypeScript SDK: <https://github.com/sandbox0-ai/sdk-js>
- Python SDK: <https://github.com/sandbox0-ai/sdk-py>

For API changes, `pkg/apispec/openapi.yaml` is the source of truth. Generated SDK code and copied OpenAPI files in other repositories should be synchronized from it rather than edited by hand.

## Known Boundaries

- Sandbox0 is a runtime boundary, not a complete agent framework. Bring your own harness or use the documented use-case templates when you want a framework gateway to live inside the sandbox boundary.
- Pause/resume does not preserve live processes, sockets, or memory. Runtime requests are routed to a committed generation; during lifecycle transitions they may wait for the transaction to commit and continue after resume.
- Cold-claim latency includes Kubernetes scheduling, CNI setup, image locality, and, when necessary, node provisioning.
- Sandbox0 selects an existing Kubernetes `RuntimeClass`; self-hosted operators must install and maintain `runsc` and the matching containerd handler when using gVisor outside a managed integration such as GKE Sandbox.
- Self-hosted production installs require deliberate choices for Kubernetes runtime isolation, CNI, PostgreSQL, S3-compatible storage, registry, ingress, and credential policy.
- Browser and computer-use workloads require templates and integrations that include the browser/runtime tools you need.
- Do not hand-edit generated OpenAPI or SDK output. Update `sandbox0/pkg/apispec/openapi.yaml`, regenerate, and synchronize.

## Contributing

Bug reports should include a minimal reproduction, relevant logs, Sandbox0 version or deployment topology, and whether the issue is on Cloud or self-hosted. Remove API keys, tokens, kubeconfigs, private repository URLs, customer data, and any other sensitive information before sharing logs.

Sandbox0 is Apache-2.0 licensed. See [LICENSE](./LICENSE).
