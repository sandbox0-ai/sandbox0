<p align="center">
  <img src="https://sandbox0.ai/sandbox0.png" alt="Sandbox0 logo" width="140" />
</p>

<p align="center">
  <a href="https://sandbox0.ai/docs"><img src="https://img.shields.io/badge/docs-sandbox0.ai-0f172a?style=for-the-badge" alt="Docs" /></a>
  <a href="https://sandbox0.ai/docs/self-hosted"><img src="https://img.shields.io/badge/self--hosted-supported-0b6bcb?style=for-the-badge" alt="Self-hosted" /></a>
  <a href="./LICENSE"><img src="https://img.shields.io/badge/license-Apache_2.0-1f8f5f?style=for-the-badge" alt="License" /></a>
</p>

Sandbox0 is a Kubernetes-native sandbox runtime for AI agents and interactive workloads that need a real `bash` environment, durable workspace state, and private deployment boundaries.

Modern AI agents need more than a disposable container or a toy code interpreter. They need to run shell commands, install dependencies, keep working directories warm across turns, persist workspace data across restarts, and work through a unified interface for interactive runtimes, for example shells, language interpreters, CLI tools, and custom REPLs.

Sandbox0 is built for that operational reality. It combines warm sandbox pools, in-pod process management, persistent volumes with snapshot workflows, node-level network enforcement, and runtime-agnostic deployment choices. Through template-level `runtimeClassName`, teams can use a standard Kubernetes runtime in development and move to stricter isolation such as gVisor or Kata in production. The result is an agent runtime that feels closer to a reusable machine than a throwaway container.

Some enterprise capabilities are protected by built-in license-based feature gates for operators running Sandbox0 in production environments.

## Why It Exists

Most teams hit the same wall when they try to ship AI agents on top of containers:

- Cold starts are too slow for agent loops that need sub-second to low-second feedback.
- A plain container is not enough when the agent needs `bash`, long-lived processes, REPL sessions, database and cache CLIs, and file operations in one environment.
- Filesystem state disappears as soon as the pod is recycled, which breaks multi-turn workflows and human handoff.
- Network and tenant isolation become fragile once many agents and users share a cluster.
- The operational model collapses under real production requirements such as private deployment, regional isolation, and upgrades.

Sandbox0 is built to solve those problems as one system, not as a pile of disconnected components.

## What Makes It Different

- Warm sandbox pools managed by `manager`, so agent claims can come from pre-created idle pods instead of waiting for a fresh boot on every task.
- `procd` inside each sandbox pod, giving Sandbox0 a first-class runtime for command execution, stateful contexts, file I/O, directory watches, and webhook-triggered workflows.
- Sandbox0 REPL contexts are a unified abstraction for interactive runtimes, so the same interface can back shells, language interpreters, CLI tools, and custom REPLs, for example `bash`, `python`, `sqlite`, or `redis-cli`.
- Persistent volumes decoupled from sandbox lifetime through `storage-proxy`, so agent workspaces, caches, checkpoints, and generated artifacts can outlive any single pod.
- Snapshot, restore, and fork-oriented volume workflows built on JuiceFS plus object storage and PostgreSQL metadata, which is exactly what long-running agent systems need for recovery and reuse.
- Node-level network control through `netd`, which watches sandbox policy, transparently redirects traffic, and applies L4/L7 enforcement close to the workload.
- Runtime-agnostic sandboxing via template `runtimeClassName`, so the same system can run on a standard Kubernetes runtime in development and move to stronger isolation such as gVisor or Kata in production.
- A deployment model that scales from a simple single-cluster setup to multi-cluster regional routing with `edge-gateway` and `scheduler`.
- Operator-first lifecycle management, so installation, reconciliation, and upgrades follow a repeatable Kubernetes-native path instead of bespoke scripts.

## Built For

- Coding agents that need a real shell, writable filesystem, package installs, REPL sessions, and multi-step task execution.
- Agent products that need durable workspaces, checkpoints, and generated artifacts to survive across turns, retries, and pod replacement.
- Browser automation and interactive runtimes where startup delay is directly visible to end users.
- Internal developer platforms that want reusable sandbox templates and persistent workspaces without building the infrastructure layer from scratch.
- Enterprise or regional deployments that need private control over storage, networking, and cluster topology.

### Architecture

```mermaid
flowchart TD
    client[Client / API SDK] --> igw[internal-gateway]

    subgraph cluster[Kubernetes Cluster - single cluster full mode]
        direction TB

        subgraph s0[Sandbox0 Services]
            direction LR
            igw --> mgr[manager]
            igw --> pods[Sandbox Pods - procd inside]
            mgr --> pods
            mgr --> netd[netd]
            mgr --> sp[storage-proxy]
        end

        subgraph mw[Middleware Dependencies]
            direction LR
            pg[(PostgreSQL - metadata and state)]
            s3[(S3 / OSS - volume data)]
            reg[(Image Registry - optional)]
        end

        igw --> pg
        sp --> pg
        sp --> s3
        mgr --> reg
    end
```

Most users start with a single-cluster deployment and only move to multi-cluster when they need regional scale-out. For deeper architecture and deployment details, see <https://sandbox0.ai/docs/self-hosted>.

## Claim A Sandbox

All examples below assume:

- `SANDBOX0_TOKEN` contains a valid API token
- `SANDBOX0_BASE_URL` optionally overrides the default endpoint for self-hosted deployments

### Python

Install:

```bash
pip install sandbox0
```

```python
import os

from sandbox0 import Client
from sandbox0.apispec.models.sandbox_config import SandboxConfig

client = Client(
    token=os.environ["SANDBOX0_TOKEN"],
    base_url=os.environ.get("SANDBOX0_BASE_URL", "http://localhost:30080"),
)

with client.sandboxes.open(
    "default",
    config=SandboxConfig(ttl=300, hard_ttl=3600),
) as sandbox:
    print(f"Sandbox ID: {sandbox.id}")
    print(f"Status: {sandbox.status}")
```

For Go, TypeScript, CLI, and full getting-started guides, see <https://sandbox0.ai/docs/get-started>.

## Self-Hosted Quickstart

The example below is a minimal `kind` installation for local evaluation.

Prerequisites:

- `kind`
- `kubectl`
- `helm`

Create a local cluster with the same Kind config used by `infra/tests/e2e`:

```bash
kind create cluster --config kind-config.yaml
```

`kind-config.yaml`:

```yaml
kind: Cluster
apiVersion: kind.x-k8s.io/v1alpha4
name: sandbox0
nodes:
- role: control-plane
  image: kindest/node:v1.35.0
  kubeadmConfigPatches:
  - |
    kind: ClusterConfiguration
    apiServer:
      extraArgs:
        enable-aggregator-routing: "true"
  extraPortMappings:
  # internal-gateway HTTP port
  - containerPort: 30080
    hostPort: 30080
  # registry port for template image push
  - containerPort: 30500
    hostPort: 30500
```

Install `infra-operator`:

```bash
helm repo add sandbox0 https://charts.sandbox0.ai
helm repo update

helm install infra-operator sandbox0/infra-operator \
    --namespace sandbox0-system \
    --create-namespace \
    --version 0.1.0-rc.5
```

Apply the minimal single-cluster sample:

It does not include `netd` or `storage-proxy`, so it does not provide network policy enforcement or volume capabilities.

```bash
kubectl apply -f https://raw.githubusercontent.com/sandbox0-ai/sandbox0/main/infra-operator/chart/samples/single-cluster/minimal.yaml
kubectl get sandbox0infra -n sandbox0-system -w
```

Get the initial admin password:

```bash
kubectl get secret admin-password -n sandbox0-system -o jsonpath='{.data.password}' | base64 -d
```

## Production Notes

- `kind` is for evaluation only and is not a production deployment shape.
- Most teams should start with the operator-managed single-cluster setup.
- Full architecture, configuration, and production deployment guidance live in the self-hosted docs.

For full deployment guidance, see <https://sandbox0.ai/docs/self-hosted>.
