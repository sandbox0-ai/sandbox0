<p align="center">
  <img src="https://sandbox0.ai/sandbox0.png" alt="Sandbox0 logo" width="140" />
</p>

<p align="center">
  <a href="https://sandbox0.ai/docs"><img src="https://img.shields.io/badge/docs-sandbox0.ai-0f172a?style=for-the-badge" alt="Docs" /></a>
  <a href="https://sandbox0.ai/docs/self-hosted"><img src="https://img.shields.io/badge/self--hosted-supported-0b6bcb?style=for-the-badge" alt="Self-hosted" /></a>
  <a href="./LICENSE"><img src="https://img.shields.io/badge/license-Apache_2.0-1f8f5f?style=for-the-badge" alt="License" /></a>
</p>

# Sandbox0

Sandbox0 is the runtime boundary for enterprise AI agent platforms.

It gives platform teams isolated, persistent sandboxes for agent work that needs to run code, edit repositories, expose per-agent HTTP services, keep workspace state, and access external systems without placing broad production credentials inside the agent runtime.

The sandbox writable root filesystem is checkpointed across pause/resume. Sandbox0 can release the runtime pod, keep the sandbox identity, and restore files written inside the sandbox when a new runtime generation starts. Volumes are a separate storage primitive for data that must outlive one sandbox identity, be shared, snapshotted, forked, or accessed directly through APIs.

Use Sandbox0 when you need to:

| Need | Sandbox0 angle |
| ---- | -------------- |
| Run untrusted code and tools | Isolated sandboxes with process, file, SSH, and service APIs. |
| Build coding agents | Stateful REPL contexts, one-shot commands, persistent rootfs across pause/resume, repo volumes, and fast template claims. |
| Run agent gateways such as Hermes or OpenClaw | Agent in Sandbox with builtin templates, mounted volumes, public service routes, and auto-resume. |
| Expose per-agent HTTP endpoints | Sandbox Services with route auth, method filtering, CORS, rate limits, timeouts, path rewrite, and resume. |
| Keep secrets outside the agent process | Credential sources, destination-scoped egress auth, SSH transparent proxying, LLMProxy, or an external AI gateway. |
| Control network and MCP access | Ordered traffic rules, protocol-aware MCP tool controls, and egress audit in the data plane. |
| Branch, evaluate, or recover agent state | Rootfs checkpoints for transparent sandbox resume, plus volumes, point-in-time snapshots, restore, and Copy-on-Write fork. |
| Own the deployment boundary | Self-hosted Kubernetes data plane with external PostgreSQL, S3/OSS, Vault-compatible storage, Redis, and registry options. |

Sandbox0 Cloud uses `https://api.sandbox0.ai` for sandboxes, templates, volumes, credentials, and team-scoped API keys. Managed Agents uses `https://agents.sandbox0.ai`.

> Sandbox0 is under active development. Prefer the SDKs and `s0` CLI over hardcoded HTTP paths, and check the docs before depending on beta surfaces.

## Choose Your Path

| Path | Use it when | Start here |
| ---- | ----------- | ---------- |
| **Raw Sandboxes** | You want direct control over processes, files, volumes, ports, templates, and network policy. | [Get started](https://sandbox0.ai/docs/get-started) |
| **Agent in Sandbox** | You want an agent framework gateway to run inside the sandbox, with its state on a volume and its API exposed through Sandbox0 routes. | [Agent in Sandbox](https://sandbox0.ai/docs/agent-in-sandbox) |
| **Managed Agents** | You want a Claude Managed Agents-compatible session and event API backed by Sandbox0 runtime primitives. | [Managed Agents](https://sandbox0.ai/docs/managed-agents) |
| **Self-hosted** | You need private deployment, data-plane ownership, regional storage boundaries, or custom runtime isolation. | [Self-hosted](https://sandbox0.ai/docs/self-hosted) |

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

Install an SDK.

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

    result = sandbox.cmd("sh -lc 'pwd && ls -la'")
    print(result.output_raw, end="")
```

More examples:

- [Get started](https://sandbox0.ai/docs/get-started)
- [Sandbox lifecycle and execution](https://sandbox0.ai/docs/sandbox)
- [Templates and warm pools](https://sandbox0.ai/docs/template)
- [Volumes, snapshots, fork, and sync](https://sandbox0.ai/docs/volume)
- [Agent in Sandbox](https://sandbox0.ai/docs/agent-in-sandbox)
- [Network policy](https://sandbox0.ai/docs/sandbox/network)
- [Credentials and egress auth](https://sandbox0.ai/docs/credential)

## Agent In Sandbox

Agent in Sandbox runs an agent framework gateway inside a Sandbox0 sandbox instead of on a cloud VM, VPS, or developer workstation.

Use this path when the agent needs a long-lived workspace, controlled network access, and a service endpoint, but you do not want the sandbox filesystem to become the place where broad production credentials live.

Sandbox0 owns the runtime boundary around the framework:

| Layer | Sandbox0 primitive |
| ----- | ------------------ |
| Runtime image | Builtin or custom Template |
| Durable agent state | SandboxVolume |
| Process restart | `cmd` Sandbox Service |
| Public HTTP entrypoint | Sandbox Service route |
| Wake-up path | Sandbox `auto_resume` plus route `resume` |
| Secret boundary | Credential sources, egress auth, LLMProxy, or an external model proxy |
| Network boundary | Sandbox network policy and protocol controls |

Self-hosted deployments seed builtin templates for `openclaw` and `hermes` when `Sandbox0Infra.spec.builtinTemplates` is omitted. Operators can pin images, tune pools, or remove either template explicitly.

| Template | Typical persistent path | What it gives you |
| -------- | ----------------------- | ----------------- |
| [`hermes`](https://sandbox0.ai/docs/agent-in-sandbox/hermes) | `/opt/data` | Hermes gateway sessions, memory, skills, and state behind Sandbox0 runtime controls. |
| [`openclaw`](https://sandbox0.ai/docs/agent-in-sandbox/openclaw) | `/home/node/.openclaw` | OpenClaw gateway, workspace, logs, and session state behind Sandbox0 route and lifecycle controls. |

Public route auth is enforced before traffic reaches the agent gateway. Paused sandboxes can be resumed by public service requests when both sandbox `auto_resume` and route `resume` are enabled. Running processes, sockets, memory, and in-flight requests are not preserved across pause/resume, so durable state should live on the mounted volume.

## Managed Agents

Sandbox0 Managed Agents is a higher-level path for developers who want a Claude Managed Agents-compatible API shape.

Application code uses the official Anthropic SDK pointed at Sandbox0's Managed Agents endpoint, while Sandbox0 provides durable sessions, event history, sandbox orchestration, persistent workspaces, network policy, and credential injection underneath.

- Managed Agents API: `https://agents.sandbox0.ai`
- Documentation: <https://sandbox0.ai/docs/managed-agents>

Managed Agents supports two execution models:

| Model | How it works | Best fit |
| ----- | ------------ | -------- |
| **Agent in sandbox** | The agent runtime process lives inside the per-session sandbox with the workspace and harness state. | Claude Code style coding agents, Codex app-server sessions, and workflows that expect local files and shell state. |
| **Sandbox as tool** | A resident runtime service owns the agent loop and claims a Sandbox0 sandbox only when isolated tool execution is needed. | Product copilots and agent workflows built around a separate application control plane. |

Use raw Sandbox0 sandboxes when you want direct control over processes, files, templates, volumes, ports, and network policy. Use Managed Agents when you want Sandbox0 to provide the session/event API and runtime attachment behind that API.

## Platform Patterns

**Coding agents**

Use a custom template with language runtimes, package managers, git, build tools, and an attached volume for the repository workspace. Use a stateful REPL for the active agent loop and one-shot commands for isolated build/test steps.

**Code interpreter, data, or browser agents**

Use a template with the required browser or data tooling, expose only the ports needed for previews, and persist downloads or generated artifacts into a volume. Apply network policy early so agent browsing and API calls stay inside the intended boundary.

**Agent gateways**

Run gateways such as Hermes or OpenClaw as `cmd` Sandbox Services. Store framework state on a mounted volume, protect public routes with Sandbox0 route auth, and keep model/provider credentials behind egress auth, LLMProxy, or an external AI gateway.

**Parallel workers**

Create one sandbox per task, eval case, branch, or user request. Share read-only inputs through volumes, snapshots, or object storage, then write outputs to separate volumes or task-specific paths. Keep orchestration outside the sandbox boundary.

**Long-running sessions**

Treat the sandbox process as runtime state and the volume as durable state. Paused sandboxes restore the writable root filesystem checkpoint, but running processes are recreated. Checkpoint progress frequently, use TTLs to pause idle compute, and keep important workspace state in volumes or external storage.

## Core Concepts

| Building block | What it is | Why platform developers care |
| -------------- | ---------- | ---------------------------- |
| **Template** | A runtime blueprint: image, resources, warm pool, mount points, and default policy. | Keeps environments reproducible and claims fast. |
| **Sandbox** | A durable identity and configuration for an isolated runtime instance. | Gives each task, user, or agent worker its own execution boundary. |
| **Root filesystem** | The sandbox writable filesystem checkpointed during pause/resume and tied to the sandbox identity. | Lets files written inside the sandbox survive runtime pod cleanup without explicit mounts. |
| **Context** | A process/session inside a sandbox, either REPL-style or one-shot command. | Lets agents choose in-process continuity or clean execution per tool call. |
| **Volume** | Persistent storage independent of a sandbox lifetime. | Keeps repositories, caches, agent memory, artifacts, checkpoints, snapshots, forks, and shared data. |
| **Service** | A public HTTP entrypoint backed by a listener, command, or function. | Exposes per-sandbox APIs, previews, webhooks, and agent gateways with route policy. |
| **Credential** | A secret source plus policy for how it may be projected or injected. | Lets agents call external services without storing raw production keys in the sandbox process. |

The short version: use templates for repeatable environments, sandboxes for isolation, the root filesystem for same-sandbox file continuity across pause/resume, contexts for process behavior, volumes for durable or shared memory, services for controlled ingress, and credentials/network policy for controlled external access.

## Persistence And Lifecycle

Sandbox0 separates runtime state from filesystem state.

- The sandbox writable root filesystem is persisted as the latest rootfs checkpoint for the same sandbox identity. Files written outside mounted volumes survive pause/resume once the pause checkpoint succeeds.
- `ttl` is a runtime soft timeout. When it expires, Sandbox0 checkpoints the writable root filesystem, pauses the sandbox, and releases runtime compute.
- `hard_ttl` is a hard sandbox lifetime. When it expires, Sandbox0 deletes the sandbox identity and durable state tied to that identity, including rootfs checkpoints.
- Pause/resume preserves sandbox identity and the latest writable root filesystem checkpoint, but not running processes, memory, sockets, PID state, or live REPL sessions.
- Volumes are the durable workspace surface for data that must outlive one sandbox identity, be shared, snapshotted, forked, restored, or accessed directly through APIs.

Design long-running agents so the runtime can be replaced and the volume or external store remains the source of truth.

## Safety, Networking, And Credentials

Sandbox0 is designed for workloads that execute code the host should not trust.

- Each sandbox is a separate runtime instance created from a template.
- Network policy can default to `block-all` and allow only explicit destinations.
- Protocol controls can restrict remote MCP tool calls after destination traffic is allowed.
- Egress auth resolves and injects credentials outside the sandbox process, so raw secrets do not need to be placed in environment variables or files inside untrusted code.
- SSH egress auth can proxy Git-over-SSH without writing the upstream private key into the sandbox.
- Sandbox Services enforce route auth, CORS, rate limits, timeouts, and path policy before public traffic reaches the sandbox.
- Self-hosted deployments let platform teams choose the Kubernetes runtime, storage, network, and regional boundary that match their security requirements.

Isolation strength depends on your deployment choices. For production self-hosting, review the self-hosted docs and choose runtime, CNI, storage, and credential policies deliberately.

## Performance Model

Agent workloads are latency-sensitive because every tool call can sit on the critical path of a user interaction.

Sandbox0 optimizes for this in these places:

- **Warm pools** keep template instances ready so a claim can reuse an idle pod instead of starting the environment from scratch.
- **Template images** move expensive package, toolchain, browser, and agent framework setup out of the request path.
- **Rootfs checkpoints and volumes** keep resumed sandboxes useful after runtime cleanup. Use the rootfs checkpoint for transparent runtime restore and volumes for explicit durable workspaces, snapshots, forks, and shared data.

For best results, put expensive environment setup into the template image, keep active task state in a volume when it must outlive the sandbox identity, and keep sandboxes short-lived enough that idle compute does not become the source of truth.

## Self-Hosting

Most application developers can start with Sandbox0 Cloud. Self-host Sandbox0 when you need private deployment, data-plane ownership, custom runtime isolation, regional storage boundaries, or tighter integration with internal infrastructure.

Self-hosting is operator-first:

1. Install `infra-operator`.
2. Apply a `Sandbox0Infra` resource.
3. Let the operator reconcile gateways, manager, storage, networking, and supporting services.

Common deployment shapes:

| Profile | Use when |
| ------- | -------- |
| Minimal single-cluster | You want a fast first install for local eval or API validation. |
| Full single-cluster | You need persistent volumes, snapshots, public sandbox services, or network controls. |
| Multi-cluster control plane | You coordinate multiple data-plane clusters in one region. |
| Multi-cluster data plane | You attach a runtime cluster to an external regional control plane. |

Start here: <https://sandbox0.ai/docs/self-hosted>

## Repository Map

This repository contains the core Sandbox0 control plane, data plane, API contract, Kubernetes operator, and docs.

Related repositories:

- CLI: <https://github.com/sandbox0-ai/s0>
- Go SDK: <https://github.com/sandbox0-ai/sdk-go>
- JavaScript/TypeScript SDK: <https://github.com/sandbox0-ai/sdk-js>
- Python SDK: <https://github.com/sandbox0-ai/sdk-py>

For API changes, `pkg/apispec/openapi.yaml` is the source of truth. Generated SDK code and copied OpenAPI files in other repositories should be synchronized from it rather than edited by hand.

## Contributing

Bug reports should include a minimal reproduction, relevant logs, Sandbox0 version or deployment mode, and whether the issue is on Cloud or self-hosted. Remove API keys, tokens, kubeconfigs, private repository URLs, customer data, and any other sensitive information before sharing logs.

Sandbox0 is Apache-2.0 licensed. See [LICENSE](./LICENSE).
