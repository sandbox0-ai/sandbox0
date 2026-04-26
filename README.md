<p align="center">
  <img src="https://sandbox0.ai/sandbox0.png" alt="Sandbox0 logo" width="140" />
</p>

<p align="center">
  <a href="https://sandbox0.ai/docs"><img src="https://img.shields.io/badge/docs-sandbox0.ai-0f172a?style=for-the-badge" alt="Docs" /></a>
  <a href="https://sandbox0.ai/docs/self-hosted"><img src="https://img.shields.io/badge/self--hosted-supported-0b6bcb?style=for-the-badge" alt="Self-hosted" /></a>
  <a href="./LICENSE"><img src="https://img.shields.io/badge/license-Apache_2.0-1f8f5f?style=for-the-badge" alt="License" /></a>
</p>

# Sandbox0

Sandbox0 is infrastructure for running AI agent work in isolated, resumable sandboxes.

It gives agent developers a place to run code, shell commands, browser tooling, build steps, repository operations, and long-running sessions without giving that work direct access to the host machine or production credentials.

Use Sandbox0 when your agent needs:

- **Isolated execution** for untrusted code, user files, generated scripts, and tool calls.
- **Stateful sessions** for REPL-style workflows such as `python`, `bash`, language servers, agent runtimes, and long-running helpers.
- **Persistent workspaces** through volumes, snapshots, restore, fork, and sync workflows.
- **Fast startup** through template-backed warm pools and warm processes.
- **Network control** with allow/deny policy and destination-scoped credential injection.
- **Custom runtimes** from your own container images, packages, tools, and resource limits.
- **Self-hosting** when you need to own the data plane, storage, network boundary, and deployment policy.

Sandbox0 Cloud uses `https://api.sandbox0.ai` for sandboxes, templates, volumes, credentials, and team-scoped API keys.

> Sandbox0 is under active development. Prefer the SDKs and `s0` CLI over hardcoded HTTP paths, and check the docs before depending on beta surfaces.

## What You Build With

| Building block | What it is                                                                                                 | Why agent developers care                                                                             |
| -------------- | ---------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| **Template**   | A runtime blueprint: image, resources, warm pool, warm processes, volume mount points, and default policy. | Keeps environments reproducible and makes startup fast.                                               |
| **Sandbox**    | An isolated runtime instance created from a template.                                                      | Gives each task, user, or agent worker its own execution boundary.                                    |
| **Context**    | A process/session inside a sandbox.                                                                        | Choose stateful REPL behavior or one-shot command execution per tool call.                            |
| **Volume**     | Persistent storage independent of a sandbox lifetime.                                                      | Keeps repositories, caches, checkpoints, artifacts, and session workspaces across sandbox recreation. |
| **Credential** | A secret source plus policy for how it may be used.                                                        | Lets agents call external services without storing raw API keys in the sandbox process.               |

The short version: use templates for repeatable environments, sandboxes for isolation, contexts for process behavior, volumes for durable memory, and credentials/network policy for controlled external access.

## Quickstart

Install the `s0` CLI:

```bash
curl -fsSL https://raw.githubusercontent.com/sandbox0-ai/s0/main/scripts/install.sh | bash
```

Windows PowerShell:

```powershell
irm https://raw.githubusercontent.com/sandbox0-ai/s0/main/scripts/install.ps1 | iex
```

Sign in, then create an API key for SDK or automation usage:

```bash
s0 auth login

# If no team is selected yet:
# s0 team list
# s0 team create --name my-team --home-region <region-id>
# s0 team use <team-id>

export SANDBOX0_TOKEN="$(s0 apikey create --name sdk-quickstart --role developer --expires-in 30d --raw)"
export SANDBOX0_BASE_URL="https://api.sandbox0.ai"
```

Install an SDK:

```bash
# Python
pip install sandbox0

# TypeScript
npm install sandbox0

# Go
go get github.com/sandbox0-ai/sdk-go
```

Claim a sandbox, run stateful code, and run a one-shot command:

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
- [Network policy](https://sandbox0.ai/docs/sandbox/network)
- [Credentials and egress auth](https://sandbox0.ai/docs/credential)

## Common Agent Patterns

**Coding agents**

Use a custom template with language runtimes, package managers, git, build tools, and an attached volume for the repository workspace. Use a REPL or warm process for the active agent loop, and one-shot commands for isolated build/test steps.

**Data or browser agents**

Use a template with the required browser or data tooling, expose only the ports needed for previews, and persist downloads or generated artifacts into a volume. Apply network policy early so agent browsing and API calls stay inside the intended boundary.

**Parallel workers**

Create one sandbox per task or user request. Share read-only inputs through volumes or object storage, and write outputs to separate volumes or task-specific paths. Keep orchestration outside the sandbox.

**Long-running sessions**

Treat the sandbox process as runtime state and the volume as durable state. Checkpoint progress frequently, use TTLs to pause idle compute, and resume or recreate sandboxes from persisted workspace state.

## Managed Agents

Sandbox0 also provides a Managed Agents path for developers who want a higher-level, Claude Managed Agents-compatible API shape.

Managed Agents sit above raw sandboxes. Application code uses the official Anthropic SDK pointed at Sandbox0's Managed Agents endpoint, while Sandbox0 provides durable sessions, event history, sandbox orchestration, persistent workspaces, network policy, and credential injection underneath.

- Managed Agents API: `https://agents.sandbox0.ai`
- Documentation: <https://sandbox0.ai/docs/managed-agents>
- Examples and runtime work: <https://github.com/sandbox0-ai/managed-agents>

Use raw Sandbox0 sandboxes when you want direct control over processes, files, templates, volumes, ports, and network policy. Use Managed Agents when you want a session/event API for agent applications and want Sandbox0 to manage the runtime attachment behind that API.

## Safety And Isolation Model

Sandbox0 is designed for workloads that execute code the host should not trust.

- Each sandbox is a separate runtime instance created from a template.
- Sandbox lifetime is controlled with `ttl` and `hard_ttl`; idle work can pause while durable state remains in volumes.
- Network policy can block or restrict outbound traffic by default.
- Egress auth resolves and injects credentials outside the sandbox process, so raw secrets do not need to be placed in environment variables or files inside untrusted code.
- Persistent data lives in volumes, not in the ephemeral sandbox filesystem.
- Self-hosted deployments let platform teams choose the Kubernetes runtime, storage, network, and regional boundary that match their security requirements.

Isolation strength depends on your deployment choices. For production self-hosting, review the self-hosted docs and choose runtime, CNI, storage, and credential policies deliberately.

## Performance Model

Agent workloads are latency-sensitive because every tool call can sit on the critical path of a user interaction.

Sandbox0 optimizes for this in three places:

- **Warm pools** keep template instances ready so a claim does not need to build the environment from scratch.
- **Warm processes** can start agent runtimes, language servers, or helpers before the sandbox is claimed.
- **Volumes** keep caches, repositories, and generated state separate from sandbox lifetime, avoiding repeated setup work.

For best results, put expensive environment setup into the template image or warm process, keep active task state in a volume, and keep sandboxes short-lived enough that idle compute does not become the source of truth.

## Self-Hosting

Most application developers can start with Sandbox0 Cloud. Self-host Sandbox0 when you need private deployment, data-plane ownership, custom runtime isolation, regional storage boundaries, or tighter integration with internal infrastructure.

Self-hosting is operator-first:

1. Install `infra-operator`.
2. Apply a `Sandbox0Infra` resource.
3. Let the operator reconcile gateways, manager, storage, networking, and supporting services.

Start here: <https://sandbox0.ai/docs/self-hosted>

## Repository Map

This repository contains the core Sandbox0 control plane, data plane, API contract, Kubernetes operator, and docs.

Related repositories:

- CLI: <https://github.com/sandbox0-ai/s0>
- Go SDK: <https://github.com/sandbox0-ai/sdk-go>
- JavaScript/TypeScript SDK: <https://github.com/sandbox0-ai/sdk-js>
- Python SDK: <https://github.com/sandbox0-ai/sdk-py>
- Managed Agents examples and runtime work: <https://github.com/sandbox0-ai/managed-agents>

For API changes, `pkg/apispec/openapi.yaml` is the source of truth. Generated SDK code and copied OpenAPI files in other repositories should be synchronized from it rather than edited by hand.

## Contributing

Bug reports should include a minimal reproduction, relevant logs, Sandbox0 version or deployment mode, and whether the issue is on Cloud or self-hosted. Remove API keys, tokens, kubeconfigs, private repository URLs, customer data, and any other sensitive information before sharing logs.

Sandbox0 is Apache-2.0 licensed. See [LICENSE](./LICENSE).
