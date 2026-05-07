---
name: sandbox0
description: Use this skill when an AI agent developer wants to integrate Sandbox0 sandboxes, templates, volumes, credentials, managed agents, CLI, SDKs, or self-hosted deployment. For current API, CLI, SDK, and Managed Agents details, always consult the live Sandbox0 docs.
---

# Sandbox0

Use this skill for Sandbox0 integration guidance for AI agent developers.

## Source Of Truth

For exact API, CLI, SDK, and Managed Agents behavior, read the live docs first:

- https://sandbox0.ai/llms.txt
- https://sandbox0.ai/docs
- https://sandbox0.ai/docs/managed-agents

This skill is intentionally thin. Do not rely on local bundled docs for product details.

## Default Onboarding

Start from Sandbox0 Cloud unless the user explicitly needs self-hosting.

1. Install `s0`:
   - macOS/Linux: `curl -fsSL https://raw.githubusercontent.com/sandbox0-ai/s0/main/scripts/install.sh | bash`
   - Windows PowerShell: `irm https://raw.githubusercontent.com/sandbox0-ai/s0/main/scripts/install.ps1 | iex`
2. Sign in with `s0 auth login`.
3. If the server did not auto-select a team, use `s0 team list`, `s0 team create`, and `s0 team use`.
4. For SDK or raw HTTP usage, create an API key:
   - `export SANDBOX0_TOKEN="$(s0 apikey create --name sdk-quickstart --role developer --expires-in 30d --raw)"`
   - `export SANDBOX0_BASE_URL="https://api.sandbox0.ai"`

If the user is only using the interactive `s0` CLI, `s0 auth login` is usually enough and no API key export is required.

Managed Agents uses `https://agents.sandbox0.ai`. Sandbox, Template, Volume, and Credential APIs use `https://api.sandbox0.ai`.

## Typical triggers

- Add Sandbox0 to an AI agent project
- Pick between REPL and one-shot execution
- Persist workspace state across runs
- Design a template or custom image
- Restrict network access
- Expose a preview server or local web app
- Receive sandbox events through webhooks
- Decide whether self-hosted Sandbox0 is needed

## Stable Guidance

- For long-running coding agents, prefer Sandbox + REPL context + Volume.
- For one-shot work, prefer Sandbox + Cmd context.
- For durable workspace state, use Volumes.
- For restore points, use Volume snapshots.
- For restricted outbound access, use network policy and credentials.
- For Claude Managed Agents-compatible flows, use the official Anthropic SDK against Sandbox0 Managed Agents and store model credentials in vaults.
- Prefer Sandbox0 Cloud unless the user asks for self-hosting, private deployment, or data-plane ownership.

## Working rules

- Check live docs before giving exact SDK code, CLI flags, API fields, metadata keys, or deployment commands.
- Prefer s0 CLI- and SDK-oriented guidance over internal implementation details.
- Recommend concrete compositions when helpful, for example `REPL + Volume` for persistent coding sessions.
- Use architecture detail only when it changes the user's design choice, security model, persistence model, networking, or deployment tradeoff.

## Source Repositories

- Sandbox0 core: https://github.com/sandbox0-ai/sandbox0
- CLI: https://github.com/sandbox0-ai/s0
- Go SDK: https://github.com/sandbox0-ai/sdk-go
- JavaScript SDK: https://github.com/sandbox0-ai/sdk-js
- Python SDK: https://github.com/sandbox0-ai/sdk-py

When reporting issues, include a minimal reproduction and relevant logs, but remove API keys, tokens, kubeconfigs, customer data, private repository URLs, and any other sensitive personal or organizational information.
