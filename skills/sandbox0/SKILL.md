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
- Managed Agents examples and runtime work: https://github.com/sandbox0-ai/managed-agents

When reporting issues, include a minimal reproduction and relevant logs, but remove API keys, tokens, kubeconfigs, customer data, private repository URLs, and any other sensitive personal or organizational information.
