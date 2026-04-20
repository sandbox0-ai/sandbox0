# Sandbox0 Integration Map

## Skill purpose

Use this skill to help AI agent developers add Sandbox0 to their own agents through the CLI or SDK.

The skill is self-contained: only rely on files inside `skills/sandbox0/`.

## Docs tree

```text
references/docs-src/
├── manifest.json
├── get-started/
│   ├── page.mdx
│   └── concepts/page.mdx
├── sandbox/
│   ├── page.mdx
│   ├── contexts/page.mdx
│   ├── files/page.mdx
│   ├── network/page.mdx
│   ├── ports/page.mdx
│   └── webhooks/page.mdx
├── managed-agents/
│   ├── page.mdx
│   ├── sdk/page.mdx
│   ├── agents/page.mdx
│   ├── environments/page.mdx
│   ├── sessions/page.mdx
│   ├── events/page.mdx
│   ├── vaults/page.mdx
│   ├── agent-engines/page.mdx
│   └── compatibility/page.mdx
├── template/
│   ├── page.mdx
│   ├── configuration/page.mdx
│   ├── images/page.mdx
│   └── pool/page.mdx
├── integrations/
│   ├── page.mdx
│   └── github-ci/page.mdx
├── volume/
│   ├── page.mdx
│   ├── mounts/page.mdx
│   ├── snapshots/page.mdx
│   └── fork/page.mdx
└── self-hosted/
    ├── page.mdx
    ├── install/page.mdx
    └── configuration/page.mdx
```

Use `references/docs-src/manifest.json` as the table of contents when you need the canonical navigation order.

## Recommended compositions

- Coding agent with a persistent workspace:
  Template + Sandbox + REPL Context + Volume
- Task worker or one-shot code execution:
  Template + Sandbox + Cmd Context
- Agent that needs quick restore points:
  Volume + Snapshot
- Agent that needs isolated branches of state:
  Volume + Fork
- Agent that runs a preview server or UI:
  Sandbox + Exposed Ports
- Agent that needs egress restrictions:
  Sandbox + Network Policy
- Agent that publishes events outward:
  Sandbox + Webhooks
- Claude Managed Agents-compatible backend:
  Managed Agents + Sandbox + Volume + Network Policy + Credential

## Response style

- Optimize for helping the user integrate Sandbox0 into their agent, not for explaining Sandbox0's internal source code.
- Prefer direct CLI or SDK guidance when possible.
- Use architecture detail only when it changes the integration design or operational tradeoff.
