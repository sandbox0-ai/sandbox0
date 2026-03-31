---
name: sandbox0
description: "Use this skill when an AI agent developer wants to configure Sandbox0 sandboxing via the CLI or SDK — claim sandboxes, execute commands, mount volumes, define network policies, expose ports, set up webhooks, design templates with custom images, or plan a self-hosted deployment. Uses only files bundled inside the skill."
---

# Sandbox0

Integrate Sandbox0 sandboxing into AI agent projects via the s0 CLI or SDK.

## Start here

1. Identify the user's integration goal (e.g., persistent workspace, one-shot execution, network restrictions).
2. Read `references/integration-map.md` to find the recommended composition.
3. Read only the relevant pages under `references/docs-src/`.
4. Confirm the recommended approach addresses the user's specific constraint (persistence, network isolation, scaling) before presenting the full solution.

## Quick-start example (Python)

```python
from sandbox0 import Client
from sandbox0.apispec.models.sandbox_config import SandboxConfig
import os

client = Client(
    token=os.environ["SANDBOX0_TOKEN"],
    base_url=os.environ.get("SANDBOX0_BASE_URL", "http://localhost:30080"),
)

with client.sandboxes.open("default", config=SandboxConfig(
    ttl=300, hard_ttl=3600,
)) as sandbox:
    result = sandbox.run("python", "print('Hello from Sandbox0!')")
    print(result.output_raw, end="")
```

CLI equivalent:

```bash
s0 sandbox create --template default --ttl 300 --hard-ttl 3600
s0 sandbox exec <sandbox-id> -- echo "Hello from Sandbox0!"
```

For Go, TypeScript, and full examples see `references/docs-src/get-started/page.mdx`.

## Typical triggers

- Add Sandbox0 to an AI agent project
- Pick between REPL and one-shot execution
- Persist workspace state across runs
- Design a template or custom image
- Restrict network access
- Expose a preview server or local web app
- Receive sandbox events through webhooks
- Decide whether self-hosted Sandbox0 is needed

## Working rules

- Prefer s0 CLI- and SDK-oriented guidance over internal implementation details.
- Recommend concrete compositions when helpful, for example `REPL + Volume` for persistent coding sessions.
- Use architecture detail only when it changes the user's design choice, security model, or deployment shape.

## Which references to read

- Topic routing and common solution shapes:
  `references/integration-map.md`
- Product docs and examples:
  `references/docs-src/<section>/.../page.mdx`
- Table of contents:
  `references/docs-src/manifest.json`
