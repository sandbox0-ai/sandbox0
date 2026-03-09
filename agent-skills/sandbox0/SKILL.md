---
name: sandbox0
description: >-
  Core skill framework for coding agents working with Sandbox0. This skill
  routes the agent to source code, OpenAPI, CLI behavior, and bundled release
  documentation in the correct priority order.
license: Apache-2.0
metadata:
  owner: sandbox0-ai
  source_priority:
    - source-code
    - pkg/apispec/openapi.yaml
    - s0-cli-help-and-implementation
    - bundled-docs
    - hosted-website-docs
---

# Sandbox0 Skill

This is the release-distributed skill framework for Sandbox0-aware coding
agents.

## Purpose

The installed artifact provides:

- a stable top-level skill entrypoint
- an explicit source-of-truth order
- a release-matched bundled docs payload
- repository references that let the agent navigate the codebase without
  maintaining a second knowledge base

## Authority Boundary

When sources disagree, the authority order is:

1. Source code
2. `pkg/apispec/openapi.yaml`
3. `s0` CLI help and implementation
4. Bundled docs included in the skill artifact
5. Hosted website docs

## Layout

- `references/` contains framework-only reference files used by the skill
- `bundled-docs/` is populated by the release bundler
- `manifest.json` is generated into the released artifact

## Task Routing

Use the reference files in `references/` to decide where to look next. This
framework intentionally avoids embedding repository-specific procedural content
directly into the checked-in skill source.
