import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { validateFast, validateRun } from "../validate.mjs";

function baseExample(overrides = {}) {
  return {
    id: "ex",
    sourceFile: "a.mdx",
    sourceType: "fence",
    language: "bash",
    code: "echo ok",
    mode: "run",
    needs: "none",
    runtime: undefined,
    deps: [],
    setup: [],
    env: [],
    order: 0,
    metadata: {},
    ...overrides,
  };
}

test("validateFast detects syntax errors", async () => {
  const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "docs-validate-fast-"));
  const manifestPath = path.join(tmpDir, "manifest.json");

  const manifest = {
    version: "1.0",
    generatedAt: new Date().toISOString(),
    docsRoot: tmpDir,
    summary: { total: 2, byLanguage: { json: 2 }, bySourceType: { fence: 2, tabs: 0 } },
    issues: [],
    examples: [
      {
        id: "json-good",
        sourceFile: "a.mdx",
        sourceType: "fence",
        language: "json",
        code: '{"ok":true}',
        mode: "syntax",
        needs: "none",
        order: 0,
        metadata: {},
      },
      {
        id: "json-bad",
        sourceFile: "a.mdx",
        sourceType: "fence",
        language: "json",
        code: '{"ok":',
        mode: "syntax",
        needs: "none",
        order: 1,
        metadata: {},
      },
    ],
  };
  await fs.writeFile(manifestPath, JSON.stringify(manifest));

  const result = await validateFast({ manifestPath });
  assert.equal(result.ok, false);
  assert.equal(result.failures.length, 1);
  assert.equal(result.failures[0].id, "json-bad");

  await fs.rm(tmpDir, { recursive: true, force: true });
});

test("validateRun supports needs filtering", async () => {
  const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "docs-validate-run-"));
  const manifestPath = path.join(tmpDir, "manifest.json");

  const manifest = {
    version: "1.0",
    generatedAt: new Date().toISOString(),
    docsRoot: tmpDir,
    summary: { total: 2, byLanguage: { bash: 2 }, bySourceType: { fence: 2, tabs: 0 } },
    issues: [],
    examples: [
      baseExample({ id: "run-none", sourceFile: "b.mdx", code: "echo ok", order: 0 }),
      baseExample({
        id: "run-infra",
        sourceFile: "b.mdx",
        code: "echo infra",
        needs: "infra",
        order: 1,
      }),
    ],
  };
  await fs.writeFile(manifestPath, JSON.stringify(manifest));

  const result = await validateRun({ manifestPath, needs: "none" });
  assert.equal(result.ok, true);
  assert.equal(result.total, 1);

  await fs.rm(tmpDir, { recursive: true, force: true });
});

test("validateRun supports go execution with declared deps", async () => {
  const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "docs-validate-go-run-"));
  const manifestPath = path.join(tmpDir, "manifest.json");

  const manifest = {
    version: "1.0",
    generatedAt: new Date().toISOString(),
    docsRoot: tmpDir,
    summary: { total: 1, byLanguage: { go: 1 }, bySourceType: { fence: 1, tabs: 0 } },
    issues: [],
    examples: [
      baseExample({
        id: "go-run",
        sourceFile: "go.mdx",
        language: "go",
        deps: [],
        code: 'import "fmt"\nfmt.Println("ok")',
      }),
    ],
  };
  await fs.writeFile(manifestPath, JSON.stringify(manifest));

  const result = await validateRun({ manifestPath, strictDeps: false });
  assert.equal(result.ok, true);
  assert.equal(result.total, 1);

  await fs.rm(tmpDir, { recursive: true, force: true });
});

test("validateRun enforces strict deps for go external imports", async () => {
  const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "docs-validate-go-deps-"));
  const manifestPath = path.join(tmpDir, "manifest.json");

  const manifest = {
    version: "1.0",
    generatedAt: new Date().toISOString(),
    docsRoot: tmpDir,
    summary: { total: 1, byLanguage: { go: 1 }, bySourceType: { fence: 1, tabs: 0 } },
    issues: [],
    examples: [
      baseExample({
        id: "go-missing-deps",
        sourceFile: "go.mdx",
        language: "go",
        code: 'import "github.com/google/uuid"\nfmt.Println(uuid.NewString())',
      }),
    ],
  };
  await fs.writeFile(manifestPath, JSON.stringify(manifest));

  const result = await validateRun({ manifestPath, strictDeps: true });
  assert.equal(result.ok, false);
  assert.equal(result.failures.length, 1);

  await fs.rm(tmpDir, { recursive: true, force: true });
});

