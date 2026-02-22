import test from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { extractExamples } from "../extract.mjs";

test("extractExamples collects fences and tabs code", async () => {
  const docsDir = path.resolve(
    "tools/docs-examples/tests/fixtures/sample-docs"
  );
  const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "docs-extract-"));
  const outFile = path.join(tmpDir, "manifest.json");

  const manifest = await extractExamples({ docsDir, outFile });

  assert.equal(manifest.summary.total, 3);
  assert.equal(manifest.summary.bySourceType.fence, 1);
  assert.equal(manifest.summary.bySourceType.tabs, 2);

  const ids = new Set(manifest.examples.map((item) => item.id));
  assert.equal(ids.size, manifest.examples.length);

  const runExamples = manifest.examples.filter((item) => item.mode === "run");
  assert.equal(runExamples.length, 2);
  const shellRun = runExamples.find((item) => item.id === "fence-1");
  assert.ok(shellRun);
  assert.deepEqual(shellRun.env, ["PATH", "HOME"]);

  const pythonRun = runExamples.find((item) => item.language === "python");
  assert.ok(pythonRun);
  assert.equal(pythonRun.runtime, "python3.11");
  assert.deepEqual(pythonRun.deps, ["pyyaml"]);
  assert.deepEqual(pythonRun.setup, ["python3 --version"]);

  const stored = JSON.parse(await fs.readFile(outFile, "utf8"));
  assert.equal(stored.summary.total, 3);

  await fs.rm(tmpDir, { recursive: true, force: true });
});

