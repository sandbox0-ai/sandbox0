#!/usr/bin/env node
import path from "node:path";
import { extractExamples } from "./extract.mjs";
import { printResult, validateFast, validateRun } from "./validate.mjs";

function parseArgs(argv) {
  const [, , command, ...rest] = argv;
  const flags = {};
  for (let i = 0; i < rest.length; i += 1) {
    const token = rest[i];
    if (!token.startsWith("--")) continue;
    const key = token.slice(2);
    const value = rest[i + 1];
    if (value && !value.startsWith("--")) {
      flags[key] = value;
      i += 1;
    } else {
      flags[key] = "true";
    }
  }
  return { command, flags };
}

function requiredFlag(flags, name) {
  if (!flags[name]) {
    throw new Error(`Missing required flag --${name}`);
  }
  return String(flags[name]);
}

function parseBooleanFlag(value, defaultValue) {
  if (value == null) return defaultValue;
  const normalized = String(value).toLowerCase();
  if (["1", "true", "yes", "on"].includes(normalized)) return true;
  if (["0", "false", "no", "off"].includes(normalized)) return false;
  return defaultValue;
}

async function main() {
  const { command, flags } = parseArgs(process.argv);
  if (!command) {
    throw new Error("Missing command. Use: extract | validate-fast | validate-run");
  }

  if (command === "extract") {
    const docsDir = requiredFlag(flags, "docs-dir");
    const outFile = requiredFlag(flags, "out-file");
    const manifest = await extractExamples({ docsDir, outFile });
    console.log(
      `Extracted ${manifest.summary.total} examples from ${path.resolve(docsDir)}`
    );
    if (manifest.issues.length > 0) {
      console.warn(`Found ${manifest.issues.length} extraction issue(s).`);
      for (const issue of manifest.issues) {
        console.warn(`[${issue.sourceFile}] ${issue.reason}`);
      }
    }
    return;
  }

  if (command === "validate-fast") {
    const manifestPath = requiredFlag(flags, "manifest");
    const result = await validateFast({ manifestPath });
    printResult(result, "docs-examples validate-fast");
    if (!result.ok) process.exitCode = 1;
    return;
  }

  if (command === "validate-run") {
    const manifestPath = requiredFlag(flags, "manifest");
    const needs = String(flags.needs || "all");
    const strictDeps = parseBooleanFlag(flags["strict-deps"], true);
    const failOnSkip = parseBooleanFlag(flags["fail-on-skip"], false);
    const result = await validateRun({ manifestPath, needs, strictDeps, failOnSkip });
    printResult(result, "docs-examples validate-run");
    if (!result.ok) process.exitCode = 1;
    return;
  }

  throw new Error(`Unknown command: ${command}`);
}

main().catch((error) => {
  console.error(error?.stack || String(error));
  process.exitCode = 1;
});

