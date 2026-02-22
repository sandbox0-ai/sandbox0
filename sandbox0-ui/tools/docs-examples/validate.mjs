import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import vm from "node:vm";
import { spawn } from "node:child_process";
import ts from "typescript";
import { parseAllDocuments as parseAllYamlDocuments } from "yaml";

const BASH_LANGS = new Set(["bash", "shell", "sh", "zsh"]);
const CLI_LANGS = new Set(["cli"]);
const JSON_LANGS = new Set(["json"]);
const YAML_LANGS = new Set(["yaml", "yml"]);
const TS_LANGS = new Set(["typescript", "ts"]);
const JS_LANGS = new Set(["javascript", "js", "mjs", "cjs"]);
const PY_LANGS = new Set(["python", "py"]);
const GO_LANGS = new Set(["go", "golang"]);

function looksLikeCommandOnlySnippet(code) {
  return /^\s*(go\s+\w+|pip\s+install|npm\s+install|pnpm\s+add|yarn\s+add)\b/m.test(
    code
  );
}

function sanitizeBash(code) {
  return code.replace(/<[^>\n]+>/g, "PLACEHOLDER");
}

/**
 * @param {string} command
 * @param {string[]} args
 * @param {{ input?: string, cwd?: string }} options
 * @returns {Promise<{ code: number, stdout: string, stderr: string }>}
 */
function runCommand(command, args, options = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd: options.cwd,
      stdio: "pipe",
      env: {
        ...process.env,
        ...(options.env || {}),
      },
    });

    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (chunk) => {
      stdout += String(chunk);
    });
    child.stderr.on("data", (chunk) => {
      stderr += String(chunk);
    });
    child.on("error", reject);
    child.on("close", (code) => {
      resolve({ code: code ?? 1, stdout, stderr });
    });

    if (typeof options.input === "string") {
      child.stdin.write(options.input);
    }
    child.stdin.end();
  });
}

function normalizeLanguage(language) {
  return String(language || "text").toLowerCase();
}

function isGoExternalImport(importPath) {
  const head = importPath.split("/")[0];
  return head.includes(".");
}

function parseGoImports(code) {
  const imports = [];
  const blockMatches = [...code.matchAll(/^\s*import\s*\(([\s\S]*?)\)\s*$/gm)];
  for (const match of blockMatches) {
    const lines = match[1]
      .split("\n")
      .map((item) => item.trim())
      .filter(Boolean);
    for (const line of lines) {
      const cleaned = line.replace(/\/\/.*$/, "").trim();
      const m = cleaned.match(/^(?:[\w.]+\s+)?"([^"]+)"$/);
      if (m) imports.push(m[1]);
    }
  }

  const singleMatches = [...code.matchAll(/^\s*import\s+(?:[\w.]+\s+)?"([^"]+)"\s*$/gm)];
  for (const match of singleMatches) {
    imports.push(match[1]);
  }
  return imports;
}

function stripGoImports(code) {
  return code
    .replace(/^\s*import\s*\([\s\S]*?\)\s*$/gm, "")
    .replace(/^\s*import\s+(?:[\w.]+\s+)?"[^"]+"\s*$/gm, "")
    .trim();
}

/**
 * @param {import("./schema.mjs").DocExample} example
 */
async function validateFastExample(example) {
  const language = normalizeLanguage(example.language);
  const code = example.code || "";

  if (BASH_LANGS.has(language)) {
    const result = await runCommand("bash", ["-n"], { input: sanitizeBash(code) });
    if (result.code !== 0) return { ok: false, detail: result.stderr.trim() };
    return { ok: true };
  }

  if (JSON_LANGS.has(language)) {
    if (/\.\.\./.test(code)) {
      return { ok: true, skipped: true, detail: "Template JSON snippet skipped in fast mode." };
    }
    try {
      JSON.parse(code);
      return { ok: true };
    } catch (error) {
      if (/^\s*\/\//m.test(code) || /\/\*/.test(code)) {
        return { ok: true, skipped: true, detail: "JSON with comments treated as jsonc." };
      }
      return { ok: false, detail: error.message };
    }
  }

  if (YAML_LANGS.has(language)) {
    try {
      const docs = parseAllYamlDocuments(code);
      for (const doc of docs) {
        if (doc.errors && doc.errors.length > 0) {
          throw doc.errors[0];
        }
      }
      return { ok: true };
    } catch (error) {
      return { ok: false, detail: error.message };
    }
  }

  if (TS_LANGS.has(language)) {
    const result = ts.transpileModule(code, {
      compilerOptions: {
        target: ts.ScriptTarget.ES2022,
        module: ts.ModuleKind.ESNext,
      },
      reportDiagnostics: true,
    });
    const diagnostics = result.diagnostics || [];
    if (diagnostics.length > 0) {
      const detail = diagnostics
        .map((d) => ts.flattenDiagnosticMessageText(d.messageText, "\n"))
        .join("\n");
      return { ok: false, detail };
    }
    return { ok: true };
  }

  if (JS_LANGS.has(language)) {
    try {
      // Syntax-only parse.
      new vm.Script(code);
      return { ok: true };
    } catch (error) {
      return { ok: false, detail: error.message };
    }
  }

  if (PY_LANGS.has(language)) {
    if (/^\s*>>>\s+/m.test(code)) {
      return { ok: true, skipped: true, detail: "Doctest snippet skipped in fast mode." };
    }
    if (looksLikeCommandOnlySnippet(code) || /`/.test(code)) {
      return { ok: true, skipped: true, detail: "Command-only snippet skipped in fast mode." };
    }

    const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "docs-py-"));
    try {
      const filePath = path.join(tmpDir, "example.py");
      await fs.writeFile(filePath, code);
      const result = await runCommand("python3", ["-m", "py_compile", filePath]);
      if (result.code !== 0) return { ok: false, detail: result.stderr.trim() };
      return { ok: true };
    } finally {
      await fs.rm(tmpDir, { recursive: true, force: true });
    }
  }

  if (GO_LANGS.has(language)) {
    if (looksLikeCommandOnlySnippet(code)) {
      return { ok: true, skipped: true, detail: "Command-only snippet skipped in fast mode." };
    }
    if (!/^\s*package\s+\w+/m.test(code) && /^\s*import\s+/m.test(code)) {
      return { ok: true, skipped: true, detail: "Partial Go snippet skipped in fast mode." };
    }
    if (!/^\s*func\s+\w+/m.test(code) && /:=/.test(code)) {
      return { ok: true, skipped: true, detail: "Go fragment snippet skipped in fast mode." };
    }

    const candidates = [];
    if (/^\s*package\s+\w+/m.test(code)) {
      candidates.push(code);
    }
    if (/^\s*import\s+/m.test(code)) {
      candidates.push(`package main\n\n${code}\n`);
    }
    candidates.push(`package main\n\nfunc _docExample() {\n${indent(code, 2)}\n}\n`);

    let lastError = "";
    for (const candidate of candidates) {
      const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "docs-go-"));
      try {
        const filePath = path.join(tmpDir, "example.go");
        await fs.writeFile(filePath, candidate);
        const result = await runCommand("gofmt", ["-e", filePath]);
        if (result.code === 0) return { ok: true };
        lastError = (result.stderr || result.stdout).trim();
      } finally {
        await fs.rm(tmpDir, { recursive: true, force: true });
      }
    }
    return { ok: false, detail: lastError || "Go syntax check failed." };
  }

  return { ok: true };
}

function indent(text, spaces) {
  const prefix = " ".repeat(spaces);
  return text
    .split("\n")
    .map((line) => (line.length === 0 ? line : `${prefix}${line}`))
    .join("\n");
}

/**
 * @param {string} manifestPath
 */
async function readManifest(manifestPath) {
  const input = await fs.readFile(path.resolve(manifestPath), "utf8");
  return JSON.parse(input);
}

/**
 * @param {{ manifestPath: string }} options
 */
export async function validateFast(options) {
  const manifest = await readManifest(options.manifestPath);
  const failures = [];
  const skipped = [];

  for (const example of manifest.examples) {
    const result = await validateFastExample(example);
    if (result.skipped) {
      skipped.push({
        id: example.id,
        sourceFile: example.sourceFile,
        language: example.language,
        detail: result.detail || "Skipped",
      });
      continue;
    }
    if (!result.ok) {
      failures.push({
        id: example.id,
        sourceFile: example.sourceFile,
        language: example.language,
        detail: result.detail || "Validation failed",
      });
    }
  }

  return {
    ok: failures.length === 0,
    total: manifest.examples.length,
    skipped,
    failures,
  };
}

/**
 * @param {Record<string, string>} by
 * @param {string} key
 * @param {number} value
 */
function addCount(by, key, value = 1) {
  by[key] = (by[key] || 0) + value;
}

/**
 * @param {import("./schema.mjs").DocExample} example
 */
function resolveAdapterName(example) {
  const lang = normalizeLanguage(example.language);
  if (BASH_LANGS.has(lang) || CLI_LANGS.has(lang)) return "shell";
  if (PY_LANGS.has(lang)) return "python";
  if (TS_LANGS.has(lang)) return "typescript";
  if (GO_LANGS.has(lang)) return "go";
  return null;
}

async function runSetupCommands(sessionCtx, example, runtimeEnv) {
  const setupCommands = Array.isArray(example.setup) ? example.setup : [];
  let ran = 0;
  for (const command of setupCommands) {
    const key = `${example.language}:${command}`;
    if (sessionCtx.completedSetups.has(key)) continue;
    const result = await runCommand("bash", ["-lc", command], {
      cwd: sessionCtx.dir,
      env: runtimeEnv,
    });
    if (result.code !== 0) {
      return {
        ok: false,
        detail: (result.stderr || result.stdout || "setup failed").trim(),
      };
    }
    sessionCtx.completedSetups.add(key);
    ran += 1;
  }
  return { ok: true, installed: ran };
}

function requiredEnvMissing(example) {
  const required = Array.isArray(example.env) ? example.env : [];
  return required.filter((name) => !process.env[name]);
}

function buildRuntimeEnv(example) {
  const env = {};
  for (const name of example.env || []) {
    if (process.env[name]) env[name] = process.env[name];
  }
  return env;
}

async function prepareShell(sessionCtx) {
  if (!sessionCtx.files.shell) {
    sessionCtx.files.shell = path.join(sessionCtx.dir, "session.sh");
    await fs.writeFile(sessionCtx.files.shell, "set -euo pipefail\n");
  }
}

async function executeShellExample(example, sessionCtx, options) {
  const runtimeEnv = buildRuntimeEnv(example);
  const missing = requiredEnvMissing(example);
  if (missing.length > 0) {
    return { ok: false, detail: `Missing required env: ${missing.join(", ")}` };
  }

  if (options.strictDeps && example.deps.length > 0) {
    return {
      ok: false,
      detail: "Shell examples do not support deps; use setup commands instead.",
    };
  }

  const setupResult = await runSetupCommands(sessionCtx, example, runtimeEnv);
  if (!setupResult.ok) return setupResult;

  await prepareShell(sessionCtx);
  await fs.appendFile(sessionCtx.files.shell, `${example.code}\n`);
  const result = await runCommand("bash", [sessionCtx.files.shell], {
    cwd: sessionCtx.dir,
    env: runtimeEnv,
  });
  return {
    ok: result.code === 0,
    detail: (result.stderr || result.stdout || "").trim(),
    installed: setupResult.installed || 0,
  };
}

async function preparePython(sessionCtx) {
  if (!sessionCtx.files.python) {
    sessionCtx.files.python = path.join(sessionCtx.dir, "session.py");
    await fs.writeFile(sessionCtx.files.python, "");
  }
}

async function installPythonDeps(example, sessionCtx, runtimeEnv) {
  let installed = 0;
  for (const dep of example.deps) {
    const key = `python:${dep}`;
    if (sessionCtx.installedDeps.has(key)) continue;
    const result = await runCommand(
      "python3",
      ["-m", "pip", "install", "--disable-pip-version-check", dep],
      { cwd: sessionCtx.dir, env: runtimeEnv }
    );
    if (result.code !== 0) {
      return {
        ok: false,
        detail: (result.stderr || result.stdout || `Failed to install ${dep}`).trim(),
      };
    }
    sessionCtx.installedDeps.add(key);
    installed += 1;
  }
  return { ok: true, installed };
}

async function executePythonExample(example, sessionCtx, options) {
  const runtimeEnv = buildRuntimeEnv(example);
  const missing = requiredEnvMissing(example);
  if (missing.length > 0) {
    return { ok: false, detail: `Missing required env: ${missing.join(", ")}` };
  }

  const setupResult = await runSetupCommands(sessionCtx, example, runtimeEnv);
  if (!setupResult.ok) return setupResult;

  const depsResult = await installPythonDeps(example, sessionCtx, runtimeEnv);
  if (!depsResult.ok) return depsResult;

  await preparePython(sessionCtx);
  await fs.appendFile(sessionCtx.files.python, `${example.code}\n`);
  const result = await runCommand("python3", [sessionCtx.files.python], {
    cwd: sessionCtx.dir,
    env: runtimeEnv,
  });
  return {
    ok: result.code === 0,
    detail: (result.stderr || result.stdout || "").trim(),
    installed: (setupResult.installed || 0) + (depsResult.installed || 0),
  };
}

async function prepareTypeScript(sessionCtx) {
  if (!sessionCtx.files.typescript) {
    sessionCtx.files.typescript = path.join(sessionCtx.dir, "session.mjs");
    await fs.writeFile(sessionCtx.files.typescript, "");
  }
}

async function installTypeScriptDeps(example, sessionCtx, runtimeEnv) {
  if (example.deps.length === 0) return { ok: true, installed: 0 };
  if (!sessionCtx.nodeInitialized) {
    const initResult = await runCommand("npm", ["init", "-y"], {
      cwd: sessionCtx.dir,
      env: runtimeEnv,
    });
    if (initResult.code !== 0) {
      return {
        ok: false,
        detail: (initResult.stderr || initResult.stdout || "npm init failed").trim(),
      };
    }
    sessionCtx.nodeInitialized = true;
  }
  let installed = 0;
  for (const dep of example.deps) {
    const key = `node:${dep}`;
    if (sessionCtx.installedDeps.has(key)) continue;
    const result = await runCommand("npm", ["install", "--no-audit", "--no-fund", dep], {
      cwd: sessionCtx.dir,
      env: runtimeEnv,
    });
    if (result.code !== 0) {
      return {
        ok: false,
        detail: (result.stderr || result.stdout || `Failed to install ${dep}`).trim(),
      };
    }
    sessionCtx.installedDeps.add(key);
    installed += 1;
  }
  return { ok: true, installed };
}

async function executeTypeScriptExample(example, sessionCtx, options) {
  const runtimeEnv = buildRuntimeEnv(example);
  const missing = requiredEnvMissing(example);
  if (missing.length > 0) {
    return { ok: false, detail: `Missing required env: ${missing.join(", ")}` };
  }

  const setupResult = await runSetupCommands(sessionCtx, example, runtimeEnv);
  if (!setupResult.ok) return setupResult;

  const depsResult = await installTypeScriptDeps(example, sessionCtx, runtimeEnv);
  if (!depsResult.ok) return depsResult;

  await prepareTypeScript(sessionCtx);
  const transpileResult = ts.transpileModule(example.code, {
    compilerOptions: {
      target: ts.ScriptTarget.ES2022,
      module: ts.ModuleKind.ESNext,
    },
    reportDiagnostics: true,
  });
  if ((transpileResult.diagnostics || []).length > 0) {
    const detail = transpileResult.diagnostics
      .map((d) => ts.flattenDiagnosticMessageText(d.messageText, "\n"))
      .join("\n");
    return { ok: false, detail };
  }
  await fs.appendFile(sessionCtx.files.typescript, `${transpileResult.outputText}\n`);
  const result = await runCommand("node", [sessionCtx.files.typescript], {
    cwd: sessionCtx.dir,
    env: runtimeEnv,
  });
  return {
    ok: result.code === 0,
    detail: (result.stderr || result.stdout || "").trim(),
    installed: (setupResult.installed || 0) + (depsResult.installed || 0),
  };
}

function isGoFullProgram(code) {
  return /^\s*package\s+main/m.test(code) && /^\s*func\s+main\s*\(/m.test(code);
}

async function prepareGo(sessionCtx) {
  if (sessionCtx.goState) return;
  sessionCtx.goState = {
    imports: new Set(),
    snippets: [],
    fullProgram: null,
    initializedMod: false,
  };
}

async function ensureGoModule(sessionCtx) {
  if (sessionCtx.goState.initializedMod) return { ok: true };
  const modName = `docsrun_${sessionCtx.key.replace(/[^a-zA-Z0-9_]/g, "_")}`;
  const result = await runCommand("go", ["mod", "init", modName], { cwd: sessionCtx.dir });
  if (result.code !== 0 && !/go.mod already exists/.test(result.stderr || "")) {
    return { ok: false, detail: (result.stderr || result.stdout || "go mod init failed").trim() };
  }
  sessionCtx.goState.initializedMod = true;
  return { ok: true };
}

async function installGoDeps(example, sessionCtx, runtimeEnv, options) {
  const goInit = await ensureGoModule(sessionCtx);
  if (!goInit.ok) return goInit;

  let installed = 0;
  for (const dep of example.deps) {
    const key = `go:${dep}`;
    if (sessionCtx.installedDeps.has(key)) continue;
    const result = await runCommand("go", ["get", dep], {
      cwd: sessionCtx.dir,
      env: runtimeEnv,
    });
    if (result.code !== 0) {
      return {
        ok: false,
        detail: (result.stderr || result.stdout || `Failed to install ${dep}`).trim(),
      };
    }
    sessionCtx.installedDeps.add(key);
    installed += 1;
  }

  const parsedImports = parseGoImports(example.code);
  const externalImports = parsedImports.filter(isGoExternalImport);
  if (options.strictDeps && externalImports.length > 0 && example.deps.length === 0) {
    return {
      ok: false,
      detail: `Go external imports require explicit deps: ${externalImports.join(", ")}`,
    };
  }

  if (installed > 0) {
    const tidy = await runCommand("go", ["mod", "tidy"], { cwd: sessionCtx.dir, env: runtimeEnv });
    if (tidy.code !== 0) {
      return { ok: false, detail: (tidy.stderr || tidy.stdout || "go mod tidy failed").trim() };
    }
  }
  return { ok: true, installed };
}

function renderGoProgram(goState) {
  if (goState.fullProgram) return goState.fullProgram;
  const importLines = Array.from(goState.imports).map((item) => `\t"${item}"`).join("\n");
  const importBlock = importLines ? `import (\n${importLines}\n)\n\n` : "";
  const body = goState.snippets.length > 0 ? goState.snippets.join("\n") : 'fmt.Println("ok")';
  const hasFmt = /fmt\./.test(body);
  const hasFmtImport = goState.imports.has("fmt");
  const finalImportBlock =
    hasFmt && !hasFmtImport
      ? `import (\n\t"fmt"\n${importLines ? `${importLines}\n` : ""})\n\n`
      : importBlock;
  return `package main\n\n${finalImportBlock}func main() {\n${indent(body, 2)}\n}\n`;
}

async function executeGoExample(example, sessionCtx, options) {
  const runtimeEnv = buildRuntimeEnv(example);
  const missing = requiredEnvMissing(example);
  if (missing.length > 0) {
    return { ok: false, detail: `Missing required env: ${missing.join(", ")}` };
  }

  await prepareGo(sessionCtx);
  const setupResult = await runSetupCommands(sessionCtx, example, runtimeEnv);
  if (!setupResult.ok) return setupResult;

  const depsResult = await installGoDeps(example, sessionCtx, runtimeEnv, options);
  if (!depsResult.ok) return depsResult;

  if (isGoFullProgram(example.code)) {
    sessionCtx.goState.fullProgram = example.code;
    sessionCtx.goState.imports.clear();
    sessionCtx.goState.snippets = [];
  } else {
    const imports = parseGoImports(example.code);
    for (const item of imports) sessionCtx.goState.imports.add(item);
    const stripped = stripGoImports(example.code);
    if (stripped) sessionCtx.goState.snippets.push(stripped);
  }

  const program = renderGoProgram(sessionCtx.goState);
  const mainFile = path.join(sessionCtx.dir, "main.go");
  await fs.writeFile(mainFile, program);
  const runResult = await runCommand("go", ["run", "main.go"], {
    cwd: sessionCtx.dir,
    env: runtimeEnv,
  });
  return {
    ok: runResult.code === 0,
    detail: (runResult.stderr || runResult.stdout || "").trim(),
    installed: (setupResult.installed || 0) + (depsResult.installed || 0),
  };
}

/**
 * @param {{ manifestPath: string, needs?: "none"|"infra"|"all", strictDeps?: boolean, failOnSkip?: boolean }} options
 */
export async function validateRun(options) {
  const manifest = await readManifest(options.manifestPath);
  const needsFilter = options.needs || "all";
  const strictDeps = options.strictDeps !== false;
  const failOnSkip = options.failOnSkip === true;
  const runExamples = manifest.examples
    .filter((item) => item.mode === "run")
    .filter((item) => needsFilter === "all" || item.needs === needsFilter);

  const sessionBuckets = new Map();
  for (const item of runExamples) {
    const key = item.session ? `session:${item.session}` : `isolated:${item.id}`;
    if (!sessionBuckets.has(key)) {
      sessionBuckets.set(key, []);
    }
    sessionBuckets.get(key).push(item);
  }

  for (const list of sessionBuckets.values()) {
    list.sort((a, b) => (a.sourceFile + a.order > b.sourceFile + b.order ? 1 : -1));
  }

  const failures = [];
  const skipped = [];
  const byLanguage = {};
  let installed = 0;
  for (const [sessionKey, examples] of sessionBuckets.entries()) {
    const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "docs-run-"));
    const sessionCtx = {
      key: sessionKey,
      dir: tmpDir,
      files: {},
      installedDeps: new Set(),
      completedSetups: new Set(),
      nodeInitialized: false,
      goState: null,
    };
    try {
      for (const example of examples) {
        const adapter = resolveAdapterName(example);
        if (!adapter) {
          skipped.push({
            id: example.id,
            sourceFile: example.sourceFile,
            language: example.language,
            detail: `Unsupported run language in ${sessionKey}`,
          });
          addCount(byLanguage, `${example.language}:skipped`, 1);
          continue;
        }

        let result = null;
        if (adapter === "shell") {
          result = await executeShellExample(example, sessionCtx, { strictDeps });
        } else if (adapter === "python") {
          result = await executePythonExample(example, sessionCtx, { strictDeps });
        } else if (adapter === "typescript") {
          result = await executeTypeScriptExample(example, sessionCtx, { strictDeps });
        } else if (adapter === "go") {
          result = await executeGoExample(example, sessionCtx, { strictDeps });
        }

        addCount(byLanguage, `${example.language}:checked`, 1);
        installed += result.installed || 0;
        addCount(byLanguage, `${example.language}:installed`, result.installed || 0);
        if (!result.ok) {
          failures.push({
            id: example.id,
            sourceFile: example.sourceFile,
            language: example.language,
            detail: (result.detail || "Run failed").trim(),
          });
          addCount(byLanguage, `${example.language}:failed`, 1);
        }
      }
    } finally {
      await fs.rm(tmpDir, { recursive: true, force: true });
    }
  }

  const ok = failures.length === 0 && (!failOnSkip || skipped.length === 0);
  return {
    ok,
    total: runExamples.length,
    strictDeps,
    installed,
    byLanguage,
    skipped,
    failures,
  };
}

/**
 * @param {{ ok: boolean, total: number, strictDeps?: boolean, installed?: number, byLanguage?: Record<string, number>, skipped?: Array<{id:string,sourceFile:string,language:string,detail:string}>, failures: Array<{id:string,sourceFile:string,language:string,detail:string}> }} result
 * @param {string} title
 */
export function printResult(result, title) {
  console.log(`${title}: checked ${result.total} examples`);
  if (typeof result.installed === "number") {
    console.log(`Installed artifacts: ${result.installed}`);
  }
  if (result.byLanguage) {
    console.log(`Language stats: ${JSON.stringify(result.byLanguage)}`);
  }
  if (result.skipped && result.skipped.length > 0) {
    console.log(`Skipped ${result.skipped.length} examples.`);
  }
  if (result.ok) {
    console.log("All examples passed.");
    return;
  }
  for (const failure of result.failures) {
    console.error(
      `[${failure.sourceFile}] (${failure.language}) ${failure.id}: ${failure.detail}`
    );
  }
}

