import fs from "node:fs/promises";
import path from "node:path";
import crypto from "node:crypto";
import vm from "node:vm";
import { compile } from "@mdx-js/mdx";
import { visit } from "unist-util-visit";
import {
  DEFAULT_MODE,
  DEFAULT_NEEDS,
  buildSummary,
  toRelativePosixPath,
} from "./schema.mjs";

const SUPPORTED_MDX_EXTENSIONS = new Set([".mdx", ".md"]);

/**
 * @param {string} docsDir
 * @returns {Promise<string[]>}
 */
async function listDocsFiles(docsDir) {
  const files = [];
  const stack = [docsDir];
  while (stack.length > 0) {
    const currentDir = stack.pop();
    const entries = await fs.readdir(currentDir, { withFileTypes: true });
    for (const entry of entries) {
      const fullPath = path.join(currentDir, entry.name);
      if (entry.isDirectory()) {
        stack.push(fullPath);
        continue;
      }
      if (SUPPORTED_MDX_EXTENSIONS.has(path.extname(entry.name))) {
        files.push(fullPath);
      }
    }
  }
  files.sort();
  return files;
}

/**
 * Parses fenced code metadata.
 * Example: mode=run needs=infra session=quickstart
 * @param {string | null | undefined} meta
 */
function parseFenceMeta(meta) {
  if (!meta) return {};
  const parsed = {};
  const parts = meta.match(/[^\s"']+|"[^"]*"|'[^']*'/g) ?? [];
  for (const part of parts) {
    const eqIdx = part.indexOf("=");
    if (eqIdx <= 0) continue;
    const key = part.slice(0, eqIdx).trim();
    const rawValue = part.slice(eqIdx + 1).trim();
    const value = rawValue.replace(/^['"]|['"]$/g, "");
    if (!key) continue;
    parsed[key] = value;
  }
  return parsed;
}

/**
 * @param {string} expression
 */
function evaluateTabsExpression(expression) {
  const wrapped = `(${expression})`;
  return vm.runInNewContext(wrapped, Object.create(null), { timeout: 1000 });
}

/**
 * @param {string} sourceFile
 * @param {"fence" | "tabs"} sourceType
 * @param {number} index
 * @param {string} language
 * @param {string} code
 */
function makeStableId(sourceFile, sourceType, index, language, code) {
  const digest = crypto
    .createHash("sha1")
    .update(`${sourceFile}|${sourceType}|${index}|${language}|${code}`)
    .digest("hex")
    .slice(0, 10);
  return `${sourceType}-${index}-${digest}`;
}

function normalizeMode(modeValue) {
  if (modeValue === "run") return "run";
  return DEFAULT_MODE;
}

function normalizeNeeds(needsValue) {
  if (needsValue === "infra") return "infra";
  return DEFAULT_NEEDS;
}

/**
 * @param {unknown} value
 * @param {string} fieldName
 * @param {string} sourceFile
 * @param {Array<any>} issues
 * @returns {string[]}
 */
function normalizeStringArray(value, fieldName, sourceFile, issues) {
  if (value == null) return [];
  if (Array.isArray(value)) {
    const badItem = value.find((item) => typeof item !== "string");
    if (badItem !== undefined) {
      issues.push({
        sourceFile,
        reason: `Invalid ${fieldName}: expected string[]`,
      });
      return [];
    }
    return Array.from(value, (item) => String(item).trim()).filter(Boolean);
  }
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed) return [];
    if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
      try {
        const parsed = JSON.parse(trimmed);
        return normalizeStringArray(parsed, fieldName, sourceFile, issues);
      } catch {
        issues.push({
          sourceFile,
          reason: `Invalid ${fieldName}: failed to parse JSON array`,
        });
        return [];
      }
    }
    return trimmed
      .split(/[|,]/)
      .map((item) => item.trim())
      .filter(Boolean);
  }
  issues.push({
    sourceFile,
    reason: `Invalid ${fieldName}: expected string or string[]`,
  });
  return [];
}

/**
 * @param {string} docsRoot
 * @param {string} filePath
 * @param {Array<any>} examples
 * @param {Array<any>} issues
 * @returns {Promise<void>}
 */
async function extractFromFile(docsRoot, filePath, examples, issues) {
  const input = await fs.readFile(filePath, "utf8");
  const sourceFile = toRelativePosixPath(docsRoot, filePath);
  let localOrder = 0;

  /** @type {import('unified').Plugin<[], any>} */
  function collectPlugin() {
    return (tree) => {
      visit(tree, "code", (node) => {
        const language = (node.lang || "text").toLowerCase();
        const meta = parseFenceMeta(node.meta);
        const code = String(node.value || "");
        const id =
          typeof meta.id === "string" && meta.id.length > 0
            ? meta.id
            : makeStableId(sourceFile, "fence", localOrder, language, code);
        examples.push({
          id,
          sourceFile,
          sourceType: "fence",
          language,
          code,
          mode: normalizeMode(meta.mode),
          needs: normalizeNeeds(meta.needs),
          runtime: typeof meta.runtime === "string" ? meta.runtime : undefined,
          deps: normalizeStringArray(meta.deps, "deps", sourceFile, issues),
          setup: normalizeStringArray(meta.setup, "setup", sourceFile, issues),
          env: normalizeStringArray(meta.env, "env", sourceFile, issues),
          session: typeof meta.session === "string" ? meta.session : undefined,
          order: localOrder,
          metadata: meta,
        });
        localOrder += 1;
      });

      visit(tree, "mdxJsxFlowElement", (node) => {
        if (node.name !== "Tabs") return;
        const attrs = node.attributes || [];
        const tabsAttr = attrs.find((attr) => attr?.name === "tabs");
        if (!tabsAttr || typeof tabsAttr.value !== "object") return;
        const expression = tabsAttr.value.value;
        if (typeof expression !== "string" || expression.trim().length === 0) return;

        let parsedTabs = null;
        try {
          parsedTabs = evaluateTabsExpression(expression);
        } catch (error) {
          issues.push({
            sourceFile,
            reason: `Failed to evaluate Tabs expression: ${error.message}`,
          });
          return;
        }

        if (!Array.isArray(parsedTabs)) {
          issues.push({
            sourceFile,
            reason: "Tabs expression is not an array.",
          });
          return;
        }

        for (const tab of parsedTabs) {
          if (!tab || typeof tab !== "object") continue;
          if (typeof tab.code !== "string") continue;
          const language = String(tab.language || "text").toLowerCase();
          const code = tab.code;
          const id =
            typeof tab.id === "string" && tab.id.length > 0
              ? tab.id
              : makeStableId(sourceFile, "tabs", localOrder, language, code);
          examples.push({
            id,
            sourceFile,
            sourceType: "tabs",
            language,
            code,
            mode: normalizeMode(tab.mode),
            needs: normalizeNeeds(tab.needs),
            runtime: typeof tab.runtime === "string" ? tab.runtime : undefined,
            deps: normalizeStringArray(tab.deps, "deps", sourceFile, issues),
            setup: normalizeStringArray(tab.setup, "setup", sourceFile, issues),
            env: normalizeStringArray(tab.env, "env", sourceFile, issues),
            session: typeof tab.session === "string" ? tab.session : undefined,
            order: localOrder,
            metadata: {
              label: typeof tab.label === "string" ? tab.label : "",
            },
          });
          localOrder += 1;
        }
      });
    };
  }

  await compile(input, {
    jsx: true,
    outputFormat: "program",
    remarkPlugins: [collectPlugin],
  });
}

/**
 * @param {{ docsDir: string, outFile: string }} options
 */
export async function extractExamples(options) {
  const docsDir = path.resolve(options.docsDir);
  const outFile = path.resolve(options.outFile);
  const files = await listDocsFiles(docsDir);

  const examples = [];
  const issues = [];
  for (const filePath of files) {
    await extractFromFile(docsDir, filePath, examples, issues);
  }

  const manifest = {
    version: "1.0",
    generatedAt: new Date().toISOString(),
    docsRoot: docsDir,
    summary: buildSummary(examples),
    examples,
    issues,
  };

  await fs.mkdir(path.dirname(outFile), { recursive: true });
  await fs.writeFile(outFile, JSON.stringify(manifest, null, 2));
  return manifest;
}

