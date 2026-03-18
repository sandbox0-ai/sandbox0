import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const appRoot = path.resolve(__dirname, "..");
const outDir = path.join(appRoot, "out", "docs");
const buildConfigPath = path.join(appRoot, "src", "generated", "docs", "build-config.json");

async function main() {
  const renderedVersions = await readRenderedVersions();

  if (renderedVersions.length === 0) {
    throw new Error("expected at least one rendered docs version");
  }

  for (const version of renderedVersions) {
    await verifyVersion(version);
  }

  if (await hasLatestDocsOutput()) {
    await verifyLegacyRedirect("get-started.html", "/docs/latest/get-started");
  }

  console.log(`verified rendered docs HTML for versions: ${renderedVersions.join(", ")}`);
}

async function readRenderedVersions() {
  const raw = await fs.readFile(buildConfigPath, "utf8");
  const parsed = JSON.parse(raw);
  return Array.isArray(parsed.renderedVersions)
    ? parsed.renderedVersions.filter((value) => typeof value === "string" && value.length > 0)
    : [];
}

async function verifyVersion(version) {
  const getStartedHtml = await readOutputHtml(version, "get-started.html");
  const configHtml = await readOutputHtml(version, path.join("self-hosted", "configuration.html"));

  assertRenderedHtml({
    version,
    fileName: "get-started.html",
    html: getStartedHtml,
    forbiddenTags: ["S0Install", "Tabs", "DocLink"],
    requiredSnippets: [
      "GitHub Releases",
      'aria-label="Copy code"',
      `href="/docs/${version}/self-hosted"`,
    ],
  });

  assertRenderedHtml({
    version,
    fileName: "self-hosted/configuration.html",
    html: configHtml,
    forbiddenTags: ["Sandbox0InfraReference"],
    requiredSnippets: [
      "This reference is generated from the `Sandbox0Infra` CRD schema.",
      "<details",
      "<table",
    ],
  });
}

async function verifyLegacyRedirect(relativePath, redirectTarget) {
  const html = await readLegacyOutputHtml(relativePath);

  if (!html.includes(`content="0;url=${redirectTarget}"`)) {
    throw new Error(
      `legacy docs redirect ${relativePath} is missing meta refresh to ${redirectTarget}`
    );
  }

  if (!html.includes(`window.location.replace(${JSON.stringify(redirectTarget)})`)) {
    throw new Error(
      `legacy docs redirect ${relativePath} is missing script redirect to ${redirectTarget}`
    );
  }
}

async function readOutputHtml(version, relativePath) {
  const filePath = path.join(outDir, version, relativePath);
  try {
    return await fs.readFile(filePath, "utf8");
  } catch (error) {
    throw new Error(`expected rendered docs output at ${path.relative(appRoot, filePath)}: ${String(error)}`);
  }
}

async function readLegacyOutputHtml(relativePath) {
  const filePath = path.join(outDir, relativePath);
  try {
    return await fs.readFile(filePath, "utf8");
  } catch (error) {
    throw new Error(`expected legacy docs redirect at ${path.relative(appRoot, filePath)}: ${String(error)}`);
  }
}

async function hasLatestDocsOutput() {
  try {
    await fs.access(path.join(outDir, "latest"));
    return true;
  } catch {
    return false;
  }
}

function assertRenderedHtml({ version, fileName, html, forbiddenTags, requiredSnippets }) {
  for (const tagName of forbiddenTags) {
    if (html.includes(`<${tagName}`)) {
      throw new Error(
        `version ${version} still contains raw <${tagName}> markup in ${fileName}; custom MDX component was not rendered`
      );
    }
  }

  for (const snippet of requiredSnippets) {
    if (!html.includes(snippet)) {
      throw new Error(
        `version ${version} is missing expected rendered HTML snippet in ${fileName}: ${snippet}`
      );
    }
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
