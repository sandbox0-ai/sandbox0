import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { execFileSync } from "node:child_process";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const appRoot = path.resolve(__dirname, "..");
const outDir = path.join(appRoot, "out");
const generatedManifestPath = path.join(appRoot, "src", "generated", "docs", "versions.generated.json");
const githubRepo = process.env.DOCS_GITHUB_REPOSITORY || process.env.GITHUB_REPOSITORY || "sandbox0-ai/sandbox0";
const githubToken = process.env.GITHUB_TOKEN || process.env.GH_TOKEN || "";
const shouldDownloadBundles =
  process.env.DOCS_DOWNLOAD_RELEASE_BUNDLES === "1" || process.env.CF_PAGES === "1";

/**
 * @typedef {{
 *   id: string;
 *   label: string;
 *   channel: "stable" | "prerelease" | "next";
 *   target?: string;
 * }} DocsVersion
 *
 * @typedef {{
 *   defaultVersion: string;
 *   versions: DocsVersion[];
 * }} DocsVersionsManifest
 */

async function main() {
  const manifest = /** @type {DocsVersionsManifest} */ (
    JSON.parse(await fs.readFile(generatedManifestPath, "utf8"))
  );
  await fs.mkdir(outDir, { recursive: true });

  if (shouldDownloadBundles) {
    await hydrateReleaseDocsBundles(manifest);
  } else {
    console.log("skipping docs bundle hydration");
  }

  await writeLegacyDocsRedirects();
}

/**
 * @param {string} repo
 * @param {string} token
 * @returns {Promise<Array<{ tag_name: string; prerelease: boolean; draft: boolean; assets?: Array<{ name: string; browser_download_url: string }> }>>}
 */
async function fetchGithubReleases(repo, token) {
  const releases = [];

  for (let page = 1; page <= 5; page += 1) {
    const url = new URL(`https://api.github.com/repos/${repo}/releases`);
    url.searchParams.set("per_page", "100");
    url.searchParams.set("page", String(page));

    const response = await fetch(url, {
      headers: {
        Accept: "application/vnd.github+json",
        "User-Agent": "sandbox0-docs-bundle-fetcher",
        ...(token ? { Authorization: `Bearer ${token}` } : {}),
      },
    });

    if (!response.ok) {
      throw new Error(`GitHub API ${response.status}`);
    }

    const pageItems = await response.json();
    if (!Array.isArray(pageItems) || pageItems.length === 0) {
      break;
    }

    releases.push(...pageItems);
    if (pageItems.length < 100) {
      break;
    }
  }

  return releases;
}

/**
 * @param {DocsVersionsManifest} manifest
 * @returns {Promise<void>}
 */
async function hydrateReleaseDocsBundles(manifest) {
  const releases = await fetchGithubReleases(githubRepo, githubToken);
  const releaseAssetByTag = new Map();

  for (const release of releases) {
    if (!release || release.draft || typeof release.tag_name !== "string") {
      continue;
    }

    const asset = Array.isArray(release.assets)
      ? release.assets.find(
          (item) => item?.name === `sandbox0-docs-${release.tag_name}.tar.gz`
        )
      : null;

    if (asset?.browser_download_url) {
      releaseAssetByTag.set(release.tag_name, asset.browser_download_url);
    }
  }

  for (const version of manifest.versions) {
    if (!version.id.startsWith("v")) {
      continue;
    }

    const downloadUrl = releaseAssetByTag.get(version.id);
    if (!downloadUrl) {
      console.warn(`missing docs bundle asset for ${version.id}`);
      continue;
    }

    const archivePath = path.join(appRoot, `.tmp-${version.id}.tar.gz`);
    console.log(`downloading docs bundle for ${version.id}`);

    const response = await fetch(downloadUrl, {
      headers: {
        Accept: "application/octet-stream",
        "User-Agent": "sandbox0-docs-bundle-fetcher",
        ...(githubToken ? { Authorization: `Bearer ${githubToken}` } : {}),
      },
    });

    if (!response.ok) {
      throw new Error(`failed to download ${downloadUrl}: ${response.status}`);
    }

    const bytes = Buffer.from(await response.arrayBuffer());
    await fs.writeFile(archivePath, bytes);
    execFileSync("tar", ["-xzf", archivePath, "-C", outDir]);
    await fs.rm(archivePath, { force: true });
  }

  const latestTarget = manifest.versions.find((version) => version.id === "latest")?.target;
  if (latestTarget?.startsWith("v")) {
    const latestDir = path.join(outDir, "docs", "latest");
    const targetDir = path.join(outDir, "docs", latestTarget);
    const targetDirExists = await pathExists(targetDir);

    if (targetDirExists) {
      await fs.rm(latestDir, { recursive: true, force: true });
      await fs.cp(targetDir, latestDir, { recursive: true });
    } else {
      console.warn(
        `latest docs target ${latestTarget} is unavailable; keeping the generated latest docs fallback`
      );
    }
  }

  console.log("hydrated release docs bundles into out/");
}

async function writeLegacyDocsRedirects() {
  const latestDir = path.join(outDir, "docs", "latest");

  if (!(await pathExists(latestDir))) {
    console.warn("skipping legacy docs redirects because out/docs/latest is missing");
    return;
  }

  const htmlFiles = await collectHtmlFiles(latestDir);

  for (const filePath of htmlFiles) {
    const relativePath = path.relative(latestDir, filePath);
    const relativeHref = toPosixPath(relativePath).replace(/\.html$/, "");
    const legacyPath = path.join(outDir, "docs", relativePath);
    const redirectTarget = `/docs/latest/${relativeHref}`;

    await fs.mkdir(path.dirname(legacyPath), { recursive: true });
    await fs.writeFile(legacyPath, renderRedirectHtml(redirectTarget));
  }

  console.log(`generated ${htmlFiles.length} legacy docs redirects to /docs/latest`);
}

/**
 * @param {string} rootDir
 * @returns {Promise<string[]>}
 */
async function collectHtmlFiles(rootDir) {
  const entries = await fs.readdir(rootDir, { withFileTypes: true });
  const files = [];

  for (const entry of entries) {
    const entryPath = path.join(rootDir, entry.name);

    if (entry.isDirectory()) {
      files.push(...(await collectHtmlFiles(entryPath)));
      continue;
    }

    if (entry.isFile() && entry.name.endsWith(".html")) {
      files.push(entryPath);
    }
  }

  return files;
}

/**
 * @param {string} redirectTarget
 * @returns {string}
 */
function renderRedirectHtml(redirectTarget) {
  const escapedTarget = escapeHtml(redirectTarget);

  return `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <title>Redirecting...</title>
    <meta http-equiv="refresh" content="0;url=${escapedTarget}" />
    <link rel="canonical" href="${escapedTarget}" />
    <meta name="robots" content="noindex" />
    <script>window.location.replace(${JSON.stringify(redirectTarget)});</script>
  </head>
  <body>
    <p>Redirecting to <a href="${escapedTarget}">${escapedTarget}</a>.</p>
  </body>
</html>
`;
}

/**
 * @param {string} value
 * @returns {string}
 */
function escapeHtml(value) {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("\"", "&quot;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;");
}

/**
 * @param {string} filePath
 * @returns {string}
 */
function toPosixPath(filePath) {
  return filePath.split(path.sep).join("/");
}

/**
 * @param {string} filePath
 * @returns {Promise<boolean>}
 */
async function pathExists(filePath) {
  try {
    await fs.access(filePath);
    return true;
  } catch {
    return false;
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
