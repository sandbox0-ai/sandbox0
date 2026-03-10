import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { execFileSync } from "node:child_process";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const appRoot = path.resolve(__dirname, "..");
const outDir = path.join(appRoot, "out");
const publicManifestPath = path.join(appRoot, "public", "docs", "versions.json");
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
  if (!shouldDownloadBundles) {
    console.log("skipping docs bundle hydration");
    return;
  }

  const manifest = /** @type {DocsVersionsManifest} */ (
    JSON.parse(await fs.readFile(publicManifestPath, "utf8"))
  );
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

  await fs.mkdir(outDir, { recursive: true });

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

    await fs.rm(latestDir, { recursive: true, force: true });
    await fs.cp(targetDir, latestDir, { recursive: true });
  }

  console.log("hydrated release docs bundles into out/");
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

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
