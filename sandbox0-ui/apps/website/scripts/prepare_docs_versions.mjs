import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const appRoot = path.resolve(__dirname, "..");
const publicManifestPath = path.join(appRoot, "public", "docs", "versions.json");
const generatedDir = path.join(appRoot, "src", "generated", "docs");
const generatedManifestPath = path.join(generatedDir, "versions.generated.json");
const buildConfigPath = path.join(generatedDir, "build-config.json");
const githubRepo = process.env.DOCS_GITHUB_REPOSITORY || process.env.GITHUB_REPOSITORY || "sandbox0-ai/sandbox0";
const githubToken = process.env.GITHUB_TOKEN || process.env.GH_TOKEN || "";
const explicitRenderVersions = parseList(process.env.DOCS_BUILD_VERSIONS);
const listedReleaseLimit = parsePositiveInt(process.env.DOCS_VERSION_SWITCHER_LIMIT, 12);

/**
 * @typedef {"stable" | "prerelease" | "next"} DocsVersionChannel
 *
 * @typedef {{
 *   id: string;
 *   label: string;
 *   channel: DocsVersionChannel;
 *   target?: string;
 *   listed?: boolean;
 * }} DocsVersion
 *
 * @typedef {{
 *   defaultVersion: string;
 *   versions: DocsVersion[];
 * }} DocsVersionsManifest
 */

async function main() {
  const cachedManifest = await readCachedManifest(generatedManifestPath);
  const releases = await fetchGithubReleases(githubRepo, githubToken);
  const manifest = buildManifest(releases, cachedManifest);
  const renderVersions = resolveRenderVersions(manifest, explicitRenderVersions);
  const manifestJson = `${JSON.stringify(manifest, null, 2)}\n`;

  await fs.mkdir(path.dirname(publicManifestPath), { recursive: true });
  await fs.writeFile(publicManifestPath, manifestJson);

  await fs.mkdir(generatedDir, { recursive: true });
  await fs.writeFile(generatedManifestPath, manifestJson);
  await fs.writeFile(
    buildConfigPath,
    `${JSON.stringify({ renderedVersions: renderVersions }, null, 2)}\n`
  );

  console.log(
    `prepared docs manifest with ${manifest.versions.length} versions; rendered versions: ${renderVersions.join(", ")}`
  );
}

/**
 * @param {string} filePath
 * @returns {Promise<DocsVersionsManifest | null>}
 */
async function readCachedManifest(filePath) {
  try {
    const raw = await fs.readFile(filePath, "utf8");
    const parsed = JSON.parse(raw);
    return isDocsVersionsManifest(parsed) ? parsed : null;
  } catch {
    return null;
  }
}

/**
 * @param {string | undefined} value
 * @returns {string[]}
 */
function parseList(value) {
  if (!value) {
    return [];
  }

  return [...new Set(value.split(",").map((item) => item.trim()).filter(Boolean))];
}

/**
 * @param {string | undefined} value
 * @param {number} fallback
 * @returns {number}
 */
function parsePositiveInt(value, fallback) {
  const parsed = Number.parseInt(value ?? "", 10);
  return Number.isInteger(parsed) && parsed > 0 ? parsed : fallback;
}

/**
 * @param {string} repo
 * @param {string} token
 * @returns {Promise<Array<{ tag_name: string; prerelease: boolean; draft: boolean; assets?: Array<{ name?: string }> }>>}
 */
async function fetchGithubReleases(repo, token) {
  const releases = [];

  for (let page = 1; page <= 5; page += 1) {
    const url = new URL(`https://api.github.com/repos/${repo}/releases`);
    url.searchParams.set("per_page", "100");
    url.searchParams.set("page", String(page));

    const headers = {
      Accept: "application/vnd.github+json",
      "User-Agent": "sandbox0-docs-versioning",
    };

    if (token) {
      headers.Authorization = `Bearer ${token}`;
    }

    try {
      const response = await fetch(url, { headers });
      if (!response.ok) {
        throw new Error(`GitHub API ${response.status}`);
      }

      const pageItems = await response.json();
      if (!Array.isArray(pageItems) || pageItems.length === 0) {
        break;
      }

      for (const item of pageItems) {
        if (!item || item.draft || typeof item.tag_name !== "string") {
          continue;
        }
        releases.push(item);
      }

      if (pageItems.length < 100) {
        break;
      }
    } catch (error) {
      console.warn(`failed to fetch GitHub releases for ${repo}: ${String(error)}`);
      return [];
    }
  }

  return releases;
}

/**
 * @param {Array<{ tag_name: string; prerelease: boolean; assets?: Array<{ name?: string }> }>} releases
 * @param {DocsVersionsManifest | null} cachedManifest
 * @returns {DocsVersionsManifest}
 */
function buildManifest(releases, cachedManifest) {
  const includePrerelease = process.env.DOCS_INCLUDE_PRERELEASE === "true";
  const actualVersions = releases
    .map((release) => {
      if (release.prerelease && !includePrerelease) {
        return null;
      }

      const id = normalizeReleaseTag(release.tag_name);
      if (!id || !hasDocsBundleAsset(release)) {
        return null;
      }

      return {
        id,
        label: id,
        channel: release.prerelease ? "prerelease" : "stable",
      };
    })
    .filter(Boolean)
    .sort(compareDocsVersionDesc);

  const includeNext =
    process.env.DOCS_INCLUDE_NEXT === "false"
      ? false
      : process.env.DOCS_INCLUDE_NEXT === "true"
        ? true
        : true;

  if (actualVersions.length === 0) {
    return cachedManifest ?? createFallbackManifest(includeNext);
  }

  actualVersions.forEach((version, index) => {
    version.listed = index < listedReleaseLimit;
  });

  const latestStable = actualVersions.find((version) => version.channel === "stable");

  const versions = [
    {
      id: "latest",
      label: "Latest",
      channel: "stable",
      target: latestStable?.id ?? (includeNext ? "next" : "latest"),
      listed: true,
    },
    ...(includeNext
      ? [
          {
            id: "next",
            label: "Next",
            channel: "next",
            listed: true,
          },
        ]
      : []),
    ...actualVersions,
  ];

  return {
    defaultVersion: "latest",
    versions,
  };
}

/**
 * @param {unknown} value
 * @returns {value is DocsVersionsManifest}
 */
function isDocsVersionsManifest(value) {
  if (!value || typeof value !== "object") {
    return false;
  }

  const candidate = /** @type {{ defaultVersion?: unknown; versions?: unknown }} */ (value);
  return typeof candidate.defaultVersion === "string" && Array.isArray(candidate.versions);
}

/**
 * @param {boolean} includeNext
 * @returns {DocsVersionsManifest}
 */
function createFallbackManifest(includeNext) {
  return {
    defaultVersion: "latest",
    versions: [
      {
        id: "latest",
        label: "Latest",
        channel: "stable",
        target: includeNext ? "next" : "latest",
        listed: true,
      },
      ...(includeNext
        ? [
            {
              id: "next",
              label: "Next",
              channel: "next",
              listed: true,
            },
          ]
        : []),
    ],
  };
}

/**
 * @param {{ tag_name: string; assets?: Array<{ name?: string }> }} release
 * @returns {boolean}
 */
function hasDocsBundleAsset(release) {
  const expectedAssetName = `sandbox0-docs-${release.tag_name}.tar.gz`;
  return Array.isArray(release.assets)
    ? release.assets.some((asset) => asset?.name === expectedAssetName)
    : false;
}

/**
 * @param {DocsVersionsManifest} manifest
 * @param {string[]} configured
 * @returns {string[]}
 */
function resolveRenderVersions(manifest, configured) {
  if (configured.length > 0) {
    return configured;
  }

  if (process.env.CF_PAGES === "1") {
    return ["latest", "next"];
  }

  return manifest.versions
    .map((version) => version.id)
    .filter((id) => id === "latest" || id === "next");
}

/**
 * @param {string} tag
 * @returns {string | null}
 */
function normalizeReleaseTag(tag) {
  if (!/^v\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?$/.test(tag)) {
    return null;
  }
  return tag;
}

/**
 * @param {DocsVersion} left
 * @param {DocsVersion} right
 */
function compareDocsVersionDesc(left, right) {
  return compareSemverDesc(left.id, right.id);
}

/**
 * @param {string} left
 * @param {string} right
 */
function compareSemverDesc(left, right) {
  const leftParsed = parseSemver(left);
  const rightParsed = parseSemver(right);

  for (const key of ["major", "minor", "patch"]) {
    if (leftParsed[key] !== rightParsed[key]) {
      return rightParsed[key] - leftParsed[key];
    }
  }

  if (leftParsed.prerelease.length === 0 && rightParsed.prerelease.length > 0) {
    return -1;
  }

  if (leftParsed.prerelease.length > 0 && rightParsed.prerelease.length === 0) {
    return 1;
  }

  const length = Math.max(leftParsed.prerelease.length, rightParsed.prerelease.length);
  for (let index = 0; index < length; index += 1) {
    const leftPart = leftParsed.prerelease[index];
    const rightPart = rightParsed.prerelease[index];

    if (leftPart === undefined) {
      return -1;
    }
    if (rightPart === undefined) {
      return 1;
    }

    const leftNumber = Number(leftPart);
    const rightNumber = Number(rightPart);
    const leftIsNumber = Number.isInteger(leftNumber) && String(leftNumber) === leftPart;
    const rightIsNumber = Number.isInteger(rightNumber) && String(rightNumber) === rightPart;

    if (leftIsNumber && rightIsNumber && leftNumber !== rightNumber) {
      return rightNumber - leftNumber;
    }

    if (leftIsNumber !== rightIsNumber) {
      return leftIsNumber ? -1 : 1;
    }

    if (leftPart !== rightPart) {
      return rightPart.localeCompare(leftPart);
    }
  }

  return 0;
}

/**
 * @param {string} version
 */
function parseSemver(version) {
  const cleaned = version.startsWith("v") ? version.slice(1) : version;
  const [core, buildMetadata] = cleaned.split("+", 2);
  void buildMetadata;
  const [base, prerelease = ""] = core.split("-", 2);
  const [major = "0", minor = "0", patch = "0"] = base.split(".");

  return {
    major: Number(major) || 0,
    minor: Number(minor) || 0,
    patch: Number(patch) || 0,
    prerelease: prerelease ? prerelease.split(".") : [],
  };
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
