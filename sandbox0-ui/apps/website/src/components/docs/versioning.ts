import manifestJson from "../../../public/docs/versions.json";
import buildConfigJson from "@/generated/docs/build-config.json";

export interface DocsVersion {
  id: string;
  label: string;
  channel: "stable" | "prerelease" | "next";
  target?: string;
  listed?: boolean;
}

export interface DocsVersionsManifest {
  defaultVersion: string;
  versions: DocsVersion[];
}

export const defaultDocsVersionsManifest =
  manifestJson as DocsVersionsManifest;

export const DOCS_DEFAULT_VERSION = defaultDocsVersionsManifest.defaultVersion;

const renderedDocsVersionIds = new Set(
  ((buildConfigJson as { renderedVersions?: string[] }).renderedVersions ?? [
    DOCS_DEFAULT_VERSION,
    "next",
  ]).filter(Boolean)
);

const DOCS_VERSION_PATTERN =
  /^v\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?$/;

function splitPathSuffix(href: string): {
  path: string;
  suffix: string;
} {
  const hashIndex = href.indexOf("#");
  const queryIndex = href.indexOf("?");
  const indexes = [hashIndex, queryIndex].filter((index) => index >= 0);
  const splitIndex = indexes.length > 0 ? Math.min(...indexes) : -1;

  if (splitIndex < 0) {
    return { path: href, suffix: "" };
  }

  return {
    path: href.slice(0, splitIndex),
    suffix: href.slice(splitIndex),
  };
}

export function isDocsVersionSegment(version: string): boolean {
  return version === "latest" || version === "next" || DOCS_VERSION_PATTERN.test(version);
}

export function isRenderedDocsVersion(version: string): boolean {
  return renderedDocsVersionIds.has(version);
}

export function getRenderedDocsVersions(): string[] {
  return [...renderedDocsVersionIds];
}

export function getDocsVersionFromPathname(pathname?: string | null): string | null {
  if (!pathname?.startsWith("/docs")) {
    return null;
  }

  const segments = pathname.split("/").filter(Boolean);
  const version = segments[1];
  return version && isDocsVersionSegment(version) ? version : null;
}

export function getResolvedDocsVersionFromPathname(pathname?: string | null): string {
  return getDocsVersionFromPathname(pathname) ?? DOCS_DEFAULT_VERSION;
}

function getDocsVersionEntry(version: string): DocsVersion | undefined {
  return defaultDocsVersionsManifest.versions.find((entry) => entry.id === version);
}

export function resolveDocsVersionTarget(version: string): string {
  const seen = new Set<string>();
  let current = version;

  while (!seen.has(current)) {
    seen.add(current);
    const target = getDocsVersionEntry(current)?.target;
    if (!target) {
      return current;
    }
    current = target;
  }

  return version;
}

export function resolveGitHubRefForDocsVersion(version: string): string {
  const target = resolveDocsVersionTarget(version);
  return target === "next" || target === "latest" ? "main" : target;
}

export function toGitHubRawHref(repo: string, version: string, filePath: string): string {
  const ref = resolveGitHubRefForDocsVersion(version);
  const normalizedPath = filePath.replace(/^\/+/, "");
  return `https://raw.githubusercontent.com/${repo}/${ref}/${normalizedPath}`;
}

export function toGitHubReleaseHref(repo: string, version: string): string {
  const ref = resolveGitHubRefForDocsVersion(version);
  if (DOCS_VERSION_PATTERN.test(ref)) {
    return `https://github.com/${repo}/releases/tag/${ref}`;
  }
  return `https://github.com/${repo}/releases/latest`;
}

export function toGitHubReadmeHref(
  repo: string,
  version: string,
  anchor?: string
): string {
  const ref = resolveGitHubRefForDocsVersion(version);
  const suffix = anchor ? `#${anchor}` : "";
  if (ref === "main") {
    return `https://github.com/${repo}${suffix}`;
  }
  return `https://github.com/${repo}/blob/${ref}/README.md${suffix}`;
}

export function getDocsContentPathFromPathname(pathname?: string | null): string {
  if (!pathname?.startsWith("/docs")) {
    return "/get-started";
  }

  const segments = pathname.split("/").filter(Boolean);
  const contentSegments = isDocsVersionSegment(segments[1] ?? "")
    ? segments.slice(2)
    : segments.slice(1);

  if (contentSegments.length === 0) {
    return "/get-started";
  }

  return `/${contentSegments.join("/")}`;
}

export function toVersionedDocsHref(version: string, href: string): string {
  if (!href.startsWith("/docs")) {
    return href;
  }

  const { path, suffix } = splitPathSuffix(href);

  if (path === "/docs") {
    return `/docs/${version}${suffix}`;
  }

  const pathSegments = path.split("/").filter(Boolean);
  const docsSegments = pathSegments.slice(1);

  if (docsSegments.length > 0 && isDocsVersionSegment(docsSegments[0])) {
    return `${path}${suffix}`;
  }

  return `/docs/${version}/${docsSegments.join("/")}${suffix}`;
}

export function toLatestDocsRedirect(pathname?: string | null): string | null {
  if (!pathname?.startsWith("/docs")) {
    return null;
  }

  const { path, suffix } = splitPathSuffix(pathname);
  const segments = path.split("/").filter(Boolean);

  if (segments.length <= 1) {
    return `/docs/${DOCS_DEFAULT_VERSION}/get-started${suffix}`;
  }

  if (isDocsVersionSegment(segments[1])) {
    return null;
  }

  return `/docs/${DOCS_DEFAULT_VERSION}/${segments.slice(1).join("/")}${suffix}`;
}
