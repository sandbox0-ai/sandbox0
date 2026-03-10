import manifestJson from "../../../public/docs/versions.json";
import buildConfigJson from "@/generated/docs/build-config.json";

export interface DocsVersion {
  id: string;
  label: string;
  channel: "stable" | "prerelease" | "next";
  target?: string;
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
