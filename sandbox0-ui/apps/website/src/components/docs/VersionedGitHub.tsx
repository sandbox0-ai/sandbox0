"use client";

import { usePathname } from "next/navigation";
import { PixelCodeBlock } from "./PixelCodeBlock";
import { DocsLink } from "./DocsLink";
import {
  getResolvedDocsVersionFromPathname,
  toGitHubRawHref,
  toGitHubReadmeHref,
  toGitHubReleaseHref,
} from "./versioning";

type RepoName = "sandbox0" | "s0";

function repoSlug(repo: RepoName): string {
  return `sandbox0-ai/${repo}`;
}

export function GitHubRawLink({
  repo,
  path,
  children,
}: {
  repo: RepoName;
  path: string;
  children: React.ReactNode;
}) {
  const pathname = usePathname();
  const version = getResolvedDocsVersionFromPathname(pathname);
  const href = toGitHubRawHref(repoSlug(repo), version, path);

  return (
    <DocsLink href={href} newTab>
      {children}
    </DocsLink>
  );
}

export function GitHubApplyCommand({
  repo,
  path,
}: {
  repo: RepoName;
  path: string;
}) {
  const pathname = usePathname();
  const version = getResolvedDocsVersionFromPathname(pathname);
  const href = toGitHubRawHref(repoSlug(repo), version, path);

  return (
    <PixelCodeBlock language="bash" scale="md" className="mb-6">
      {`kubectl apply -f ${href}`}
    </PixelCodeBlock>
  );
}

export function S0Install() {
  const pathname = usePathname();
  const version = getResolvedDocsVersionFromPathname(pathname);
  const shellInstallHref = toGitHubRawHref(repoSlug("s0"), version, "scripts/install.sh");
  const powershellInstallHref = toGitHubRawHref(repoSlug("s0"), version, "scripts/install.ps1");
  const releaseHref = toGitHubReleaseHref(repoSlug("s0"), version);
  const readmeHref = toGitHubReadmeHref(repoSlug("s0"), version, "installation");

  return (
    <>
      <p className="mb-4 leading-relaxed text-muted">macOS and Linux:</p>
      <PixelCodeBlock language="bash" scale="md" className="mb-6">
        {`curl -fsSL ${shellInstallHref} | bash`}
      </PixelCodeBlock>

      <p className="mb-4 leading-relaxed text-muted">Windows PowerShell:</p>
      <PixelCodeBlock language="powershell" scale="md" className="mb-6">
        {`irm ${powershellInstallHref} | iex`}
      </PixelCodeBlock>

      <p className="mb-4 leading-relaxed text-muted">Or with Go:</p>
      <PixelCodeBlock language="bash" scale="md" className="mb-6">
        {"go install github.com/sandbox0-ai/s0/cmd/s0@latest"}
      </PixelCodeBlock>

      <p className="mb-4 leading-relaxed text-muted">
        Manual release archives are available from{" "}
        <DocsLink href={releaseHref} newTab>
          GitHub Releases
        </DocsLink>
        . See the{" "}
        <DocsLink href={readmeHref} newTab>
          s0 README
        </DocsLink>{" "}
        for platform-specific manual install steps.
      </p>
    </>
  );
}
