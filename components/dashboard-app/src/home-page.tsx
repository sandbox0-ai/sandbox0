import Image from "next/image";
import {
  PixelBadge,
  PixelButton,
  PixelCard,
  PixelLayout,
} from "@sandbox0/ui";

import {
  requireDashboardHomeRender,
  type DashboardConfigResolver,
  type DashboardPageSearchParams,
} from "./internal/auth-pages";
import type { DashboardSession } from "./internal/types";

export interface DashboardHomePageOptions {
  brandName?: string;
  footerText?: string;
}

function formatRelativeTime(isoString: string): string {
  const date = new Date(isoString);
  const now = new Date();
  const diffMs = now.getTime() - date.getTime();
  const diffMins = Math.floor(diffMs / 60000);
  const diffHours = Math.floor(diffMins / 60);
  const diffDays = Math.floor(diffHours / 24);

  if (diffMins < 1) return "just now";
  if (diffMins < 60) return `${diffMins}m ago`;
  if (diffHours < 24) return `${diffHours}h ago`;
  return `${diffDays}d ago`;
}

function SandboxStatusBadge({ status, paused }: { status: string; paused: boolean }) {
  if (paused) {
    return <PixelBadge variant="warning">Paused</PixelBadge>;
  }
  if (status === "running") {
    return <PixelBadge variant="success">Running</PixelBadge>;
  }
  if (status === "stopped") {
    return <PixelBadge variant="danger">Stopped</PixelBadge>;
  }
  return <PixelBadge>{status}</PixelBadge>;
}

function DashboardHomeView({
  session,
  brandName,
  footerText,
}: Required<DashboardHomePageOptions> & { session: DashboardSession }) {
  const runningSandboxes = session.sandboxes.filter(
    (s) => !s.paused && s.status === "running",
  ).length;

  return (
    <PixelLayout>
      <header className="flex items-center justify-between border-b border-foreground/10 p-4">
        <div className="flex items-center gap-3">
          <Image
            src="/sandbox0.png"
            alt={brandName}
            width={32}
            height={32}
            className="pixel-art"
            data-pixel
          />
          <h1 className="font-pixel text-sm">{brandName}</h1>
        </div>
        <nav className="flex gap-4">
          <a href="/">
            <PixelButton variant="primary" scale="sm">
              Overview
            </PixelButton>
          </a>
          <a href="/volumes">
            <PixelButton variant="secondary" scale="sm">
              Volumes
            </PixelButton>
          </a>
        </nav>
        <div className="flex items-center gap-3">
          {session.user && (
            <span className="text-xs text-muted font-mono">{session.user.email}</span>
          )}
          <form action="/api/auth/logout" method="post">
            <PixelButton variant="secondary" scale="sm" type="submit">
              Logout
            </PixelButton>
          </form>
        </div>
      </header>

      <main className="flex-1 p-6">
        <div className="mb-8">
          <h2 className="mb-1 font-pixel text-lg">
            Welcome back{session.user?.name ? `, ${session.user.name}` : ""}
          </h2>
          <p className="text-muted text-sm">
            {session.mode === "global-gateway" ? "Global Gateway" : "Single Cluster"} ·{" "}
            {session.configuredRegionalURL ?? session.configuredGlobalURL}
          </p>
        </div>

        <div className="mb-8 grid grid-cols-1 gap-6 md:grid-cols-3">
          <PixelCard header="Sandboxes" interactive accent>
            <p className="mb-2 text-4xl font-pixel text-accent">{runningSandboxes}</p>
            <p className="mb-4 text-sm text-muted">Running instances</p>
            <a href="/volumes">
              <PixelButton variant="primary" scale="sm">
                Manage Volumes
              </PixelButton>
            </a>
          </PixelCard>

          <PixelCard header="Templates" interactive>
            <p className="mb-2 text-4xl font-pixel">{session.templates.length}</p>
            <p className="text-sm text-muted">Available templates</p>
          </PixelCard>

          <PixelCard header="Volumes" interactive>
            <p className="mb-2 text-4xl font-pixel">{session.volumes.length}</p>
            <p className="text-sm text-muted">Persistent volumes</p>
          </PixelCard>
        </div>

        {session.sandboxes.length > 0 ? (
          <div className="space-y-4">
            <h3 className="mb-4 font-pixel text-sm">Recent Sandboxes</h3>
            {session.sandboxes.map((sandbox) => (
              <PixelCard key={sandbox.id} scale="sm" interactive>
                <div className="flex items-center justify-between">
                  <div>
                    <p className="font-mono text-sm">{sandbox.id}</p>
                    <p className="text-xs text-muted">
                      Template: {sandbox.templateID} ·{" "}
                      {formatRelativeTime(sandbox.createdAt)}
                    </p>
                  </div>
                  <div className="flex items-center gap-3">
                    <SandboxStatusBadge
                      status={sandbox.status}
                      paused={sandbox.paused}
                    />
                  </div>
                </div>
              </PixelCard>
            ))}
          </div>
        ) : (
          <PixelCard header="No Sandboxes">
            <p className="text-sm text-muted">
              No sandboxes found. Create one from a template to get started.
            </p>
          </PixelCard>
        )}

        {session.templates.length > 0 && (
          <div className="mt-8 space-y-4">
            <h3 className="mb-4 font-pixel text-sm">Available Templates</h3>
            <div className="grid grid-cols-1 gap-4 md:grid-cols-2 lg:grid-cols-3">
              {session.templates.map((template) => (
                <PixelCard key={template.templateID} scale="sm" interactive>
                  <p className="font-mono text-sm">{template.templateID}</p>
                  <p className="text-xs text-muted mt-1">
                    {template.scope} · {formatRelativeTime(template.createdAt)}
                  </p>
                </PixelCard>
              ))}
            </div>
          </div>
        )}
      </main>

      <footer className="border-t border-foreground/10 p-4 text-center text-xs text-muted">
        {footerText}
      </footer>
    </PixelLayout>
  );
}

export function createDashboardHomePage(
  resolveConfig: DashboardConfigResolver,
  options?: DashboardHomePageOptions,
) {
  const resolvedOptions: Required<DashboardHomePageOptions> = {
    brandName: options?.brandName ?? "SANDBOX0",
    footerText: options?.footerText ?? "Sandbox0 Dashboard v0.0.1",
  };

  return async function DashboardHomePage({
    searchParams,
  }: DashboardPageSearchParams) {
    const session = await requireDashboardHomeRender(resolveConfig, searchParams);
    return <DashboardHomeView session={session} {...resolvedOptions} />;
  };
}
