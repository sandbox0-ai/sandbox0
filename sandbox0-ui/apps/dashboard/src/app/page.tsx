import { cookies, headers } from "next/headers";
import Image from "next/image";
import { redirect } from "next/navigation";
import {
  PixelBadge,
  PixelCallout,
  PixelCard,
  PixelHeading,
  PixelLayout,
} from "@sandbox0/ui";

import {
  dashboardRefreshTokenCookieName,
  readBearerToken,
  resolveDashboardAuthProviders,
  resolveDashboardRuntimeConfig,
  resolveDashboardSession,
} from "@sandbox0/dashboard-core";

export const dynamic = "force-dynamic";

function formatMode(mode: string) {
  return mode === "global-directory" ? "Global Directory" : "Single Cluster";
}

function formatTimestamp(value: string) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return new Intl.DateTimeFormat("en", {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  }).format(date);
}

export default async function DashboardHome({
  searchParams,
}: {
  searchParams?: Promise<{
    login_error?: string;
    refreshed?: string;
    team_switched?: string;
  }>;
}) {
  const config = resolveDashboardRuntimeConfig();
  const headerStore = await headers();
  const cookieStore = await cookies();
  const token = readBearerToken(headerStore.get("authorization"), cookieStore);
  const session = await resolveDashboardSession(config, { bearerToken: token });
  const authProviders = await resolveDashboardAuthProviders(config);
  const params = searchParams ? await searchParams : undefined;
  const loginError = params?.login_error;
  const refreshed = params?.refreshed === "1";
  const teamSwitched = params?.team_switched === "1";
  const refreshToken = cookieStore.get(dashboardRefreshTokenCookieName)?.value;
  const oidcProviders = authProviders.providers.filter(
    (provider) => provider.type === "oidc",
  );
  const builtinProvider = authProviders.providers.find(
    (provider) => provider.type === "builtin",
  );

  if (!session.authenticated && refreshToken && !refreshed && !loginError) {
    redirect("/api/auth/refresh");
  }

  return (
    <PixelLayout scanlines>
      <header className="border-b border-foreground/10 bg-background/95 backdrop-blur">
        <div className="mx-auto flex w-full max-w-6xl items-center justify-between gap-4 px-4 py-4">
          <a href={session.siteURL} className="flex items-center gap-3">
            <Image
              src="/sandbox0.png"
              alt="Sandbox0"
              width={32}
              height={32}
              className="pixel-art"
              data-pixel
            />
            <div>
              <div className="font-pixel text-xs tracking-tight">SANDBOX0</div>
              <div className="text-[10px] uppercase tracking-[0.24em] text-muted">
                Dashboard
              </div>
            </div>
          </a>

          <div className="flex items-center gap-3">
            <PixelBadge variant="accent">{formatMode(session.mode)}</PixelBadge>
            <a
              href={`${session.siteURL}/docs/latest/get-started`}
              className="inline-flex items-center justify-center border border-foreground/20 bg-surface px-3 py-1.5 text-xs text-foreground transition-colors hover:bg-foreground hover:text-background"
            >
              Docs
            </a>
          </div>
        </div>
      </header>

      <main className="mx-auto flex w-full max-w-6xl flex-1 flex-col gap-6 px-4 py-8">
        <section className="grid gap-4 lg:grid-cols-[minmax(0,1.2fr)_minmax(280px,0.8fr)]">
          <PixelCard accent>
            <PixelHeading as="h1" tone="site" className="text-2xl md:text-4xl">
              Unified <span className="text-accent">control plane</span>
            </PixelHeading>
            <p className="mt-4 max-w-2xl text-sm leading-7 text-muted">
              This dashboard runs as a dedicated control-plane site while
              adapting to either a single-cluster deployment or a
              global-directory plus regional-gateway topology.
            </p>
            <div className="mt-6 flex flex-wrap gap-3">
              <a
                href="/api/session"
                className="inline-flex items-center justify-center border border-foreground/20 bg-surface px-3 py-1.5 text-xs text-foreground transition-colors hover:bg-foreground hover:text-background"
              >
                View session JSON
              </a>
            </div>
          </PixelCard>

          <PixelCard header="Control Plane" scale="md">
            <dl className="space-y-3 text-sm">
              <div className="flex items-center justify-between gap-4">
                <dt className="text-muted">Mode</dt>
                <dd className="font-mono">{session.mode}</dd>
              </div>
              <div className="flex items-center justify-between gap-4">
                <dt className="text-muted">Global</dt>
                <dd className="max-w-[18rem] truncate font-mono text-xs">
                  {session.configuredGlobalURL ?? "not used"}
                </dd>
              </div>
              <div className="flex items-center justify-between gap-4">
                <dt className="text-muted">Regional</dt>
                <dd className="max-w-[18rem] truncate font-mono text-xs">
                  {session.configuredRegionalURL ?? "pending"}
                </dd>
              </div>
              <div className="flex items-center justify-between gap-4">
                <dt className="text-muted">Auth</dt>
                <dd>
                  {session.authenticated
                    ? "authenticated"
                    : "not authenticated"}
                </dd>
              </div>
            </dl>
          </PixelCard>
        </section>

        {!session.authenticated && (
          <section className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_minmax(320px,0.9fr)]">
            <PixelCard header="Sign In" accent>
              <div className="space-y-4 text-sm">
                <p className="leading-7 text-muted">
                  The dashboard does not own an OIDC login screen. When an
                  external identity provider is configured, sign-in is delegated
                  to the upstream control plane and the dashboard restores a
                  cookie-backed browser session after the callback completes.
                </p>

                {oidcProviders.length > 0 && (
                  <div className="space-y-3">
                    {oidcProviders.map((provider) => (
                      <a
                        key={provider.id}
                        href={`/api/auth/oidc/${provider.id}/login`}
                        className="inline-flex w-full items-center justify-center border border-foreground/20 bg-surface px-4 py-3 text-sm text-foreground transition-colors hover:bg-foreground hover:text-background"
                      >
                        Continue with {provider.name}
                      </a>
                    ))}
                  </div>
                )}

                {builtinProvider && (
                  <form
                    action="/api/auth/login"
                    method="post"
                    className="space-y-3 border border-foreground/10 bg-surface/60 p-4"
                  >
                    <div className="space-y-1">
                      <label
                        htmlFor="email"
                        className="text-xs uppercase tracking-[0.18em] text-muted"
                      >
                        Email
                      </label>
                      <input
                        id="email"
                        name="email"
                        type="email"
                        autoComplete="email"
                        required
                        className="w-full border border-foreground/20 bg-background px-3 py-2 text-sm text-foreground outline-none transition-colors focus:border-accent"
                      />
                    </div>

                    <div className="space-y-1">
                      <label
                        htmlFor="password"
                        className="text-xs uppercase tracking-[0.18em] text-muted"
                      >
                        Password
                      </label>
                      <input
                        id="password"
                        name="password"
                        type="password"
                        autoComplete="current-password"
                        required
                        className="w-full border border-foreground/20 bg-background px-3 py-2 text-sm text-foreground outline-none transition-colors focus:border-accent"
                      />
                    </div>

                    <button
                      type="submit"
                      className="inline-flex w-full items-center justify-center border border-accent bg-accent px-4 py-3 text-sm text-background transition-colors hover:border-foreground hover:bg-foreground"
                    >
                      Sign in with email and password
                    </button>
                  </form>
                )}

                {!builtinProvider && oidcProviders.length === 0 && (
                  <PixelCallout
                    title="No auth providers available"
                    type="warning"
                  >
                    The configured control plane did not expose any browser
                    sign-in providers.
                  </PixelCallout>
                )}
              </div>
            </PixelCard>

            <PixelCard header="Session Model">
              <ul className="space-y-3 text-sm text-muted">
                <li>
                  OIDC providers redirect the browser to the configured IdP
                  login page, such as Auth0.
                </li>
                <li>
                  Built-in email/password is rendered only when the upstream
                  control plane exposes the builtin provider.
                </li>
                <li>
                  Successful login is converted into HttpOnly dashboard cookies
                  before the page resolves the active team and regional
                  workspace.
                </li>
              </ul>
            </PixelCard>
          </section>
        )}

        {session.errors.length > 0 && (
          <PixelCallout title="Session resolution warning" type="warning">
            {session.errors.join(" ")}
          </PixelCallout>
        )}

        {authProviders.errors.length > 0 && (
          <PixelCallout title="Auth provider warning" type="warning">
            {authProviders.errors.join(" ")}
          </PixelCallout>
        )}

        {loginError && (
          <PixelCallout title="Login failed" type="warning">
            {loginError}
          </PixelCallout>
        )}

        {teamSwitched && (
          <PixelCallout title="Active team updated">
            The dashboard refreshed the browser session and re-resolved the
            active regional workspace for the selected team.
          </PixelCallout>
        )}

        <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
          <PixelCard header="Current User">
            <div className="space-y-2 text-sm">
              <p className="font-mono text-foreground">
                {session.user?.email ?? "Unauthenticated"}
              </p>
              <p className="text-muted">
                {session.user?.name ?? "No active browser session"}
              </p>
            </div>
          </PixelCard>

          <PixelCard header="Teams">
            <div className="space-y-2 text-sm">
              <p className="font-pixel text-3xl text-accent">
                {session.teams.length}
              </p>
              <p className="text-muted">
                {session.activeTeam?.teamID
                  ? `Active team ${session.activeTeam.teamID}`
                  : "Active team pending"}
              </p>
              {session.authenticated && session.teams.length > 1 && (
                <form
                  action="/api/team/select"
                  method="post"
                  className="pt-2"
                >
                  <label
                    htmlFor="team_id"
                    className="mb-1 block text-[10px] uppercase tracking-[0.18em] text-muted"
                  >
                    Switch active team
                  </label>
                  <div className="flex gap-2">
                    <select
                      id="team_id"
                      name="team_id"
                      defaultValue={session.activeTeam?.teamID}
                      className="min-w-0 flex-1 border border-foreground/20 bg-background px-3 py-2 text-sm text-foreground outline-none transition-colors focus:border-accent"
                    >
                      {session.teams.map((team) => (
                        <option key={team.id} value={team.id}>
                          {team.name}
                          {team.homeRegionID ? ` · ${team.homeRegionID}` : ""}
                        </option>
                      ))}
                    </select>
                    <button
                      type="submit"
                      className="inline-flex items-center justify-center border border-foreground/20 bg-surface px-3 py-2 text-xs text-foreground transition-colors hover:bg-foreground hover:text-background"
                    >
                      Switch
                    </button>
                  </div>
                </form>
              )}
            </div>
          </PixelCard>

          <PixelCard header="Recent Sandboxes">
            <div className="space-y-2 text-sm">
              <p className="font-pixel text-3xl text-accent">
                {session.sandboxes.length}
              </p>
              <p className="text-muted">
                Topology-aware summary from the resolved resource control plane.
              </p>
            </div>
          </PixelCard>

          <PixelCard header="Templates">
            <div className="space-y-2 text-sm">
              <p className="font-pixel text-3xl text-accent">
                {session.templates.length}
              </p>
              <p className="text-muted">
                Available templates from the current control plane.
              </p>
            </div>
          </PixelCard>
        </section>

        <section className="grid gap-4 lg:grid-cols-[minmax(0,1.05fr)_minmax(0,0.95fr)]">
          <PixelCard header="Resolved Workspace" accent>
            <dl className="grid gap-3 text-sm md:grid-cols-2">
              <div>
                <dt className="text-xs uppercase tracking-[0.18em] text-muted">
                  Team
                </dt>
                <dd className="mt-1 font-mono">
                  {session.activeTeam?.teamID ?? "pending"}
                </dd>
              </div>
              <div>
                <dt className="text-xs uppercase tracking-[0.18em] text-muted">
                  Region
                </dt>
                <dd className="mt-1 font-mono">
                  {session.activeTeam?.homeRegionID ?? "pending"}
                </dd>
              </div>
              <div>
                <dt className="text-xs uppercase tracking-[0.18em] text-muted">
                  Regional URL
                </dt>
                <dd className="mt-1 truncate font-mono text-xs">
                  {session.configuredRegionalURL ?? "pending"}
                </dd>
              </div>
              <div>
                <dt className="text-xs uppercase tracking-[0.18em] text-muted">
                  Site URL
                </dt>
                <dd className="mt-1 truncate font-mono text-xs">
                  {session.siteURL}
                </dd>
              </div>
            </dl>
          </PixelCard>

          <PixelCard header="Next Implementation Slice">
            <ul className="space-y-3 text-sm text-muted">
              <li>
                Replace summary cards with real team switch and sandbox list
                actions.
              </li>
              <li>
                Send interactive terminal and websocket flows directly to the
                resolved regional gateway.
              </li>
              <li>
                Introduce refresh-token based browser session renewal before
                access token expiry.
              </li>
            </ul>

            {session.authenticated && (
              <form
                action="/api/auth/logout"
                method="post"
                className="mt-6"
              >
                <button
                  type="submit"
                  className="inline-flex items-center justify-center border border-foreground/20 bg-surface px-3 py-1.5 text-xs text-foreground transition-colors hover:bg-foreground hover:text-background"
                >
                  Sign out
                </button>
              </form>
            )}
          </PixelCard>
        </section>

        <section className="grid gap-4 lg:grid-cols-2">
          <PixelCard header="Latest Sandboxes">
            <div className="space-y-3">
              {session.sandboxes.length === 0 ? (
                <p className="text-sm text-muted">No sandbox data yet.</p>
              ) : (
                session.sandboxes.map((sandbox) => (
                  <div
                    key={sandbox.id}
                    className="flex items-center justify-between gap-4 border-b border-foreground/10 pb-3 last:border-0 last:pb-0"
                  >
                    <div>
                      <div className="font-mono text-sm">{sandbox.id}</div>
                      <div className="text-xs text-muted">
                        Template {sandbox.templateID} ·{" "}
                        {formatTimestamp(sandbox.createdAt)}
                      </div>
                    </div>
                    <div className="flex items-center gap-2">
                      {sandbox.paused && <PixelBadge>paused</PixelBadge>}
                      <PixelBadge
                        variant={
                          sandbox.status === "running" ? "accent" : "default"
                        }
                      >
                        {sandbox.status}
                      </PixelBadge>
                    </div>
                  </div>
                ))
              )}
            </div>
          </PixelCard>

          <PixelCard header="Available Templates">
            <div className="space-y-3">
              {session.templates.length === 0 ? (
                <p className="text-sm text-muted">No template data yet.</p>
              ) : (
                session.templates.slice(0, 6).map((template) => (
                  <div
                    key={template.templateID}
                    className="flex items-center justify-between gap-4 border-b border-foreground/10 pb-3 last:border-0 last:pb-0"
                  >
                    <div>
                      <div className="font-mono text-sm">
                        {template.templateID}
                      </div>
                      <div className="text-xs text-muted">
                        {template.scope} · {formatTimestamp(template.createdAt)}
                      </div>
                    </div>
                    <PixelBadge>{template.scope}</PixelBadge>
                  </div>
                ))
              )}
            </div>
          </PixelCard>
        </section>
      </main>
    </PixelLayout>
  );
}
