import { cookies } from "next/headers";
import { redirect } from "next/navigation";
import Image from "next/image";
import { PixelButton, PixelCard, PixelInput, PixelLayout } from "@sandbox0/ui";
import {
  dashboardRefreshTokenCookieName,
  readBearerToken,
  resolveDashboardAuthProviders,
  resolveDashboardRuntimeConfig,
  resolveDashboardSession,
} from "@sandbox0/dashboard-core";

interface DashboardHomeProps {
  searchParams: Promise<{ login_error?: string }>;
}

function DashboardHomeView() {
  return (
    <PixelLayout>
      {/* Header */}
      <header className="flex items-center justify-between p-4 border-b border-foreground/10">
        <div className="flex items-center gap-3">
          <Image
            src="/sandbox0.png"
            alt="Sandbox0"
            width={32}
            height={32}
            className="pixel-art"
            data-pixel
          />
          <h1 className="font-pixel text-sm">SANDBOX0</h1>
        </div>
        <nav className="flex gap-4">
          <PixelButton variant="secondary" scale="sm">
            Sandboxes
          </PixelButton>
          <PixelButton variant="secondary" scale="sm">
            Templates
          </PixelButton>
          <PixelButton variant="secondary" scale="sm">
            Settings
          </PixelButton>
        </nav>
      </header>

      {/* Main Content */}
      <main className="flex-1 p-6">
        <div className="mb-8">
          <h2 className="font-pixel text-lg mb-2">Welcome back</h2>
          <p className="text-muted">Manage your AI sandboxes</p>
        </div>

        {/* Quick Actions */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
          <PixelCard
            header="New Sandbox"
            interactive
            accent
          >
            <p className="text-sm text-muted mb-4">
              Create a new sandbox instance from a template
            </p>
            <PixelButton variant="primary" scale="sm">
              + Create
            </PixelButton>
          </PixelCard>

          <PixelCard header="Running" interactive>
            <p className="text-4xl font-pixel text-accent mb-2">3</p>
            <p className="text-sm text-muted">Active sandboxes</p>
          </PixelCard>

          <PixelCard header="Storage" interactive>
            <p className="text-4xl font-pixel mb-2">12.4 GB</p>
            <p className="text-sm text-muted">Total volume usage</p>
          </PixelCard>
        </div>

        {/* Search */}
        <div className="max-w-md mb-8">
          <PixelInput
            label="Search Sandboxes"
            placeholder="Enter sandbox name or ID..."
          />
        </div>

        {/* Sandbox List Preview */}
        <div className="space-y-4">
          <h3 className="font-pixel text-sm mb-4">Recent Sandboxes</h3>
          {["sandbox-dev-001", "sandbox-test-002", "sandbox-prod-003"].map(
            (name) => (
              <PixelCard key={name} scale="sm" interactive>
                <div className="flex items-center justify-between">
                  <div>
                    <p className="font-mono text-sm">{name}</p>
                    <p className="text-xs text-muted">Running · 2h 34m</p>
                  </div>
                  <div className="flex gap-2">
                    <PixelButton variant="secondary" scale="sm">
                      Terminal
                    </PixelButton>
                    <PixelButton variant="secondary" scale="sm">
                      Stop
                    </PixelButton>
                  </div>
                </div>
              </PixelCard>
            )
          )}
        </div>
      </main>

      {/* Footer */}
      <footer className="p-4 border-t border-foreground/10 text-center text-xs text-muted">
        Sandbox0 Dashboard v0.0.1
      </footer>
    </PixelLayout>
  );
}

export default async function DashboardHome({ searchParams }: DashboardHomeProps) {
  const { login_error: loginError } = await searchParams;
  const config = resolveDashboardRuntimeConfig();
  const cookieStore = await cookies();
  const accessToken = readBearerToken(null, cookieStore);
  const refreshToken = cookieStore.get(dashboardRefreshTokenCookieName)?.value;
  const session = await resolveDashboardSession(config, { bearerToken: accessToken });

  if (!session.authenticated) {
    if (refreshToken) {
      redirect("/api/auth/refresh");
    }

    const { providers } = await resolveDashboardAuthProviders(config);
    const oidcProviders = providers.filter((provider) => provider.type === "oidc");

    if (loginError || oidcProviders.length !== 1) {
      const loginURL = new URL("/login", config.siteURL);
      if (loginError) {
        loginURL.searchParams.set("login_error", loginError);
      }
      redirect(`${loginURL.pathname}${loginURL.search}`);
    }

    const [provider] = oidcProviders;
    if (provider?.externalAuthPortalUrl) {
      redirect(provider.externalAuthPortalUrl);
    }
    if (provider) {
      redirect(`/api/auth/oidc/${encodeURIComponent(provider.id)}/login`);
    }
  }

  return <DashboardHomeView />;
}
