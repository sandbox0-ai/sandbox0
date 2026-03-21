import { cookies } from "next/headers";
import { redirect } from "next/navigation";

import {
  dashboardRefreshTokenCookieName,
  readBearerToken,
  resolveDashboardAuthProviders,
  resolveDashboardRuntimeConfig,
  resolveDashboardSession,
} from "@sandbox0/dashboard-core";
import { LoginView } from "./LoginView";

interface LoginPageProps {
  searchParams: Promise<{ login_error?: string }>;
}

export default async function LoginPage({ searchParams }: LoginPageProps) {
  const { login_error: loginError } = await searchParams;
  const config = resolveDashboardRuntimeConfig();
  const cookieStore = await cookies();
  const accessToken = readBearerToken(null, cookieStore);
  const refreshToken = cookieStore.get(dashboardRefreshTokenCookieName)?.value;
  const session = await resolveDashboardSession(config, { bearerToken: accessToken });

  if (session.authenticated) {
    redirect("/");
  }
  if (refreshToken && !loginError) {
    redirect("/api/auth/refresh");
  }

  const { providers } = await resolveDashboardAuthProviders(config);

  const oidcProviders = providers.filter((p) => p.type === "oidc");

  // Auto-redirect when exactly one OIDC provider is configured and no error is
  // being displayed. Skips the selection page to remove an extra click.
  if (oidcProviders.length === 1 && !loginError) {
    const [provider] = oidcProviders;
    if (provider) {
      if (provider.externalAuthPortalUrl) {
        redirect(provider.externalAuthPortalUrl);
      }
      redirect(`/api/auth/oidc/${encodeURIComponent(provider.id)}/login`);
    }
  }

  return <LoginView providers={providers} loginError={loginError} />;
}
