import { redirect } from "next/navigation";

import {
  resolveDashboardAuthProviders,
  resolveDashboardRuntimeConfig,
} from "@sandbox0/dashboard-core";
import { LoginView } from "./LoginView";

interface LoginPageProps {
  searchParams: Promise<{ login_error?: string }>;
}

export default async function LoginPage({ searchParams }: LoginPageProps) {
  const { login_error: loginError } = await searchParams;
  const config = resolveDashboardRuntimeConfig();
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
