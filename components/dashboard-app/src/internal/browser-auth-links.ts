import type { DashboardAuthProvider } from "./types";

export function dashboardLoginPath(loginError?: string): string {
  if (!loginError) {
    return "/login";
  }

  return `/login?login_error=${encodeURIComponent(loginError)}`;
}

export function dashboardOnboardingPath(onboardingError?: string): string {
  if (!onboardingError) {
    return "/onboarding";
  }

  return `/onboarding?onboarding_error=${encodeURIComponent(onboardingError)}`;
}

export function dashboardProviderLoginPath(providerID: string): string {
  return `/api/auth/oidc/${encodeURIComponent(providerID)}/login`;
}

export function resolveDashboardProviderLoginTarget(
  provider: DashboardAuthProvider,
): string {
  if (provider.externalAuthPortalUrl) {
    return provider.externalAuthPortalUrl;
  }

  return dashboardProviderLoginPath(provider.id);
}
