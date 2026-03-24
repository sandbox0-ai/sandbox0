import type { DashboardAuthProvider, DashboardRegion, DashboardRuntimeConfig, DashboardSession } from "./types";
import {
  dashboardRefreshTokenCookieName,
  listRegions,
  resolveDashboardAuthProviders,
} from "./auth";
import {
  dashboardLoginPath,
  dashboardOnboardingPath,
  resolveDashboardProviderLoginTarget,
} from "./browser-auth-links";
import { readBearerToken, resolveDashboardSession } from "./session";

export interface DashboardCookieStore {
  get(name: string): { value: string } | undefined;
}

export interface DashboardRedirectResult {
  kind: "redirect";
  location: string;
}

export interface DashboardRenderResult {
  kind: "render";
  session: DashboardSession;
}

export interface DashboardLoginRenderResult {
  kind: "render";
  providers: DashboardAuthProvider[];
  loginError?: string;
}

export interface DashboardOnboardingRenderResult {
  kind: "render";
  session: DashboardSession;
  regions: DashboardRegion[];
}

export async function resolveDashboardHomeEntry(
  config: DashboardRuntimeConfig,
  cookieStore: DashboardCookieStore,
  options?: {
    loginError?: string;
    fetchImpl?: typeof fetch;
  },
): Promise<DashboardRenderResult | DashboardRedirectResult> {
  const fetchImpl = options?.fetchImpl ?? fetch;
  const accessToken = readBearerToken(null, cookieStore);
  const refreshToken = cookieStore.get(dashboardRefreshTokenCookieName)?.value;
  const session = await resolveDashboardSession(
    config,
    { bearerToken: accessToken },
    fetchImpl,
  );

  if (session.authenticated && session.needsOnboarding) {
    return { kind: "redirect", location: dashboardOnboardingPath() };
  }

  if (session.authenticated) {
    return { kind: "render", session };
  }

  if (refreshToken) {
    return { kind: "redirect", location: "/api/auth/refresh" };
  }

  const { providers } = await resolveDashboardAuthProviders(config, fetchImpl);
  const oidcProviders = providers.filter((provider) => provider.type === "oidc");

  if (options?.loginError || oidcProviders.length !== 1) {
    return {
      kind: "redirect",
      location: dashboardLoginPath(options?.loginError),
    };
  }

  const [provider] = oidcProviders;
  if (!provider) {
    return { kind: "redirect", location: dashboardLoginPath(options?.loginError) };
  }

  return {
    kind: "redirect",
    location: resolveDashboardProviderLoginTarget(provider),
  };
}

export async function resolveDashboardOnboardingEntry(
  config: DashboardRuntimeConfig,
  cookieStore: DashboardCookieStore,
  options?: {
    fetchImpl?: typeof fetch;
  },
): Promise<DashboardOnboardingRenderResult | DashboardRedirectResult> {
  const fetchImpl = options?.fetchImpl ?? fetch;
  const accessToken = readBearerToken(null, cookieStore);
  const refreshToken = cookieStore.get(dashboardRefreshTokenCookieName)?.value;
  const session = await resolveDashboardSession(
    config,
    { bearerToken: accessToken },
    fetchImpl,
  );

  if (!session.authenticated) {
    if (refreshToken) {
      return { kind: "redirect", location: "/api/auth/refresh" };
    }
    return { kind: "redirect", location: dashboardLoginPath() };
  }

  if (!session.needsOnboarding) {
    return { kind: "redirect", location: "/" };
  }

  const token = readBearerToken(null, cookieStore);
  const { regions } = token
    ? await listRegions(config, token, fetchImpl)
    : { regions: [] };

  return { kind: "render", session, regions };
}

export async function resolveDashboardLoginEntry(
  config: DashboardRuntimeConfig,
  cookieStore: DashboardCookieStore,
  options?: {
    loginError?: string;
    fetchImpl?: typeof fetch;
  },
): Promise<DashboardLoginRenderResult | DashboardRedirectResult> {
  const fetchImpl = options?.fetchImpl ?? fetch;
  const accessToken = readBearerToken(null, cookieStore);
  const refreshToken = cookieStore.get(dashboardRefreshTokenCookieName)?.value;
  const session = await resolveDashboardSession(
    config,
    { bearerToken: accessToken },
    fetchImpl,
  );

  if (session.authenticated && session.needsOnboarding) {
    return { kind: "redirect", location: dashboardOnboardingPath() };
  }

  if (session.authenticated) {
    return { kind: "redirect", location: "/" };
  }

  if (refreshToken && !options?.loginError) {
    return { kind: "redirect", location: "/api/auth/refresh" };
  }

  const { providers } = await resolveDashboardAuthProviders(config, fetchImpl);
  const oidcProviders = providers.filter((provider) => provider.type === "oidc");

  if (oidcProviders.length === 1 && !options?.loginError) {
    const [provider] = oidcProviders;
    if (provider) {
      return {
        kind: "redirect",
        location: resolveDashboardProviderLoginTarget(provider),
      };
    }
  }

  return {
    kind: "render",
    providers,
    loginError: options?.loginError,
  };
}
