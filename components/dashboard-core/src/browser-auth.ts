import type { DashboardAuthProvider, DashboardRuntimeConfig } from "./types";
import {
  dashboardRefreshTokenCookieName,
  resolveDashboardAuthProviders,
} from "./auth";
import {
  dashboardLoginPath,
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
}

export interface DashboardLoginRenderResult {
  kind: "render";
  providers: DashboardAuthProvider[];
  loginError?: string;
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

  if (session.authenticated) {
    return { kind: "render" };
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
