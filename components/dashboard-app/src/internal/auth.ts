import { NextResponse } from "next/server";

import { resolveDashboardControlPlaneURL } from "./config";
import {
  createDashboardControlPlaneSDK,
  readSDKResponseError,
  resolveSDKErrorMessage,
} from "./sdk";
import type {
  DashboardAuthProvider,
  DashboardAuthProviderType,
  DashboardRegion,
  DashboardRuntimeConfig,
} from "./types";

interface LoginResponse {
  access_token: string;
  refresh_token: string;
  expires_at: number;
  regional_session?: DashboardRegionalSession;
}

export interface DashboardRegionalSession {
  region_id: string;
  regional_gateway_url?: string | null;
  token: string;
  expires_at: number;
}

const dashboardHomePath = "/";

export const dashboardAccessTokenCookieName = "sandbox0_access_token";
export const dashboardRefreshTokenCookieName = "sandbox0_refresh_token";
export const dashboardRegionalAccessTokenCookieName =
  "sandbox0_regional_access_token";
export const dashboardRegionalGatewayURLCookieName =
  "sandbox0_regional_gateway_url";
export const dashboardRegionalRegionIDCookieName = "sandbox0_region_id";
export const dashboardRegionalExpiresAtCookieName =
  "sandbox0_regional_expires_at";

function toLoginResponse(data: {
  accessToken: string;
  refreshToken: string;
  expiresAt: number;
  regionalSession?: {
    regionId: string;
    regionalGatewayUrl?: string | null;
    token: string;
    expiresAt: number;
  };
}): LoginResponse {
  return {
    access_token: data.accessToken,
    refresh_token: data.refreshToken,
    expires_at: data.expiresAt,
    regional_session: data.regionalSession
      ? {
          region_id: data.regionalSession.regionId,
          regional_gateway_url: data.regionalSession.regionalGatewayUrl,
          token: data.regionalSession.token,
          expires_at: data.regionalSession.expiresAt,
        }
      : undefined,
  };
}

function toDashboardAuthProviders(
  providers: Array<{
    id: string;
    name: string;
    type: string;
    externalAuthPortalUrl?: string;
  }> = [],
): DashboardAuthProvider[] {
  return providers.flatMap((provider) => {
    if (provider.type !== "oidc" && provider.type !== "builtin") {
      return [];
    }

    const entry: DashboardAuthProvider = {
      id: provider.id,
      name: provider.name,
      type: provider.type as DashboardAuthProviderType,
    };

    if (
      provider.type === "oidc" &&
      typeof provider.externalAuthPortalUrl === "string" &&
      provider.externalAuthPortalUrl !== ""
    ) {
      entry.externalAuthPortalUrl = provider.externalAuthPortalUrl;
    }

    return [entry];
  });
}

function parseOIDCCallbackQuery(rawQuery: string): {
  code?: string;
  state?: string;
} {
  const search = rawQuery.startsWith("?") ? rawQuery.slice(1) : rawQuery;
  const params = new URLSearchParams(search);

  return {
    code: params.get("code") ?? undefined,
    state: params.get("state") ?? undefined,
  };
}

export function dashboardCookieNames() {
  return {
    accessToken: dashboardAccessTokenCookieName,
    refreshToken: dashboardRefreshTokenCookieName,
    regionalAccessToken: dashboardRegionalAccessTokenCookieName,
    regionalGatewayURL: dashboardRegionalGatewayURLCookieName,
    regionalRegionID: dashboardRegionalRegionIDCookieName,
    regionalExpiresAt: dashboardRegionalExpiresAtCookieName,
  };
}

export async function resolveDashboardAuthProviders(
  config: DashboardRuntimeConfig,
  fetchImpl: typeof fetch = fetch,
): Promise<{ providers: DashboardAuthProvider[]; errors: string[] }> {
  const baseURL = resolveDashboardControlPlaneURL(config);
  if (!baseURL) {
    return {
      providers: [],
      errors: ["dashboard auth is missing a control-plane base URL"],
    };
  }

  try {
    const sdk = await createDashboardControlPlaneSDK(baseURL, {
      fetch: fetchImpl,
    });
    const response = await sdk.auth.authProvidersGet({ cache: "no-store" });
    const providers = toDashboardAuthProviders(response.data?.providers);

    return { providers, errors: [] };
  } catch (error) {
    return {
      providers: [],
      errors: [
        await resolveSDKErrorMessage(error, "failed to resolve auth providers"),
      ],
    };
  }
}

export async function exchangeBuiltinLogin(
  config: DashboardRuntimeConfig,
  email: string,
  password: string,
  fetchImpl: typeof fetch = fetch,
): Promise<{ tokens?: LoginResponse; error?: string }> {
  const baseURL = resolveDashboardControlPlaneURL(config);
  if (!baseURL) {
    return { error: "dashboard auth is missing a control-plane base URL" };
  }

  try {
    const sdk = await createDashboardControlPlaneSDK(baseURL, {
      fetch: fetchImpl,
    });
    const response = await sdk.auth.authLoginPost({
      loginRequest: { email, password },
    });
    if (!response.data) {
      return { error: "/auth/login returned an empty response" };
    }

    return { tokens: toLoginResponse(response.data) };
  } catch (error) {
    return {
      error: await resolveSDKErrorMessage(error, "failed to complete login"),
    };
  }
}

export async function exchangeRefreshToken(
  config: DashboardRuntimeConfig,
  refreshToken: string,
  fetchImpl: typeof fetch = fetch,
): Promise<{ tokens?: LoginResponse; error?: string }> {
  const baseURL = resolveDashboardControlPlaneURL(config);
  if (!baseURL) {
    return { error: "dashboard auth is missing a control-plane base URL" };
  }

  try {
    const sdk = await createDashboardControlPlaneSDK(baseURL, {
      fetch: fetchImpl,
    });
    const response = await sdk.auth.authRefreshPost({
      refreshRequest: { refreshToken },
    });
    if (!response.data) {
      return { error: "/auth/refresh returned an empty response" };
    }

    return { tokens: toLoginResponse(response.data) };
  } catch (error) {
    return {
      error: await resolveSDKErrorMessage(error, "failed to refresh session"),
    };
  }
}

export async function listRegions(
  config: DashboardRuntimeConfig,
  accessToken: string,
  fetchImpl: typeof fetch = fetch,
): Promise<{ regions: DashboardRegion[]; error?: string }> {
  const baseURL = resolveDashboardControlPlaneURL(config);
  if (!baseURL) {
    return {
      regions: [],
      error: "dashboard auth is missing a control-plane base URL",
    };
  }

  try {
    const sdk = await createDashboardControlPlaneSDK(baseURL, {
      token: accessToken,
      fetch: fetchImpl,
    });
    const response = await sdk.regions.regionsGet({ cache: "no-store" });
    const regions = (response.data?.regions ?? []).map((r) => ({
      id: r.id,
      displayName: r.displayName ?? null,
      enabled: r.enabled,
    }));
    return { regions };
  } catch (error) {
    return {
      regions: [],
      error: await resolveSDKErrorMessage(error, "failed to list regions"),
    };
  }
}

export async function createFirstTeam(
  config: DashboardRuntimeConfig,
  accessToken: string,
  teamName: string,
  homeRegionId: string,
  fetchImpl: typeof fetch = fetch,
): Promise<{ teamID?: string; error?: string }> {
  const baseURL = resolveDashboardControlPlaneURL(config);
  if (!baseURL) {
    return { error: "dashboard auth is missing a control-plane base URL" };
  }

  try {
    const sdk = await createDashboardControlPlaneSDK(baseURL, {
      token: accessToken,
      fetch: fetchImpl,
    });
    const response = await sdk.teams.teamsPost({
      createTeamRequest: { name: teamName, homeRegionId },
    });
    const teamData = response.data;
    if (!teamData) {
      return { error: "/teams returned an empty response" };
    }

    return { teamID: teamData.id };
  } catch (error) {
    return {
      error: await resolveSDKErrorMessage(error, "failed to create team"),
    };
  }
}

export async function updateDefaultTeam(
  config: DashboardRuntimeConfig,
  accessToken: string,
  teamID: string,
  fetchImpl: typeof fetch = fetch,
): Promise<{ ok: boolean; error?: string }> {
  const baseURL = resolveDashboardControlPlaneURL(config);
  if (!baseURL) {
    return {
      ok: false,
      error: "dashboard auth is missing a control-plane base URL",
    };
  }

  try {
    const sdk = await createDashboardControlPlaneSDK(baseURL, {
      token: accessToken,
      fetch: fetchImpl,
    });
    await sdk.users.tenantActivePut({
      updateUserRequest: { defaultTeamId: teamID },
    });
    return { ok: true };
  } catch (error) {
    return {
      ok: false,
      error: await resolveSDKErrorMessage(
        error,
        "failed to update default team",
      ),
    };
  }
}

export async function exchangeOIDCCallback(
  config: DashboardRuntimeConfig,
  providerID: string,
  rawQuery: string,
  fetchImpl: typeof fetch = fetch,
): Promise<{ tokens?: LoginResponse; error?: string }> {
  const baseURL = resolveDashboardControlPlaneURL(config);
  if (!baseURL) {
    return { error: "dashboard auth is missing a control-plane base URL" };
  }

  const { code, state } = parseOIDCCallbackQuery(rawQuery);
  if (!code || !state) {
    return { error: "oidc callback is missing code or state" };
  }

  try {
    const sdk = await createDashboardControlPlaneSDK(baseURL, {
      fetch: fetchImpl,
    });
    const response = await sdk.auth.authOidcProviderCallbackGet(
      { provider: providerID, code, state },
      { cache: "no-store" },
    );
    if (!response.data) {
      return { error: `/auth/oidc/${providerID}/callback returned an empty response` };
    }

    return { tokens: toLoginResponse(response.data) };
  } catch (error) {
    return {
      error: await resolveSDKErrorMessage(error, "failed to complete oidc login"),
    };
  }
}

export async function resolveOIDCLoginLocation(
  config: DashboardRuntimeConfig,
  providerID: string,
  fetchImpl: typeof fetch = fetch,
): Promise<{ location?: string; error?: string }> {
  const baseURL = resolveDashboardControlPlaneURL(config);
  if (!baseURL) {
    return { error: "dashboard auth is missing a control-plane base URL" };
  }

  try {
    const sdk = await createDashboardControlPlaneSDK(baseURL, {
      fetch: fetchImpl,
    });
    await sdk.auth.authOidcProviderLoginGetRaw(
      {
        provider: providerID,
        returnUrl: dashboardHomePath,
      },
      {
        redirect: "manual",
        cache: "no-store",
      },
    );
  } catch (error) {
    const response = await readSDKResponseError(error);
    if (response) {
      if (response.status >= 300 && response.status < 400) {
        const location = response.headers.get("location");
        if (!location) {
          return { error: "oidc login did not return a redirect location" };
        }

        return { location };
      }
    }

    return {
      error: await resolveSDKErrorMessage(error, "failed to initiate oidc login"),
    };
  }

  return { error: "oidc login did not return a redirect location" };
}

export async function forwardLogout(
  config: DashboardRuntimeConfig,
  accessToken: string | undefined,
  fetchImpl: typeof fetch = fetch,
): Promise<void> {
  const baseURL = resolveDashboardControlPlaneURL(config);
  if (!baseURL || !accessToken) {
    return;
  }

  try {
    const sdk = await createDashboardControlPlaneSDK(baseURL, {
      token: accessToken,
      fetch: fetchImpl,
    });
    await sdk.auth.authLogoutPost();
  } catch {
    // Ignore upstream logout failures and clear browser cookies locally.
  }
}

export function setDashboardAuthCookies(
  response: NextResponse,
  config: DashboardRuntimeConfig,
  tokens: LoginResponse,
): void {
  const secure = config.siteURL.startsWith("https://");
  const maxAge = Math.max(0, tokens.expires_at - Math.floor(Date.now() / 1000));

  response.cookies.set(dashboardAccessTokenCookieName, tokens.access_token, {
    httpOnly: true,
    sameSite: "lax",
    secure,
    path: dashboardHomePath,
    maxAge,
  });
  response.cookies.set(dashboardRefreshTokenCookieName, tokens.refresh_token, {
    httpOnly: true,
    sameSite: "lax",
    secure,
    path: dashboardHomePath,
  });

  if (
    tokens.regional_session?.token &&
    tokens.regional_session?.region_id &&
    tokens.regional_session?.expires_at
  ) {
    const regionalMaxAge = Math.max(
      0,
      tokens.regional_session.expires_at - Math.floor(Date.now() / 1000),
    );

    response.cookies.set(
      dashboardRegionalAccessTokenCookieName,
      tokens.regional_session.token,
      {
        httpOnly: true,
        sameSite: "lax",
        secure,
        path: dashboardHomePath,
        maxAge: regionalMaxAge,
      },
    );
    response.cookies.set(
      dashboardRegionalRegionIDCookieName,
      tokens.regional_session.region_id,
      {
        httpOnly: true,
        sameSite: "lax",
        secure,
        path: dashboardHomePath,
        maxAge: regionalMaxAge,
      },
    );
    response.cookies.set(
      dashboardRegionalExpiresAtCookieName,
      String(tokens.regional_session.expires_at),
      {
        httpOnly: true,
        sameSite: "lax",
        secure,
        path: dashboardHomePath,
        maxAge: regionalMaxAge,
      },
    );
    response.cookies.set(
      dashboardRegionalGatewayURLCookieName,
      tokens.regional_session.regional_gateway_url ?? "",
      {
        httpOnly: true,
        sameSite: "lax",
        secure,
        path: dashboardHomePath,
        maxAge: regionalMaxAge,
      },
    );
    return;
  }

  response.cookies.set(dashboardRegionalAccessTokenCookieName, "", {
    httpOnly: true,
    sameSite: "lax",
    secure,
    path: dashboardHomePath,
    maxAge: 0,
  });
  response.cookies.set(dashboardRegionalGatewayURLCookieName, "", {
    httpOnly: true,
    sameSite: "lax",
    secure,
    path: dashboardHomePath,
    maxAge: 0,
  });
  response.cookies.set(dashboardRegionalRegionIDCookieName, "", {
    httpOnly: true,
    sameSite: "lax",
    secure,
    path: dashboardHomePath,
    maxAge: 0,
  });
  response.cookies.set(dashboardRegionalExpiresAtCookieName, "", {
    httpOnly: true,
    sameSite: "lax",
    secure,
    path: dashboardHomePath,
    maxAge: 0,
  });
}

export function clearDashboardAuthCookies(
  response: NextResponse,
  config: DashboardRuntimeConfig,
): void {
  const secure = config.siteURL.startsWith("https://");

  response.cookies.set(dashboardAccessTokenCookieName, "", {
    httpOnly: true,
    sameSite: "lax",
    secure,
    path: dashboardHomePath,
    maxAge: 0,
  });
  response.cookies.set(dashboardRefreshTokenCookieName, "", {
    httpOnly: true,
    sameSite: "lax",
    secure,
    path: dashboardHomePath,
    maxAge: 0,
  });
  response.cookies.set(dashboardRegionalAccessTokenCookieName, "", {
    httpOnly: true,
    sameSite: "lax",
    secure,
    path: dashboardHomePath,
    maxAge: 0,
  });
  response.cookies.set(dashboardRegionalGatewayURLCookieName, "", {
    httpOnly: true,
    sameSite: "lax",
    secure,
    path: dashboardHomePath,
    maxAge: 0,
  });
  response.cookies.set(dashboardRegionalRegionIDCookieName, "", {
    httpOnly: true,
    sameSite: "lax",
    secure,
    path: dashboardHomePath,
    maxAge: 0,
  });
  response.cookies.set(dashboardRegionalExpiresAtCookieName, "", {
    httpOnly: true,
    sameSite: "lax",
    secure,
    path: dashboardHomePath,
    maxAge: 0,
  });
}
