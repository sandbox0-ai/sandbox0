import { NextResponse } from "next/server";

import type { DashboardRuntimeConfig } from "./types";
import {
  clearDashboardAuthCookies,
  createFirstTeam,
  dashboardCookieNames,
  dashboardRefreshTokenCookieName,
  exchangeBuiltinLogin,
  exchangeOIDCCallback,
  exchangeRefreshToken,
  forwardLogout,
  resolveDashboardAuthProviders,
  resolveOIDCLoginLocation,
  setDashboardAuthCookies,
  updateDefaultTeam,
} from "./auth";
import { dashboardLoginPath, dashboardOnboardingPath } from "./browser-auth-links";

function dashboardHomeRedirectURL(
  siteURL: string,
  options?: {
    refreshed?: boolean;
    loginError?: string;
  },
): URL {
  const url = new URL("/", siteURL);
  if (options?.refreshed) {
    url.searchParams.set("refreshed", "1");
  }
  if (options?.loginError) {
    url.searchParams.set("login_error", options.loginError);
  }
  return url;
}

function dashboardLoginRedirectURL(siteURL: string, error?: string): URL {
  return new URL(dashboardLoginPath(error), siteURL);
}

export async function handleDashboardAuthProvidersRequest(
  config: DashboardRuntimeConfig,
) {
  const providers = await resolveDashboardAuthProviders(config);
  return NextResponse.json(providers);
}

export async function handleDashboardBuiltinLoginRequest(
  config: DashboardRuntimeConfig,
  request: Request,
) {
  const formData = await request.formData();
  const email = String(formData.get("email") ?? "").trim();
  const password = String(formData.get("password") ?? "");

  if (!email || !password) {
    return NextResponse.redirect(
      dashboardLoginRedirectURL(config.siteURL, "email and password are required"),
      { status: 303 },
    );
  }

  const result = await exchangeBuiltinLogin(config, email, password);
  if (!result.tokens) {
    return NextResponse.redirect(
      dashboardLoginRedirectURL(config.siteURL, result.error ?? "login failed"),
      { status: 303 },
    );
  }

  const response = NextResponse.redirect(
    dashboardHomeRedirectURL(config.siteURL),
    { status: 303 },
  );
  setDashboardAuthCookies(response, config, result.tokens);
  return response;
}

export async function handleDashboardLogoutRequest(
  config: DashboardRuntimeConfig,
  request: Request,
  cookieStore: { get(name: string): { value: string } | undefined },
) {
  const accessToken = cookieStore.get(dashboardCookieNames().accessToken)?.value;

  await forwardLogout(config, accessToken);

  const response = NextResponse.redirect(
    dashboardHomeRedirectURL(config.siteURL),
    { status: 303 },
  );
  clearDashboardAuthCookies(response, config);
  return response;
}

export async function handleDashboardRefreshRequest(
  config: DashboardRuntimeConfig,
  request: Request,
  cookieStore: { get(name: string): { value: string } | undefined },
) {
  const refreshToken = cookieStore.get(dashboardRefreshTokenCookieName)?.value;

  if (!refreshToken) {
    const response = NextResponse.redirect(
      dashboardHomeRedirectURL(config.siteURL, {
        refreshed: true,
        loginError: "session expired, please sign in again",
      }),
      { status: 303 },
    );
    clearDashboardAuthCookies(response, config);
    return response;
  }

  const result = await exchangeRefreshToken(config, refreshToken);
  if (!result.tokens) {
    const response = NextResponse.redirect(
      dashboardHomeRedirectURL(config.siteURL, {
        refreshed: true,
        loginError: result.error ?? "session expired, please sign in again",
      }),
      { status: 303 },
    );
    clearDashboardAuthCookies(response, config);
    return response;
  }

  const response = NextResponse.redirect(
    dashboardHomeRedirectURL(config.siteURL, { refreshed: true }),
    { status: 303 },
  );
  setDashboardAuthCookies(response, config, result.tokens);
  return response;
}

export async function handleDashboardOIDCLoginRequest(
  config: DashboardRuntimeConfig,
  request: Request,
  providerID: string,
) {
  const result = await resolveOIDCLoginLocation(config, providerID);
  if (!result.location) {
    return NextResponse.redirect(
      dashboardLoginRedirectURL(
        config.siteURL,
        result.error ?? "oidc login failed",
      ),
      { status: 303 },
    );
  }

  return NextResponse.redirect(result.location, { status: 302 });
}

export async function handleDashboardOIDCCallbackRequest(
  config: DashboardRuntimeConfig,
  request: Request,
  providerID: string,
  options?: {
    fetchImpl?: typeof fetch;
  },
) {
  const rawQuery = new URL(request.url).search;
  const result = await exchangeOIDCCallback(
    config,
    providerID,
    rawQuery,
    options?.fetchImpl,
  );
  if (!result.tokens) {
    return NextResponse.redirect(
      dashboardLoginRedirectURL(
        config.siteURL,
        result.error ?? "oidc callback failed",
      ),
      { status: 303 },
    );
  }

  const response = NextResponse.redirect(
    dashboardHomeRedirectURL(config.siteURL),
    { status: 303 },
  );
  setDashboardAuthCookies(response, config, result.tokens);
  return response;
}

export async function handleDashboardOnboardingRequest(
  config: DashboardRuntimeConfig,
  request: Request,
  cookieStore: { get(name: string): { value: string } | undefined },
  options?: {
    fetchImpl?: typeof fetch;
  },
) {
  const fetchImpl = options?.fetchImpl ?? fetch;
  const formData = await request.formData();
  const teamName = String(formData.get("team_name") ?? "").trim();
  const homeRegionId = String(formData.get("home_region_id") ?? "").trim();

  if (!teamName || !homeRegionId) {
    return NextResponse.redirect(
      new URL(
        dashboardOnboardingPath("team name and home region are required"),
        config.siteURL,
      ),
      { status: 303 },
    );
  }

  let accessToken = cookieStore.get(dashboardCookieNames().accessToken)?.value;
  const refreshToken = cookieStore.get(dashboardRefreshTokenCookieName)?.value;

  let refreshedTokens:
    | {
        access_token: string;
        refresh_token: string;
        expires_at: number;
      }
    | undefined;

  if (!accessToken && refreshToken) {
    const refreshed = await exchangeRefreshToken(config, refreshToken, fetchImpl);
    if (!refreshed.tokens) {
      const response = NextResponse.redirect(
        dashboardHomeRedirectURL(config.siteURL, {
          loginError: refreshed.error ?? "session expired, please sign in again",
        }),
        { status: 303 },
      );
      clearDashboardAuthCookies(response, config);
      return response;
    }
    refreshedTokens = refreshed.tokens;
    accessToken = refreshed.tokens.access_token;
  }

  if (!accessToken) {
    const response = NextResponse.redirect(
      dashboardHomeRedirectURL(config.siteURL, {
        loginError: "browser session not found, please sign in again",
      }),
      { status: 303 },
    );
    clearDashboardAuthCookies(response, config);
    return response;
  }

  const createResult = await createFirstTeam(
    config,
    accessToken,
    teamName,
    homeRegionId,
    fetchImpl,
  );
  if (!createResult.teamID) {
    return NextResponse.redirect(
      new URL(
        dashboardOnboardingPath(createResult.error ?? "failed to create team"),
        config.siteURL,
      ),
      { status: 303 },
    );
  }

  const updateResult = await updateDefaultTeam(
    config,
    accessToken,
    createResult.teamID,
    fetchImpl,
  );
  if (!updateResult.ok) {
    return NextResponse.redirect(
      new URL(
        dashboardOnboardingPath(updateResult.error ?? "failed to activate team"),
        config.siteURL,
      ),
      { status: 303 },
    );
  }

  if (!refreshToken) {
    return NextResponse.redirect(dashboardHomeRedirectURL(config.siteURL), {
      status: 303,
    });
  }

  const finalRefresh = await exchangeRefreshToken(config, refreshToken, fetchImpl);
  if (!finalRefresh.tokens) {
    return NextResponse.redirect(
      dashboardHomeRedirectURL(config.siteURL, {
        loginError: finalRefresh.error ?? "team created but session refresh failed",
      }),
      { status: 303 },
    );
  }

  const response = NextResponse.redirect(dashboardHomeRedirectURL(config.siteURL), {
    status: 303,
  });
  setDashboardAuthCookies(response, config, finalRefresh.tokens);
  return response;
}
