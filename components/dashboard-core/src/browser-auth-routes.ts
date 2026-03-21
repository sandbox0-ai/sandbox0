import { NextResponse } from "next/server";

import type { DashboardRuntimeConfig } from "./types";
import {
  clearDashboardAuthCookies,
  dashboardCookieNames,
  dashboardRefreshTokenCookieName,
  exchangeBuiltinLogin,
  exchangeOIDCCallback,
  exchangeRefreshToken,
  forwardLogout,
  resolveDashboardAuthProviders,
  resolveOIDCLoginLocation,
  setDashboardAuthCookies,
} from "./auth";
import { dashboardLoginPath } from "./browser-auth-links";

function dashboardHomeRedirectURL(
  requestURL: string,
  options?: {
    refreshed?: boolean;
    loginError?: string;
  },
): URL {
  const url = new URL("/", requestURL);
  if (options?.refreshed) {
    url.searchParams.set("refreshed", "1");
  }
  if (options?.loginError) {
    url.searchParams.set("login_error", options.loginError);
  }
  return url;
}

function dashboardLoginRedirectURL(requestURL: string, error?: string): URL {
  return new URL(dashboardLoginPath(error), requestURL);
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
      dashboardLoginRedirectURL(request.url, "email and password are required"),
      { status: 303 },
    );
  }

  const result = await exchangeBuiltinLogin(config, email, password);
  if (!result.tokens) {
    return NextResponse.redirect(
      dashboardLoginRedirectURL(request.url, result.error ?? "login failed"),
      { status: 303 },
    );
  }

  const response = NextResponse.redirect(
    dashboardHomeRedirectURL(request.url),
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
    dashboardHomeRedirectURL(request.url),
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
      dashboardHomeRedirectURL(request.url, {
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
      dashboardHomeRedirectURL(request.url, {
        refreshed: true,
        loginError: result.error ?? "session expired, please sign in again",
      }),
      { status: 303 },
    );
    clearDashboardAuthCookies(response, config);
    return response;
  }

  const response = NextResponse.redirect(
    dashboardHomeRedirectURL(request.url, { refreshed: true }),
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
        request.url,
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
) {
  const rawQuery = new URL(request.url).search;
  const result = await exchangeOIDCCallback(config, providerID, rawQuery);
  if (!result.tokens) {
    return NextResponse.redirect(
      dashboardLoginRedirectURL(
        request.url,
        result.error ?? "oidc callback failed",
      ),
      { status: 303 },
    );
  }

  const response = NextResponse.redirect(
    dashboardHomeRedirectURL(request.url),
    { status: 303 },
  );
  setDashboardAuthCookies(response, config, result.tokens);
  return response;
}
