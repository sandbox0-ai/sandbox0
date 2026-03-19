import { cookies } from "next/headers";
import { NextResponse } from "next/server";

import {
  clearDashboardAuthCookies,
  dashboardRefreshTokenCookieName,
  exchangeRefreshToken,
  resolveDashboardRuntimeConfig,
  setDashboardAuthCookies,
} from "@sandbox0/dashboard-core";

function refreshRedirectURL(
  requestURL: string,
  error?: string,
): URL {
  const url = new URL("/", requestURL);
  url.searchParams.set("refreshed", "1");
  if (error) {
    url.searchParams.set("login_error", error);
  }
  return url;
}

export async function GET(request: Request) {
  const config = resolveDashboardRuntimeConfig();
  const cookieStore = await cookies();
  const refreshToken = cookieStore.get(dashboardRefreshTokenCookieName)?.value;

  if (!refreshToken) {
    const response = NextResponse.redirect(
      refreshRedirectURL(request.url, "session expired, please sign in again"),
      { status: 303 },
    );
    clearDashboardAuthCookies(response, config);
    return response;
  }

  const result = await exchangeRefreshToken(config, refreshToken);
  if (!result.tokens) {
    const response = NextResponse.redirect(
      refreshRedirectURL(
        request.url,
        result.error ?? "session expired, please sign in again",
      ),
      { status: 303 },
    );
    clearDashboardAuthCookies(response, config);
    return response;
  }

  const response = NextResponse.redirect(refreshRedirectURL(request.url), {
    status: 303,
  });
  setDashboardAuthCookies(response, config, result.tokens);
  return response;
}
