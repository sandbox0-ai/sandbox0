import { cookies } from "next/headers";
import { NextResponse } from "next/server";

import {
  clearDashboardAuthCookies,
  dashboardAccessTokenCookieName,
  dashboardRefreshTokenCookieName,
  exchangeRefreshToken,
  resolveDashboardRuntimeConfig,
  setDashboardAuthCookies,
  updateDefaultTeam,
} from "@sandbox0/dashboard-core";

function dashboardURL(
  requestURL: string,
  basePath: string,
  error?: string,
  switched?: boolean,
): URL {
  const url = new URL(basePath, requestURL);
  if (switched) {
    url.searchParams.set("team_switched", "1");
  }
  if (error) {
    url.searchParams.set("login_error", error);
  }
  return url;
}

export async function POST(request: Request) {
  const config = resolveDashboardRuntimeConfig();
  const cookieStore = await cookies();
  const formData = await request.formData();
  const teamID = String(formData.get("team_id") ?? "").trim();
  let accessToken = cookieStore.get(dashboardAccessTokenCookieName)?.value;
  const refreshToken = cookieStore.get(dashboardRefreshTokenCookieName)?.value;

  if (!teamID) {
    return NextResponse.redirect(
      dashboardURL(
        request.url,
        config.dashboardBasePath,
        "team_id is required",
      ),
      { status: 303 },
    );
  }

  let refreshedTokens:
    | {
        access_token: string;
        refresh_token: string;
        expires_at: number;
      }
    | undefined;

  if (!accessToken && refreshToken) {
    const refreshed = await exchangeRefreshToken(config, refreshToken);
    if (!refreshed.tokens) {
      const response = NextResponse.redirect(
        dashboardURL(
          request.url,
          config.dashboardBasePath,
          refreshed.error ?? "session expired, please sign in again",
        ),
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
      dashboardURL(
        request.url,
        config.dashboardBasePath,
        "browser session not found, please sign in again",
      ),
      { status: 303 },
    );
    clearDashboardAuthCookies(response, config);
    return response;
  }

  let updateResult = await updateDefaultTeam(config, accessToken, teamID);
  if (!updateResult.ok && refreshToken) {
    const refreshed = await exchangeRefreshToken(config, refreshToken);
    if (refreshed.tokens) {
      refreshedTokens = refreshed.tokens;
      accessToken = refreshed.tokens.access_token;
      updateResult = await updateDefaultTeam(config, accessToken, teamID);
    }
  }

  if (!updateResult.ok) {
    return NextResponse.redirect(
      dashboardURL(
        request.url,
        config.dashboardBasePath,
        updateResult.error ?? "failed to switch active team",
      ),
      { status: 303 },
    );
  }

  if (!refreshToken) {
    return NextResponse.redirect(
      dashboardURL(
        request.url,
        config.dashboardBasePath,
        "team updated but browser session could not be refreshed",
        true,
      ),
      { status: 303 },
    );
  }

  const finalRefresh = await exchangeRefreshToken(config, refreshToken);
  if (!finalRefresh.tokens) {
    const response = NextResponse.redirect(
      dashboardURL(
        request.url,
        config.dashboardBasePath,
        finalRefresh.error ?? "team updated but session refresh failed",
        true,
      ),
      { status: 303 },
    );
    if (refreshedTokens) {
      setDashboardAuthCookies(response, config, refreshedTokens);
    }
    return response;
  }

  const response = NextResponse.redirect(
    dashboardURL(request.url, config.dashboardBasePath, undefined, true),
    { status: 303 },
  );
  setDashboardAuthCookies(response, config, finalRefresh.tokens);
  return response;
}
