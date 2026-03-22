import { cookies, headers } from "next/headers";
import { NextResponse } from "next/server";

import type { DashboardConfigResolver } from "./internal/auth-pages";
import {
  clearDashboardAuthCookies,
  dashboardAccessTokenCookieName,
  dashboardRegionalAccessTokenCookieName,
  dashboardRegionalExpiresAtCookieName,
  dashboardRegionalGatewayURLCookieName,
  dashboardRegionalRegionIDCookieName,
  dashboardRefreshTokenCookieName,
  type DashboardRegionalSession,
  exchangeRefreshToken,
  resolveDashboardAuthProviders,
  setDashboardAuthCookies,
  updateDefaultTeam,
} from "./internal/auth";
import { readBearerToken, resolveDashboardSession } from "./internal/session";

function readRegionalSessionFromCookies(
  cookieStore: Awaited<ReturnType<typeof cookies>>,
): DashboardRegionalSession | undefined {
  const token = cookieStore.get(dashboardRegionalAccessTokenCookieName)?.value;
  const regionID = cookieStore.get(dashboardRegionalRegionIDCookieName)?.value;
  const expiresAt = Number(
    cookieStore.get(dashboardRegionalExpiresAtCookieName)?.value ?? "",
  );
  const regionalGatewayURL = cookieStore.get(
    dashboardRegionalGatewayURLCookieName,
  )?.value;

  if (!token || !regionID || !Number.isFinite(expiresAt) || expiresAt <= 0) {
    return undefined;
  }

  return {
    token,
    region_id: regionID,
    expires_at: expiresAt,
    regional_gateway_url: regionalGatewayURL || undefined,
  };
}

export function createDashboardSessionRoute(
  resolveConfig: DashboardConfigResolver,
) {
  return async function GET() {
    const headerStore = await headers();
    const cookieStore = await cookies();
    const config = resolveConfig();

    const token = readBearerToken(headerStore.get("authorization"), cookieStore);
    let session = await resolveDashboardSession(config, {
      bearerToken: token,
      regionalSession: readRegionalSessionFromCookies(cookieStore),
    });

    if (!session.authenticated) {
      const refreshToken = cookieStore.get(
        dashboardRefreshTokenCookieName,
      )?.value;
      if (refreshToken) {
        const refreshed = await exchangeRefreshToken(config, refreshToken);
        if (refreshed.tokens) {
          session = await resolveDashboardSession(config, {
            bearerToken: refreshed.tokens.access_token,
            regionalSession: refreshed.tokens.regional_session,
          });
          const authProviders = await resolveDashboardAuthProviders(config);
          const response = NextResponse.json({
            data: {
              session,
              authProviders: authProviders.providers,
              authErrors: authProviders.errors,
            },
          });
          setDashboardAuthCookies(response, config, refreshed.tokens);
          return response;
        }
      }
    }

    const authProviders = await resolveDashboardAuthProviders(config);
    return NextResponse.json({
      data: {
        session,
        authProviders: authProviders.providers,
        authErrors: authProviders.errors,
      },
    });
  };
}

function dashboardURL(
  requestURL: string,
  error?: string,
  switched?: boolean,
): URL {
  const url = new URL("/", requestURL);
  if (switched) {
    url.searchParams.set("team_switched", "1");
  }
  if (error) {
    url.searchParams.set("login_error", error);
  }
  return url;
}

export function createDashboardTeamSelectRoute(
  resolveConfig: DashboardConfigResolver,
) {
  return async function POST(request: Request) {
    const config = resolveConfig();
    const cookieStore = await cookies();
    const formData = await request.formData();
    const teamID = String(formData.get("team_id") ?? "").trim();
    let accessToken = cookieStore.get(dashboardAccessTokenCookieName)?.value;
    const refreshToken = cookieStore.get(dashboardRefreshTokenCookieName)?.value;

    if (!teamID) {
      return NextResponse.redirect(
        dashboardURL(request.url, "team_id is required"),
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
        dashboardURL(request.url, "browser session not found, please sign in again"),
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
          updateResult.error ?? "failed to switch active team",
        ),
        { status: 303 },
      );
    }

    if (!refreshToken) {
      return NextResponse.redirect(
        dashboardURL(
          request.url,
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
      dashboardURL(request.url, undefined, true),
      { status: 303 },
    );
    setDashboardAuthCookies(response, config, finalRefresh.tokens);
    return response;
  };
}
