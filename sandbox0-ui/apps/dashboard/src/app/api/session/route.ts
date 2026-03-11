import { cookies, headers } from "next/headers";
import { NextResponse } from "next/server";

import {
  dashboardRefreshTokenCookieName,
  exchangeRefreshToken,
  readBearerToken,
  resolveDashboardRuntimeConfig,
  resolveDashboardSession,
  setDashboardAuthCookies,
} from "@sandbox0/dashboard-core";

export const dynamic = "force-dynamic";

export async function GET() {
  const headerStore = await headers();
  const cookieStore = await cookies();
  const config = resolveDashboardRuntimeConfig();

  const token = readBearerToken(headerStore.get("authorization"), cookieStore);
  let session = await resolveDashboardSession(config, { bearerToken: token });

  if (!session.authenticated) {
    const refreshToken = cookieStore.get(
      dashboardRefreshTokenCookieName,
    )?.value;
    if (refreshToken) {
      const refreshed = await exchangeRefreshToken(config, refreshToken);
      if (refreshed.tokens) {
        session = await resolveDashboardSession(config, {
          bearerToken: refreshed.tokens.access_token,
        });
        const response = NextResponse.json({ data: session });
        setDashboardAuthCookies(response, config, refreshed.tokens);
        return response;
      }
    }
  }

  return NextResponse.json({ data: session });
}
