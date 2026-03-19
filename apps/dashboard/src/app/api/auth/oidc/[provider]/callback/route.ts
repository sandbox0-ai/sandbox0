import { NextResponse } from "next/server";

import {
  exchangeOIDCCallback,
  resolveDashboardRuntimeConfig,
  setDashboardAuthCookies,
} from "@sandbox0/dashboard-core";

function dashboardURL(requestURL: string, error?: string): URL {
  const value = error
    ? `/?login_error=${encodeURIComponent(error)}`
    : "/";
  return new URL(value, requestURL);
}

// This callback assumes the control plane OIDC base URL is configured to point
// at the public dashboard auth surface, for example /api/auth/...
// The route then proxies the callback to the actual control-plane service and
// converts the returned token pair into dashboard cookies.
export async function GET(
  request: Request,
  { params }: { params: Promise<{ provider: string }> },
) {
  const config = resolveDashboardRuntimeConfig();
  const { provider } = await params;
  const rawQuery = new URL(request.url).search;
  const result = await exchangeOIDCCallback(config, provider, rawQuery);
  if (!result.tokens) {
    return NextResponse.redirect(
      dashboardURL(request.url, result.error ?? "oidc callback failed"),
      { status: 303 },
    );
  }

  const response = NextResponse.redirect(dashboardURL(request.url), {
    status: 303,
  });
  setDashboardAuthCookies(response, config, result.tokens);
  return response;
}
