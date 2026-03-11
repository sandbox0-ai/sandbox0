import { NextResponse } from "next/server";

import {
  exchangeBuiltinLogin,
  resolveDashboardRuntimeConfig,
  setDashboardAuthCookies,
} from "@sandbox0/dashboard-core";

function redirectURL(
  requestURL: string,
  basePath: string,
  error?: string,
): URL {
  const value = error
    ? `${basePath}?login_error=${encodeURIComponent(error)}`
    : basePath;
  return new URL(value, requestURL);
}

export async function POST(request: Request) {
  const config = resolveDashboardRuntimeConfig();
  const formData = await request.formData();
  const email = String(formData.get("email") ?? "").trim();
  const password = String(formData.get("password") ?? "");

  if (!email || !password) {
    return NextResponse.redirect(
      redirectURL(
        request.url,
        config.dashboardBasePath,
        "email and password are required",
      ),
      { status: 303 },
    );
  }

  const result = await exchangeBuiltinLogin(config, email, password);
  if (!result.tokens) {
    return NextResponse.redirect(
      redirectURL(
        request.url,
        config.dashboardBasePath,
        result.error ?? "login failed",
      ),
      { status: 303 },
    );
  }

  const response = NextResponse.redirect(
    redirectURL(request.url, config.dashboardBasePath),
    { status: 303 },
  );
  setDashboardAuthCookies(response, config, result.tokens);
  return response;
}
