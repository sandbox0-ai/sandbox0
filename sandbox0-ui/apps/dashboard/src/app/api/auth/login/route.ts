import { NextResponse } from "next/server";

import {
  exchangeBuiltinLogin,
  resolveDashboardRuntimeConfig,
  setDashboardAuthCookies,
} from "@sandbox0/dashboard-core";

function redirectURL(requestURL: string, error?: string): URL {
  const value = error
    ? `/?login_error=${encodeURIComponent(error)}`
    : "/";
  return new URL(value, requestURL);
}

export async function POST(request: Request) {
  const config = resolveDashboardRuntimeConfig();
  const formData = await request.formData();
  const email = String(formData.get("email") ?? "").trim();
  const password = String(formData.get("password") ?? "");

  if (!email || !password) {
    return NextResponse.redirect(
      redirectURL(request.url, "email and password are required"),
      { status: 303 },
    );
  }

  const result = await exchangeBuiltinLogin(config, email, password);
  if (!result.tokens) {
    return NextResponse.redirect(
      redirectURL(request.url, result.error ?? "login failed"),
      { status: 303 },
    );
  }

  const response = NextResponse.redirect(redirectURL(request.url), { status: 303 });
  setDashboardAuthCookies(response, config, result.tokens);
  return response;
}
