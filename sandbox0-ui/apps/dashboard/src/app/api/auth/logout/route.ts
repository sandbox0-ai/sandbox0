import { cookies } from "next/headers";
import { NextResponse } from "next/server";

import {
  clearDashboardAuthCookies,
  dashboardCookieNames,
  forwardLogout,
  resolveDashboardRuntimeConfig,
} from "@sandbox0/dashboard-core";

function redirectURL(requestURL: string): URL {
  return new URL("/", requestURL);
}

export async function POST(request: Request) {
  const config = resolveDashboardRuntimeConfig();
  const cookieStore = await cookies();
  const accessToken = cookieStore.get(
    dashboardCookieNames().accessToken,
  )?.value;

  await forwardLogout(config, accessToken);

  const response = NextResponse.redirect(
    redirectURL(request.url),
    {
      status: 303,
    },
  );
  clearDashboardAuthCookies(response, config);
  return response;
}
