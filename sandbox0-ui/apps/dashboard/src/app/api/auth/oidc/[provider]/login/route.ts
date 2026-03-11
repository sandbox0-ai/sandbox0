import { NextResponse } from "next/server";

import { resolveOIDCLoginLocation } from "@/lib/controlplane/auth";
import { resolveDashboardRuntimeConfig } from "@/lib/controlplane/config";

function dashboardURL(
  requestURL: string,
  basePath: string,
  error?: string,
): URL {
  const value = error
    ? `${basePath}?login_error=${encodeURIComponent(error)}`
    : basePath;
  return new URL(value, requestURL);
}

export async function GET(
  request: Request,
  { params }: { params: Promise<{ provider: string }> },
) {
  const config = resolveDashboardRuntimeConfig();
  const { provider } = await params;
  const result = await resolveOIDCLoginLocation(config, provider);
  if (!result.location) {
    return NextResponse.redirect(
      dashboardURL(
        request.url,
        config.dashboardBasePath,
        result.error ?? "oidc login failed",
      ),
      { status: 303 },
    );
  }

  return NextResponse.redirect(result.location, { status: 302 });
}
