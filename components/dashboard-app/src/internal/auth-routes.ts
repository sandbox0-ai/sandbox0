import { cookies } from "next/headers";

import {
  handleDashboardAuthProvidersRequest,
  handleDashboardBuiltinLoginRequest,
  handleDashboardLogoutRequest,
  handleDashboardOIDCCallbackRequest,
  handleDashboardOIDCLoginRequest,
  handleDashboardOnboardingRequest,
  handleDashboardRefreshRequest,
} from "./browser-auth-routes";
import type { DashboardRuntimeConfig } from "./types";

type RouteDashboardConfigResolver = () => DashboardRuntimeConfig;

export function createDashboardAuthProvidersRoute(
  resolveConfig: RouteDashboardConfigResolver,
) {
  return async function GET() {
    return handleDashboardAuthProvidersRequest(resolveConfig());
  };
}

export function createDashboardBuiltinLoginRoute(
  resolveConfig: RouteDashboardConfigResolver,
) {
  return async function POST(request: Request) {
    return handleDashboardBuiltinLoginRequest(resolveConfig(), request);
  };
}

export function createDashboardLogoutRoute(
  resolveConfig: RouteDashboardConfigResolver,
) {
  return async function POST(request: Request) {
    return handleDashboardLogoutRequest(
      resolveConfig(),
      request,
      await cookies(),
    );
  };
}

export function createDashboardRefreshRoute(
  resolveConfig: RouteDashboardConfigResolver,
) {
  return async function GET(request: Request) {
    return handleDashboardRefreshRequest(
      resolveConfig(),
      request,
      await cookies(),
    );
  };
}

export function createDashboardOIDCLoginRoute(
  resolveConfig: RouteDashboardConfigResolver,
) {
  return async function GET(
    request: Request,
    { params }: { params: Promise<{ provider: string }> },
  ) {
    const { provider } = await params;
    return handleDashboardOIDCLoginRequest(resolveConfig(), request, provider);
  };
}

export function createDashboardOIDCCallbackRoute(
  resolveConfig: RouteDashboardConfigResolver,
) {
  return async function GET(
    request: Request,
    { params }: { params: Promise<{ provider: string }> },
  ) {
    const { provider } = await params;
    return handleDashboardOIDCCallbackRequest(resolveConfig(), request, provider);
  };
}

export function createDashboardOnboardingRoute(
  resolveConfig: RouteDashboardConfigResolver,
) {
  return async function POST(request: Request) {
    return handleDashboardOnboardingRequest(
      resolveConfig(),
      request,
      await cookies(),
    );
  };
}
