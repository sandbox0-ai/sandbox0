import {
  handleDashboardOIDCCallbackRequest,
  resolveDashboardRuntimeConfig,
} from "@sandbox0/dashboard-core";

// This callback assumes the control plane OIDC base URL is configured to point
// at the public dashboard auth surface, for example /api/auth/...
// The route then proxies the callback to the actual control-plane service and
// converts the returned token pair into dashboard cookies.
export async function GET(
  request: Request,
  { params }: { params: Promise<{ provider: string }> },
) {
  const { provider } = await params;
  return handleDashboardOIDCCallbackRequest(
    resolveDashboardRuntimeConfig(),
    request,
    provider,
  );
}
