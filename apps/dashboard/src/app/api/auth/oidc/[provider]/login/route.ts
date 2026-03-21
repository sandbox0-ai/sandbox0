import {
  handleDashboardOIDCLoginRequest,
  resolveDashboardRuntimeConfig,
} from "@sandbox0/dashboard-core";

export async function GET(
  request: Request,
  { params }: { params: Promise<{ provider: string }> },
) {
  const { provider } = await params;
  return handleDashboardOIDCLoginRequest(
    resolveDashboardRuntimeConfig(),
    request,
    provider,
  );
}
