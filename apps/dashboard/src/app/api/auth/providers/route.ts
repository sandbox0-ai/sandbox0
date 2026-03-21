import {
  handleDashboardAuthProvidersRequest,
  resolveDashboardRuntimeConfig,
} from "@sandbox0/dashboard-core";

export async function GET() {
  return handleDashboardAuthProvidersRequest(resolveDashboardRuntimeConfig());
}
