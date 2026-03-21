import {
  handleDashboardBuiltinLoginRequest,
  resolveDashboardRuntimeConfig,
} from "@sandbox0/dashboard-core";

export async function POST(request: Request) {
  return handleDashboardBuiltinLoginRequest(
    resolveDashboardRuntimeConfig(),
    request,
  );
}
