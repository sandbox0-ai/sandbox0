import { cookies } from "next/headers";

import {
  handleDashboardRefreshRequest,
  resolveDashboardRuntimeConfig,
} from "@sandbox0/dashboard-core";

export async function GET(request: Request) {
  return handleDashboardRefreshRequest(
    resolveDashboardRuntimeConfig(),
    request,
    await cookies(),
  );
}
