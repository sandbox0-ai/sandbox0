import { cookies } from "next/headers";

import {
  handleDashboardLogoutRequest,
  resolveDashboardRuntimeConfig,
} from "@sandbox0/dashboard-core";

export async function POST(request: Request) {
  return handleDashboardLogoutRequest(
    resolveDashboardRuntimeConfig(),
    request,
    await cookies(),
  );
}
