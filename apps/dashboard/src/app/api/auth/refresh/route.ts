import {
  createDashboardRefreshRoute,
  resolveDashboardRuntimeConfig,
} from "@sandbox0/dashboard-core";

export const GET = createDashboardRefreshRoute(resolveDashboardRuntimeConfig);
