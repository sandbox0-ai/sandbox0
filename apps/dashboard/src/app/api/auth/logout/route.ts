import {
  createDashboardLogoutRoute,
  resolveDashboardRuntimeConfig,
} from "@sandbox0/dashboard-core";

export const POST = createDashboardLogoutRoute(resolveDashboardRuntimeConfig);
