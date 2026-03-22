import {
  createDashboardOIDCLoginRoute,
  resolveDashboardRuntimeConfig,
} from "@sandbox0/dashboard-core";

export const GET = createDashboardOIDCLoginRoute(resolveDashboardRuntimeConfig);
