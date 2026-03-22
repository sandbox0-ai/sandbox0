import {
  createDashboardAuthProvidersRoute,
  resolveDashboardRuntimeConfig,
} from "@sandbox0/dashboard-core";

export const GET = createDashboardAuthProvidersRoute(resolveDashboardRuntimeConfig);
