import {
  createDashboardBuiltinLoginRoute,
  resolveDashboardRuntimeConfig,
} from "@sandbox0/dashboard-core";

export const POST = createDashboardBuiltinLoginRoute(resolveDashboardRuntimeConfig);
