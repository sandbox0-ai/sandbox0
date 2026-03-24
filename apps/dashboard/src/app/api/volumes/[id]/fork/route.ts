import {
  createDashboardVolumeForkRoute,
  resolveDashboardRuntimeConfig,
} from "@sandbox0/dashboard-app";

const { POST } = createDashboardVolumeForkRoute(resolveDashboardRuntimeConfig);

export { POST };
