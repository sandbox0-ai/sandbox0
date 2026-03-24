import {
  createDashboardVolumesRoute,
  resolveDashboardRuntimeConfig,
} from "@sandbox0/dashboard-app";

const { GET, POST } = createDashboardVolumesRoute(resolveDashboardRuntimeConfig);

export { GET, POST };
