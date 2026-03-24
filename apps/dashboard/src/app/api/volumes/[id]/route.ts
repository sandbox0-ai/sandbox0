import {
  createDashboardVolumeRoute,
  resolveDashboardRuntimeConfig,
} from "@sandbox0/dashboard-app";

const { GET, DELETE } = createDashboardVolumeRoute(resolveDashboardRuntimeConfig);

export { GET, DELETE };
