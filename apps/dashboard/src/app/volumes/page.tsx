import {
  createDashboardVolumesPage,
  resolveDashboardRuntimeConfig,
} from "@sandbox0/dashboard-app";

export const dynamic = "force-dynamic";

export default createDashboardVolumesPage(resolveDashboardRuntimeConfig);
