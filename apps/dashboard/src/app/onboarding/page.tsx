import {
  createDashboardOnboardingPage,
  resolveDashboardRuntimeConfig,
} from "@sandbox0/dashboard-app";

export const dynamic = "force-dynamic";

export default createDashboardOnboardingPage(resolveDashboardRuntimeConfig);
