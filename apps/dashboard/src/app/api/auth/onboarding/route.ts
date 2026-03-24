import {
  createDashboardOnboardingRoute,
  resolveDashboardRuntimeConfig,
} from "@sandbox0/dashboard-app";

export const POST = createDashboardOnboardingRoute(resolveDashboardRuntimeConfig);
