import type {
  DashboardControlPlaneMode,
  DashboardRuntimeConfig,
} from "./types";

const defaultDashboardBasePath = "/dashboard";

function normalizeMode(value: string | undefined): DashboardControlPlaneMode {
  if (value === "global-directory") {
    return value;
  }
  return "single-cluster";
}

function defaultSiteURL(nodeEnv: string | undefined): string {
  if (nodeEnv === "development") {
    return "http://localhost:4300";
  }
  return "https://sandbox0.ai";
}

export function resolveDashboardRuntimeConfig(
  env: NodeJS.ProcessEnv = process.env,
): DashboardRuntimeConfig {
  const mode = normalizeMode(env.SANDBOX0_DASHBOARD_MODE);
  const siteURL =
    env.SANDBOX0_DASHBOARD_SITE_URL ?? defaultSiteURL(env.NODE_ENV);

  if (mode === "global-directory") {
    return {
      mode,
      dashboardBasePath: defaultDashboardBasePath,
      siteURL,
      globalDirectoryURL:
        env.SANDBOX0_DASHBOARD_GLOBAL_DIRECTORY_URL ??
        env.SANDBOX0_BASE_URL ??
        "https://api.sandbox0.ai",
    };
  }

  return {
    mode,
    dashboardBasePath: defaultDashboardBasePath,
    siteURL,
    singleClusterURL:
      env.SANDBOX0_DASHBOARD_SINGLE_CLUSTER_URL ??
      env.SANDBOX0_BASE_URL ??
      "http://localhost:30080",
  };
}

export function resolveDashboardControlPlaneURL(
  config: DashboardRuntimeConfig,
): string | undefined {
  if (config.mode === "global-directory") {
    return config.globalDirectoryURL;
  }

  return config.singleClusterURL;
}
