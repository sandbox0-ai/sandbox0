import assert from "node:assert/strict";
import test from "node:test";

import { resolveDashboardRuntimeConfig } from "./config";

test("resolveDashboardRuntimeConfig defaults to single-cluster in development", () => {
  const config = resolveDashboardRuntimeConfig({
    NODE_ENV: "development",
  });

  assert.equal(config.mode, "single-cluster");
  assert.equal(config.dashboardBasePath, "/dashboard");
  assert.equal(config.siteURL, "http://localhost:4300");
  assert.equal(config.singleClusterURL, "http://localhost:30080");
});

test("resolveDashboardRuntimeConfig supports global-directory mode", () => {
  const config = resolveDashboardRuntimeConfig({
    NODE_ENV: "production",
    SANDBOX0_DASHBOARD_MODE: "global-directory",
    SANDBOX0_DASHBOARD_GLOBAL_DIRECTORY_URL: "https://api.sandbox0.ai",
    SANDBOX0_DASHBOARD_SITE_URL: "https://sandbox0.ai",
  });

  assert.equal(config.mode, "global-directory");
  assert.equal(config.globalDirectoryURL, "https://api.sandbox0.ai");
  assert.equal(config.siteURL, "https://sandbox0.ai");
});
