import assert from "node:assert/strict";
import test from "node:test";

import type { DashboardRuntimeConfig } from "./types";
import { readBearerToken, resolveDashboardSession } from "./session";

const singleClusterConfig: DashboardRuntimeConfig = {
  mode: "single-cluster",
  dashboardBasePath: "/dashboard",
  siteURL: "https://sandbox0.ai",
  singleClusterURL: "https://single.example.com",
};

const globalDirectoryConfig: DashboardRuntimeConfig = {
  mode: "global-directory",
  dashboardBasePath: "/dashboard",
  siteURL: "https://sandbox0.ai",
  globalDirectoryURL: "https://global.example.com",
};

test("readBearerToken prefers Authorization header", () => {
  const token = readBearerToken("Bearer header-token", {
    get(name: string) {
      if (name === "sandbox0_access_token") {
        return { value: "cookie-token" };
      }
      return undefined;
    },
  });

  assert.equal(token, "header-token");
});

test("resolveDashboardSession returns unauthenticated session without token", async () => {
  const session = await resolveDashboardSession(singleClusterConfig, {});

  assert.equal(session.authenticated, false);
  assert.equal(session.mode, "single-cluster");
  assert.deepEqual(session.teams, []);
});

test("resolveDashboardSession resolves single-cluster metadata", async () => {
  const fetchCalls: string[] = [];
  const fetchImpl: typeof fetch = async (input) => {
    const url = String(input);
    fetchCalls.push(url);

    if (url.endsWith("/users/me")) {
      return new Response(
        JSON.stringify({
          data: {
            id: "u_123",
            email: "dev@example.com",
            name: "Dev User",
            default_team_id: "team_1",
            email_verified: true,
            is_admin: false,
          },
        }),
      );
    }
    if (url.endsWith("/teams")) {
      return new Response(
        JSON.stringify({
          data: {
            teams: [
              {
                id: "team_1",
                name: "Personal Team",
                slug: "personal-team",
                home_region_id: "local",
              },
            ],
          },
        }),
      );
    }
    if (url.includes("/api/v1/sandboxes")) {
      return new Response(
        JSON.stringify({
          data: {
            sandboxes: [
              {
                id: "sb_1",
                template_id: "default",
                status: "running",
                paused: false,
                created_at: "2026-03-12T00:00:00Z",
                expires_at: "2026-03-12T01:00:00Z",
              },
            ],
          },
        }),
      );
    }
    if (url.endsWith("/api/v1/templates")) {
      return new Response(
        JSON.stringify({
          data: {
            templates: [
              {
                template_id: "default",
                scope: "global",
                created_at: "2026-03-12T00:00:00Z",
              },
            ],
          },
        }),
      );
    }

    throw new Error(`unexpected url ${url}`);
  };

  const session = await resolveDashboardSession(
    singleClusterConfig,
    { bearerToken: "test-token" },
    fetchImpl,
  );

  assert.equal(session.authenticated, true);
  assert.equal(session.user?.id, "u_123");
  assert.equal(session.activeTeam?.teamID, "team_1");
  assert.equal(session.sandboxes.length, 1);
  assert.equal(session.templates.length, 1);
  assert.equal(fetchCalls.length, 4);
});

test("resolveDashboardSession resolves global-directory metadata and region routing", async () => {
  const fetchImpl: typeof fetch = async (input, init) => {
    const url = String(input);

    if (url === "https://global.example.com/users/me") {
      return new Response(
        JSON.stringify({
          data: {
            id: "u_123",
            email: "dev@example.com",
            name: "Dev User",
            default_team_id: "team_1",
            email_verified: true,
            is_admin: false,
          },
        }),
      );
    }
    if (url === "https://global.example.com/teams") {
      return new Response(
        JSON.stringify({
          data: {
            teams: [
              {
                id: "team_1",
                name: "Cloud Team",
                slug: "cloud-team",
                home_region_id: "aws/us-east-1",
              },
            ],
          },
        }),
      );
    }
    if (url === "https://global.example.com/tenant/active") {
      return new Response(
        JSON.stringify({
          data: {
            user_id: "u_123",
            team_id: "team_1",
            home_region_id: "aws/us-east-1",
            default_team: true,
            edge_gateway_url: "https://use1.example.com",
          },
        }),
      );
    }
    if (url === "https://global.example.com/auth/region-token") {
      assert.equal(init?.method, "POST");
      return new Response(
        JSON.stringify({
          data: {
            region_id: "aws/us-east-1",
            edge_gateway_url: "https://use1.example.com",
            token: "region-token",
            expires_at: 123456,
          },
        }),
      );
    }
    if (url === "https://use1.example.com/api/v1/sandboxes?limit=5") {
      return new Response(
        JSON.stringify({
          data: {
            sandboxes: [],
          },
        }),
      );
    }
    if (url === "https://use1.example.com/api/v1/templates") {
      return new Response(
        JSON.stringify({
          data: {
            templates: [],
          },
        }),
      );
    }

    throw new Error(`unexpected url ${url}`);
  };

  const session = await resolveDashboardSession(
    globalDirectoryConfig,
    { bearerToken: "global-token" },
    fetchImpl,
  );

  assert.equal(session.authenticated, true);
  assert.equal(session.activeTeam?.homeRegionID, "aws/us-east-1");
  assert.equal(session.configuredRegionalURL, "https://use1.example.com");
});
