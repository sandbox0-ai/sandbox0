import assert from "node:assert/strict";
import test from "node:test";

import type { DashboardRuntimeConfig } from "./types";
import { readBearerToken, resolveDashboardSession } from "./session";

const singleClusterConfig: DashboardRuntimeConfig = {
  mode: "single-cluster",
  siteURL: "https://sandbox0.ai",
  singleClusterURL: "https://single.example.com",
};

const globalGatewayConfig: DashboardRuntimeConfig = {
  mode: "global-gateway",
  siteURL: "https://sandbox0.ai",
  globalGatewayURL: "https://global.example.com",
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
    if (url.endsWith("/api/v1/sandboxvolumes")) {
      return new Response(
        JSON.stringify({
          data: [],
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
  assert.equal(fetchCalls.length, 5);
});

test("resolveDashboardSession resolves global-gateway metadata and region routing", async () => {
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
            regional_gateway_url: "https://use1.example.com",
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
            regional_gateway_url: "https://use1.example.com",
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
    if (url === "https://use1.example.com/api/v1/sandboxvolumes") {
      return new Response(
        JSON.stringify({
          data: [],
        }),
      );
    }

    throw new Error(`unexpected url ${url}`);
  };

  const session = await resolveDashboardSession(
    globalGatewayConfig,
    { bearerToken: "global-token" },
    fetchImpl,
  );

  assert.equal(session.authenticated, true);
  assert.equal(session.activeTeam?.homeRegionID, "aws/us-east-1");
  assert.equal(session.configuredRegionalURL, "https://use1.example.com");
});

test("resolveDashboardSession returns needsOnboarding when user has no teams in global-gateway mode", async () => {
  const fetchImpl: typeof fetch = async (input) => {
    const url = String(input);

    if (url === "https://global.example.com/users/me") {
      return new Response(
        JSON.stringify({
          data: {
            id: "u_new",
            email: "new@example.com",
            name: "New User",
            default_team_id: null,
            email_verified: true,
            is_admin: false,
          },
        }),
      );
    }
    if (url === "https://global.example.com/teams") {
      return new Response(
        JSON.stringify({
          data: { teams: [] },
        }),
      );
    }

    throw new Error(`unexpected url ${url}`);
  };

  const session = await resolveDashboardSession(
    globalGatewayConfig,
    { bearerToken: "global-token" },
    fetchImpl,
  );

  assert.equal(session.authenticated, true);
  assert.equal(session.needsOnboarding, true);
  assert.equal(session.activeTeam, undefined);
  assert.deepEqual(session.teams, []);
  assert.equal(session.user?.id, "u_new");
});

test("resolveDashboardSession returns needsOnboarding when default team has no home region", async () => {
  const fetchImpl: typeof fetch = async (input) => {
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
                home_region_id: null,
              },
            ],
          },
        }),
      );
    }

    throw new Error(`unexpected url ${url}`);
  };

  const session = await resolveDashboardSession(
    globalGatewayConfig,
    { bearerToken: "global-token" },
    fetchImpl,
  );

  assert.equal(session.authenticated, true);
  assert.equal(session.needsOnboarding, true);
  assert.equal(session.activeTeam, undefined);
  assert.match(session.errors[0] ?? "", /home region/i);
});

test("resolveDashboardSession returns needsOnboarding when active team is not routable", async () => {
  const fetchImpl: typeof fetch = async (input) => {
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
          error: {
            message: "active team home region is not routable",
          },
        }),
        { status: 409 },
      );
    }

    throw new Error(`unexpected url ${url}`);
  };

  const session = await resolveDashboardSession(
    globalGatewayConfig,
    { bearerToken: "global-token" },
    fetchImpl,
  );

  assert.equal(session.authenticated, true);
  assert.equal(session.needsOnboarding, true);
  assert.equal(session.configuredRegionalURL, undefined);
  assert.equal(session.errors[0], "active team home region is not routable");
});

test("resolveDashboardSession uses regional session directly when available", async () => {
  const seenURLs: string[] = [];
  const fetchImpl: typeof fetch = async (input) => {
    const url = String(input);
    seenURLs.push(url);

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
    if (url === "https://use1.example.com/api/v1/sandboxes?limit=5") {
      return new Response(JSON.stringify({ data: { sandboxes: [] } }));
    }
    if (url === "https://use1.example.com/api/v1/templates") {
      return new Response(JSON.stringify({ data: { templates: [] } }));
    }
    if (url === "https://use1.example.com/api/v1/sandboxvolumes") {
      return new Response(JSON.stringify({ data: [] }));
    }

    throw new Error(`unexpected url ${url}`);
  };

  const session = await resolveDashboardSession(
    globalGatewayConfig,
    {
      bearerToken: "global-token",
      regionalSession: {
        region_id: "aws/us-east-1",
        regional_gateway_url: "https://use1.example.com",
        token: "regional-token",
        expires_at: 1893456000,
      },
    },
    fetchImpl,
  );

  assert.equal(session.authenticated, true);
  assert.equal(session.activeTeam?.teamID, "team_1");
  assert.equal(session.configuredRegionalURL, "https://use1.example.com");
  assert.deepEqual(seenURLs, [
    "https://global.example.com/users/me",
    "https://global.example.com/teams",
    "https://use1.example.com/api/v1/sandboxes?limit=5",
    "https://use1.example.com/api/v1/templates",
    "https://use1.example.com/api/v1/sandboxvolumes",
  ]);
});
