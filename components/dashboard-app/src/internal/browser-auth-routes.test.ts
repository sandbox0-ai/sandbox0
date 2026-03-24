import assert from "node:assert/strict";
import test from "node:test";

import { handleDashboardOIDCCallbackRequest, handleDashboardOnboardingRequest } from "./browser-auth-routes";
import type { DashboardRuntimeConfig } from "./types";

const config: DashboardRuntimeConfig = {
  mode: "global-gateway",
  siteURL: "https://cloud.sandbox0.ai",
  globalGatewayURL: "https://api.sandbox0.ai",
};

test("onboarding creates team and refreshes session on success", async () => {
  const calls: string[] = [];
  const fetchImpl: typeof fetch = async (input, init) => {
    const url = String(input);
    calls.push(`${init?.method ?? "GET"} ${url}`);

    if (url === "https://api.sandbox0.ai/teams" && init?.method === "POST") {
      return new Response(
        JSON.stringify({
          success: true,
          data: {
            id: "team_new",
            name: "My Team",
            slug: "my-team",
            home_region_id: "aws/us-east-1",
            created_at: "2026-03-25T00:00:00Z",
            updated_at: "2026-03-25T00:00:00Z",
          },
        }),
        { status: 201 },
      );
    }
    if (url === "https://api.sandbox0.ai/tenant/active" && init?.method === "PUT") {
      return new Response(JSON.stringify({ success: true }));
    }
    if (url === "https://api.sandbox0.ai/auth/refresh" && init?.method === "POST") {
      return new Response(
        JSON.stringify({
          data: {
            access_token: "new-access-token",
            refresh_token: "new-refresh-token",
            expires_at: Math.floor(Date.now() / 1000) + 3600,
          },
        }),
      );
    }

    throw new Error(`unexpected request: ${init?.method ?? "GET"} ${url}`);
  };

  const formData = new FormData();
  formData.set("team_name", "My Team");
  formData.set("home_region_id", "aws/us-east-1");

  const response = await handleDashboardOnboardingRequest(
    config,
    new Request("https://cloud.sandbox0.ai/api/auth/onboarding", {
      method: "POST",
      body: formData,
    }),
    {
      get(name: string) {
        if (name === "sandbox0_access_token") return { value: "old-access-token" };
        if (name === "sandbox0_refresh_token") return { value: "old-refresh-token" };
        return undefined;
      },
    },
    { fetchImpl },
  );

  assert.equal(response.status, 303);
  assert.equal(response.headers.get("location"), "https://cloud.sandbox0.ai/");
});

test("onboarding redirects to onboarding page with error when required fields are missing", async () => {
  const formData = new FormData();
  formData.set("team_name", "");
  formData.set("home_region_id", "");

  const response = await handleDashboardOnboardingRequest(
    config,
    new Request("https://cloud.sandbox0.ai/api/auth/onboarding", {
      method: "POST",
      body: formData,
    }),
    { get: () => undefined },
  );

  assert.equal(response.status, 303);
  const location = response.headers.get("location") ?? "";
  assert.ok(
    location.startsWith("https://cloud.sandbox0.ai/onboarding"),
    `expected onboarding redirect, got ${location}`,
  );
});

test("oidc callback redirects to configured site url instead of internal request host", async () => {
  const response = await handleDashboardOIDCCallbackRequest(
    config,
    new Request(
      "http://0.0.0.0:4401/api/auth/oidc/supabase/callback?code=code-123&state=state-456",
    ),
    "supabase",
    {
      fetchImpl: async (input, init) => {
        assert.equal(
          String(input),
          "https://api.sandbox0.ai/auth/oidc/supabase/callback?code=code-123&state=state-456",
        );
        assert.equal(init?.method, "GET");
        return new Response(
          JSON.stringify({
            data: {
              access_token: "oidc-access-token",
              refresh_token: "oidc-refresh-token",
              expires_at: Math.floor(Date.now() / 1000) + 3600,
            },
          }),
        );
      },
    },
  );

  assert.equal(response.status, 303);
  assert.equal(response.headers.get("location"), "https://cloud.sandbox0.ai/");
});
