import assert from "node:assert/strict";
import test from "node:test";

import { NextResponse } from "next/server";

import {
  clearDashboardAuthCookies,
  exchangeRefreshToken,
  resolveDashboardAuthProviders,
  resolveOIDCLoginLocation,
  setDashboardAuthCookies,
  updateDefaultTeam,
} from "./auth";
import type { DashboardRuntimeConfig } from "./types";

const singleClusterConfig: DashboardRuntimeConfig = {
  mode: "single-cluster",
  siteURL: "https://sandbox0.ai",
  singleClusterURL: "https://single.example.com",
};

test("resolveDashboardAuthProviders parses builtin and oidc providers", async () => {
  const result = await resolveDashboardAuthProviders(
    singleClusterConfig,
    async () =>
      new Response(
        JSON.stringify({
          data: {
            providers: [
              { id: "auth0", name: "Auth0", type: "oidc" },
              { id: "builtin", name: "Email & Password", type: "builtin" },
            ],
          },
        }),
      ),
  );

  assert.deepEqual(result.errors, []);
  assert.equal(result.providers.length, 2);
  assert.equal(result.providers[0]?.id, "auth0");
  assert.equal(result.providers[1]?.type, "builtin");
});

test("resolveOIDCLoginLocation relays upstream redirect targets", async () => {
  const result = await resolveOIDCLoginLocation(
    singleClusterConfig,
    "auth0",
    async () =>
      new Response(null, {
        status: 302,
        headers: {
          Location: "https://tenant.auth0.com/authorize?state=abc",
        },
      }),
  );

  assert.equal(result.error, undefined);
  assert.equal(result.location, "https://tenant.auth0.com/authorize?state=abc");
});

test("exchangeRefreshToken returns new tokens from the control plane", async () => {
  const result = await exchangeRefreshToken(
    singleClusterConfig,
    "refresh-token",
    async (input, init) => {
      assert.equal(String(input), "https://single.example.com/auth/refresh");
      assert.equal(init?.method, "POST");
      return new Response(
        JSON.stringify({
          data: {
            access_token: "new-access-token",
            refresh_token: "new-refresh-token",
            expires_at: Math.floor(Date.now() / 1000) + 3600,
          },
        }),
      );
    },
  );

  assert.equal(result.error, undefined);
  assert.equal(result.tokens?.access_token, "new-access-token");
  assert.equal(result.tokens?.refresh_token, "new-refresh-token");
});

test("updateDefaultTeam sends the selected team to the control plane", async () => {
  const result = await updateDefaultTeam(
    singleClusterConfig,
    "access-token",
    "team_2",
    async (input, init) => {
      assert.equal(String(input), "https://single.example.com/tenant/active");
      assert.equal(init?.method, "PUT");
      const headers = new Headers(init?.headers);
      assert.equal(headers.get("authorization"), "Bearer access-token");
      assert.equal(headers.get("content-type"), "application/json");
      assert.equal(init?.body, JSON.stringify({ default_team_id: "team_2" }));
      return new Response(JSON.stringify({ data: { id: "user_1" } }));
    },
  );

  assert.equal(result.ok, true);
  assert.equal(result.error, undefined);
});

test("setDashboardAuthCookies stores dashboard auth cookies", () => {
  const response = NextResponse.json({ ok: true });
  setDashboardAuthCookies(response, singleClusterConfig, {
    access_token: "access-token",
    refresh_token: "refresh-token",
    expires_at: Math.floor(Date.now() / 1000) + 3600,
  });

  const accessCookie = response.cookies.get("sandbox0_access_token");
  const refreshCookie = response.cookies.get("sandbox0_refresh_token");
  assert.equal(accessCookie?.value, "access-token");
  assert.equal(refreshCookie?.value, "refresh-token");
  assert.equal(accessCookie?.httpOnly, true);
  assert.equal(accessCookie?.path, "/");
});

test("clearDashboardAuthCookies expires dashboard auth cookies", () => {
  const response = NextResponse.json({ ok: true });
  clearDashboardAuthCookies(response, singleClusterConfig);

  const accessCookie = response.cookies.get("sandbox0_access_token");
  const refreshCookie = response.cookies.get("sandbox0_refresh_token");
  assert.equal(accessCookie?.maxAge, 0);
  assert.equal(refreshCookie?.maxAge, 0);
});
