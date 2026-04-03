import assert from "node:assert/strict";
import test from "node:test";

import { NextResponse } from "next/server";

import {
  clearDashboardAuthCookies,
  exchangeOIDCCallback,
  exchangeRefreshToken,
  resolveDashboardAuthProviders,
  resolveOIDCLoginLocation,
  setDashboardAuthCookies,
  updateDefaultTeam,
} from "./auth";
import {
  resolveDashboardHomeEntry,
  resolveDashboardLoginEntry,
} from "./browser-auth";
import {
  dashboardLoginPath,
  dashboardProviderLoginPath,
} from "./browser-auth-links";
import type { DashboardRuntimeConfig } from "./types";

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

function createUnsignedJWT(payload: Record<string, unknown>): string {
  const encode = (value: object) =>
    Buffer.from(JSON.stringify(value))
      .toString("base64")
      .replace(/\+/g, "-")
      .replace(/\//g, "_")
      .replace(/=+$/g, "");

  return `${encode({ alg: "none", typ: "JWT" })}.${encode(payload)}.`;
}

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

test("resolveDashboardAuthProviders parses externalAuthPortalUrl from oidc providers", async () => {
  const result = await resolveDashboardAuthProviders(
    singleClusterConfig,
    async () =>
      new Response(
        JSON.stringify({
          data: {
            providers: [
              {
                id: "okta",
                name: "Okta",
                type: "oidc",
                external_auth_portal_url: "https://portal.example.com/login",
              },
              { id: "builtin", name: "Email & Password", type: "builtin" },
            ],
          },
        }),
      ),
  );

  assert.deepEqual(result.errors, []);
  assert.equal(result.providers.length, 2);
  assert.equal(
    result.providers[0]?.externalAuthPortalUrl,
    "https://portal.example.com/login",
  );
  assert.equal(result.providers[1]?.externalAuthPortalUrl, undefined);
});

test("resolveDashboardHomeEntry redirects directly to the only oidc provider", async () => {
  const result = await resolveDashboardHomeEntry(
    singleClusterConfig,
    { get: () => undefined },
    {
      fetchImpl: async () =>
        new Response(
          JSON.stringify({
            data: {
              providers: [{ id: "auth0", name: "Auth0", type: "oidc" }],
            },
          }),
        ),
    },
  );

  assert.deepEqual(result, {
    kind: "redirect",
    location: dashboardProviderLoginPath("auth0"),
  });
});

test("resolveDashboardHomeEntry falls back to /login when multiple oidc providers exist", async () => {
  const result = await resolveDashboardHomeEntry(
    singleClusterConfig,
    { get: () => undefined },
    {
      fetchImpl: async () =>
        new Response(
          JSON.stringify({
            data: {
              providers: [
                { id: "auth0", name: "Auth0", type: "oidc" },
                { id: "okta", name: "Okta", type: "oidc" },
              ],
            },
          }),
        ),
    },
  );

  assert.deepEqual(result, {
    kind: "redirect",
    location: "/login",
  });
});

test("resolveDashboardHomeEntry redirects system admins to onboarding when onboarding is pending", async () => {
  const result = await resolveDashboardHomeEntry(
    globalGatewayConfig,
    {
      get(name: string) {
        if (name === "sandbox0_access_token") {
          return { value: "access-token" };
        }
        return undefined;
      },
    },
    {
      fetchImpl: async (input) => {
        const url = String(input);
        if (url === "https://global.example.com/users/me") {
          return new Response(
            JSON.stringify({
              data: {
                id: "u_admin",
                email: "admin@example.com",
                name: "Admin",
                default_team_id: null,
                email_verified: true,
                is_admin: true,
              },
            }),
          );
        }
        if (url === "https://global.example.com/teams") {
          return new Response(JSON.stringify({ data: { teams: [] } }));
        }
        throw new Error(`unexpected url ${url}`);
      },
    },
  );

  assert.deepEqual(result, {
    kind: "redirect",
    location: "/onboarding",
  });
});

test("resolveDashboardLoginEntry reuses external auth portal url", async () => {
  const result = await resolveDashboardLoginEntry(
    singleClusterConfig,
    { get: () => undefined },
    {
      fetchImpl: async () =>
        new Response(
          JSON.stringify({
            data: {
              providers: [
                {
                  id: "corp",
                  name: "Corporate SSO",
                  type: "oidc",
                  external_auth_portal_url: "https://portal.example.com/login",
                },
              ],
            },
          }),
        ),
    },
  );

  assert.deepEqual(result, {
    kind: "redirect",
    location: "https://portal.example.com/login",
  });
});

test("resolveDashboardHomeEntry falls back to login when refresh already failed", async () => {
  const result = await resolveDashboardHomeEntry(
    {
      ...globalGatewayConfig,
      siteURL: "https://cloud.sandbox0.ai",
      cookieDomains: ["sandbox0.ai"],
    },
    {
      get(name: string) {
        if (name === "sandbox0_refresh_token") {
          return { value: "stale-refresh-token" };
        }
        return undefined;
      },
    },
    {
      loginError: "invalid refresh token",
      fetchImpl: async (input) => {
        const url = String(input);
        if (url === "https://global.example.com/auth/providers") {
          return new Response(
            JSON.stringify({
              data: {
                providers: [{ id: "supabase", name: "Supabase", type: "oidc" }],
              },
            }),
          );
        }
        throw new Error(`unexpected url ${url}`);
      },
    },
  );

  assert.deepEqual(result, {
    kind: "redirect",
    location: "/login?login_error=invalid%20refresh%20token",
  });
});

test("resolveDashboardLoginEntry sends authenticated system admins to onboarding when onboarding is pending", async () => {
  const result = await resolveDashboardLoginEntry(
    globalGatewayConfig,
    {
      get(name: string) {
        if (name === "sandbox0_access_token") {
          return { value: "access-token" };
        }
        return undefined;
      },
    },
    {
      fetchImpl: async (input) => {
        const url = String(input);
        if (url === "https://global.example.com/users/me") {
          return new Response(
            JSON.stringify({
              data: {
                id: "u_admin",
                email: "admin@example.com",
                name: "Admin",
                default_team_id: null,
                email_verified: true,
                is_admin: true,
              },
            }),
          );
        }
        if (url === "https://global.example.com/teams") {
          return new Response(JSON.stringify({ data: { teams: [] } }));
        }
        throw new Error(`unexpected url ${url}`);
      },
    },
  );

  assert.deepEqual(result, {
    kind: "redirect",
    location: "/onboarding",
  });
});

test("dashboardLoginPath encodes login errors", () => {
  assert.equal(
    dashboardLoginPath("session expired, please sign in again"),
    "/login?login_error=session%20expired%2C%20please%20sign%20in%20again",
  );
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
            regional_session: {
              region_id: "aws/us-east-1",
              regional_gateway_url: "https://use1.example.com",
              token: "regional-access-token",
              expires_at: Math.floor(Date.now() / 1000) + 900,
            },
          },
        }),
      );
    },
  );

  assert.equal(result.error, undefined);
  assert.equal(result.tokens?.access_token, "new-access-token");
  assert.equal(result.tokens?.refresh_token, "new-refresh-token");
  assert.equal(result.tokens?.regional_session?.token, "regional-access-token");
});

test("exchangeOIDCCallback exchanges code and state through the sdk auth api", async () => {
  const result = await exchangeOIDCCallback(
    singleClusterConfig,
    "auth0",
    "?code=code-123&state=state-456",
    async (input, init) => {
      assert.equal(
        String(input),
        "https://single.example.com/auth/oidc/auth0/callback?code=code-123&state=state-456",
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
  );

  assert.equal(result.error, undefined);
  assert.equal(result.tokens?.access_token, "oidc-access-token");
  assert.equal(result.tokens?.refresh_token, "oidc-refresh-token");
});

test("exchangeOIDCCallback relays upstream redirect targets", async () => {
  const result = await exchangeOIDCCallback(
    singleClusterConfig,
    "auth0",
    "?code=code-123&state=state-456",
    async (input, init) => {
      assert.equal(
        String(input),
        "https://single.example.com/auth/oidc/auth0/callback?code=code-123&state=state-456",
      );
      assert.equal(init?.method, "GET");
      assert.equal(init?.redirect, "manual");
      return new Response(null, {
        status: 302,
        headers: {
          location: "http://127.0.0.1:39123/callback?access_token=token",
        },
      });
    },
  );

  assert.equal(result.error, undefined);
  assert.equal(result.tokens, undefined);
  assert.equal(
    result.redirectLocation,
    "http://127.0.0.1:39123/callback?access_token=token",
  );
});

test("exchangeOIDCCallback rejects callbacks without code or state", async () => {
  const result = await exchangeOIDCCallback(
    singleClusterConfig,
    "auth0",
    "?code=code-123",
  );

  assert.equal(result.tokens, undefined);
  assert.equal(result.error, "oidc callback is missing code or state");
});

test("updateDefaultTeam sends the selected team to the control plane", async () => {
  const result = await updateDefaultTeam(
    singleClusterConfig,
    "access-token",
    "team_2",
    async (input, init) => {
      assert.equal(String(input), "https://single.example.com/users/me");
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
  const expiresAt = Math.floor(Date.now() / 1000) + 3600;
  const regionalExpiresAt = Math.floor(Date.now() / 1000) + 900;
  const response = NextResponse.json({ ok: true });
  setDashboardAuthCookies(response, singleClusterConfig, {
    access_token: createUnsignedJWT({ exp: expiresAt }),
    refresh_token: "refresh-token",
    expires_at: expiresAt,
    regional_session: {
      region_id: "aws/us-east-1",
      regional_gateway_url: "https://use1.example.com",
      token: createUnsignedJWT({ exp: regionalExpiresAt }),
      expires_at: regionalExpiresAt,
    },
  });

  const accessCookie = response.cookies.get("sandbox0_access_token");
  const refreshCookie = response.cookies.get("sandbox0_refresh_token");
  const regionalAccessCookie = response.cookies.get(
    "sandbox0_regional_access_token",
  );
  const regionalGatewayCookie = response.cookies.get(
    "sandbox0_regional_gateway_url",
  );
  assert.equal(accessCookie?.value, createUnsignedJWT({ exp: expiresAt }));
  assert.equal(refreshCookie?.value, "refresh-token");
  assert.equal(
    regionalAccessCookie?.value,
    createUnsignedJWT({ exp: regionalExpiresAt }),
  );
  assert.equal(regionalGatewayCookie?.value, "https://use1.example.com");
  assert.equal(accessCookie?.httpOnly, true);
  assert.equal(accessCookie?.path, "/");
  assert.equal(accessCookie?.maxAge, 3600);
  assert.equal(regionalAccessCookie?.maxAge, 900);
});

test("setDashboardAuthCookies writes canonical parent-domain cookies when configured", () => {
  const expiresAt = Math.floor(Date.now() / 1000) + 3600;
  const accessToken = createUnsignedJWT({ exp: expiresAt });
  const response = NextResponse.json({ ok: true });
  setDashboardAuthCookies(response, {
    ...singleClusterConfig,
    siteURL: "https://cloud.sandbox0.ai",
    cookieDomains: ["sandbox0.ai"],
  }, {
    access_token: accessToken,
    refresh_token: "refresh-token",
    expires_at: expiresAt,
    regional_session: {
      region_id: "aws/us-east-1",
      regional_gateway_url: "https://use1.example.com",
      token: createUnsignedJWT({ exp: Math.floor(Date.now() / 1000) + 900 }),
      expires_at: Math.floor(Date.now() / 1000) + 900,
    },
  });

  const setCookieHeader = response.headers.get("set-cookie") ?? "";
  assert.match(
    setCookieHeader,
    /sandbox0_refresh_token=refresh-token; Path=\/; Domain=sandbox0\.ai/i,
  );
  assert.match(
    setCookieHeader,
    /sandbox0_access_token=.*; Path=\/; .*Domain=sandbox0\.ai/i,
  );
});

test("setDashboardAuthCookies falls back to session cookies when expires_at is unusable", () => {
  const accessToken = createUnsignedJWT({
    exp: Math.floor(Date.now() / 1000) + 3600,
  });
  const response = NextResponse.json({ ok: true });
  setDashboardAuthCookies(response, singleClusterConfig, {
    access_token: accessToken,
    refresh_token: "refresh-token",
    expires_at: 1,
  });

  const accessCookie = response.cookies.get("sandbox0_access_token");
  assert.equal(accessCookie?.value, accessToken);
  assert.equal(accessCookie?.maxAge, 3600);
});

test("clearDashboardAuthCookies expires dashboard auth cookies", () => {
  const response = NextResponse.json({ ok: true });
  clearDashboardAuthCookies(response, singleClusterConfig);

  const accessCookie = response.cookies.get("sandbox0_access_token");
  const refreshCookie = response.cookies.get("sandbox0_refresh_token");
  const regionalAccessCookie = response.cookies.get(
    "sandbox0_regional_access_token",
  );
  assert.equal(accessCookie?.maxAge, 0);
  assert.equal(refreshCookie?.maxAge, 0);
  assert.equal(regionalAccessCookie?.maxAge, 0);
});

test("clearDashboardAuthCookies also expires configured parent-domain cookies", () => {
  const response = NextResponse.json({ ok: true });
  clearDashboardAuthCookies(response, {
    ...singleClusterConfig,
    siteURL: "https://cloud.sandbox0.ai",
    cookieDomains: ["sandbox0.ai"],
  });

  const setCookieHeader = response.headers.get("set-cookie") ?? "";
  assert.match(setCookieHeader, /sandbox0_refresh_token=;/);
  assert.match(setCookieHeader, /Domain=sandbox0\.ai/i);
  assert.match(setCookieHeader, /sandbox0_regional_access_token=;/);
});
