import type {
  DashboardActiveTeam,
  DashboardRuntimeConfig,
  DashboardSandboxSummary,
  DashboardSession,
  DashboardTeam,
  DashboardTemplateSummary,
  DashboardUser,
} from "./types";
import { dashboardAccessTokenCookieName } from "./auth";

export interface SessionAuthInput {
  bearerToken?: string;
}

type FetchLike = typeof fetch;

interface Envelope<T> {
  data?: T;
  error?: {
    message?: string;
  };
}

interface TeamListResponse {
  teams?: Array<{
    id: string;
    name: string;
    slug: string;
    owner_id?: string | null;
    home_region_id?: string | null;
  }>;
}

interface SandboxListResponse {
  sandboxes?: Array<{
    id: string;
    template_id: string;
    status: string;
    paused: boolean;
    cluster_id?: string | null;
    created_at: string;
    expires_at: string;
  }>;
}

interface TemplateListResponse {
  templates?: Array<{
    template_id: string;
    scope: string;
    created_at: string;
  }>;
}

interface RegionTokenResponse {
  region_id: string;
  edge_gateway_url?: string | null;
  token: string;
  expires_at: number;
}

interface UserResponse {
  id: string;
  email: string;
  name: string;
  avatar_url?: string | null;
  default_team_id?: string | null;
  email_verified: boolean;
  is_admin: boolean;
}

interface ActiveTeamResponse {
  user_id: string;
  team_id: string;
  team_role?: string;
  home_region_id: string;
  default_team?: boolean;
  edge_gateway_url?: string | null;
}

function joinURL(baseURL: string, path: string): string {
  const base = new URL(baseURL);
  const normalizedPath = path.startsWith("/") ? path : `/${path}`;
  return new URL(
    normalizedPath,
    `${base.toString().replace(/\/$/, "")}/`,
  ).toString();
}

async function fetchEnvelope<T>(
  fetchImpl: FetchLike,
  baseURL: string,
  path: string,
  token: string,
  init?: RequestInit,
): Promise<T> {
  const headers = new Headers(init?.headers ?? {});
  headers.set("Authorization", `Bearer ${token}`);
  if (init?.body && !headers.has("Content-Type")) {
    headers.set("Content-Type", "application/json");
  }

  const response = await fetchImpl(joinURL(baseURL, path), {
    ...init,
    headers,
    cache: "no-store",
  });

  const payload = (await response
    .json()
    .catch(() => null)) as Envelope<T> | null;
  if (!response.ok) {
    throw new Error(
      payload?.error?.message ?? `${path} returned ${response.status}`,
    );
  }
  if (!payload?.data) {
    throw new Error(`${path} returned an empty response`);
  }
  return payload.data;
}

function toUser(user: UserResponse): DashboardUser {
  return {
    id: user.id,
    email: user.email,
    name: user.name,
    avatarUrl: user.avatar_url ?? null,
    defaultTeamID: user.default_team_id ?? null,
    emailVerified: user.email_verified,
    isAdmin: user.is_admin,
  };
}

function toTeams(data: TeamListResponse | undefined): DashboardTeam[] {
  return (data?.teams ?? []).map((team) => ({
    id: team.id,
    name: team.name,
    slug: team.slug,
    ownerID: team.owner_id ?? null,
    homeRegionID: team.home_region_id ?? null,
  }));
}

function toSandboxes(
  data: SandboxListResponse | undefined,
): DashboardSandboxSummary[] {
  return (data?.sandboxes ?? []).map((sandbox) => ({
    id: sandbox.id,
    templateID: sandbox.template_id,
    status: sandbox.status,
    paused: sandbox.paused,
    clusterID: sandbox.cluster_id ?? null,
    createdAt: sandbox.created_at,
    expiresAt: sandbox.expires_at,
  }));
}

function toTemplates(
  data: TemplateListResponse | undefined,
): DashboardTemplateSummary[] {
  return (data?.templates ?? []).map((template) => ({
    templateID: template.template_id,
    scope: template.scope,
    createdAt: template.created_at,
  }));
}

function deriveSingleClusterActiveTeam(
  user: DashboardUser,
  teams: DashboardTeam[],
  regionalURL: string,
): DashboardActiveTeam | undefined {
  const defaultTeam =
    teams.find((team) => team.id === user.defaultTeamID) ?? teams[0];
  if (!defaultTeam) {
    return undefined;
  }

  return {
    userID: user.id,
    teamID: defaultTeam.id,
    homeRegionID: defaultTeam.homeRegionID ?? "local",
    defaultTeam: defaultTeam.id === user.defaultTeamID,
    edgeGatewayURL: regionalURL,
  };
}

export function readBearerToken(
  authorizationHeader: string | null,
  cookies: Pick<{ get(name: string): { value: string } | undefined }, "get">,
): string | undefined {
  if (authorizationHeader?.startsWith("Bearer ")) {
    return authorizationHeader.slice("Bearer ".length).trim();
  }

  const cookieNames = [
    "__Host-sandbox0_access_token",
    dashboardAccessTokenCookieName,
    "sandbox0_token",
  ];
  for (const cookieName of cookieNames) {
    const token = cookies.get(cookieName)?.value?.trim();
    if (token) {
      return token;
    }
  }

  return undefined;
}

export async function resolveDashboardSession(
  config: DashboardRuntimeConfig,
  auth: SessionAuthInput,
  fetchImpl: FetchLike = fetch,
): Promise<DashboardSession> {
  const baseSession: DashboardSession = {
    authenticated: false,
    mode: config.mode,
    dashboardBasePath: config.dashboardBasePath,
    siteURL: config.siteURL,
    configuredGlobalURL: config.globalDirectoryURL,
    configuredRegionalURL:
      config.mode === "single-cluster" ? config.singleClusterURL : undefined,
    teams: [],
    sandboxes: [],
    templates: [],
    errors: [],
  };

  const token = auth.bearerToken?.trim();
  if (!token) {
    return baseSession;
  }

  if (config.mode === "single-cluster") {
    const baseURL = config.singleClusterURL;
    if (!baseURL) {
      return {
        ...baseSession,
        errors: ["single-cluster mode is missing a control-plane base URL"],
      };
    }

    try {
      const user = toUser(
        await fetchEnvelope<UserResponse>(
          fetchImpl,
          baseURL,
          "/users/me",
          token,
        ),
      );
      const teams = toTeams(
        await fetchEnvelope<TeamListResponse>(
          fetchImpl,
          baseURL,
          "/teams",
          token,
        ),
      );
      const activeTeam = deriveSingleClusterActiveTeam(user, teams, baseURL);
      const sandboxes = toSandboxes(
        await fetchEnvelope<SandboxListResponse>(
          fetchImpl,
          baseURL,
          "/api/v1/sandboxes?limit=5",
          token,
        ),
      );
      const templates = toTemplates(
        await fetchEnvelope<TemplateListResponse>(
          fetchImpl,
          baseURL,
          "/api/v1/templates",
          token,
        ),
      );

      return {
        ...baseSession,
        authenticated: true,
        configuredRegionalURL: baseURL,
        user,
        teams,
        activeTeam,
        sandboxes,
        templates,
      };
    } catch (error) {
      return {
        ...baseSession,
        errors: [
          error instanceof Error ? error.message : "failed to resolve session",
        ],
      };
    }
  }

  const globalURL = config.globalDirectoryURL;
  if (!globalURL) {
    return {
      ...baseSession,
      errors: ["global-directory mode is missing a global base URL"],
    };
  }

  try {
    const user = toUser(
      await fetchEnvelope<UserResponse>(
        fetchImpl,
        globalURL,
        "/users/me",
        token,
      ),
    );
    const teams = toTeams(
      await fetchEnvelope<TeamListResponse>(
        fetchImpl,
        globalURL,
        "/teams",
        token,
      ),
    );
    const activeTeamData = await fetchEnvelope<ActiveTeamResponse>(
      fetchImpl,
      globalURL,
      "/tenant/active",
      token,
    );
    const activeTeam: DashboardActiveTeam = {
      userID: activeTeamData.user_id,
      teamID: activeTeamData.team_id,
      teamRole: activeTeamData.team_role,
      homeRegionID: activeTeamData.home_region_id,
      defaultTeam: Boolean(activeTeamData.default_team),
      edgeGatewayURL: activeTeamData.edge_gateway_url ?? null,
    };

    let regionalURL = activeTeam.edgeGatewayURL ?? undefined;
    let regionToken = token;
    if (regionalURL) {
      const regionTokenData = await fetchEnvelope<RegionTokenResponse>(
        fetchImpl,
        globalURL,
        "/auth/region-token",
        token,
        {
          method: "POST",
          body: JSON.stringify({ team_id: activeTeam.teamID }),
        },
      );
      regionalURL = regionTokenData.edge_gateway_url ?? regionalURL;
      regionToken = regionTokenData.token;
    }

    const sandboxes = regionalURL
      ? toSandboxes(
          await fetchEnvelope<SandboxListResponse>(
            fetchImpl,
            regionalURL,
            "/api/v1/sandboxes?limit=5",
            regionToken,
          ),
        )
      : [];
    const templates = regionalURL
      ? toTemplates(
          await fetchEnvelope<TemplateListResponse>(
            fetchImpl,
            regionalURL,
            "/api/v1/templates",
            regionToken,
          ),
        )
      : [];

    return {
      ...baseSession,
      authenticated: true,
      configuredRegionalURL: regionalURL,
      user,
      teams,
      activeTeam,
      sandboxes,
      templates,
    };
  } catch (error) {
    return {
      ...baseSession,
      errors: [
        error instanceof Error ? error.message : "failed to resolve session",
      ],
    };
  }
}
