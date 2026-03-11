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
import { createDashboardControlPlaneSDK, resolveSDKErrorMessage } from "./sdk";

export interface SessionAuthInput {
  bearerToken?: string;
}

type FetchLike = typeof fetch;

function toUser(user: {
  id: string;
  email: string;
  name: string;
  avatarUrl?: string | null;
  defaultTeamId?: string | null;
  emailVerified: boolean;
  isAdmin: boolean;
}): DashboardUser {
  return {
    id: user.id,
    email: user.email,
    name: user.name,
    avatarUrl: user.avatarUrl ?? null,
    defaultTeamID: user.defaultTeamId ?? null,
    emailVerified: user.emailVerified,
    isAdmin: user.isAdmin,
  };
}

function toTeams(
  teams: Array<{
    id: string;
    name: string;
    slug: string;
    ownerId?: string | null;
    homeRegionId?: string | null;
  }> = [],
): DashboardTeam[] {
  return teams.map((team) => ({
    id: team.id,
    name: team.name,
    slug: team.slug,
    ownerID: team.ownerId ?? null,
    homeRegionID: team.homeRegionId ?? null,
  }));
}

function toSandboxes(
  sandboxes: Array<{
    id: string;
    templateId: string;
    status: string;
    paused: boolean;
    clusterId?: string | null;
    createdAt: Date;
    expiresAt: Date;
  }> = [],
): DashboardSandboxSummary[] {
  return sandboxes.map((sandbox) => ({
    id: sandbox.id,
    templateID: sandbox.templateId,
    status: sandbox.status,
    paused: sandbox.paused,
    clusterID: sandbox.clusterId ?? null,
    createdAt: sandbox.createdAt.toISOString(),
    expiresAt: sandbox.expiresAt.toISOString(),
  }));
}

function toTemplates(
  templates: Array<{
    templateId: string;
    scope: string;
    createdAt: Date;
  }> = [],
): DashboardTemplateSummary[] {
  return templates.map((template) => ({
    templateID: template.templateId,
    scope: template.scope,
    createdAt: template.createdAt.toISOString(),
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
      const sdk = await createDashboardControlPlaneSDK(baseURL, {
        token,
        fetch: fetchImpl,
      });
      const [userResponse, teamResponse, sandboxResponse, templateResponse] =
        await Promise.all([
          sdk.users.usersMeGet(),
          sdk.teams.teamsGet(),
          sdk.sandboxes.apiV1SandboxesGet({ limit: 5 }),
          sdk.templates.apiV1TemplatesGet(),
        ]);

      const userData = userResponse.data;
      if (!userData) {
        throw new Error("/users/me returned an empty response");
      }

      const user = toUser(userData);
      const teams = toTeams(teamResponse.data?.teams);
      const activeTeam = deriveSingleClusterActiveTeam(user, teams, baseURL);
      const sandboxes = toSandboxes(sandboxResponse.data?.sandboxes);
      const templates = toTemplates(templateResponse.data?.templates);

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
        errors: [await resolveSDKErrorMessage(error, "failed to resolve session")],
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
    const globalSDK = await createDashboardControlPlaneSDK(globalURL, {
      token,
      fetch: fetchImpl,
    });
    const [userResponse, teamResponse, activeTeamResponse] = await Promise.all([
      globalSDK.users.usersMeGet(),
      globalSDK.teams.teamsGet(),
      globalSDK.tenant.tenantActiveGet(),
    ]);

    const userData = userResponse.data;
    const activeTeamData = activeTeamResponse.data;
    if (!userData) {
      throw new Error("/users/me returned an empty response");
    }
    if (!activeTeamData) {
      throw new Error("/tenant/active returned an empty response");
    }

    const user = toUser(userData);
    const teams = toTeams(teamResponse.data?.teams);
    const activeTeam: DashboardActiveTeam = {
      userID: activeTeamData.userId,
      teamID: activeTeamData.teamId,
      teamRole: activeTeamData.teamRole,
      homeRegionID: activeTeamData.homeRegionId,
      defaultTeam: Boolean(activeTeamData.defaultTeam),
      edgeGatewayURL: activeTeamData.edgeGatewayUrl ?? null,
    };

    let regionalURL = activeTeam.edgeGatewayURL ?? undefined;
    let regionToken = token;
    if (regionalURL) {
      const regionTokenResponse = await globalSDK.tenant.authRegionTokenPost({
        issueRegionTokenRequest: { teamId: activeTeam.teamID },
      });
      const regionTokenData = regionTokenResponse.data;
      if (!regionTokenData) {
        throw new Error("/auth/region-token returned an empty response");
      }

      regionalURL = regionTokenData.edgeGatewayUrl ?? regionalURL;
      regionToken = regionTokenData.token;
    }

    const sandboxes = regionalURL
      ? await (
          await createDashboardControlPlaneSDK(regionalURL, {
            token: regionToken,
            fetch: fetchImpl,
          })
        ).sandboxes.apiV1SandboxesGet({ limit: 5 })
      : undefined;
    const templates = regionalURL
      ? await (
          await createDashboardControlPlaneSDK(regionalURL, {
            token: regionToken,
            fetch: fetchImpl,
          })
        ).templates.apiV1TemplatesGet()
      : undefined;

    return {
      ...baseSession,
      authenticated: true,
      configuredRegionalURL: regionalURL,
      user,
      teams,
      activeTeam,
      sandboxes: toSandboxes(sandboxes?.data?.sandboxes),
      templates: toTemplates(templates?.data?.templates),
    };
  } catch (error) {
    return {
      ...baseSession,
      errors: [await resolveSDKErrorMessage(error, "failed to resolve session")],
    };
  }
}
