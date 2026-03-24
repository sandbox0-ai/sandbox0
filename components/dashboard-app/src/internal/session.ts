import type {
  DashboardActiveTeam,
  DashboardRuntimeConfig,
  DashboardSandboxSummary,
  DashboardSession,
  DashboardTeam,
  DashboardTemplateSummary,
  DashboardUser,
  DashboardVolumeSummary,
} from "./types";
import {
  type DashboardRegionalSession,
  dashboardAccessTokenCookieName,
  dashboardRegionalAccessTokenCookieName,
  dashboardRegionalExpiresAtCookieName,
  dashboardRegionalGatewayURLCookieName,
  dashboardRegionalRegionIDCookieName,
} from "./auth";
import {
  createDashboardControlPlaneSDK,
  readSDKResponseError,
  resolveSDKErrorMessage,
} from "./sdk";

export interface SessionAuthInput {
  bearerToken?: string;
  regionalSession?: DashboardRegionalSession;
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

function toVolumes(
  volumes: Array<{
    id: string;
    teamId: string;
    userId: string;
    sourceVolumeId?: string | null;
    cacheSize: string;
    bufferSize: string;
    accessMode?: string;
    writeback?: boolean;
    createdAt: Date;
    updatedAt: Date;
  }> = [],
): DashboardVolumeSummary[] {
  return volumes.map((volume) => ({
    id: volume.id,
    teamID: volume.teamId,
    userID: volume.userId,
    sourceVolumeID: volume.sourceVolumeId ?? null,
    cacheSize: volume.cacheSize,
    bufferSize: volume.bufferSize,
    accessMode: volume.accessMode as DashboardVolumeSummary["accessMode"],
    writeback: volume.writeback,
    createdAt: volume.createdAt.toISOString(),
    updatedAt: volume.updatedAt.toISOString(),
  }));
}

export interface DashboardRegionalAuthInfo {
  regionalURL: string;
  token: string;
}

export function resolveRegionalAuthFromCookies(
  config: DashboardRuntimeConfig,
  cookieStore: Pick<{ get(name: string): { value: string } | undefined }, "get">,
): DashboardRegionalAuthInfo | null {
  const token = readBearerToken(null, cookieStore);
  if (!token) {
    return null;
  }

  if (config.mode === "single-cluster") {
    const url = config.singleClusterURL;
    if (!url) {
      return null;
    }
    return { regionalURL: url, token };
  }

  const regionalToken = cookieStore.get(dashboardRegionalAccessTokenCookieName)?.value;
  const regionalURL = cookieStore.get(dashboardRegionalGatewayURLCookieName)?.value;
  const regionID = cookieStore.get(dashboardRegionalRegionIDCookieName)?.value;
  const expiresAt = Number(
    cookieStore.get(dashboardRegionalExpiresAtCookieName)?.value ?? "",
  );

  if (
    regionalToken &&
    regionalURL &&
    regionID &&
    Number.isFinite(expiresAt) &&
    expiresAt > Math.floor(Date.now() / 1000)
  ) {
    return { regionalURL, token: regionalToken };
  }

  return null;
}

function readRegionalGatewayURL(value: unknown): string | null {
  if (!value || typeof value !== "object") {
    return null;
  }

  const obj = value as Record<string, unknown>;
  const candidate =
    obj.regionalGatewayURL ??
    obj.regionalGatewayUrl ??
    obj.RegionalGatewayUrl;

  return typeof candidate === "string" && candidate.length > 0
    ? candidate
    : null;
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
    regionalGatewayURL: regionalURL,
  };
}

function deriveGlobalActiveTeam(
  user: DashboardUser,
  teams: DashboardTeam[],
  regionalSession?: DashboardRegionalSession,
): DashboardActiveTeam | undefined {
  const defaultTeam =
    teams.find((team) => team.id === user.defaultTeamID) ?? teams[0];
  if (!defaultTeam) {
    return undefined;
  }

  const homeRegionID =
    defaultTeam.homeRegionID ?? regionalSession?.region_id ?? null;
  if (!homeRegionID) {
    return undefined;
  }

  return {
    userID: user.id,
    teamID: defaultTeam.id,
    homeRegionID,
    defaultTeam: defaultTeam.id === user.defaultTeamID,
    regionalGatewayURL: regionalSession?.regional_gateway_url ?? null,
  };
}

function isRegionalSessionUsable(
  activeTeam: DashboardActiveTeam | undefined,
  regionalSession: DashboardRegionalSession | undefined,
): boolean {
  if (!activeTeam || !regionalSession) {
    return false;
  }
  if (!regionalSession.token || !regionalSession.regional_gateway_url) {
    return false;
  }
  return activeTeam.homeRegionID === regionalSession.region_id;
}

function resolveOnboardingRequirement(
  user: DashboardUser,
  teams: DashboardTeam[],
  regionalSession?: DashboardRegionalSession,
): { required: boolean; error?: string } {
  if (teams.length === 0) {
    return { required: true };
  }

  const activeTeam = deriveGlobalActiveTeam(user, teams, regionalSession);
  if (activeTeam) {
    return { required: false };
  }

  return {
    required: true,
    error:
      "Your workspace does not have a home region yet. Ask a system administrator to add a region, then complete onboarding.",
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
    siteURL: config.siteURL,
    configuredGlobalURL: config.globalGatewayURL,
    configuredRegionalURL:
      config.mode === "single-cluster" ? config.singleClusterURL : undefined,
    teams: [],
    sandboxes: [],
    templates: [],
    volumes: [],
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
      const [userResponse, teamResponse, sandboxResponse, templateResponse, volumeResponse] =
        await Promise.all([
          sdk.users.usersMeGet(),
          sdk.teams.teamsGet(),
          sdk.sandboxes.apiV1SandboxesGet({ limit: 5 }),
          sdk.templates.apiV1TemplatesGet(),
          sdk.volumes.apiV1SandboxvolumesGet(),
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
      const volumes = toVolumes(volumeResponse.data ?? []);

      return {
        ...baseSession,
        authenticated: true,
        configuredRegionalURL: baseURL,
        regionalToken: token,
        user,
        teams,
        activeTeam,
        sandboxes,
        templates,
        volumes,
      };
    } catch (error) {
      return {
        ...baseSession,
        errors: [await resolveSDKErrorMessage(error, "failed to resolve session")],
      };
    }
  }

  const globalURL = config.globalGatewayURL;
  if (!globalURL) {
    return {
      ...baseSession,
      errors: ["global-gateway mode is missing a global base URL"],
    };
  }

  try {
    const globalSDK = await createDashboardControlPlaneSDK(globalURL, {
      token,
      fetch: fetchImpl,
    });
    const [userResponse, teamResponse] = await Promise.all([
      globalSDK.users.usersMeGet(),
      globalSDK.teams.teamsGet(),
    ]);

    const userData = userResponse.data;
    if (!userData) {
      throw new Error("/users/me returned an empty response");
    }

    const user = toUser(userData);
    const teams = toTeams(teamResponse.data?.teams);

    const onboarding = resolveOnboardingRequirement(user, teams, auth.regionalSession);
    if (onboarding.required) {
      return {
        ...baseSession,
        authenticated: true,
        needsOnboarding: true,
        user,
        teams,
        errors: onboarding.error ? [onboarding.error] : [],
      };
    }

    let activeTeam = deriveGlobalActiveTeam(user, teams, auth.regionalSession);
    let regionalURL =
      isRegionalSessionUsable(activeTeam, auth.regionalSession)
        ? auth.regionalSession?.regional_gateway_url ?? undefined
        : undefined;
    let regionToken =
      isRegionalSessionUsable(activeTeam, auth.regionalSession)
        ? auth.regionalSession?.token ?? token
        : token;

    if (!regionalURL) {
      let activeTeamResponse;
      try {
        activeTeamResponse = await globalSDK.tenant.tenantActiveGet();
      } catch (error) {
        const response = await readSDKResponseError(error);
        if (response && (response.status === 400 || response.status === 409)) {
          return {
            ...baseSession,
            authenticated: true,
            needsOnboarding: true,
            user,
            teams,
            errors: [
              await resolveSDKErrorMessage(
                error,
                "Your workspace is missing a routable region. Ask a system administrator to finish region setup, then complete onboarding.",
              ),
            ],
          };
        }
        throw error;
      }
      const activeTeamData = activeTeamResponse.data;
      if (!activeTeamData) {
        throw new Error("/tenant/active returned an empty response");
      }

      activeTeam = {
        userID: activeTeamData.userId,
        teamID: activeTeamData.teamId,
        teamRole: activeTeamData.teamRole,
        homeRegionID: activeTeamData.homeRegionId,
        defaultTeam: Boolean(activeTeamData.defaultTeam),
        regionalGatewayURL: readRegionalGatewayURL(activeTeamData),
      };
      regionalURL = activeTeam.regionalGatewayURL ?? undefined;
    }

    if (regionalURL && regionToken === token) {
      const regionTokenResponse = await globalSDK.tenant.authRegionTokenPost({
        issueRegionTokenRequest: { teamId: activeTeam?.teamID },
      });
      const regionTokenData = regionTokenResponse.data;
      if (!regionTokenData) {
        throw new Error("/auth/region-token returned an empty response");
      }

      regionalURL = readRegionalGatewayURL(regionTokenData) ?? regionalURL;
      regionToken = regionTokenData.token;
    }

    const [sandboxResponse, templateResponse, volumeResponse] = await (regionalURL
      ? (async () => {
          const regionalSDK = await createDashboardControlPlaneSDK(regionalURL, {
            token: regionToken,
            fetch: fetchImpl,
          });
          return Promise.all([
            regionalSDK.sandboxes.apiV1SandboxesGet({ limit: 5 }),
            regionalSDK.templates.apiV1TemplatesGet(),
            regionalSDK.volumes.apiV1SandboxvolumesGet(),
          ]);
        })()
      : Promise.resolve([undefined, undefined, undefined] as const));

    return {
      ...baseSession,
      authenticated: true,
      configuredRegionalURL: regionalURL,
      regionalToken: regionToken !== token ? regionToken : undefined,
      user,
      teams,
      activeTeam,
      sandboxes: toSandboxes(sandboxResponse?.data?.sandboxes),
      templates: toTemplates(templateResponse?.data?.templates),
      volumes: toVolumes(volumeResponse?.data ?? []),
    };
  } catch (error) {
    return {
      ...baseSession,
      errors: [await resolveSDKErrorMessage(error, "failed to resolve session")],
    };
  }
}
