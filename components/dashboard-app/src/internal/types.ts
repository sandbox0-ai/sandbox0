export type DashboardControlPlaneMode = "single-cluster" | "global-gateway";

export interface DashboardRuntimeConfig {
  mode: DashboardControlPlaneMode;
  siteURL: string;
  singleClusterURL?: string;
  globalGatewayURL?: string;
}

export type DashboardAuthProviderType = "oidc" | "builtin";

export interface DashboardAuthProvider {
  id: string;
  name: string;
  type: DashboardAuthProviderType;
  /**
   * When set, browser login for this provider should redirect to this external URL
   * instead of initiating the OIDC flow through the standard /api/auth/oidc/{id}/login route.
   * Used for deployments that host their own authorization portal.
   */
  externalAuthPortalUrl?: string;
}

export interface DashboardUser {
  id: string;
  email: string;
  name: string;
  avatarUrl?: string | null;
  defaultTeamID?: string | null;
  emailVerified: boolean;
  isAdmin: boolean;
}

export interface DashboardTeam {
  id: string;
  name: string;
  slug: string;
  ownerID?: string | null;
  homeRegionID?: string | null;
}

export interface DashboardActiveTeam {
  userID: string;
  teamID: string;
  teamRole?: string;
  homeRegionID: string;
  defaultTeam: boolean;
  regionalGatewayURL?: string | null;
}

export interface DashboardSandboxSummary {
  id: string;
  templateID: string;
  status: string;
  paused: boolean;
  clusterID?: string | null;
  createdAt: string;
  expiresAt: string;
}

export interface DashboardTemplateSummary {
  templateID: string;
  scope: string;
  createdAt: string;
}

export type DashboardVolumeAccessMode = "RWO" | "ROX" | "RWX";

export interface DashboardVolumeSummary {
  id: string;
  teamID: string;
  userID: string;
  sourceVolumeID?: string | null;
  cacheSize: string;
  bufferSize: string;
  accessMode?: DashboardVolumeAccessMode;
  writeback?: boolean;
  createdAt: string;
  updatedAt: string;
}

export interface DashboardSession {
  authenticated: boolean;
  mode: DashboardControlPlaneMode;
  siteURL: string;
  configuredGlobalURL?: string;
  configuredRegionalURL?: string;
  regionalToken?: string;
  user?: DashboardUser;
  teams: DashboardTeam[];
  activeTeam?: DashboardActiveTeam;
  sandboxes: DashboardSandboxSummary[];
  templates: DashboardTemplateSummary[];
  volumes: DashboardVolumeSummary[];
  errors: string[];
}
