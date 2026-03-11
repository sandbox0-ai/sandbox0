export type DashboardControlPlaneMode = "single-cluster" | "global-directory";

export interface DashboardRuntimeConfig {
  mode: DashboardControlPlaneMode;
  dashboardBasePath: string;
  siteURL: string;
  singleClusterURL?: string;
  globalDirectoryURL?: string;
}

export type DashboardAuthProviderType = "oidc" | "builtin";

export interface DashboardAuthProvider {
  id: string;
  name: string;
  type: DashboardAuthProviderType;
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
  edgeGatewayURL?: string | null;
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

export interface DashboardSession {
  authenticated: boolean;
  mode: DashboardControlPlaneMode;
  dashboardBasePath: string;
  siteURL: string;
  configuredGlobalURL?: string;
  configuredRegionalURL?: string;
  user?: DashboardUser;
  teams: DashboardTeam[];
  activeTeam?: DashboardActiveTeam;
  sandboxes: DashboardSandboxSummary[];
  templates: DashboardTemplateSummary[];
  errors: string[];
}
