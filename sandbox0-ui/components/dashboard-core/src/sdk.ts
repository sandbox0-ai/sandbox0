type FetchLike = typeof fetch;
type Sandbox0Module = typeof import("sandbox0");

export interface DashboardControlPlaneSDK {
  auth: InstanceType<Sandbox0Module["apis"]["AuthApi"]>;
  users: InstanceType<Sandbox0Module["apis"]["UsersApi"]>;
  teams: InstanceType<Sandbox0Module["apis"]["TeamsApi"]>;
  tenant: InstanceType<Sandbox0Module["apis"]["TenantApi"]>;
  regions: InstanceType<Sandbox0Module["apis"]["RegionsApi"]>;
  sandboxes: InstanceType<Sandbox0Module["apis"]["SandboxesApi"]>;
  templates: InstanceType<Sandbox0Module["apis"]["TemplatesApi"]>;
}

async function loadSandbox0(): Promise<Sandbox0Module> {
  return import("sandbox0");
}

export async function createDashboardControlPlaneSDK(
  baseURL: string,
  options?: {
    token?: string;
    fetch?: FetchLike;
  },
): Promise<DashboardControlPlaneSDK> {
  const { apis, runtime } = await loadSandbox0();
  const configuration = new runtime.Configuration({
    basePath: baseURL,
    accessToken: options?.token ? async () => options.token ?? "" : undefined,
    fetchApi: options?.fetch,
  });

  return {
    auth: new apis.AuthApi(configuration),
    users: new apis.UsersApi(configuration),
    teams: new apis.TeamsApi(configuration),
    tenant: new apis.TenantApi(configuration),
    regions: new apis.RegionsApi(configuration),
    sandboxes: new apis.SandboxesApi(configuration),
    templates: new apis.TemplatesApi(configuration),
  };
}

export async function resolveSDKErrorMessage(
  error: unknown,
  fallback: string,
): Promise<string> {
  const { runtime } = await loadSandbox0();

  if (error instanceof runtime.ResponseError) {
    const payload = (await error.response
      .clone()
      .json()
      .catch(() => null)) as
      | {
          error?: {
            message?: string;
          };
        }
      | null;

    return payload?.error?.message ?? fallback;
  }

  return error instanceof Error ? error.message : fallback;
}
