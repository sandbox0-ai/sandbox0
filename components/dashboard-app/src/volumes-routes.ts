import { cookies } from "next/headers";
import { NextResponse } from "next/server";

import type { DashboardConfigResolver } from "./internal/auth-pages";
import { createDashboardControlPlaneSDK, resolveSDKErrorMessage } from "./internal/sdk";
import { readBearerToken, resolveRegionalAuthFromCookies } from "./internal/session";

function unauthorizedResponse() {
  return NextResponse.json(
    { error: { message: "Unauthorized" } },
    { status: 401 },
  );
}

function noRegionalUrlResponse() {
  return NextResponse.json(
    { error: { message: "No regional gateway URL available for this session" } },
    { status: 503 },
  );
}

async function resolveVolumesSDK(resolveConfig: DashboardConfigResolver) {
  const cookieStore = await cookies();
  const config = resolveConfig();
  const auth = resolveRegionalAuthFromCookies(config, cookieStore);

  if (!auth) {
    const token = readBearerToken(null, cookieStore);
    if (!token) {
      return { sdk: null, unauthorized: true };
    }
    return { sdk: null, noRegion: true };
  }

  const sdk = await createDashboardControlPlaneSDK(auth.regionalURL, {
    token: auth.token,
  });

  return { sdk };
}

export function createDashboardVolumesRoute(resolveConfig: DashboardConfigResolver) {
  return {
    async GET() {
      const { sdk, unauthorized, noRegion } = await resolveVolumesSDK(resolveConfig);

      if (unauthorized) return unauthorizedResponse();
      if (noRegion || !sdk) return noRegionalUrlResponse();

      try {
        const response = await sdk.volumes.apiV1SandboxvolumesGet();
        return NextResponse.json({ data: response.data ?? [] });
      } catch (error) {
        const message = await resolveSDKErrorMessage(error, "Failed to list volumes");
        return NextResponse.json({ error: { message } }, { status: 500 });
      }
    },

    async POST(request: Request) {
      const { sdk, unauthorized, noRegion } = await resolveVolumesSDK(resolveConfig);

      if (unauthorized) return unauthorizedResponse();
      if (noRegion || !sdk) return noRegionalUrlResponse();

      try {
        const body = (await request.json().catch(() => null)) as Record<
          string,
          unknown
        > | null;

        const response = await sdk.volumes.apiV1SandboxvolumesPost({
          createSandboxVolumeRequest: {
            cacheSize: typeof body?.cache_size === "string" ? body.cache_size : undefined,
            bufferSize:
              typeof body?.buffer_size === "string" ? body.buffer_size : undefined,
            writeback:
              typeof body?.writeback === "boolean" ? body.writeback : undefined,
          },
        });

        return NextResponse.json({ data: response.data }, { status: 201 });
      } catch (error) {
        const message = await resolveSDKErrorMessage(error, "Failed to create volume");
        return NextResponse.json({ error: { message } }, { status: 500 });
      }
    },
  };
}

export function createDashboardVolumeRoute(resolveConfig: DashboardConfigResolver) {
  return {
    async GET(_request: Request, { params }: { params: Promise<{ id: string }> }) {
      const { id } = await params;
      const { sdk, unauthorized, noRegion } = await resolveVolumesSDK(resolveConfig);

      if (unauthorized) return unauthorizedResponse();
      if (noRegion || !sdk) return noRegionalUrlResponse();

      try {
        const response = await sdk.volumes.apiV1SandboxvolumesIdGet({ id });
        return NextResponse.json({ data: response.data });
      } catch (error) {
        const message = await resolveSDKErrorMessage(error, "Failed to get volume");
        return NextResponse.json({ error: { message } }, { status: 500 });
      }
    },

    async DELETE(_request: Request, { params }: { params: Promise<{ id: string }> }) {
      const { id } = await params;
      const { sdk, unauthorized, noRegion } = await resolveVolumesSDK(resolveConfig);

      if (unauthorized) return unauthorizedResponse();
      if (noRegion || !sdk) return noRegionalUrlResponse();

      try {
        await sdk.volumes.apiV1SandboxvolumesIdDelete({ id });
        return NextResponse.json({ data: { deleted: true } });
      } catch (error) {
        const message = await resolveSDKErrorMessage(error, "Failed to delete volume");
        return NextResponse.json({ error: { message } }, { status: 500 });
      }
    },
  };
}

export function createDashboardVolumeForkRoute(resolveConfig: DashboardConfigResolver) {
  return {
    async POST(_request: Request, { params }: { params: Promise<{ id: string }> }) {
      const { id } = await params;
      const { sdk, unauthorized, noRegion } = await resolveVolumesSDK(resolveConfig);

      if (unauthorized) return unauthorizedResponse();
      if (noRegion || !sdk) return noRegionalUrlResponse();

      try {
        const response = await sdk.volumes.apiV1SandboxvolumesIdForkPost({
          id,
          forkVolumeRequest: {},
        });
        return NextResponse.json({ data: response.data }, { status: 201 });
      } catch (error) {
        const message = await resolveSDKErrorMessage(error, "Failed to fork volume");
        return NextResponse.json({ error: { message } }, { status: 500 });
      }
    },
  };
}
