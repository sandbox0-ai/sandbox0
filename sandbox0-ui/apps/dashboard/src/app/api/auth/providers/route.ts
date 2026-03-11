import { NextResponse } from "next/server";

import { resolveDashboardRuntimeConfig } from "@/lib/controlplane/config";
import { resolveDashboardAuthProviders } from "@/lib/controlplane/auth";

export async function GET() {
  const config = resolveDashboardRuntimeConfig();
  const providers = await resolveDashboardAuthProviders(config);
  return NextResponse.json(providers);
}
