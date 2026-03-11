import { NextResponse } from "next/server";

import {
  resolveDashboardAuthProviders,
  resolveDashboardRuntimeConfig,
} from "@sandbox0/dashboard-core";

export async function GET() {
  const config = resolveDashboardRuntimeConfig();
  const providers = await resolveDashboardAuthProviders(config);
  return NextResponse.json(providers);
}
