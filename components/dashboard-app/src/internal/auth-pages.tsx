import { cookies } from "next/headers";
import { redirect } from "next/navigation";

import { resolveDashboardHomeEntry, resolveDashboardLoginEntry } from "./browser-auth";
import { DashboardLoginView } from "./login-view";
import type { DashboardRuntimeConfig, DashboardSession } from "./types";

export interface DashboardPageSearchParams {
  searchParams: Promise<{ login_error?: string }>;
}

export interface DashboardLoginViewOptions {
  logoSrc?: string;
  brandName?: string;
  title?: string;
  builtinLoginPath?: string;
}

export type DashboardConfigResolver = () => DashboardRuntimeConfig;

export function createDashboardLoginPage(
  resolveConfig: DashboardConfigResolver,
  options?: DashboardLoginViewOptions,
) {
  return async function DashboardLoginPage({
    searchParams,
  }: DashboardPageSearchParams) {
    const { login_error: loginError } = await searchParams;
    const result = await resolveDashboardLoginEntry(
      resolveConfig(),
      await cookies(),
      { loginError },
    );

    if (result.kind === "redirect") {
      redirect(result.location);
    }

    return (
      <DashboardLoginView
        providers={result.providers}
        loginError={result.loginError}
        {...options}
      />
    );
  };
}

export async function requireDashboardHomeRender(
  resolveConfig: DashboardConfigResolver,
  searchParams: Promise<{ login_error?: string }>,
): Promise<DashboardSession> {
  const { login_error: loginError } = await searchParams;
  const result = await resolveDashboardHomeEntry(
    resolveConfig(),
    await cookies(),
    { loginError },
  );

  if (result.kind === "redirect") {
    redirect(result.location);
  }

  return result.session;
}

export async function requireDashboardAuth(
  resolveConfig: DashboardConfigResolver,
): Promise<DashboardSession> {
  const result = await resolveDashboardHomeEntry(
    resolveConfig(),
    await cookies(),
  );

  if (result.kind === "redirect") {
    redirect(result.location);
  }

  return result.session;
}
