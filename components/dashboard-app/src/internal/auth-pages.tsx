import { cookies } from "next/headers";
import { redirect } from "next/navigation";

import {
  resolveDashboardHomeEntry,
  resolveDashboardLoginEntry,
  resolveDashboardOnboardingEntry,
} from "./browser-auth";
import { DashboardLoginView } from "./login-view";
import { DashboardOnboardingView } from "./onboarding-view";
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

export interface DashboardOnboardingViewOptions {
  logoSrc?: string;
  brandName?: string;
  onboardingPath?: string;
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

export interface DashboardOnboardingPageSearchParams {
  searchParams: Promise<{ onboarding_error?: string }>;
}

export function createDashboardOnboardingPage(
  resolveConfig: DashboardConfigResolver,
  options?: DashboardOnboardingViewOptions,
) {
  return async function DashboardOnboardingPage({
    searchParams,
  }: DashboardOnboardingPageSearchParams) {
    const { onboarding_error: onboardingError } = await searchParams;
    const result = await resolveDashboardOnboardingEntry(
      resolveConfig(),
      await cookies(),
    );

    if (result.kind === "redirect") {
      redirect(result.location);
    }

    return (
      <DashboardOnboardingView
        onboardingError={onboardingError ?? result.session.errors[0]}
        userEmail={result.session.user?.email}
        regions={result.regions}
        {...options}
      />
    );
  };
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
