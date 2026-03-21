import { cookies } from "next/headers";
import { redirect } from "next/navigation";

import {
  DashboardLoginView,
  resolveDashboardLoginEntry,
  resolveDashboardRuntimeConfig,
} from "@sandbox0/dashboard-core";

interface LoginPageProps {
  searchParams: Promise<{ login_error?: string }>;
}

export default async function LoginPage({ searchParams }: LoginPageProps) {
  const { login_error: loginError } = await searchParams;
  const config = resolveDashboardRuntimeConfig();
  const result = await resolveDashboardLoginEntry(
    config,
    await cookies(),
    { loginError },
  );

  if (result.kind === "redirect") {
    redirect(result.location);
  }

  return <DashboardLoginView providers={result.providers} loginError={result.loginError} />;
}
