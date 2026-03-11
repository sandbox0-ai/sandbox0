export function resolveDashboardHref(): string {
  return process.env.NEXT_PUBLIC_DASHBOARD_URL ?? "/dashboard";
}
