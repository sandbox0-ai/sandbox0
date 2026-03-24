export interface DashboardNavItem {
  href: string;
  label: string;
  scope: "shared" | "extension";
  description: string;
}

export const defaultDashboardNavigation: DashboardNavItem[] = [
  {
    href: "/",
    label: "Overview",
    scope: "shared",
    description: "Workspace entrypoint provided by the shared dashboard app.",
  },
  {
    href: "/volumes",
    label: "Volumes",
    scope: "shared",
    description: "Manage persistent sandbox volumes, snapshots, and forks.",
  },
];

export function extendDashboardNavigation(
  extensionItems: DashboardNavItem[],
): DashboardNavItem[] {
  return [...defaultDashboardNavigation, ...extensionItems];
}
