import { PixelLayout } from "@sandbox0/ui";

import {
  requireDashboardAuth,
  type DashboardConfigResolver,
} from "./internal/auth-pages";
import { DashboardVolumesView } from "./internal/volumes-view";

export interface DashboardVolumesPageOptions {
  brandName?: string;
}

export function createDashboardVolumesPage(
  resolveConfig: DashboardConfigResolver,
  options?: DashboardVolumesPageOptions,
) {
  return async function DashboardVolumesPage() {
    const session = await requireDashboardAuth(resolveConfig);

    return (
      <PixelLayout>
        <DashboardVolumesView
          volumes={session.volumes}
          brandName={options?.brandName}
        />
      </PixelLayout>
    );
  };
}
