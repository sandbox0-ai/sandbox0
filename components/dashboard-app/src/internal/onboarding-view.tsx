"use client";

import Image from "next/image";
import { useState } from "react";
import {
  PixelBox,
  PixelButton,
  PixelCard,
  PixelHeading,
  PixelInput,
  PixelLayout,
  PixelSelect,
} from "@sandbox0/ui";

import type { DashboardRegion } from "./types";

interface RegionOption {
  value: string;
  label: string;
}

export interface DashboardOnboardingViewProps {
  onboardingError?: string;
  userEmail?: string;
  logoSrc?: string;
  brandName?: string;
  onboardingPath?: string;
  regions?: DashboardRegion[];
}

export function DashboardOnboardingView({
  onboardingError,
  userEmail,
  logoSrc = "/sandbox0.png",
  brandName = "SANDBOX0",
  onboardingPath = "/api/auth/onboarding",
  regions = [],
}: DashboardOnboardingViewProps) {
  const enabledRegions = regions.filter((r) => r.enabled);
  const regionOptions: RegionOption[] = enabledRegions.map((r) => ({
    value: r.id,
    label: r.displayName ? `${r.displayName} (${r.id})` : r.id,
  }));

  const [selectedRegion, setSelectedRegion] = useState(
    regionOptions[0]?.value ?? "",
  );

  const noRegionsAvailable = regionOptions.length === 0;

  return (
    <PixelLayout>
      <div className="flex-1 flex items-center justify-center p-6">
        <div className="w-full max-w-sm space-y-6">
          <div className="mb-2 flex flex-col items-center gap-3">
            <Image
              src={logoSrc}
              alt={brandName}
              width={48}
              height={48}
              className="pixel-art"
              data-pixel
            />
            <PixelHeading as="h1" tone="site">{brandName}</PixelHeading>
            <p className="text-center text-sm text-muted">
              {userEmail ? `Welcome, ${userEmail}.` : "Welcome."}{" "}
              {noRegionsAvailable
                ? "Your account is ready."
                : "Create your first team to get started."}
            </p>
          </div>

          {onboardingError && (
            <PixelBox className="border-red-500 px-4 py-3 text-sm text-red-400">
              {onboardingError}
            </PixelBox>
          )}

          {noRegionsAvailable ? (
            <PixelCard>
              <p className="text-sm text-muted text-center">
                No regions are available yet. Please contact your system
                administrator to add a region before you can create a team.
              </p>
            </PixelCard>
          ) : (
            <PixelCard>
              <form method="POST" action={onboardingPath} className="space-y-4">
                <PixelInput
                  label="Team Name"
                  name="team_name"
                  type="text"
                  placeholder="My Team"
                  required
                />

                <div>
                  <label className="block text-[10px] font-pixel uppercase tracking-[0.06em] mb-1.5 text-muted">
                    Home Region
                  </label>
                  <PixelSelect
                    options={regionOptions}
                    value={selectedRegion}
                    onValueChange={setSelectedRegion}
                    ariaLabel="Select home region"
                    scale="md"
                    className="w-full"
                  />
                  <input
                    type="hidden"
                    name="home_region_id"
                    value={selectedRegion}
                  />
                  <p className="mt-1.5 text-[10px] text-muted">
                    The region where your team&apos;s sandboxes will run.
                  </p>
                </div>

                <PixelButton
                  type="submit"
                  variant="primary"
                  scale="md"
                  className="w-full"
                >
                  Create Team
                </PixelButton>
              </form>
            </PixelCard>
          )}
        </div>
      </div>
    </PixelLayout>
  );
}
