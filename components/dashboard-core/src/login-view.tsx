"use client";

import Image from "next/image";
import {
  PixelBox,
  PixelButton,
  PixelCard,
  PixelHeading,
  PixelInput,
  PixelLayout,
} from "@sandbox0/ui";

import type { DashboardAuthProvider } from "./types";
import { resolveDashboardProviderLoginTarget } from "./browser-auth-links";

export interface DashboardLoginViewProps {
  providers: DashboardAuthProvider[];
  loginError?: string;
  logoSrc?: string;
  brandName?: string;
  title?: string;
  builtinLoginPath?: string;
}

export function DashboardLoginView({
  providers,
  loginError,
  logoSrc = "/sandbox0.png",
  brandName = "SANDBOX0",
  title = "Sign in to your workspace",
  builtinLoginPath = "/api/auth/login",
}: DashboardLoginViewProps) {
  const oidcProviders = providers.filter((provider) => provider.type === "oidc");
  const builtinProvider = providers.find((provider) => provider.type === "builtin");

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
            <p className="text-center text-sm text-muted">{title}</p>
          </div>

          {loginError && (
            <PixelBox className="border-red-500 px-4 py-3 text-sm text-red-400">
              {loginError}
            </PixelBox>
          )}

          {oidcProviders.length > 0 && (
            <div className="space-y-3">
              {oidcProviders.map((provider) => (
                <a
                  key={provider.id}
                  href={resolveDashboardProviderLoginTarget(provider)}
                  className="block w-full"
                >
                  <PixelButton
                    variant="secondary"
                    scale="md"
                    className="w-full"
                  >
                    Continue with {provider.name}
                  </PixelButton>
                </a>
              ))}
            </div>
          )}

          {oidcProviders.length > 0 && builtinProvider && (
            <div className="flex items-center gap-3">
              <div className="h-px flex-1 bg-foreground/10" />
              <span className="text-xs text-muted">or</span>
              <div className="h-px flex-1 bg-foreground/10" />
            </div>
          )}

          {builtinProvider && (
            <PixelCard>
              <form
                method="POST"
                action={builtinLoginPath}
                className="space-y-4"
              >
                <PixelInput
                  label="Email"
                  name="email"
                  type="email"
                  placeholder="you@example.com"
                  autoComplete="email"
                  required
                />
                <PixelInput
                  label="Password"
                  name="password"
                  type="password"
                  placeholder="••••••••"
                  autoComplete="current-password"
                  required
                />
                <PixelButton
                  type="submit"
                  variant="primary"
                  scale="md"
                  className="w-full"
                >
                  Sign In
                </PixelButton>
              </form>
            </PixelCard>
          )}

          {providers.length === 0 && (
            <PixelBox className="px-4 py-3 text-center text-sm text-muted">
              No login providers are configured for this deployment.
            </PixelBox>
          )}
        </div>
      </div>
    </PixelLayout>
  );
}
