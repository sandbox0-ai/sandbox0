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
import type { DashboardAuthProvider } from "@sandbox0/dashboard-core";

interface LoginViewProps {
  providers: DashboardAuthProvider[];
  loginError?: string;
}

export function LoginView({ providers, loginError }: LoginViewProps) {
  const oidcProviders = providers.filter((p) => p.type === "oidc");
  const builtinProvider = providers.find((p) => p.type === "builtin");

  function oidcHref(provider: DashboardAuthProvider): string {
    if (provider.externalAuthPortalUrl) {
      return provider.externalAuthPortalUrl;
    }
    return `/api/auth/oidc/${encodeURIComponent(provider.id)}/login`;
  }

  return (
    <PixelLayout>
      <div className="flex-1 flex items-center justify-center p-6">
        <div className="w-full max-w-sm space-y-6">
          {/* Logo */}
          <div className="flex flex-col items-center gap-3 mb-2">
            <Image
              src="/sandbox0.png"
              alt="Sandbox0"
              width={48}
              height={48}
              className="pixel-art"
              data-pixel
            />
            <PixelHeading as="h1" tone="site">SANDBOX0</PixelHeading>
            <p className="text-muted text-sm text-center">
              Sign in to your workspace
            </p>
          </div>

          {loginError && (
            <PixelBox className="border-red-500 text-red-400 text-sm px-4 py-3">
              {loginError}
            </PixelBox>
          )}

          {oidcProviders.length > 0 && (
            <div className="space-y-3">
              {oidcProviders.map((provider) => (
                <a
                  key={provider.id}
                  href={oidcHref(provider)}
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
              <div className="flex-1 h-px bg-foreground/10" />
              <span className="text-xs text-muted">or</span>
              <div className="flex-1 h-px bg-foreground/10" />
            </div>
          )}

          {builtinProvider && (
            <PixelCard>
              <form method="POST" action="/api/auth/login" className="space-y-4">
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
            <PixelBox className="text-muted text-sm px-4 py-3 text-center">
              No login providers are configured for this deployment.
            </PixelBox>
          )}
        </div>
      </div>
    </PixelLayout>
  );
}
