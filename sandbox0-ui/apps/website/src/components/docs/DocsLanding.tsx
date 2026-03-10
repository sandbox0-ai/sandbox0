import React from "react";
import { PixelCard, PixelButton, PixelBadge } from "@sandbox0/ui";
import { DocsLink } from "./DocsLink";

export function DocsHero({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="mb-12">
      <h1 className="font-pixel text-3xl mb-4">{title}</h1>
      <div className="text-xl text-muted leading-relaxed">{children}</div>
    </div>
  );
}

export function CardGrid({ children }: { children: React.ReactNode }) {
  return <div className="grid grid-cols-1 md:grid-cols-2 gap-6">{children}</div>;
}

export function LinkCard({
  title,
  href,
  cta = "Learn More",
  children,
}: {
  title: string;
  href: string;
  cta?: string;
  children: React.ReactNode;
}) {
  return (
    <PixelCard scale="md" interactive>
      <div className="mb-3">
        <h3 className="font-pixel text-xs mb-2">{title}</h3>
        <div className="text-sm text-muted mb-4">{children}</div>
        <PixelButton variant="secondary" scale="sm">
          <DocsLink href={href}>{cta} →</DocsLink>
        </PixelButton>
      </div>
    </PixelCard>
  );
}

export function ResourceList({ children }: { children: React.ReactNode }) {
  return <div className="space-y-4">{children}</div>;
}

export function ResourceItem({
  badge,
  description,
  href,
  cta = "Docs →",
}: {
  badge: string;
  description: React.ReactNode;
  href: string;
  cta?: string;
}) {
  return (
    <div className="flex items-center gap-4">
      <PixelBadge variant="accent" size="md">
        {badge}
      </PixelBadge>
      <span className="text-sm text-muted">{description}</span>
      <DocsLink
        href={href}
        className="ml-auto text-accent text-sm font-medium hover:text-foreground transition-colors"
      >
        {cta}
      </DocsLink>
    </div>
  );
}
