"use client";

import React from "react";
import { cn } from "../utils";

export type PixelHeadingTone = "site" | "docs";
export type PixelHeadingAs = "h1" | "h2" | "h3" | "h4" | "h5" | "h6";

export interface PixelHeadingProps extends React.HTMLAttributes<HTMLHeadingElement> {
  /**
   * Which semantic heading tag to render.
   * Defaults to `h2`.
   */
  as?: PixelHeadingAs;
  /**
   * Visual preset.
   * - site: landing/marketing pages
   * - docs: documentation pages (smaller pixel sizes, better scanability)
   */
  tone?: PixelHeadingTone;
  /**
   * Optional leading element (e.g. a decorative "#" for docs H1).
   */
  leading?: React.ReactNode;
  /** Optional className for the leading wrapper. */
  leadingClassName?: string;
}

const docsClasses: Record<PixelHeadingAs, string> = {
  h1: "font-pixel text-xl md:text-2xl mt-4 mb-8 first:mt-0 text-foreground tracking-tight leading-relaxed scroll-mt-[80px]",
  h2: "font-pixel text-sm md:text-base mt-8 mb-4 pb-2 border-b border-foreground/15 text-foreground scroll-mt-[80px]",
  h3: "font-pixel text-[10px] md:text-xs mt-6 mb-3 text-foreground/90 uppercase tracking-widest scroll-mt-[80px]",
  h4: "font-medium text-sm mt-4 mb-2 text-foreground scroll-mt-[80px]",
  h5: "font-medium text-sm mt-4 mb-2 text-foreground scroll-mt-[80px]",
  h6: "font-medium text-sm mt-4 mb-2 text-foreground scroll-mt-[80px]",
};

const siteClasses: Record<PixelHeadingAs, string> = {
  h1: "font-pixel text-2xl md:text-4xl leading-relaxed text-foreground",
  h2: "font-pixel text-xl md:text-2xl leading-relaxed text-foreground",
  h3: "font-pixel text-sm md:text-base leading-relaxed text-foreground",
  h4: "font-pixel text-xs leading-relaxed text-foreground",
  h5: "font-pixel text-xs leading-relaxed text-foreground",
  h6: "font-pixel text-xs leading-relaxed text-foreground",
};

export function PixelHeading({
  as = "h2",
  tone = "docs",
  leading,
  leadingClassName,
  className,
  children,
  ...props
}: PixelHeadingProps) {
  const Tag: React.ElementType = as;
  const base = tone === "site" ? siteClasses[as] : docsClasses[as];

  return (
    <Tag className={cn(base, className)} {...props}>
      {leading ? <span className={leadingClassName}>{leading}</span> : null}
      {children}
    </Tag>
  );
}

