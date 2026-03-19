import React from "react";
import type { PixelBaseProps } from "../types";
import { cn, getPixelShadowClass } from "../utils";

export interface PixelCardProps
  extends PixelBaseProps,
    React.HTMLAttributes<HTMLDivElement> {
  children?: React.ReactNode;
  /** Card header content */
  header?: React.ReactNode;
  /** Card footer content */
  footer?: React.ReactNode;
  /** Whether the card is interactive (hoverable) */
  interactive?: boolean;
}

/**
 * PixelCard - A container component for grouping related content
 * 
 * Commonly used for dashboard widgets, feature showcases, and content cards.
 * Supports optional header and footer sections.
 * 
 * @example
 * ```tsx
 * <PixelCard 
 *   header={<h3>Sandbox Status</h3>}
 *   footer={<PixelButton>View Details</PixelButton>}
 *   interactive
 * >
 *   Your sandbox is running...
 * </PixelCard>
 * ```
 */
export function PixelCard({
  children,
  scale = "md",
  accent = false,
  header,
  footer,
  interactive = false,
  className,
  ...props
}: PixelCardProps) {
  return (
    <div
      className={cn(
        "bg-surface flex flex-col",
        getPixelShadowClass(scale, accent),
        interactive && "cursor-pointer transition-all duration-150",
        interactive && "hover:-translate-x-[2px] hover:-translate-y-[2px]",
        interactive && (accent ? "hover:shadow-pixel-md-accent" : "hover:shadow-pixel-md"),
        className
      )}
      {...props}
    >
      {header && (
        <div className="px-4 py-3 border-b border-foreground/15 font-pixel text-xs">
          {header}
        </div>
      )}
      <div className="p-4 flex-1">{children}</div>
      {footer && (
        <div className="px-4 py-3 border-t border-foreground/15">
          {footer}
        </div>
      )}
    </div>
  );
}
