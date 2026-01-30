import React from "react";
import type { PixelBaseProps } from "../types";
import { cn } from "../utils";

export interface PixelLayoutProps
  extends Omit<PixelBaseProps, "scale">,
    React.HTMLAttributes<HTMLDivElement> {
  children?: React.ReactNode;
  /** Whether to show the CRT scanline effect */
  scanlines?: boolean;
  /** Whether to show the outer frame border */
  framed?: boolean;
}

/**
 * PixelLayout - Page-level container with Level 3 (Macro) styling
 * 
 * Wraps entire pages or major sections with a retro "monitor frame" aesthetic.
 * Optional scanline overlay for extra authenticity.
 * 
 * @example
 * ```tsx
 * <PixelLayout framed scanlines>
 *   <Navbar />
 *   <main>{children}</main>
 *   <Footer />
 * </PixelLayout>
 * ```
 */
export function PixelLayout({
  children,
  scanlines = false,
  framed = false,
  className,
  ...props
}: PixelLayoutProps) {
  return (
    <div
      className={cn(
        "min-h-screen bg-background text-foreground",
        "flex flex-col",
        framed && "m-4 lg:m-8",
        // Level 3 pixel border
        framed && "shadow-pixel-lg",
        className
      )}
      {...props}
    >
      {children}
      
      {/* CRT Scanline overlay */}
      {scanlines && (
        <div
          className="pointer-events-none fixed inset-0 z-50 opacity-[0.03]"
          style={{
            background:
              "repeating-linear-gradient(0deg, transparent, transparent 1px, rgba(0,0,0,0.3) 1px, rgba(0,0,0,0.3) 2px)",
          }}
          aria-hidden="true"
        />
      )}
    </div>
  );
}
