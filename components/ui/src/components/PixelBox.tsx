import React from "react";
import type { PixelScale, PixelBaseProps } from "../types";
import { cn, getPixelShadowClass } from "../utils";

export interface PixelBoxProps
  extends PixelBaseProps,
    React.HTMLAttributes<HTMLDivElement> {
  children?: React.ReactNode;
  /** HTML element to render */
  as?: "div" | "section" | "article" | "aside" | "main" | "header" | "footer";
}

/**
 * PixelBox - The foundational container component
 * 
 * A flexible container that applies pixel-style borders based on the scale.
 * Use this as the base for custom containers or as a general-purpose wrapper.
 * 
 * @example
 * ```tsx
 * <PixelBox scale="md" accent>
 *   Content with 2px pixel border and accent shadow
 * </PixelBox>
 * ```
 */
export function PixelBox({
  children,
  scale = "md",
  accent = false,
  className,
  as: Component = "div",
  ...props
}: PixelBoxProps) {
  return (
    <Component
      className={cn(
        "bg-surface p-4",
        getPixelShadowClass(scale, accent),
        className
      )}
      {...props}
    >
      {children}
    </Component>
  );
}
