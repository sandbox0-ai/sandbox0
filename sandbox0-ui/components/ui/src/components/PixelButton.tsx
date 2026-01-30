import React from "react";
import type { PixelScale, PixelBaseProps } from "../types";
import { cn, getPixelShadowClass } from "../utils";

export interface PixelButtonProps
  extends PixelBaseProps,
    React.ButtonHTMLAttributes<HTMLButtonElement> {
  children: React.ReactNode;
  /** Button variant */
  variant?: "primary" | "secondary";
}

/**
 * PixelButton - Interactive button with pixel-style press effect
 * 
 * Features a satisfying "press down" animation on click, simulating
 * the tactile feel of physical arcade buttons.
 * 
 * @example
 * ```tsx
 * <PixelButton variant="primary" onClick={handleClick}>
 *   Deploy Sandbox
 * </PixelButton>
 * ```
 */
export function PixelButton({
  children,
  scale = "md",
  accent = true,
  variant = "primary",
  className,
  disabled,
  ...props
}: PixelButtonProps) {
  const variantClasses = {
    primary: "bg-accent text-white hover:bg-white hover:text-accent",
    secondary: "bg-surface text-foreground hover:bg-foreground hover:text-background",
  };

  // Press effect: translate and reduce shadow on active
  const pressEffect = {
    sm: "active:translate-x-[1px] active:translate-y-[1px] active:shadow-[1px_1px_0_0_var(--color-accent)]",
    md: "active:translate-x-[2px] active:translate-y-[2px] active:shadow-[2px_2px_0_0_var(--color-accent)]",
    lg: "active:translate-x-[4px] active:translate-y-[4px] active:shadow-[4px_4px_0_0_var(--color-accent)]",
  };

  const paddingClasses = {
    sm: "px-3 py-1.5 text-xs",
    md: "px-4 py-2 text-sm font-pixel",
    lg: "px-6 py-3 text-base font-pixel",
  };

  return (
    <button
      className={cn(
        "inline-flex items-center justify-center",
        "transition-all duration-75",
        "disabled:opacity-50 disabled:cursor-not-allowed disabled:active:translate-x-0 disabled:active:translate-y-0",
        variantClasses[variant],
        getPixelShadowClass(scale, accent),
        pressEffect[scale],
        paddingClasses[scale],
        className
      )}
      disabled={disabled}
      {...props}
    >
      {children}
    </button>
  );
}
