import React from "react";
import { cn } from "../utils";

export interface PixelBadgeProps extends React.HTMLAttributes<HTMLSpanElement> {
  children: React.ReactNode;
  variant?: "default" | "accent" | "success" | "warning" | "danger";
  size?: "sm" | "md";
}

/**
 * PixelBadge - Small label for status, tags, and metadata
 * 
 * Used for API method labels (GET, POST), version tags, status indicators, etc.
 * Compact pixel styling that doesn't overwhelm the content.
 * 
 * @example
 * ```tsx
 * <PixelBadge variant="accent">GET</PixelBadge>
 * <PixelBadge variant="success">v1.0</PixelBadge>
 * ```
 */
export function PixelBadge({
  children,
  variant = "default",
  size = "sm",
  className,
  ...props
}: PixelBadgeProps) {
  const variantClasses = {
    default: "bg-surface text-foreground border-foreground/25",
    accent: "bg-accent text-white border-accent/60",
    success: "bg-green-600 text-white border-green-600/60",
    warning: "bg-yellow-600 text-black border-yellow-600/60",
    danger: "bg-red-600 text-white border-red-600/60",
  };

  const sizeClasses = {
    sm: "px-2 py-0.5 text-xs",
    md: "px-3 py-1 text-sm",
  };

  return (
    <span
      className={cn(
        "inline-flex items-center font-mono font-medium border",
        variantClasses[variant],
        sizeClasses[size],
        className
      )}
      {...props}
    >
      {children}
    </span>
  );
}
