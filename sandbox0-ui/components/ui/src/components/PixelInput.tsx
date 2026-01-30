import React from "react";
import type { PixelBaseProps } from "../types";
import { cn } from "../utils";

export interface PixelInputProps
  extends Omit<PixelBaseProps, "scale">,
    Omit<React.InputHTMLAttributes<HTMLInputElement>, "size"> {
  /** Label text */
  label?: string;
  /** Error message */
  error?: string;
  /** Helper text */
  helper?: string;
}

/**
 * PixelInput - Text input with Level 1 (Micro) pixel styling
 * 
 * Inputs are always rendered at Level 1 scale for maximum readability.
 * On focus, displays an accent-colored glow effect.
 * 
 * @example
 * ```tsx
 * <PixelInput 
 *   label="Sandbox Name"
 *   placeholder="my-sandbox"
 *   error={errors.name}
 * />
 * ```
 */
export function PixelInput({
  label,
  error,
  helper,
  className,
  id,
  ...props
}: PixelInputProps) {
  const inputId = id || label?.toLowerCase().replace(/\s+/g, "-");

  return (
    <div className="flex flex-col gap-1.5">
      {label && (
        <label
          htmlFor={inputId}
          className="text-sm text-foreground font-medium"
        >
          {label}
        </label>
      )}
      <input
        id={inputId}
        className={cn(
          // Base styles
          "w-full px-3 py-2 text-sm font-mono",
          "bg-background text-foreground",
          "placeholder:text-muted",
          // Level 1 pixel border
          "border border-foreground/25",
          // Focus state - accent glow
          "focus:outline-none focus:border-accent",
          "focus:shadow-[0_0_0_2px_var(--color-accent)]",
          // Error state
          error && "border-red-500 focus:border-red-500 focus:shadow-[0_0_0_2px_rgb(239,68,68)]",
          // Disabled state
          "disabled:opacity-50 disabled:cursor-not-allowed",
          className
        )}
        {...props}
      />
      {(error || helper) && (
        <p
          className={cn(
            "text-xs",
            error ? "text-red-500" : "text-muted"
          )}
        >
          {error || helper}
        </p>
      )}
    </div>
  );
}
