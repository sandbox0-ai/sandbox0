"use client";

import React, { useEffect, useMemo, useRef, useState } from "react";
import type { PixelBaseProps } from "../types";
import { cn, getPixelShadowClass } from "../utils";

export interface PixelSelectOption {
  value: string;
  label: string;
}

export interface PixelSelectProps extends Omit<PixelBaseProps, "accent"> {
  value: string;
  options: PixelSelectOption[];
  onValueChange: (value: string) => void;
  ariaLabel: string;
  className?: string;
  triggerClassName?: string;
  menuClassName?: string;
}

/**
 * PixelSelect - Lightweight pixel-style select menu built from primitives.
 *
 * Uses a button + menu instead of the browser native select so the control
 * stays visually consistent across browsers and apps.
 */
export function PixelSelect({
  value,
  options,
  onValueChange,
  ariaLabel,
  scale = "sm",
  className,
  triggerClassName,
  menuClassName,
}: PixelSelectProps) {
  const rootRef = useRef<HTMLDivElement | null>(null);
  const [open, setOpen] = useState(false);

  const currentOption = useMemo(() => {
    return options.find((option) => option.value === value) ?? options[0];
  }, [options, value]);

  useEffect(() => {
    function handlePointerDown(event: MouseEvent) {
      if (!rootRef.current?.contains(event.target as Node)) {
        setOpen(false);
      }
    }

    function handleEscape(event: KeyboardEvent) {
      if (event.key === "Escape") {
        setOpen(false);
      }
    }

    document.addEventListener("mousedown", handlePointerDown);
    document.addEventListener("keydown", handleEscape);

    return () => {
      document.removeEventListener("mousedown", handlePointerDown);
      document.removeEventListener("keydown", handleEscape);
    };
  }, []);

  const triggerPadding = {
    sm: "px-2.5 py-2 text-[10px]",
    md: "px-3 py-2.5 text-[11px]",
    lg: "px-3.5 py-3 text-xs",
  };

  const itemPadding = {
    sm: "px-2.5 py-2 text-[10px]",
    md: "px-3 py-2.5 text-[11px]",
    lg: "px-3.5 py-3 text-xs",
  };

  return (
    <div ref={rootRef} className={cn("relative", className)}>
      <button
        type="button"
        aria-haspopup="listbox"
        aria-expanded={open}
        aria-label={ariaLabel}
        onClick={() => setOpen((value) => !value)}
        className={cn(
          "group flex w-full items-center justify-between gap-3",
          "bg-transparent text-muted transition-colors",
          "hover:text-foreground focus:outline-none focus:text-foreground",
          "disabled:cursor-not-allowed disabled:opacity-50",
          getPixelShadowClass(scale, false),
          open && "text-foreground",
          triggerPadding[scale],
          triggerClassName
        )}
      >
        <span className="truncate font-pixel uppercase tracking-[0.06em]">
          {currentOption?.label ?? value}
        </span>
        <svg
          aria-hidden="true"
          viewBox="0 0 8 5"
          className={cn(
            "h-[5px] w-2 shrink-0 text-muted/70 transition-transform transition-colors duration-150 group-hover:text-foreground",
            open && "rotate-180"
          )}
          shapeRendering="crispEdges"
          fill="currentColor"
        >
          <rect x="0" y="0" width="1" height="1" />
          <rect x="1" y="1" width="1" height="1" />
          <rect x="2" y="2" width="1" height="1" />
          <rect x="5" y="2" width="1" height="1" />
          <rect x="6" y="1" width="1" height="1" />
          <rect x="7" y="0" width="1" height="1" />
          <rect x="3" y="3" width="1" height="1" />
          <rect x="4" y="3" width="1" height="1" />
        </svg>
      </button>

      {open ? (
        <div
          role="listbox"
          aria-label={ariaLabel}
          className={cn(
            "absolute bottom-full left-0 right-0 z-20 mb-2 overflow-hidden",
            "bg-background shadow-[4px_4px_0_0_rgba(255,255,255,0.08)]",
            getPixelShadowClass(scale, false),
            menuClassName
          )}
        >
          {options.map((option) => {
            const active = option.value === value;

            return (
              <button
                key={option.value}
                type="button"
                role="option"
                aria-selected={active}
                onClick={() => {
                  setOpen(false);
                  if (option.value !== value) {
                    onValueChange(option.value);
                  }
                }}
                className={cn(
                  "flex w-full items-center justify-between text-left",
                  "font-pixel uppercase tracking-[0.06em] transition-colors",
                  active
                    ? "bg-accent/10 text-accent"
                    : "text-muted hover:bg-foreground/[0.03] hover:text-foreground",
                  itemPadding[scale]
                )}
              >
                <span>{option.label}</span>
                {active ? <span className="h-[4px] w-[4px] bg-accent" /> : null}
              </button>
            );
          })}
        </div>
      ) : null}
    </div>
  );
}
