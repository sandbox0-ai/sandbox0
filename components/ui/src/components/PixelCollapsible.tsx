"use client";

import React, { useId, useState } from "react";
import type { PixelBaseProps, PixelScale } from "../types";
import { cn, getPixelShadowClass } from "../utils";

// ─── Pixel-art chevron icon ──────────────────────────────────────────────────
// Same glyph as PixelSelect's chevron for visual consistency across the library.
// Rotates 180° via CSS when the panel is open.
function PixelChevron({ className }: { className?: string }) {
  return (
    <svg
      aria-hidden="true"
      viewBox="0 0 8 5"
      className={cn("h-[5px] w-2 shrink-0", className)}
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
  );
}

// ─── PixelCollapsible ────────────────────────────────────────────────────────

export interface PixelCollapsibleProps extends PixelBaseProps {
  /** The trigger label — rendered inside the header button */
  trigger: React.ReactNode;
  /** Content revealed when expanded */
  children: React.ReactNode;
  /** Controlled open state */
  open?: boolean;
  /** Called when the open state should change */
  onOpenChange?: (open: boolean) => void;
  /** Initial open state for uncontrolled usage */
  defaultOpen?: boolean;
  /** Extra classes applied to the trigger button */
  triggerClassName?: string;
  /** Extra classes applied to the content panel */
  contentClassName?: string;
}

/**
 * PixelCollapsible — A pixel-art accordion / disclosure primitive.
 *
 * Renders a full-width trigger button paired with a collapsible content panel.
 * Works in both controlled and uncontrolled modes.
 *
 * Pixel design details:
 * - Trigger uses the shared pixel shadow scale system
 * - Shadow switches to accent color when the panel is open
 * - Pixel-art chevron (identical to PixelSelect) rotates 180° on open
 * - Press effect mirrors PixelButton's tactile "push" animation
 * - Content panel is separated by a 2px accent border
 * - No border-radius anywhere (enforced by globals.css)
 *
 * @example
 * ```tsx
 * // Uncontrolled
 * <PixelCollapsible trigger="Configuration" defaultOpen>
 *   <p>Panel content here.</p>
 * </PixelCollapsible>
 *
 * // Controlled (e.g. FAQ accordion)
 * <PixelCollapsible
 *   trigger="What is Sandbox0?"
 *   open={activeId === "faq-1"}
 *   onOpenChange={(open) => setActiveId(open ? "faq-1" : null)}
 * >
 *   Sandbox0 is an AI agent sandbox...
 * </PixelCollapsible>
 * ```
 */
export function PixelCollapsible({
  trigger,
  children,
  scale = "md",
  accent = false,
  open: controlledOpen,
  onOpenChange,
  defaultOpen = false,
  className,
  triggerClassName,
  contentClassName,
}: PixelCollapsibleProps) {
  const [internalOpen, setInternalOpen] = useState(defaultOpen);
  const uid = useId();
  const panelId = `pixel-collapsible-${uid.replace(/:/g, "")}`;

  const isOpen = controlledOpen !== undefined ? controlledOpen : internalOpen;

  const handleToggle = () => {
    const next = !isOpen;
    if (controlledOpen === undefined) {
      setInternalOpen(next);
    }
    onOpenChange?.(next);
  };

  // Pixel "press down" effect — matches PixelButton's active state
  const pressEffect: Record<PixelScale, string> = {
    sm: "active:translate-x-[1px] active:translate-y-[1px]",
    md: "active:translate-x-[2px] active:translate-y-[2px]",
    lg: "active:translate-x-[4px] active:translate-y-[4px]",
  };

  return (
    <div className={cn("flex flex-col", className)}>
      {/* ── Trigger ── */}
      <button
        type="button"
        aria-expanded={isOpen}
        aria-controls={panelId}
        onClick={handleToggle}
        className={cn(
          "group flex w-full items-center justify-between gap-4",
          "bg-surface px-4 py-3",
          "font-pixel text-[10px] uppercase tracking-[0.06em]",
          "transition-all duration-75",
          // Idle: muted text; hover: full foreground
          isOpen ? "text-foreground" : "text-muted hover:text-foreground",
          // Shadow: accent when open so the active state is unmistakable
          getPixelShadowClass(scale, isOpen || accent),
          pressEffect[scale],
          triggerClassName
        )}
      >
        <span>{trigger}</span>

        <PixelChevron
          className={cn(
            "transition-transform duration-150",
            isOpen
              ? "rotate-180 text-accent"
              : "text-muted/70 group-hover:text-foreground"
          )}
        />
      </button>

      {/* ── Content panel ── */}
      {isOpen && (
        <div
          id={panelId}
          role="region"
          className={cn(
            "bg-background",
            // 2px accent top-border signals the panel belongs to the trigger above
            "border-t-2 border-accent/40",
            contentClassName
          )}
        >
          {children}
        </div>
      )}
    </div>
  );
}

// ─── PixelCollapsiblePanel ───────────────────────────────────────────────────

export interface PixelCollapsiblePanelProps {
  /**
   * Whether the panel is visible. When false the panel is unmounted entirely
   * so it does not take up space or affect tab order.
   */
  open: boolean;
  children: React.ReactNode;
  /** Extra classes for the panel wrapper */
  className?: string;
}

/**
 * PixelCollapsiblePanel — The content panel half of a "split" collapsible.
 *
 * Use this when the trigger lives in a different part of the layout than the
 * panel (e.g. a sticky header bar whose dropdown must render outside the flex
 * container). The caller owns the `open` state and wires the toggle separately.
 *
 * Visual contract:
 * - `bg-surface` background (elevated above page background)
 * - 2px accent top border (consistent with PixelCollapsible's panel)
 * - No padding — callers add their own to control the content area
 *
 * @example
 * ```tsx
 * // In a header layout:
 * <nav>
 *   <div className="header-bar flex items-center justify-between">
 *     <Logo />
 *     <button onClick={() => setOpen(v => !v)} aria-expanded={open}>
 *       <PixelMenuIcon open={open} />
 *     </button>
 *   </div>
 *
 *   <PixelCollapsiblePanel open={open} className="lg:hidden">
 *     <div className="px-4 py-4">
 *       nav links…
 *     </div>
 *   </PixelCollapsiblePanel>
 * </nav>
 * ```
 */
export function PixelCollapsiblePanel({
  open,
  children,
  className,
}: PixelCollapsiblePanelProps) {
  if (!open) return null;

  return (
    <div
      className={cn(
        "bg-surface",
        "border-t-2 border-accent/40",
        className
      )}
    >
      {children}
    </div>
  );
}
