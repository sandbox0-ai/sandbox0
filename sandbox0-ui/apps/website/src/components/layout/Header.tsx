"use client";

import React from "react";
import Image from "next/image";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { PixelButton } from "@sandbox0/ui";
import { getResolvedDocsVersionFromPathname } from "@/components/docs/versioning";

interface HeaderProps {
  onMenuClick?: () => void;
  isSidebarOpen?: boolean;
  /**
   * Header position style.
   * - sticky: default for docs
   * - fixed: useful for the homepage hero
   */
  position?: "sticky" | "fixed";
  /** Optional override for background opacity. */
  background?: "solid" | "translucent";
}

export function Header({
  onMenuClick,
  isSidebarOpen,
  position = "sticky",
  background = "solid",
}: HeaderProps) {
  const pathname = usePathname();
  const isDocs = pathname?.startsWith("/docs");
  const docsVersion = getResolvedDocsVersionFromPathname(pathname);

  return (
    <nav
      className={[
        position === "fixed" ? "fixed top-0 left-0 right-0" : "sticky top-0",
        "z-50 backdrop-blur border-b border-foreground/10",
        background === "translucent" ? "bg-background/90" : "bg-background/95",
      ].join(" ")}
    >
      <div className="max-w-[1400px] mx-auto px-4 lg:px-8 py-3 flex items-center justify-between gap-4">
        {/* Logo */}
        <Link href="/" className="flex items-center gap-3 shrink-0">
          <Image
            src="/sandbox0.png"
            alt="Sandbox0"
            width={32}
            height={32}
            className="pixel-art"
            data-pixel
          />
          <span className="font-pixel text-xs tracking-tight">SANDBOX0</span>
        </Link>

        {/* Desktop Navigation */}
        <div className="hidden lg:flex items-center gap-8">
          <Link
            href={`/docs/${docsVersion}/get-started`}
            className={`text-sm font-medium transition-colors ${
              isDocs ? "text-accent" : "text-muted hover:text-foreground"
            }`}
          >
            Docs
          </Link>
          <a
            href="https://github.com/sandbox0-ai/sandbox0"
            target="_blank"
            rel="noopener noreferrer"
            className="text-sm text-muted hover:text-foreground transition-colors"
          >
            GitHub
          </a>
          <PixelButton
            variant="primary"
            scale="sm"
            onClick={() => {
              window.location.href = "mailto:contact@sandbox0.ai";
            }}
          >
            Contact
          </PixelButton>
        </div>

        {/* Mobile Actions */}
        <div className="flex lg:hidden items-center gap-2">
          {/* Mobile Menu Button - Only show if onMenuClick is provided */}
          {onMenuClick && (
            <button
              className="p-2 text-foreground"
              onClick={onMenuClick}
              aria-label="Toggle menu"
            >
              <svg
                width="24"
                height="24"
                viewBox="0 0 24 24"
                fill="none"
                stroke="currentColor"
                strokeWidth="2"
              >
                {isSidebarOpen ? (
                  <>
                    <line x1="18" y1="6" x2="6" y2="18" />
                    <line x1="6" y1="6" x2="18" y2="18" />
                  </>
                ) : (
                  <>
                    <line x1="3" y1="12" x2="21" y2="12" />
                    <line x1="3" y1="6" x2="21" y2="6" />
                    <line x1="3" y1="18" x2="21" y2="18" />
                  </>
                )}
              </svg>
            </button>
          )}
        </div>
      </div>
    </nav>
  );
}
