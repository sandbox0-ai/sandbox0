"use client";

import React from "react";
import Image from "next/image";
import Link from "next/link";

export function Footer() {
  return (
    <footer className="border-t border-foreground/10">
      <div className="max-w-[1400px] mx-auto px-4 lg:px-8 py-4 flex flex-col md:flex-row items-center justify-between gap-8">
        <div className="flex items-center gap-3">
          <Image
            src="/sandbox0.png"
            alt="Sandbox0"
            width={32}
            height={32}
            className="pixel-art"
            data-pixel
          />
          <span className="font-pixel text-xs tracking-tight">SANDBOX0</span>
        </div>

        <div className="flex flex-wrap justify-center gap-8 text-sm text-muted">
          <Link
            href="/docs/latest/get-started"
            className="hover:text-foreground transition-colors"
          >
            Documentation
          </Link>
          <a
            href="https://github.com/sandbox0-ai/sandbox0"
            target="_blank"
            rel="noopener noreferrer"
            className="hover:text-foreground transition-colors"
          >
            GitHub
          </a>
          <a
            href="mailto:contact@sandbox0.ai"
            className="hover:text-foreground transition-colors"
          >
            contact@sandbox0.ai
          </a>
        </div>

        <div className="flex flex-col items-center md:items-end gap-1">
          <p className="text-xs text-muted">© 2026 Sandbox0</p>
          <p className="text-[10px] text-muted/50 font-mono uppercase tracking-widest">
            AI-Native Infrastructure
          </p>
        </div>
      </div>
    </footer>
  );
}
