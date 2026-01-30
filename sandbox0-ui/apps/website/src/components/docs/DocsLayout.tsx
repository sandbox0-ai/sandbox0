"use client";

import React, { useState } from "react";
import { usePathname } from "next/navigation";
import { Header } from "@/components/layout/Header";
import { PixelSidebar } from "./PixelSidebar";
import { PixelTableOfContents } from "./PixelTableOfContents";
import { docsNavigation } from "./docs";

export interface DocsLayoutProps {
  children: React.ReactNode;
  currentPath?: string;
}

/**
 * DocsLayout - Main layout for documentation pages
 * 
 * Features:
 * - Sticky sidebar navigation
 * - Mobile-responsive hamburger menu
 * - Top navigation bar with search (future)
 * - Modern spacing with pixel accents
 */
export function DocsLayout({ children, currentPath: propPath }: DocsLayoutProps) {
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const pathname = usePathname();
  const currentPath = propPath ?? pathname;

  return (
    <div className="min-h-screen bg-background">
      <Header onMenuClick={() => setSidebarOpen(!sidebarOpen)} isSidebarOpen={sidebarOpen} />

      <div className="max-w-[1400px] mx-auto">
        <div className="flex">
          {/* Sidebar */}
          <aside
            className={`
              fixed lg:sticky top-[57px] left-0 h-[calc(100vh-57px)] z-40
              w-64 bg-background border-r border-foreground/5
              overflow-y-auto transition-transform duration-200
              ${sidebarOpen ? "translate-x-0" : "-translate-x-full lg:translate-x-0"}
            `}
          >
            <div className="px-6 relative h-full">
              {/* Decorative pixel at the bottom of sidebar if needed, but let's keep it clean */}
              <PixelSidebar items={docsNavigation} currentPath={currentPath} />
            </div>
          </aside>

          {/* Mobile Overlay */}
          {sidebarOpen && (
            <div
              className="fixed inset-0 bg-black/50 z-30 lg:hidden"
              onClick={() => setSidebarOpen(false)}
            />
          )}

          {/* Main Content */}
          <main className="flex-1 lg:ml-0 w-full lg:w-auto min-w-0">
            <div className="flex">
              <article className="flex-1 max-w-4xl mx-auto px-6 lg:px-12 py-12 min-w-0">
                {children}
              </article>
              
              {/* Table of Contents - Desktop Only */}
              <aside className="hidden xl:block w-64 py-12">
                <PixelTableOfContents />
              </aside>
            </div>
          </main>
        </div>
      </div>
    </div>
  );
}
