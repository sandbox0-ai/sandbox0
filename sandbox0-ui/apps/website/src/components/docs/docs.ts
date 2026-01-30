import type { PixelSidebarItem } from "./PixelSidebar";

/**
 * Documentation Navigation Structure
 * 
 * Defines the sidebar navigation for the docs site.
 * Organized by major sections: Getting Started, SDKs, CLI, API Reference
 */
export const docsNavigation: PixelSidebarItem[] = [
  {
    label: "GETTING STARTED",
    href: "/docs",
    items: [
      { label: "Introduction", href: "/docs" },
      { label: "Quick Start", href: "/docs/quickstart" },
      { label: "Core Concepts", href: "/docs/concepts" },
      { label: "Authentication", href: "/docs/authentication" },
    ],
  },
  {
    label: "SDKs",
    href: "/docs/sdks",
    items: [
      { label: "Python SDK", href: "/docs/sdks/python" },
      { label: "JavaScript SDK", href: "/docs/sdks/javascript" },
      { label: "Go SDK", href: "/docs/sdks/go" },
    ],
  },
  {
    label: "CLI",
    href: "/docs/cli",
    items: [
      { label: "Installation", href: "/docs/cli/installation" },
      { label: "Commands", href: "/docs/cli/commands" },
      { label: "Configuration", href: "/docs/cli/configuration" },
    ],
  },
  {
    label: "API REFERENCE",
    href: "/docs/api",
    items: [
      { label: "Overview", href: "/docs/api" },
      { label: "Sandboxes", href: "/docs/api/sandboxes" },
      { label: "Processes", href: "/docs/api/processes" },
      { label: "Files", href: "/docs/api/files" },
      { label: "Volumes", href: "/docs/api/volumes" },
    ],
  },
];
