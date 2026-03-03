import type { PixelSidebarItem } from "@/components/docs/PixelSidebar";

export const docsNavigation: PixelSidebarItem[] = [
  {
    label: "GET STARTED",
    href: "/docs/get-started",
    items: [
      { label: "Overview", href: "/docs/get-started" },
      { label: "Authentication", href: "/docs/get-started/authentication" },
      { label: "Concepts", href: "/docs/get-started/concepts" },
    ],
  },
  {
    label: "SANDBOX",
    href: "/docs/sandbox",
    items: [
      { label: "Overview", href: "/docs/sandbox" },
      { label: "Contexts", href: "/docs/sandbox/contexts" },
      { label: "Files", href: "/docs/sandbox/files" },
      { label: "Network", href: "/docs/sandbox/network" },
      { label: "Ports", href: "/docs/sandbox/ports" },
      { label: "Webhooks", href: "/docs/sandbox/webhooks" },
    ],
  },
  {
    label: "TEMPLATE",
    href: "/docs/template",
    items: [
      { label: "Overview", href: "/docs/template" },
      { label: "Custom Images", href: "/docs/template/images" },
      { label: "Warm Pool", href: "/docs/template/pool" },
      { label: "Configuration", href: "/docs/template/configuration" },
    ],
  },
  {
    label: "VOLUME",
    href: "/docs/volume",
    items: [
      { label: "Overview", href: "/docs/volume" },
      { label: "Mounts", href: "/docs/volume/mounts" },
      { label: "Snapshots", href: "/docs/volume/snapshots" },
      { label: "Fork", href: "/docs/volume/fork" },
    ],
  },
  {
    label: "SELF-HOSTED",
    href: "/docs/self-hosted",
    items: [
      { label: "Overview", href: "/docs/self-hosted" },
      { label: "Architecture", href: "/docs/self-hosted/architecture" },
      { label: "Install", href: "/docs/self-hosted/install" },
      { label: "Deploy Scenarios", href: "/docs/self-hosted/deploy-scenarios" },
      { label: "Configuration", href: "/docs/self-hosted/configuration" },
      { label: "Troubleshooting", href: "/docs/self-hosted/troubleshooting" },
    ],
  },
];
