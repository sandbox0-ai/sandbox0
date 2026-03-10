import { DocsRouteGate } from "@/components/docs/DocsRouteGate";

export default function Layout({ children }: { children: React.ReactNode }) {
  return <DocsRouteGate>{children}</DocsRouteGate>;
}
