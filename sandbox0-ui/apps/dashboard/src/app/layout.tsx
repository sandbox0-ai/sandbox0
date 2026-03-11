import type { Metadata } from "next";
import "@sandbox0/ui/globals.css";

export const metadata: Metadata = {
  title: "Sandbox0 Dashboard",
  description: "Topology-aware control plane for Sandbox0 deployments",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" className="dark">
      <body>{children}</body>
    </html>
  );
}
