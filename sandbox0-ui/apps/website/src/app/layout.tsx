import type { Metadata } from "next";
import "@sandbox0/ui/globals.css";

export const metadata: Metadata = {
  title: "Sandbox0 Docs",
  description:
    "Open-source documentation for Sandbox0 architecture, runtime behavior, APIs, and self-hosted deployment.",
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
