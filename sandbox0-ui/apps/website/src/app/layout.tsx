import type { Metadata } from "next";
import "@sandbox0/ui/globals.css";

export const metadata: Metadata = {
  title: "Sandbox0 - AI-Native Sandbox Infrastructure",
  description:
    "Persistent storage, session state retention, 100ms cold start, and easy private deployment.",
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
