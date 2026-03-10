"use client";

import { useEffect } from "react";
import { usePathname, useRouter } from "next/navigation";
import { DocsLayout } from "@/components/docs/DocsLayout";
import { toLatestDocsRedirect } from "@/components/docs/versioning";

export function DocsRouteGate({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const router = useRouter();
  const redirectTarget = toLatestDocsRedirect(pathname);

  useEffect(() => {
    if (redirectTarget) {
      router.replace(redirectTarget);
    }
  }, [redirectTarget, router]);

  if (redirectTarget) {
    return null;
  }

  return <DocsLayout>{children}</DocsLayout>;
}
