"use client";

import { useEffect, useState } from "react";
import { usePathname, useRouter } from "next/navigation";
import { PixelSelect } from "@sandbox0/ui";
import {
  defaultDocsVersionsManifest,
  getDocsContentPathFromPathname,
  getResolvedDocsVersionFromPathname,
} from "@/components/docs/versioning";

export function DocsVersionSelect() {
  const pathname = usePathname();
  const router = useRouter();
  const currentVersion = getResolvedDocsVersionFromPathname(pathname);
  const contentPath = getDocsContentPathFromPathname(pathname);
  const [manifest, setManifest] = useState(defaultDocsVersionsManifest);

  useEffect(() => {
    let cancelled = false;

    fetch("/docs/versions.json", { cache: "no-store" })
      .then(async (response) => {
        if (!response.ok) {
          throw new Error(`HTTP ${response.status}`);
        }
        return response.json();
      })
      .then((nextManifest) => {
        if (!cancelled && nextManifest?.versions) {
          setManifest(nextManifest);
        }
      })
      .catch(() => {});

    return () => {
      cancelled = true;
    };
  }, []);

  return (
    <div className="pb-8 pt-2">
      <PixelSelect
        ariaLabel="Select documentation version"
        value={currentVersion}
        options={manifest.versions.map((version) => ({
          value: version.id,
          label: version.label,
        }))}
        onValueChange={(value) => {
          router.push(`/docs/${value}${contentPath}`);
        }}
        scale="sm"
      />
    </div>
  );
}
