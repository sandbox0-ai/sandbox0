import { notFound, redirect } from "next/navigation";
import { defaultDocsPageSlug, docsPageSlugs, renderDocsPage } from "@/app/docs/content";
import {
  DOCS_DEFAULT_VERSION,
  getRenderedDocsVersions,
  isDocsVersionSegment,
  isRenderedDocsVersion,
} from "@/components/docs/versioning";

export const dynamicParams = false;

export function generateStaticParams() {
  const versionedParams = getRenderedDocsVersions().flatMap((version) => [
    { version, slug: [] as string[] },
    ...docsPageSlugs.map((pageSlug) => ({
      version,
      slug: pageSlug.split("/"),
    })),
  ]);

  const legacyParams = docsPageSlugs.map((pageSlug) => {
    const [legacyVersion, ...legacySlug] = pageSlug.split("/");
    return {
      version: legacyVersion,
      slug: legacySlug,
    };
  });

  return [...versionedParams, ...legacyParams];
}

export default async function VersionedDocsPage({
  params,
}: {
  params: Promise<{
    version: string;
    slug?: string[];
  }>;
}) {
  const resolvedParams = await params;
  const slug = resolvedParams.slug ?? [];

  if (!isRenderedDocsVersion(resolvedParams.version)) {
    if (isDocsVersionSegment(resolvedParams.version)) {
      notFound();
    }

    redirect(`/docs/${DOCS_DEFAULT_VERSION}/${[resolvedParams.version, ...slug].join("/")}`);
  }

  if (resolvedParams.version === DOCS_DEFAULT_VERSION && slug.length === 0) {
    redirect(`/docs/${DOCS_DEFAULT_VERSION}/${defaultDocsPageSlug}`);
  }

  if (resolvedParams.version !== DOCS_DEFAULT_VERSION && slug.length === 0) {
    redirect(`/docs/${resolvedParams.version}/${defaultDocsPageSlug}`);
  }

  return renderDocsPage(slug);
}
