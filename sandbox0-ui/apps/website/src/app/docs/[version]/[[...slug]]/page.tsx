import { notFound, redirect } from "next/navigation";
import { docsPageSlugs, renderDocsPage } from "@/app/docs/content";
import {
  DOCS_DEFAULT_VERSION,
  getRenderedDocsVersions,
  isRenderedDocsVersion,
} from "@/components/docs/versioning";

export const dynamicParams = false;

export function generateStaticParams() {
  return getRenderedDocsVersions().flatMap((version) => [
    { version, slug: [] as string[] },
    ...docsPageSlugs.map((pageSlug) => ({
      version,
      slug: pageSlug.split("/"),
    })),
  ]);
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
    notFound();
  }

  if (resolvedParams.version === DOCS_DEFAULT_VERSION && slug.length === 0) {
    redirect(`/docs/${DOCS_DEFAULT_VERSION}/get-started`);
  }

  if (resolvedParams.version !== DOCS_DEFAULT_VERSION && slug.length === 0) {
    redirect(`/docs/${resolvedParams.version}/get-started`);
  }

  return renderDocsPage(slug);
}
