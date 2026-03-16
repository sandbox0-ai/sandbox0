import { PixelLayout } from "@sandbox0/ui";
import { Header } from "@/components/layout/Header";
import { Footer } from "@/components/layout/Footer";
import {
  CardGrid,
  DocsHero,
  LinkCard,
  ResourceItem,
  ResourceList,
} from "@/components/docs/DocsLanding";
import {
  DOCS_DEFAULT_VERSION,
  toGitHubReleaseHref,
  toGitHubReadmeHref,
} from "@/components/docs/versioning";

const docsVersion = DOCS_DEFAULT_VERSION;

export default function Home() {
  return (
    <PixelLayout>
      <Header />

      <main className="mx-auto w-full max-w-5xl px-6 py-16 lg:px-8">
        <DocsHero title="Sandbox0 Documentation">
          Open-source docs, API references, self-hosted deployment guidance,
          and architecture details for Sandbox0 all live here. Marketing pages
          move to the private cloud repository; this site stays focused on the
          product docs and source of truth.
        </DocsHero>

        <CardGrid>
          <LinkCard
            title="Get Started"
            href={`/docs/${docsVersion}/get-started`}
            cta="Open Quickstart"
          >
            Install the CLI or SDK, claim a sandbox, and run the first
            persistent workload.
          </LinkCard>
          <LinkCard
            title="Architecture"
            href={`/docs/${docsVersion}/architecture`}
            cta="Read Architecture"
          >
            Review the control-plane and data-plane split, runtime lifecycle,
            storage, and networking model.
          </LinkCard>
          <LinkCard
            title="Self-Hosted"
            href={`/docs/${docsVersion}/self-hosted`}
            cta="View Deployment Docs"
          >
            Follow operator-first deployment guidance for single-cluster and
            multi-cluster setups.
          </LinkCard>
          <LinkCard
            title="Templates and Volumes"
            href={`/docs/${docsVersion}/templates`}
            cta="Explore Runtime APIs"
          >
            Understand template selection, volume persistence, snapshot flows,
            and runtime behavior.
          </LinkCard>
        </CardGrid>

        <section className="mt-14">
          <h2 className="font-pixel text-base text-foreground">
            Open-source resources
          </h2>
          <p className="mt-3 max-w-3xl text-sm leading-7 text-muted">
            The canonical docs content, generated references, and versioned
            release bundles stay in this repository. Use the links below to
            navigate between docs and source.
          </p>

          <div className="mt-8">
            <ResourceList>
              <ResourceItem
                badge="Docs"
                description="Browse the versioned documentation entrypoint."
                href={`/docs/${docsVersion}/get-started`}
                cta="Open docs"
              />
              <ResourceItem
                badge="GitHub"
                description="Inspect the open-source repository and current main branch."
                href={toGitHubReadmeHref("sandbox0-ai/sandbox0", docsVersion)}
                cta="View repository"
              />
              <ResourceItem
                badge="Release"
                description="Check published releases and versioned docs bundles."
                href={toGitHubReleaseHref("sandbox0-ai/sandbox0", docsVersion)}
                cta="View releases"
              />
            </ResourceList>
          </div>
        </section>
      </main>

      <Footer />
    </PixelLayout>
  );
}
