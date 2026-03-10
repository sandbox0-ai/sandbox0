import { notFound } from "next/navigation";

import GetStartedPage from "@/app/docs/get-started/page.mdx";
import GetStartedConceptsPage from "@/app/docs/get-started/concepts/page.mdx";
import SandboxPage from "@/app/docs/sandbox/page.mdx";
import SandboxContextsPage from "@/app/docs/sandbox/contexts/page.mdx";
import SandboxFilesPage from "@/app/docs/sandbox/files/page.mdx";
import SandboxNetworkPage from "@/app/docs/sandbox/network/page.mdx";
import SandboxPortsPage from "@/app/docs/sandbox/ports/page.mdx";
import SandboxWebhooksPage from "@/app/docs/sandbox/webhooks/page.mdx";
import TemplatePage from "@/app/docs/template/page.mdx";
import TemplateConfigurationPage from "@/app/docs/template/configuration/page.mdx";
import TemplateImagesPage from "@/app/docs/template/images/page.mdx";
import TemplatePoolPage from "@/app/docs/template/pool/page.mdx";
import VolumePage from "@/app/docs/volume/page.mdx";
import VolumeForkPage from "@/app/docs/volume/fork/page.mdx";
import VolumeMountsPage from "@/app/docs/volume/mounts/page.mdx";
import VolumeSnapshotsPage from "@/app/docs/volume/snapshots/page.mdx";
import SelfHostedPage from "@/app/docs/self-hosted/page.mdx";
import SelfHostedConfigurationPage from "@/app/docs/self-hosted/configuration/page.mdx";
import SelfHostedInstallPage from "@/app/docs/self-hosted/install/page.mdx";

const docsPageRegistry: Record<string, React.ComponentType> = {
  "get-started": GetStartedPage,
  "get-started/concepts": GetStartedConceptsPage,
  "sandbox": SandboxPage,
  "sandbox/contexts": SandboxContextsPage,
  "sandbox/files": SandboxFilesPage,
  "sandbox/network": SandboxNetworkPage,
  "sandbox/ports": SandboxPortsPage,
  "sandbox/webhooks": SandboxWebhooksPage,
  "template": TemplatePage,
  "template/configuration": TemplateConfigurationPage,
  "template/images": TemplateImagesPage,
  "template/pool": TemplatePoolPage,
  "volume": VolumePage,
  "volume/fork": VolumeForkPage,
  "volume/mounts": VolumeMountsPage,
  "volume/snapshots": VolumeSnapshotsPage,
  "self-hosted": SelfHostedPage,
  "self-hosted/configuration": SelfHostedConfigurationPage,
  "self-hosted/install": SelfHostedInstallPage,
};

export const docsPageSlugs = Object.keys(docsPageRegistry);

export function renderDocsPage(slug: string[]) {
  const pageKey = slug.join("/");
  const Page = docsPageRegistry[pageKey];

  if (!Page) {
    notFound();
  }

  return <Page />;
}
