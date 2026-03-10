"use client";

import {
  PixelLayout,
  PixelCard,
  PixelButton,
  PixelBox,
  PixelHeading,
} from "@sandbox0/ui";
import { Header } from "@/components/layout/Header";
import { Footer } from "@/components/layout/Footer";

const runtimeCapabilities = [
  {
    title: "Template Runtime",
    body:
      "Templates define the execution environment for each sandbox: image, resources, warm pool, and default network policy.",
  },
  {
    title: "Persistent Volume",
    body:
      "Volumes are the persistent layer for sandbox state, with snapshot, restore, fork, and reuse flows.",
  },
  {
    title: "Sub-200ms Cold Start",
    body:
      "Warm pools keep sandbox runtimes ready so bash, Python, and app-serving environments can start quickly.",
  },
  {
    title: "Network Control",
    body:
      "Built-in network policy support gives you egress control, DNS protections, and runtime policy enforcement.",
  },
];

const platformSections = [
  {
    title: "Persistent agent runtime",
    body:
      "Sandbox0 gives agents an isolated runtime with durable state, so each session can keep its workspace, processes, and environment boundaries intact across real work.",
  },
  {
    title: "Operator-first self-hosting",
    body:
      "Deploy Sandbox0 by installing infra-operator and applying a Sandbox0Infra resource. Single-cluster is the fastest path; multi-cluster is available when regional scale-out matters.",
  },
  {
    title: "Interfaces for real agent workflows",
    body:
      "Use the s0 CLI or SDKs to claim sandboxes, run bash and Python sessions, execute commands, expose app ports, and manage volumes from your application code.",
  },
];

export default function Home() {
  return (
    <PixelLayout>
      <Header position="fixed" background="translucent" />

      <section className="min-h-screen px-4 pt-28 pb-20">
        <div className="max-w-5xl mx-auto flex min-h-[calc(100vh-7rem)] flex-col items-center justify-center">

          <PixelHeading
            as="h1"
            tone="site"
            className="text-center text-3xl leading-[1.05] tracking-[-0.1em] md:text-5xl lg:text-6xl"
          >
            Sandbox
            <span className="text-accent"> for agents</span>
          </PixelHeading>

          <p className="mt-6 max-w-xl text-center text-sm leading-7 text-muted md:text-base">
            Isolated runtime, persistent workspace state, and network control
            in one layer for AI agents.
          </p>

          <div className="mt-8 flex flex-wrap justify-center gap-4">
            <PixelButton
              variant="primary"
              scale="lg"
              accent
              onClick={() => {
                window.location.href = "/docs/latest/get-started";
              }}
            >
              READ DOCS
            </PixelButton>
            <PixelButton
              variant="secondary"
              scale="lg"
              onClick={() => {
                window.open("https://github.com/sandbox0-ai/sandbox0", "_blank", "noopener,noreferrer");
              }}
            >
              VIEW GITHUB
            </PixelButton>
          </div>

          <PixelBox scale="lg" className="mt-14 w-full max-w-3xl text-left">
            <div className="flex items-center gap-2 mb-3 pb-3 border-b-2 border-foreground/20">
              <div className="w-3 h-3 bg-red-500" />
              <div className="w-3 h-3 bg-yellow-500" />
              <div className="w-3 h-3 bg-green-500" />
              <span className="ml-2 text-xs text-muted font-mono">
                sandbox-runtime-session
              </span>
            </div>
            <pre className="font-mono text-sm text-accent overflow-x-auto">
              <code>{`from sandbox0 import Client
import os

client = Client(
    token=os.environ["SANDBOX0_TOKEN"],
    base_url=os.environ.get("SANDBOX0_BASE_URL", "http://localhost:30080"),
)

sandbox = client.claim_sandbox(template="default")
sandbox.run("sqlite", ".open /workspace/demo.db")
sandbox.run("sqlite", "create table if not exists runs (n integer);")
sandbox.run("sqlite", "insert into runs values (42);")
result = sandbox.run("sqlite", "select n from runs;")
print(result.output_raw, end="")`}</code>
            </pre>
          </PixelBox>

          <div className="mt-6 grid w-full max-w-3xl gap-3 sm:grid-cols-3">
            <PixelBox className="text-center text-sm">
              <div className="font-pixel text-[10px] uppercase tracking-[0.2em] text-accent">
                Runtime
              </div>
              <p className="mt-2 text-muted">Warm, isolated execution.</p>
            </PixelBox>
            <PixelBox className="text-center text-sm">
              <div className="font-pixel text-[10px] uppercase tracking-[0.2em] text-accent">
                Volume
              </div>
              <p className="mt-2 text-muted">State that persists.</p>
            </PixelBox>
            <PixelBox className="text-center text-sm">
              <div className="font-pixel text-[10px] uppercase tracking-[0.2em] text-accent">
                Network
              </div>
              <p className="mt-2 text-muted">Policy built in.</p>
            </PixelBox>
          </div>
        </div>
      </section>

      <section className="px-4 pb-20">
        <div className="max-w-6xl mx-auto">
          <PixelHeading as="h2" tone="site" className="mb-10 text-center">
            Built For <span className="text-accent">AI Agents</span>
          </PixelHeading>

          <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-6">
            {runtimeCapabilities.map((item) => (
              <PixelCard key={item.title} header={item.title} scale="md" accent>
                <p className="text-muted">{item.body}</p>
              </PixelCard>
            ))}
          </div>
        </div>
      </section>

      <section className="px-4 pb-20">
        <div className="max-w-6xl mx-auto grid gap-6 lg:grid-cols-[minmax(0,1.05fr)_minmax(320px,0.95fr)]">
          <PixelCard header="How Sandbox0 is structured" scale="lg" accent>
            <div className="space-y-4 text-muted">
              <p>
                Sandbox0 provides isolated runtimes for agents that need to
                execute code, serve apps, manage files, and enforce network
                policy without giving up deployment control.
              </p>
              <p>
                Templates describe the runtime environment. Volumes add durable
                storage for outputs, caches, and working state so sandbox
                sessions can survive restarts and handoffs.
              </p>
              <p>
                In the common single-cluster deployment, `internal-gateway` and
                `manager` are the core services. `procd` runs inside each
                sandbox pod and handles process execution, file operations, and
                volume mount workflows.
              </p>
            </div>
          </PixelCard>

          <div className="grid gap-6">
            {platformSections.map((item) => (
              <PixelCard key={item.title} header={item.title} scale="md">
                <p className="text-muted">{item.body}</p>
              </PixelCard>
            ))}
          </div>
        </div>
      </section>

      <section className="px-4 pb-20">
        <div className="max-w-6xl mx-auto grid gap-6 lg:grid-cols-3">
          <PixelCard header="Typical workflow" scale="md" accent>
            <p className="text-muted">
              Your agent selects a template, claims a sandbox, runs bash or
              Python sessions, and attaches volumes when the work needs to
              persist.
            </p>
          </PixelCard>
          <PixelCard header="Storage model" scale="md">
            <p className="text-muted">
              Volumes are first-class. They hold persistent workspace data,
              caches, and artifacts instead of forcing every sandbox session to
              be ephemeral.
            </p>
          </PixelCard>
          <PixelCard header="Deployment target" scale="md">
            <p className="text-muted">
              Sandbox0 is designed for enterprise self-hosting in your own
              regional Kubernetes environment, with clear control plane and data
              plane separation.
            </p>
          </PixelCard>
        </div>
      </section>

      <section className="py-20 px-4 border-t border-foreground/10">
        <div className="max-w-3xl mx-auto text-center">
          <PixelHeading as="h2" tone="site" className="mb-6">
            Start With The Docs
          </PixelHeading>
          <p className="text-muted mb-8">
            The docs cover architecture, quickstart, sandbox runtime behavior,
            volume workflows, templates, and self-hosted configuration.
          </p>
          <PixelButton
            variant="primary"
            scale="lg"
            accent
            onClick={() => {
              window.location.href = "/docs/latest/get-started";
            }}
          >
            OPEN GET STARTED
          </PixelButton>
        </div>
      </section>

      <Footer />
    </PixelLayout>
  );
}
