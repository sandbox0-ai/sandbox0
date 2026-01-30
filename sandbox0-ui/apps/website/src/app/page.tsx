import { PixelLayout, PixelCard, PixelButton, PixelBox, PixelHeading } from "@sandbox0/ui";
import Image from "next/image";
import { Header } from "@/components/layout/Header";
import { Footer } from "@/components/layout/Footer";

export default function Home() {
  return (
    <PixelLayout>
      <Header position="fixed" background="translucent" />

      {/* Hero Section */}
      <section className="min-h-screen flex flex-col items-center justify-center px-4 pt-20">
        <div className="text-center max-w-4xl">
          {/* Pixel Art Logo */}
          <div className="mb-8 flex justify-center">
            <Image
              src="/sandbox0.png"
              alt="Sandbox0"
              width={120}
              height={120}
              className="pixel-art"
              data-pixel
            />
          </div>

          {/* Headline */}
          <PixelHeading as="h1" tone="site" className="mb-6">
            AI-NATIVE
            <br />
            <span className="text-accent">SANDBOX</span>
            <br />
            INFRASTRUCTURE
          </PixelHeading>

          {/* Subheadline */}
          <p className="text-lg md:text-xl text-muted mb-8 max-w-2xl mx-auto">
            Persistent storage, session state retention, 100ms cold start, and
            easy private deployment.
          </p>

          {/* CTA Buttons */}
          <div className="flex flex-wrap gap-4 justify-center mb-12">
            <PixelButton variant="primary" scale="lg" accent>
              START BUILDING
            </PixelButton>
            <PixelButton variant="secondary" scale="lg">
              VIEW DOCS
            </PixelButton>
          </div>

          {/* Terminal Preview */}
          <PixelBox scale="lg" className="text-left max-w-2xl mx-auto">
            <div className="flex items-center gap-2 mb-3 pb-3 border-b-2 border-foreground/20">
              <div className="w-3 h-3 bg-red-500" />
              <div className="w-3 h-3 bg-yellow-500" />
              <div className="w-3 h-3 bg-green-500" />
              <span className="ml-2 text-xs text-muted font-mono">
                sandbox-dev-001
              </span>
            </div>
            <pre className="font-mono text-sm text-accent overflow-x-auto">
              <code>{`$ sandbox0 create --template python
✓ Sandbox created in 98ms
✓ Volume mounted: /workspace
✓ Session restored

sandbox@dev-001:~$ python main.py
Hello from Sandbox0!`}</code>
            </pre>
          </PixelBox>
        </div>
      </section>

      {/* Features Section */}
      <section className="py-20 px-4">
        <div className="max-w-6xl mx-auto">
          <PixelHeading as="h2" tone="site" className="text-center mb-12">
            WHY <span className="text-accent">SANDBOX0</span>?
          </PixelHeading>

          <div className="grid grid-cols-1 md:grid-cols-3 gap-8">
            <PixelCard header="⚡ 100ms Cold Start" scale="md" accent>
              <p className="text-muted">
                Pre-warmed sandbox pools ensure your AI agents start instantly.
                No waiting, just coding.
              </p>
            </PixelCard>

            <PixelCard header="💾 Persistent Storage" scale="md" accent>
              <p className="text-muted">
                JuiceFS-powered volumes that survive restarts. Snapshots,
                restore, and share with ease.
              </p>
            </PixelCard>

            <PixelCard header="🔒 Network Isolation" scale="md" accent>
              <p className="text-muted">
                Fine-grained network policies. IP/CIDR filtering, DNS spoofing
                protection built-in.
              </p>
            </PixelCard>

            <PixelCard header="🎯 Session State" scale="md">
              <p className="text-muted">
                REPL sessions persist across connections. Pick up exactly where
                you left off.
              </p>
            </PixelCard>

            <PixelCard header="☁️ Easy Deployment" scale="md">
              <p className="text-muted">
                Cloud-native Kubernetes architecture. Deploy to your own
                infrastructure in minutes.
              </p>
            </PixelCard>

            <PixelCard header="🔌 E2B Compatible" scale="md">
              <p className="text-muted">
                Drop-in E2B compatibility layer. Migrate existing integrations
                seamlessly.
              </p>
            </PixelCard>
          </div>
        </div>
      </section>

      {/* CTA Section */}
      <section className="py-20 px-4 border-t border-foreground/10">
        <div className="max-w-2xl mx-auto text-center">
          <PixelHeading as="h2" tone="site" className="mb-6">
            READY TO BUILD?
          </PixelHeading>
          <p className="text-muted mb-8">
            Join developers building the next generation of AI applications.
          </p>
          <PixelButton variant="primary" scale="lg" accent>
            GET STARTED FREE
          </PixelButton>
        </div>
      </section>

      <Footer />
    </PixelLayout>
  );
}
