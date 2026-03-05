"use client";

import React from "react";

type MermaidDiagramProps = {
  chart: string;
  className?: string;
};

let mermaidApi: any | null = null;
let initialized = false;

export function MermaidDiagram({ chart, className }: MermaidDiagramProps) {
  const containerRef = React.useRef<HTMLDivElement>(null);
  const [error, setError] = React.useState<string | null>(null);

  React.useEffect(() => {
    let cancelled = false;

    async function render() {
      try {
        const mermaidModule = await import("mermaid");
        const mermaid = mermaidModule.default;
        if (!initialized) {
          mermaid.initialize({
            startOnLoad: false,
            securityLevel: "strict",
            theme: "base",
            themeVariables: {
              darkMode: true,
              background: "#1A1A1A",
              primaryColor: "#111111",
              primaryTextColor: "#F5F5F5",
              primaryBorderColor: "#F97316",
              lineColor: "#888888",
              secondaryColor: "#0F0F0F",
              secondaryTextColor: "#E5E5E5",
              secondaryBorderColor: "#3A3A3A",
              tertiaryColor: "#000000",
              tertiaryTextColor: "#D4D4D4",
              tertiaryBorderColor: "#333333",
              clusterBkg: "#101010",
              clusterBorder: "#3A3A3A",
              defaultLinkColor: "#A3A3A3",
              edgeLabelBackground: "#111111",
              fontFamily: "\"JetBrains Mono\", \"Fira Code\", monospace",
            },
          });
          initialized = true;
        }
        mermaidApi = mermaid;

        const id = `mmd-${Math.random().toString(36).slice(2, 10)}`;
        const { svg } = await mermaidApi.render(id, chart);
        if (cancelled || !containerRef.current) return;
        containerRef.current.innerHTML = svg;
        const renderedSvg = containerRef.current.querySelector("svg");
        if (renderedSvg) {
          renderedSvg.style.maxWidth = "100%";
          renderedSvg.style.height = "auto";
          renderedSvg.style.background = "transparent";
          renderedSvg.style.display = "block";
          renderedSvg.style.margin = "0 auto";
        }
        setError(null);
      } catch (e) {
        if (!cancelled) {
          setError(e instanceof Error ? e.message : "Failed to render Mermaid diagram");
        }
      }
    }

    render();
    return () => {
      cancelled = true;
    };
  }, [chart]);

  if (error) {
    return (
      <pre className="bg-surface p-4 mb-6 overflow-x-auto shadow-pixel-sm border border-foreground/15 font-mono text-sm">
        {chart}
      </pre>
    );
  }

  return (
    <div
      className={className}
      ref={containerRef}
      aria-label="Mermaid diagram"
    />
  );
}
