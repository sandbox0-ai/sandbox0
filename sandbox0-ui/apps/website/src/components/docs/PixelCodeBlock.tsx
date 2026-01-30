"use client";

import React, { useMemo, useState } from "react";
import { cn, getPixelShadowClass, type PixelScale } from "@sandbox0/ui";
import Prism from "prismjs";
import "prismjs/components/prism-bash";
import "prismjs/components/prism-json";
import "prismjs/components/prism-javascript";
import "prismjs/components/prism-typescript";
import "prismjs/components/prism-python";
import "prismjs/components/prism-yaml";

export interface PixelCodeBlockProps
  extends React.HTMLAttributes<HTMLPreElement> {
  children: React.ReactNode;
  /** Language label to display */
  language?: string;
  /** Whether to show line numbers */
  showLineNumbers?: boolean;
  /** File name to display in header */
  filename?: string;
  scale?: PixelScale;
  accent?: boolean;
}

function normalizeLanguage(language?: string): string | undefined {
  if (!language) return undefined;
  const lang = language.toLowerCase().trim();

  const aliases: Record<string, string> = {
    js: "javascript",
    jsx: "javascript",
    ts: "typescript",
    tsx: "typescript",
    yml: "yaml",
    shell: "bash",
    sh: "bash",
    zsh: "bash",
  };

  return aliases[lang] ?? lang;
}

function toCodeString(children: React.ReactNode): string {
  if (typeof children === "string") return children;
  
  // Recursively extract text from children
  const getText = (node: React.ReactNode): string => {
    if (!node) return "";
    if (typeof node === "string" || typeof node === "number") return String(node);
    if (Array.isArray(node)) return node.map(getText).join("");
    if (React.isValidElement(node)) return getText((node.props as any).children);
    return "";
  };
  
  return getText(children);
}

/**
 * PixelCodeBlock - Code block with modern syntax highlighting and pixel styling
 */
export function PixelCodeBlock({
  children,
  scale = "md",
  accent = false,
  language,
  filename,
  className,
  showLineNumbers,
  ...props
}: PixelCodeBlockProps) {
  const [copied, setCopied] = useState(false);

  const rawCode = useMemo(() => toCodeString(children).replace(/\n$/, ""), [children]);
  const prismLanguage = useMemo(() => normalizeLanguage(language), [language]);

  const highlightedHtml = useMemo(() => {
    const lang = prismLanguage;
    const grammar =
      (lang && (Prism.languages as any)[lang]) || (Prism.languages as any).plain;

    try {
      return Prism.highlight(rawCode, grammar, lang ?? "plain");
    } catch {
      return rawCode
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;");
    }
  }, [prismLanguage, rawCode]);

  const label = filename || prismLanguage;

  async function handleCopy() {
    try {
      await navigator.clipboard.writeText(rawCode);
    } catch {
      const textarea = document.createElement("textarea");
      textarea.value = rawCode;
      textarea.style.position = "fixed";
      textarea.style.left = "-9999px";
      document.body.appendChild(textarea);
      textarea.focus();
      textarea.select();
      document.execCommand("copy");
      document.body.removeChild(textarea);
    }

    setCopied(true);
    window.setTimeout(() => setCopied(false), 1200);
  }

  return (
    <div className={cn("relative", className)}>
      <pre
        className={cn(
          "pixel-codeblock group relative",
          "bg-surface overflow-x-auto font-mono text-sm leading-relaxed",
          getPixelShadowClass(scale, accent),
          "p-4",
          label && "pt-10"
        )}
        {...props}
      >
        {label && (
          <div className="pointer-events-none absolute left-4 top-3 text-[10px] font-mono text-muted opacity-80">
            {label}
          </div>
        )}

        <button
          type="button"
          onClick={handleCopy}
          aria-label={copied ? "Copied" : "Copy code"}
          className={cn(
            "absolute right-3 top-3",
            "inline-flex items-center justify-center",
            "h-8 w-8",
            "bg-background/30 hover:bg-background/50",
            copied ? "text-accent" : "text-muted hover:text-foreground",
            "shadow-pixel-sm",
            "transition-colors"
          )}
        >
          {copied ? (
            <svg
              width="16"
              height="16"
              viewBox="0 0 16 16"
              fill="none"
              xmlns="http://www.w3.org/2000/svg"
              aria-hidden="true"
              shapeRendering="crispEdges"
            >
              <rect x="3" y="8" width="2" height="2" fill="currentColor" />
              <rect x="5" y="10" width="2" height="2" fill="currentColor" />
              <rect x="7" y="8" width="2" height="2" fill="currentColor" />
              <rect x="9" y="6" width="2" height="2" fill="currentColor" />
              <rect x="11" y="4" width="2" height="2" fill="currentColor" />
            </svg>
          ) : (
            <svg
              width="16"
              height="16"
              viewBox="0 0 16 16"
              fill="none"
              xmlns="http://www.w3.org/2000/svg"
              aria-hidden="true"
              shapeRendering="crispEdges"
            >
              <path
                d="M6 2H14V10H6V2Z"
                stroke="currentColor"
                strokeWidth="2"
              />
              <path
                d="M2 6H10V14H2V6Z"
                stroke="currentColor"
                strokeWidth="2"
              />
            </svg>
          )}
        </button>

        <code
          className={cn(
            "block whitespace-pre",
            "pr-12"
          )}
          dangerouslySetInnerHTML={{ __html: highlightedHtml }}
        />
      </pre>
    </div>
  );
}
