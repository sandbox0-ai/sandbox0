import React from "react";
import {
  PixelCallout,
  PixelBadge,
  PixelHeading,
} from "@sandbox0/ui";
import { PixelCodeBlock } from "./PixelCodeBlock";
import { PixelTabs } from "./PixelTabs";
import {
  DocsHero,
  CardGrid,
  LinkCard,
  ResourceList,
  ResourceItem,
} from "./DocsLanding";
import type { MDXComponents } from "mdx/types";

function cx(...classes: Array<string | undefined | null | false>) {
  return classes.filter(Boolean).join(" ");
}

type LinkRowProps = React.HTMLAttributes<HTMLDivElement> & {
  /**
   * Pipe-separated list of "label=url" pairs.
   *
   * Example:
   *   links="Discord=https://discord.gg/sandbox0|GitHub=https://github.com/sandbox0|Email=mailto:support@sandbox0.ai"
   */
  links?: string;
  /** Optional override for the rendered <a> className. */
  linkClassName?: string;
};

function LinkRow({
  links,
  className,
  linkClassName,
  children,
  ...props
}: LinkRowProps) {
  const items =
    typeof links === "string" && links.trim().length > 0
      ? links
          .split("|")
          .map((part) => part.trim())
          .filter(Boolean)
          .flatMap((part) => {
            const i = part.indexOf("=");
            if (i <= 0) return [];
            const label = part.slice(0, i).trim();
            const href = part.slice(i + 1).trim();
            if (!label || !href) return [];
            return [{ label, href }];
          })
      : null;

  return (
    <div className={cx("flex flex-wrap gap-4 mt-4", className)} {...props}>
      {items
        ? items.map((it) => (
            <a
              key={`${it.label}:${it.href}`}
              href={it.href}
              className={cx(
                "text-accent hover:text-white transition-colors font-medium",
                linkClassName
              )}
            >
              {it.label}
            </a>
          ))
        : children}
    </div>
  );
}

type TerminalBlockProps = React.HTMLAttributes<HTMLDivElement> & {
  /**
   * Newline-separated terminal lines.
   *
   * Example:
   *   lines={"$ s0 create --template python\\n✓ Sandbox created\\nsandbox-id: sb_abc123"}
   */
  lines: string;
};

function TerminalBlock({ lines, className, ...props }: TerminalBlockProps) {
  const normalized = typeof lines === "string" ? lines.replace(/\r\n/g, "\n") : "";
  const rows = normalized
    .split("\n")
    .map((l) => l.replace(/\s+$/, ""))
    .filter((l, idx, arr) => !(idx === arr.length - 1 && l === ""));

  return (
    <div
      className={cx(
        "bg-surface",
        "border border-foreground/15",
        "shadow-pixel-sm",
        "p-4",
        "font-mono text-sm",
        "mb-6",
        className
      )}
      {...props}
    >
      {rows.map((line, i) => {
        const trimmed = line.trimStart();
        const isPrompt = trimmed.startsWith("$");
        const isSuccess = trimmed.startsWith("✓") || trimmed.toLowerCase().startsWith("success");
        const isError = trimmed.startsWith("✕") || trimmed.toLowerCase().startsWith("error");

        return (
          <div
            key={i}
            className={cx(
              "whitespace-pre-wrap break-words",
              isPrompt && "text-accent",
              isSuccess && "text-green-500",
              isError && "text-red-500",
              !isPrompt && !isSuccess && !isError && "text-muted"
            )}
          >
            {line}
          </div>
        );
      })}
    </div>
  );
}

/**
 * Endpoint - Syntactic sugar for API documentation routes
 * Displays an HTTP method badge and the endpoint path.
 */
function Endpoint({ method, children }: { method: string; children: React.ReactNode }) {
  const methodVariant = {
    GET: "success",
    POST: "accent",
    PUT: "warning",
    PATCH: "warning",
    DELETE: "danger",
  }[method.toUpperCase()] || "default";

  return (
    <div className="flex items-center gap-3 mb-4">
      <PixelBadge variant={methodVariant as any} size="sm">
        {method.toUpperCase()}
      </PixelBadge>
      <code className="text-sm font-mono text-muted">{children}</code>
    </div>
  );
}

/**
 * Custom MDX Components for Documentation
 * 
 * Maps standard Markdown elements to styled pixel components.
 * Provides a modern, readable documentation experience with pixel accents.
 */
export const mdxComponents: MDXComponents = {
  // Headings with pixel styling
  h1: ({ children, ...props }) => (
    <PixelHeading
      as="h1"
      tone="docs"
      leading="#"
      leadingClassName="inline-block mr-3 text-accent text-sm md:text-base opacity-70"
      {...props}
    >
      {children}
    </PixelHeading>
  ),
  h2: ({ children, ...props }) => (
    <PixelHeading as="h2" tone="docs" {...props}>
      {children}
    </PixelHeading>
  ),
  h3: ({ children, ...props }) => (
    <PixelHeading as="h3" tone="docs" {...props}>
      {children}
    </PixelHeading>
  ),
  h4: ({ children, ...props }) => (
    <PixelHeading as="h4" tone="docs" {...props}>
      {children}
    </PixelHeading>
  ),

  // Paragraphs and text
  p: ({ children, ...props }) => (
    <p className="mb-4 leading-relaxed text-muted" {...props}>
      {children}
    </p>
  ),

  // Lists
  ul: ({ children, ...props }) => (
    <ul className="mb-4 ml-6 space-y-2 list-none" {...props}>
      {children}
    </ul>
  ),
  ol: ({ children, ...props }) => (
    <ol className="mb-4 ml-6 space-y-2 list-decimal list-inside" {...props}>
      {children}
    </ol>
  ),
  li: ({ children, ...props }) => (
    <li className="text-muted" {...props}>
      <span className="inline-block w-2 h-2 bg-accent mr-3 -ml-6 translate-y-[-2px]" />
      {children}
    </li>
  ),

  // Code blocks
  pre: ({ children, ...props }: any) => {
    // MDX usually renders fenced code blocks as: <pre><code className="language-...">...</code></pre>
    const childrenArray = React.Children.toArray(children);
    
    // Find the code element - handle both string type and potential component type
    const codeElement = childrenArray.find((child: any) => {
      if (!React.isValidElement(child)) return false;
      const type = child.type;
      return type === "code" || (child.props as any)?.mdxType === "code" || (typeof type === 'function' && type.name === 'code');
    }) as any;

    if (!codeElement) {
      return (
        <pre className="bg-surface p-4 mb-6 overflow-x-auto shadow-pixel-sm border border-foreground/15 font-mono text-sm" {...props}>
          {children}
        </pre>
      );
    }

    const className = codeElement?.props?.className || "";
    const language = typeof className === "string" ? className.replace("language-", "") : undefined;
    const codeChildren = codeElement?.props?.children;

    return (
      <PixelCodeBlock language={language} scale="md" className="mb-6">
        {codeChildren}
      </PixelCodeBlock>
    );
  },
  code: ({ children, className, ...props }: any) => {
    // Inline code
    if (!className) {
      return (
        <code
          className={cx(
            // GitHub-like inline code memory: subtle background + padding + border
            "inline-block align-baseline px-1.5 py-0.5",
            "font-mono text-[0.92em] leading-tight",
            "bg-foreground/4 text-foreground",
            "border border-foreground/15",
            // Pixel flavor: sharp corners + tiny pixel-ish shadow
            "rounded-none",
            "shadow-[1px_1px_0_0_rgba(0,0,0,0.18)] dark:shadow-[1px_1px_0_0_rgba(255,255,255,0.10)]"
          )}
          {...props}
        >
          {children}
        </code>
      );
    }
    // Block code (handled by pre)
    return (
      <code className={className} {...props}>
        {children}
      </code>
    );
  },

  // Links
  a: ({ children, href, ...props }) => (
    <a
      href={href}
      className="text-accent hover:text-foreground transition-colors font-medium"
      {...props}
    >
      {children}
    </a>
  ),

  // Blockquotes
  blockquote: ({ children, ...props }) => (
    <blockquote
      className="mb-4 pl-4 border-l-2 border-accent/70 text-muted italic"
      {...props}
    >
      {children}
    </blockquote>
  ),

  // Tables
  table: ({ children, ...props }) => (
    <div className="mb-6 overflow-x-auto">
      <table
        className="w-full border border-foreground/15 text-sm"
        {...props}
      >
        {children}
      </table>
    </div>
  ),
  thead: ({ children, ...props }) => (
    <thead className="bg-surface" {...props}>
      {children}
    </thead>
  ),
  th: ({ children, ...props }) => (
    <th
      className="px-4 py-2 text-left font-pixel text-xs border border-foreground/12"
      {...props}
    >
      {children}
    </th>
  ),
  td: ({ children, ...props }) => (
    <td
      className="px-4 py-2 text-muted border border-foreground/12"
      {...props}
    >
      {children}
    </td>
  ),

  // Horizontal rule
  hr: (props) => (
    <hr className="my-8 border-t border-foreground/15" {...props} />
  ),

  // Custom components for documentation
  Callout: ({ className, ...props }: React.ComponentProps<typeof PixelCallout>) => (
    <PixelCallout
      {...props}
      className={cx("my-6", className)}
    />
  ),
  Badge: PixelBadge,
  Tabs: PixelTabs,
  CodeBlock: PixelCodeBlock,
  LinkRow,
  TerminalBlock,
  Endpoint,
  
  // Landing Page Components
  DocsHero,
  CardGrid,
  LinkCard,
  ResourceList,
  ResourceItem,
};
