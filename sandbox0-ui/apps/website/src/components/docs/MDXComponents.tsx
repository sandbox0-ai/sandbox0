import React from "react";
import {
  PixelCallout,
  PixelBadge,
  PixelHeading,
} from "@sandbox0/ui";
import { PixelCodeBlock } from "./PixelCodeBlock";
import { PixelTabs } from "./PixelTabs";
import { MermaidDiagram } from "./MermaidDiagram";
import {
  DocsHero,
  CardGrid,
  LinkCard,
  ResourceList,
  ResourceItem,
} from "./DocsLanding";
import { Sandbox0InfraReference } from "./Sandbox0InfraReference";
import { DocsLink } from "./DocsLink";
import { GitHubApplyCommand, GitHubRawLink, S0Install } from "./VersionedGitHub";
import type { MDXComponents } from "mdx/types";

function cx(...classes: Array<string | undefined | null | false>) {
  return classes.filter(Boolean).join(" ");
}

function extractText(node: React.ReactNode): string {
  if (typeof node === "string" || typeof node === "number") {
    return String(node);
  }
  if (Array.isArray(node)) {
    return node.map(extractText).join("");
  }
  if (React.isValidElement(node)) {
    const element = node as React.ReactElement<{ children?: React.ReactNode }>;
    return extractText(element.props.children);
  }
  return "";
}

function toHeadingId(children: React.ReactNode, fallbackId?: string): string | undefined {
  if (fallbackId) return fallbackId;
  const text = extractText(children)
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9\s-]/g, "")
    .replace(/\s+/g, "-")
    .replace(/-+/g, "-")
    .replace(/(^-|-$)/g, "");
  return text || undefined;
}

type HeadingLevel = "h2" | "h3" | "h4";
type HeadingProps = React.ComponentPropsWithoutRef<HeadingLevel>;

function HeadingWithAnchor(level: HeadingLevel, props: HeadingProps) {
  const { children, id, className, ...rest } = props;
  const headingId = toHeadingId(children, id);

  return (
    <PixelHeading
      as={level}
      tone="docs"
      id={headingId}
      className={cx(
        "group scroll-mt-24",
        "[&_code]:!text-[1em] [&_code]:!leading-[1] [&_code]:align-baseline [&_code]:!font-pixel [&_code]:!text-inherit",
        className
      )}
      {...rest}
    >
      <span>{children}</span>
      {headingId ? (
        <a
          href={`#${headingId}`}
          aria-label={`Link to ${extractText(children) || level}`}
          className={cx(
            "ml-2 inline-flex align-middle text-accent/80 hover:text-accent",
            "opacity-0 group-hover:opacity-100 focus:opacity-100 transition-opacity"
          )}
        >
          #
        </a>
      ) : null}
    </PixelHeading>
  );
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
            <DocsLink
              key={`${it.label}:${it.href}`}
              href={it.href}
              className={cx(
                "text-accent hover:text-white transition-colors font-medium",
                linkClassName
              )}
            >
              {it.label}
            </DocsLink>
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
    <div className="flex items-baseline gap-3 mb-4">
      <PixelBadge variant={methodVariant as any} size="sm">
        {method.toUpperCase()}
      </PixelBadge>
      <code className="block text-sm font-mono text-muted">
        {children}
      </code>
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
    HeadingWithAnchor("h2", { children, ...props })
  ),
  h3: ({ children, ...props }) => (
    HeadingWithAnchor("h3", { children, ...props })
  ),
  h4: ({ children, ...props }) => (
    HeadingWithAnchor("h4", { children, ...props })
  ),

  // Paragraphs and text
  p: ({ children, ...props }) => (
    <p className="mb-4 leading-relaxed text-muted" {...props}>
      {children}
    </p>
  ),

  // Lists
  ul: ({ children, ...props }) => (
    <ul className="mb-4 ml-4 space-y-2 list-none pixel-list" {...props}>
      {children}
    </ul>
  ),
  ol: ({ children, ...props }) => (
    <ol className="mb-4 ml-6 space-y-2 list-decimal list-outside" {...props}>
      {children}
    </ol>
  ),
  li: ({ children, ...props }) => (
    <li className="text-muted" {...props}>
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

    if (language === "mermaid") {
      const chart = Array.isArray(codeChildren) ? codeChildren.join("") : String(codeChildren ?? "");
      return (
        <div className="mb-6 overflow-x-auto border border-foreground/15 bg-surface p-4 shadow-pixel-sm">
          <MermaidDiagram chart={chart} />
        </div>
      );
    }

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
            // Inline code: subtle background + padding without framed borders
            "inline-block align-baseline px-1.5 py-0.5",
            "font-mono text-[0.92em] leading-tight",
            "bg-foreground/4 text-foreground",
            "rounded-none"
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
    <DocsLink
      href={href ?? "#"}
      className="text-accent hover:text-foreground transition-colors font-medium"
      {...props}
    >
      {children}
    </DocsLink>
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
        className="w-full border border-muted/35 text-sm"
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
      className="px-4 py-2 text-left font-pixel text-xs border border-muted/35"
      {...props}
    >
      {children}
    </th>
  ),
  td: ({ children, ...props }) => (
    <td
      className="px-4 py-2 text-muted border border-muted/30"
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
  DocLink: DocsLink,
  GitHubRawLink,
  GitHubApplyCommand,
  S0Install,
  
  // Landing Page Components
  DocsHero,
  CardGrid,
  LinkCard,
  ResourceList,
  ResourceItem,
  Sandbox0InfraReference,
};
