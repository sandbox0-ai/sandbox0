import type { MDXComponents } from "mdx/types";
import { mdxComponents } from "@/components/docs/MDXComponents";

/**
 * Global MDX components mapping for App Router.
 *
 * Important: this implementation is RSC-safe (no React context),
 * so it works even when MDX provider injection is enabled.
 */
export function useMDXComponents(components: MDXComponents): MDXComponents {
  return {
    ...mdxComponents,
    ...components,
  };
}

