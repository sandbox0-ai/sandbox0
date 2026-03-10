"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { getResolvedDocsVersionFromPathname, toVersionedDocsHref } from "@/components/docs/versioning";

function isExternalHref(href: string): boolean {
  return /^(https?:)?\/\//.test(href) || href.startsWith("mailto:") || href.startsWith("tel:");
}

export type DocsLinkProps = React.AnchorHTMLAttributes<HTMLAnchorElement> & {
  href: string;
  newTab?: boolean;
};

export function DocsLink({
  href,
  newTab,
  children,
  className,
  rel,
  ...props
}: DocsLinkProps) {
  const pathname = usePathname();
  const version = getResolvedDocsVersionFromPathname(pathname);
  const resolvedHref =
    href.startsWith("/docs") ? toVersionedDocsHref(version, href) : href;
  const external = isExternalHref(resolvedHref);
  const openNewTab = Boolean(newTab);

  if (external || resolvedHref.startsWith("#")) {
    return (
      <a
        href={resolvedHref}
        className={className}
        target={openNewTab ? "_blank" : undefined}
        rel={openNewTab ? "noopener noreferrer" : rel}
        {...props}
      >
        {children}
      </a>
    );
  }

  const { download: _download, ...linkProps } = props;
  return (
    <Link
      href={resolvedHref}
      className={className}
      target={openNewTab ? "_blank" : undefined}
      rel={openNewTab ? "noopener noreferrer" : rel}
      {...linkProps}
    >
      {children}
    </Link>
  );
}
