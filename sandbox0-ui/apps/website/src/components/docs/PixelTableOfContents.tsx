"use client";

import React, { useEffect, useState } from "react";
import { usePathname } from "next/navigation";
import { cn } from "@sandbox0/ui";

interface TocItem {
  id: string;
  text: string;
  level: number;
}

/**
 * PixelTableOfContents - Right-side navigation for the current page
 */
export function PixelTableOfContents() {
  const [headings, setHeadings] = useState<TocItem[]>([]);
  const [activeId, setActiveId] = useState<string>("");
  const pathname = usePathname();

  useEffect(() => {
    // Find all headings within the main article
    const article = document.querySelector("article");
    if (!article) return;

    // Small delay to ensure MDX content is rendered
    const timer = setTimeout(() => {
      const elements = Array.from(article.querySelectorAll("h2, h3"));
      const seenIds = new Set<string>();
      
      const items: TocItem[] = elements.map((el) => {
        // Generate a base ID
        let baseId = el.id || el.textContent
          ?.toLowerCase()
          .replace(/[^a-z0-9]+/g, "-")
          .replace(/(^-|-$)/g, "") || "heading";
          
        // Ensure uniqueness
        let uniqueId = baseId;
        let counter = 1;
        while (seenIds.has(uniqueId)) {
          uniqueId = `${baseId}-${counter}`;
          counter++;
        }
        
        seenIds.add(uniqueId);
        
        // Apply the ID back to the element if it didn't have one or if it was a duplicate
        if (el.id !== uniqueId) {
          el.id = uniqueId;
        }

        return {
          id: uniqueId,
          text: el.textContent?.replace(/^#\s*/, "") || "",
          level: parseInt(el.tagName.substring(1)),
        };
      });

      setHeadings(items);

      // Intersection Observer to highlight active heading
      const observer = new IntersectionObserver(
        (entries) => {
          entries.forEach((entry) => {
            if (entry.isIntersecting) {
              setActiveId(entry.target.id);
            }
          });
        },
        { rootMargin: "-100px 0% -80% 0%" }
      );

      elements.forEach((el) => observer.observe(el));

      // Initial check for current scroll position
      const scrollPos = window.scrollY;
      const findActiveHeading = () => {
        let currentActive = "";
        for (const el of elements) {
          const rect = el.getBoundingClientRect();
          // If the heading is above the middle of the viewport or visible
          if (rect.top < 150) {
            currentActive = el.id;
          } else {
            break;
          }
        }
        if (currentActive) {
          setActiveId(currentActive);
        } else if (items.length > 0 && scrollPos < 100) {
          // If we're at the very top, default to first heading
          setActiveId(items[0].id);
        }
      };

      findActiveHeading();

      return () => observer.disconnect();
    }, 100);

    return () => clearTimeout(timer);
  }, [pathname]);

  if (headings.length === 0) return null;

  return (
    <div className="sticky top-[100px] h-fit pl-8 border-l border-foreground/5">
      <h4 className="font-pixel text-[10px] text-foreground/50 uppercase tracking-widest mb-4">
        On This Page
      </h4>
      <nav className="space-y-3">
        {headings.map((heading) => (
          <a
            key={heading.id}
            href={`#${heading.id}`}
            className={cn(
              "block text-sm transition-all duration-200 hover:text-accent",
              heading.level === 3 ? "ml-4" : "",
              activeId === heading.id
                ? "text-accent font-medium"
                : "text-muted"
            )}
          >
            {activeId === heading.id && (
              <span className="inline-block w-1.5 h-1.5 bg-accent mr-2 translate-y-[-1px]" />
            )}
            {heading.text}
          </a>
        ))}
      </nav>
    </div>
  );
}
