import React from "react";
import { cn } from "@sandbox0/ui";

export interface PixelSidebarItem {
  label: string;
  href: string;
  items?: PixelSidebarItem[];
}

export interface PixelSidebarProps {
  items: PixelSidebarItem[];
  currentPath?: string;
  className?: string;
}

/**
 * PixelSidebar - Navigation sidebar for documentation
 */
export function PixelSidebar({
  items,
  currentPath,
  className,
}: PixelSidebarProps) {
  return (
    <nav className={cn("py-8", className)}>
      <div className="space-y-8">
        {items.map((section, idx) => {
          const isSectionActive = currentPath === section.href;
          return (
            <div key={idx}>
              <a
                href={section.href}
                className={cn(
                  "flex items-center gap-2 mb-3 font-pixel text-xs transition-colors duration-200",
                  isSectionActive
                    ? "text-accent"
                    : "text-foreground hover:text-accent"
                )}
              >
                {isSectionActive && (
                  <span className="w-1.5 h-1.5 bg-accent" />
                )}
                {section.label}
              </a>
              {section.items && section.items.length > 0 && (
                <ul className="ml-2 space-y-1 border-l border-foreground/10">
                  {section.items.map((item, itemIdx) => {
                    const isActive = currentPath === item.href;
                    return (
                      <li key={itemIdx}>
                        <a
                          href={item.href}
                          className={cn(
                            "group relative flex items-center px-4 py-2 text-sm transition-all duration-200",
                            isActive
                              ? "text-accent bg-accent/5 font-medium"
                              : "text-muted hover:text-foreground hover:bg-foreground/[0.02]"
                          )}
                        >
                          {/* Active indicator pixel */}
                          {isActive && (
                            <>
                              <span className="absolute left-[-1px] top-1/2 -translate-y-1/2 w-[2px] h-4 bg-accent" />
                              <span className="absolute left-0 top-[calc(50%-8px)] w-1 h-[2px] bg-accent" />
                              <span className="absolute left-0 top-[calc(50%+6px)] w-1 h-[2px] bg-accent" />
                            </>
                          )}
                          
                          {/* Hover pixel - only if not active */}
                          {!isActive && (
                            <span className="absolute left-[-1px] top-1/2 -translate-y-1/2 w-[2px] h-0 bg-foreground/20 transition-all duration-200 group-hover:h-3" />
                          )}

                          {item.label}
                        </a>
                        
                        {item.items && item.items.length > 0 && (
                          <ul className="ml-4 mt-1 space-y-1 border-l border-foreground/5">
                            {item.items.map((subItem, subIdx) => {
                              const isSubActive = currentPath === subItem.href;
                              return (
                                <li key={subIdx}>
                                  <a
                                    href={subItem.href}
                                    className={cn(
                                      "group relative flex items-center px-4 py-1.5 text-sm transition-all duration-200",
                                      isSubActive
                                        ? "text-accent bg-accent/5"
                                        : "text-muted hover:text-foreground hover:bg-foreground/[0.02]"
                                    )}
                                  >
                                    {isSubActive && (
                                      <span className="absolute left-[-1px] top-1/2 -translate-y-1/2 w-[2px] h-3 bg-accent/70" />
                                    )}
                                    {subItem.label}
                                  </a>
                                </li>
                              );
                            })}
                          </ul>
                        )}
                      </li>
                    );
                  })}
                </ul>
              )}
            </div>
          );
        })}
      </div>
    </nav>
  );
}
