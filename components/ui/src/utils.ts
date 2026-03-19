import type { PixelScale } from "./types";

/**
 * Utility to merge class names
 */
export function cn(...classes: (string | undefined | false)[]): string {
  return classes.filter(Boolean).join(" ");
}

/**
 * Get pixel shadow class based on scale
 */
export function getPixelShadowClass(
  scale: PixelScale,
  accent: boolean = false
): string {
  const shadowMap = {
    sm: accent ? "shadow-pixel-sm-accent" : "shadow-pixel-sm",
    md: accent ? "shadow-pixel-md-accent" : "shadow-pixel-md",
    lg: accent ? "shadow-pixel-lg-accent" : "shadow-pixel-lg",
  };
  return shadowMap[scale];
}

/**
 * Get font class based on scale
 * 
 * Adaptive strategy:
 * - sm: Body font (highest resolution for small text)
 * - md: Pixel font (the "sweet spot" for pixelated vibe)
 * - lg: Pixel font (larger but still readable)
 */
export function getPixelFontClass(scale: PixelScale): string {
  const fontMap = {
    sm: "font-body text-sm",
    md: "font-pixel text-[10px] leading-relaxed",
    lg: "font-pixel text-xs leading-relaxed",
  };
  return fontMap[scale];
}
