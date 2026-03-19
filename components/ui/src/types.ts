/**
 * Pixel Scale Levels
 * 
 * - sm (Level 1 - Micro): 1px borders, for text, icons, inputs
 * - md (Level 2 - Midi): 2px borders, for buttons, cards, toasts
 * - lg (Level 3 - Macro): 4px borders, for modals, layouts, hero sections
 */
export type PixelScale = "sm" | "md" | "lg";

/**
 * Common props for all pixel components
 */
export interface PixelBaseProps {
  /** The pixel scale level */
  scale?: PixelScale;
  /** Whether to use accent color for shadow */
  accent?: boolean;
  /** Additional class names */
  className?: string;
}
