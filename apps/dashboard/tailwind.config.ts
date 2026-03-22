import type { Config } from "tailwindcss";

const config: Config = {
  darkMode: "class",
  content: [
    "./src/**/*.{js,ts,jsx,tsx,mdx}",
    "../../components/ui/src/**/*.{js,ts,jsx,tsx}",
    "../../components/dashboard-core/src/**/*.{js,ts,jsx,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        background: "#000000",
        foreground: "#FFFFFF",
        accent: "rgb(var(--color-accent-rgb) / <alpha-value>)",
        surface: "#1A1A1A",
        primary: "rgb(var(--color-accent-rgb) / <alpha-value>)",
        muted: "#888888",
      },
      fontFamily: {
        pixel: ['"Press Start 2P"', "monospace"],
        body: ["Inter", "system-ui", "sans-serif"],
        mono: ['"JetBrains Mono"', '"Fira Code"', "monospace"],
      },
      boxShadow: {
        // Softer pixel shadows: keep the crisp offset, reduce visual weight.
        "pixel-sm": "1px 1px 0 0 rgba(0,0,0,0.65)",
        "pixel-sm-accent": "1px 1px 0 0 rgb(var(--color-accent-rgb) / 0.85)",
        "pixel-md": [
          "-1px 0 0 0 rgba(255,255,255,0.28)",
          "1px 0 0 0 rgba(255,255,255,0.28)",
          "0 -1px 0 0 rgba(255,255,255,0.28)",
          "0 1px 0 0 rgba(255,255,255,0.28)",
          "3px 3px 0 0 rgba(0,0,0,0.65)",
        ].join(", "),
        "pixel-md-accent": [
          "-1px 0 0 0 rgba(255,255,255,0.28)",
          "1px 0 0 0 rgba(255,255,255,0.28)",
          "0 -1px 0 0 rgba(255,255,255,0.28)",
          "0 1px 0 0 rgba(255,255,255,0.28)",
          "3px 3px 0 0 rgb(var(--color-accent-rgb) / 0.85)",
        ].join(", "),
        "pixel-md-inset": [
          "inset -1px 0 0 0 rgba(255,255,255,0.22)",
          "inset 1px 0 0 0 rgba(255,255,255,0.22)",
          "inset 0 -1px 0 0 rgba(255,255,255,0.22)",
          "inset 0 1px 0 0 rgba(255,255,255,0.22)",
        ].join(", "),
        "pixel-lg": [
          "-2px 0 0 0 rgba(255,255,255,0.28)",
          "2px 0 0 0 rgba(255,255,255,0.28)",
          "0 -2px 0 0 rgba(255,255,255,0.28)",
          "0 2px 0 0 rgba(255,255,255,0.28)",
          "4px 4px 0 0 rgba(0,0,0,0.65)",
        ].join(", "),
        "pixel-lg-accent": [
          "-2px 0 0 0 rgba(255,255,255,0.28)",
          "2px 0 0 0 rgba(255,255,255,0.28)",
          "0 -2px 0 0 rgba(255,255,255,0.28)",
          "0 2px 0 0 rgba(255,255,255,0.28)",
          "4px 4px 0 0 rgb(var(--color-accent-rgb) / 0.85)",
        ].join(", "),
      },
      borderRadius: {
        none: "0",
      },
      animation: {
        "pixel-pulse": "pixel-pulse 1s steps(2) infinite",
        "pixel-blink": "pixel-blink 0.5s steps(1) infinite",
      },
      keyframes: {
        "pixel-pulse": {
          "0%, 100%": { opacity: "1" },
          "50%": { opacity: "0.5" },
        },
        "pixel-blink": {
          "0%, 100%": { opacity: "1" },
          "50%": { opacity: "0" },
        },
      },
    },
  },
  plugins: [],
};

export default config;
