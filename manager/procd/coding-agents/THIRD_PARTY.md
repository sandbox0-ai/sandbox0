# Coding Agent Packages

The `coding-agent` template installs these pinned third-party packages:

| Package | Version | Upstream license |
|---------|---------|------------------|
| `@openai/codex` | `0.152.0` | Apache-2.0 |
| `@anthropic-ai/claude-code` | `2.1.257` | See the license distributed with the package and Anthropic's applicable terms |
| `opencode-ai` | `1.18.25` | MIT |
| `@earendil-works/pi-coding-agent` | `0.84.4` | MIT |
| `@moonshot-ai/kimi-code` | `2.1.1` | MIT |
| Kimi native SDK | `21406fb4c805cc8c715e6d1f16ad3fb5f25f4fe3` (matching CLI 2.1.1) | MIT |
| `zai-org/ZCode` | `328c1a0c0ffaa5a4f65e8fa199af5e4c20706e5f` | Apache-2.0 |
| `@playwright/cli` | `0.1.19` | Apache-2.0 |
| `ws` | `8.21.0` | MIT |
| `ttyd` | `1.7.7` | MIT |
| noVNC standalone viewer | `1.5.0` | MPL-2.0 and bundled BSD notices; upstream license files retained in `/opt/sandbox0-browser/novnc` |
| Google Chrome Stable | Stable channel, resolved during the image build | [Google Chrome Terms of Service](https://www.google.com/chrome/terms/) |

This inventory does not replace the license and notice files distributed in each npm package. Review the current upstream terms before publishing a derived image, especially for packages that do not use an open-source license.
