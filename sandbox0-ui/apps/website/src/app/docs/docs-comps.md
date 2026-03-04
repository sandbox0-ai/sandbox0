# Docs Components & Syntax Guide

This document describes all available syntax sugar, components, and usage guidelines for the Sandbox0 Docs site.

## Tech Stack

- **Next.js 15** (App Router)
- **MDX** (Markdown + React Components)
- **Tailwind CSS** (Pixel-art themed)
- **Prism.js** (Syntax highlighting)

---

## Basic Markdown Syntax

### Headings

```mdx
# H1 Heading (page title, one per page)
## H2 Heading (section title)
### H3 Heading (subsection)
#### H4 Heading (finer divisions)
```

### Paragraphs and Text

```mdx
Regular paragraph text.

**Bold text**
*Italic text*
`Inline code`

~~Strikethrough~~
```

### Lists

```mdx
- Unordered list item
- Another item
  - Nested item

1. Ordered list item
2. Second item
   1. Nested ordered item
```

### Blockquotes

```mdx
> This is a blockquote
> Can span multiple lines
```

### Links and Images

```mdx
[Link text](https://example.com)

![Alt text](/path/to/image.png)
```

### Horizontal Rule

```mdx
---
```

### Tables

```mdx
| Field | Type | Description |
|-------|------|-------------|
| id | string | Unique identifier |
| name | string | Display name |
| active | boolean | Whether active |
```

---

## MDX Components

### Callout (Info Box)

Use to highlight important information, warnings, or tips.

```mdx
<Callout variant="info">
  This is an informational callout
</Callout>

<Callout variant="success">
  Operation successful! Green accent
</Callout>

<Callout variant="warning">
  Warning message, yellow accent
</Callout>

<Callout variant="danger">
  Error or dangerous operation, red accent
</Callout>
```

**Available variants**: `info` | `success` | `warning` | `danger`

**Use cases**:
- `info`: General notes, tips
- `success`: Success states, recommended practices
- `warning`: Cautions, potential issues
- `danger`: Errors, deprecated practices, dangerous operations

---

### Badge (Status Tag)

Use for status indicators, version labels, etc.

```mdx
<Badge variant="default">Default</Badge>
<Badge variant="accent">Accent</Badge>
<Badge variant="success">Success</Badge>
<Badge variant="warning">Warning</Badge>
<Badge variant="danger">Danger</Badge>
```

**Available variants**: `default` | `accent` | `success` | `warning` | `danger`

---

### Tabs (Multi-language Code Examples)

Use to display multi-language code examples or multiple options.

#### Code Tabs (Recommended)

Use `code` and `language` props for syntax-highlighted code:

```mdx
<Tabs
  tabs={[
    { label: "Go", language: "go", code: "package main\n\nfunc main() {\n    fmt.Println(\"Hello\")\n}" },
    { label: "Python", language: "python", code: "print(\"Hello\")" },
    { label: "TypeScript", language: "typescript", code: "console.log(\"Hello\");" },
    { label: "CLI", language: "bash", code: "s0 sandbox create" }
  ]}
/>
```

#### Content Tabs (For Mixed Content)

Use `content` prop for React nodes:

```mdx
<Tabs
  tabs={[
    { label: "Option A", content: (<div>Content A</div>) },
    { label: "Option B", content: (<div>Content B</div>) }
  ]}
/>
```

**Features**:
- Tab selection persists to localStorage
- Multiple Tabs components on the same page sync selection
- Standard order: Go → Python → TypeScript → CLI

**Guidelines** (per docs-spec.md):
- Use `<Tabs>` for multi-language examples, do not create separate docs per language
- Keep examples semantically consistent across languages
- Mark unimplemented languages clearly as TODO

---

### TerminalBlock (Terminal Output)

Use to display command-line interactions and output.

```mdx
<TerminalBlock lines={
`$ s0 sandbox create --template python
✓ Sandbox created successfully
sandbox-id: sb_abc123

$ s0 sandbox exec sb_abc123 -- echo "Hello"
Hello`
} />
```

**Features**:
- Pixel-style terminal appearance
- Auto-detects `$` prompt
- Best for CLI operation flows

---

### Endpoint (API Endpoint Display)

Use to display API endpoint information.

```mdx
<Endpoint method="GET">
/api/v1/sandboxes
</Endpoint>

<Endpoint method="POST">
/api/v1/sandboxes
</Endpoint>

<Endpoint method="PUT">
/api/v1/sandboxes/{sandbox_id}
</Endpoint>

<Endpoint method="DELETE">
/api/v1/sandboxes/{sandbox_id}
</Endpoint>
```

**Available methods**: `GET` | `POST` | `PUT` | `DELETE` | `PATCH`

---

### Code Blocks

Standard fenced code blocks with syntax highlighting:

```mdx
```bash
npm install sandbox0
```

```python
from sandbox0 import Client

client = Client(api_key="your-api-key")
```

```typescript
import { Client } from 'sandbox0';

const client = new Client({ token: 'your-api-key' });
```

```go
import sandbox0 "github.com/sandbox0-ai/sdk-go"

client := sandbox0.NewClient(sandbox0.WithToken("your-api-key"))
```

```json
{
  "sandbox_id": "sb_abc123",
  "status": "running"
}
```

```yaml
apiVersion: sandbox0.ai/v1alpha1
kind: Sandbox0Infra
metadata:
  name: example
```
```

**Supported languages**: bash, python, javascript, typescript, go, json, yaml, dockerfile, shell, etc.

**Features**:
- Auto copy button
- Syntax highlighting
- Optional line numbers

---

### DocsHero (Docs Homepage Hero)

Use for the hero section at the top of docs homepage.

```mdx
<DocsHero title="Welcome to Sandbox0 Docs">
  Build isolated execution environments for AI Agents
</DocsHero>
```

---

### CardGrid + LinkCard (Card Grid)

Use to display navigation links or feature overviews.

```mdx
<CardGrid>
  <LinkCard
    title="Get Started"
    href="/docs/get-started"
    cta="Get Started"
  >
    Get up and running with Sandbox0 in 5 minutes
  </LinkCard>

  <LinkCard
    title="Sandbox API"
    href="/docs/sandbox"
    cta="View Docs"
  >
    Sandbox lifecycle and execution management
  </LinkCard>

  <LinkCard
    title="Volume Storage"
    href="/docs/volume"
    cta="Learn More"
  >
    Persistent storage and snapshots
  </LinkCard>
</CardGrid>
```

---

### ResourceList + ResourceItem (Resource List)

Use to display lists of tutorials, guides, etc.

```mdx
<ResourceList>
  <ResourceItem
    badge="Tutorial"
    description="Complete getting started tutorial"
    href="/docs/get-started/quickstart"
  />

  <ResourceItem
    badge="Guide"
    description="Advanced configuration and best practices"
    href="/docs/guides/advanced"
  />

  <ResourceItem
    badge="Reference"
    description="Complete API reference documentation"
    href="/docs/api-reference"
  />
</ResourceList>
```

---

### LinkRow (Link Row)

Use to display social media or external resource links.

```mdx
<LinkRow
  links="Discord=https://discord.gg/sandbox0|GitHub=https://github.com/sandbox0|Email=mailto:support@sandbox0.ai"
/>
```

**Format**: `Display Name=URL`, multiple links separated by `|`

---

## File Structure

```
src/app/docs/
├── page.mdx                    # Docs homepage
├── layout.tsx                  # Docs layout
├── docs.ts                     # Navigation config
├── docs-spec.md                # Documentation spec
├── docs-comps.md               # This file
├── get-started/
│   ├── page.mdx
│   ├── authentication/
│   │   └── page.mdx
│   └── concepts/
│       └── page.mdx
├── sandbox/
│   ├── page.mdx
│   ├── contexts/
│   │   └── page.mdx
│   ├── files/
│   │   └── page.mdx
│   ├── network/
│   │   └── page.mdx
│   ├── ports/
│   │   └── page.mdx
│   └── webhooks/
│       └── page.mdx
├── volume/
│   ├── page.mdx
│   ├── mounts/
│   │   └── page.mdx
│   └── snapshots/
│       └── page.mdx
├── template/
│   ├── page.mdx
│   ├── spec/
│   │   └── page.mdx
│   └── visibility/
│       └── page.mdx
└── self-hosted/
    ├── page.mdx
    ├── architecture/
    │   └── page.mdx
    ├── install/
    │   └── page.mdx
    ├── deploy-scenarios/
    │   └── page.mdx
    ├── configuration/
    │   └── page.mdx
    ├── upgrade/
    │   └── page.mdx
    └── troubleshooting/
        └── page.mdx
```

---

## Navigation Configuration

Navigation structure is defined in `docs.ts`:

```typescript
export const docsNavigation: NavSection[] = [
  {
    label: "GET STARTED",
    href: "/docs/get-started",
    items: [
      { label: "Overview", href: "/docs/get-started" },
      { label: "Authentication", href: "/docs/get-started/authentication" },
      { label: "Concepts", href: "/docs/get-started/concepts" },
    ],
  },
  // ... other sections
];
```

After adding new pages, update the navigation config in `docs.ts`.

---

## Style Guidelines

### Pixel Art Theme

- Use pixel font (font-pixel) for headings
- Sharp corners (rounded-none or rounded-sm)
- Pixel-style shadows
- Retro gaming color scheme

### Color Semantics

- `background`: Page background
- `surface`: Card/component background
- `foreground`: Primary text
- `accent`: Highlight/link color
- `success`: Success state
- `warning`: Warning state
- `danger`: Error state

---

## Writing Guidelines

1. **Content Sources**: All technical content must derive from authoritative sources (see docs-spec.md)
2. **Language**: Documentation content in English; code and variable names in English
3. **Code Examples**: Must be runnable and consistent with current API behavior
4. **Tabs Usage**: Use Tabs for multi-language examples, do not create language subdirectories
5. **Link Format**: Use relative paths `/docs/xxx` instead of absolute paths
6. **API Paths**: Use `/api/v1/...` format consistent with OpenAPI

---

## Common Issues

### Q: How to add a new MDX component?

1. Define the component in `src/components/docs/MDXComponents.tsx`
2. Create component file under `src/components/docs/`
3. Export and add to MDXComponents mapping

### Q: How to modify navigation structure?

Edit `src/app/docs/docs.ts`.

### Q: Code block doesn't support a language?

Check `Prism` language support config, may need to add new language definition in code block component.

### Q: Tabs not syncing across pages?

Tabs use localStorage for persistence, ensure label names are consistent across pages.

---

## Reference Links

- [MDX Official Docs](https://mdxjs.com/)
- [Next.js App Router](https://nextjs.org/docs/app)
- [Tailwind CSS](https://tailwindcss.com/docs)
- [Prism Syntax Highlighting](https://prismjs.com/)
