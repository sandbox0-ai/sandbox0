# 文档写作指南（Markdown / MDX 支持说明）

本目录（`apps/website/src/app/docs/`）下的文档页面主要使用 **MDX**：在 Markdown 基础语法之上，允许直接写 JSX 组件（例如 `<Callout />`、`<Tabs />`）。

> 说明：下面的“支持语法”以当前站点的 MDX 渲染与组件映射为准（见 `apps/website/src/components/docs/MDXComponents.tsx`）。

---

## 1. 如何新建文档

- **新建页面**：在 `apps/website/src/app/docs/<slug>/page.mdx` 创建一个 MDX 页面。
- **纯说明文档**：可以放置 `*.md` 或 `*.mdx` 文件用于存档/协作，但是否作为路由页面渲染以 `page.mdx` 为准。

---

## 2. 支持的 Markdown 基础语法

### 2.1 标题（Headings）

    # 一级标题
    ## 二级标题
    ### 三级标题
    #### 四级标题

### 2.2 段落（Paragraph）

正常换行分段即可：

    这是第一段。
    
    这是第二段。

### 2.3 列表（Lists）

无序列表：

    - 条目 1
    - 条目 2
      - 子条目

有序列表：

    1. 第一项
    2. 第二项

### 2.4 链接（Links）

    [Sandbox0 官网](https://sandbox0.ai)

### 2.5 行内代码（Inline code）

    使用 `s0` CLI 创建沙箱。

### 2.6 代码块（Fenced code block）

使用三引号 fenced code：

    三个反引号 + bash
    s0 create --template python
    三个反引号

> 站点会把 fenced code 渲染成统一样式的代码块组件。

### 2.7 引用（Blockquote）

    > 这是一段引用说明。

### 2.8 表格（Tables）

    | 字段 | 说明 |
    | ---- | ---- |
    | id   | 沙箱 ID |
    | name | 名称 |

### 2.9 分割线（Horizontal rule）

    ---

---

## 3. 支持的 MDX / 自定义组件（推荐用法）

下面这些组件可以直接在 `*.mdx` 中使用（不需要 import），用于减少手写 `<div className="...">` 之类的样板代码。

### 3.1 `Callout`

用于提示/警告/说明卡片。

    <Callout type="info" title="提示">
      这里是说明内容。
    </Callout>

- **type**：`info` / `success` / `warning` / `danger`
- **title**：可选标题
- **className**：可选，支持额外样式
- **默认间距**：`Callout` 会自动带垂直间距（方便写文档，不用再套外层 `div` 来加 `mt-...`）

### 3.2 `Tabs`

用于多语言/多选项切换展示。

    <Tabs
      tabs={[
        { label: "Python", content: <CodeBlock language="bash">{`pip install sandbox0`}</CodeBlock> },
        { label: "JavaScript", content: <CodeBlock language="bash">{`npm install @sandbox0/sdk`}</CodeBlock> },
      ]}
    />

### 3.3 `CodeBlock`

当你需要在 JSX 中直接渲染代码块（例如配合 `Tabs`），推荐用它：

    <CodeBlock language="python" filename="example.py" scale="md">
    {`print("hello")`}
    </CodeBlock>

> 普通三引号 fenced code 也支持；`CodeBlock` 更适合在 JSX 结构里组合使用。

### 3.4 `Badge`

用于标记小标签。

    <Badge variant="accent" size="md">Beta</Badge>

### 3.5 `LinkRow`（链接行语法糖）

用于“横向链接列表”，避免手写 `<div className="flex ...">`。

    <LinkRow links="Discord=https://discord.gg/sandbox0|GitHub=https://github.com/sandbox0|Email=mailto:support@sandbox0.ai" />

- **links 格式**：用 `|` 分隔多个链接；每个链接是 `label=url`
- **className**：控制外层容器（默认 `flex flex-wrap gap-4 mt-4`）
- **linkClassName**：控制每个链接的样式（默认带 hover）

### 3.6 `ResourceList` / `ResourceItem`（通用资源列表）

用于“徽章 + 描述 + 右侧链接”的通用列表（SDK 列表只是其中一种用法）。

    <ResourceList>
      <ResourceItem
        badge="Python"
        description="Full-featured Python SDK with async support"
        href="/docs/sdks/python"
      />
      <ResourceItem
        badge="Go"
        description="High-performance Go SDK with full API coverage"
        href="/docs/sdks/go"
        cta="查看文档 →"
      />
    </ResourceList>

### 3.7 `TerminalBlock`（终端输出语法糖）

用于展示 CLI 命令输出，会自动根据行首符号进行着色。

    <TerminalBlock lines={"$ s0 create --template python\n✓ Sandbox created in 98ms\nsandbox-id: sb_abc123"} />

- **lines**：使用 `\n` 分隔的多行字符串。
- **自动着色规则**：
  - `$` 开头：渲染为 `text-accent`（命令提示符）
  - `✓` 或 `success` 开头：渲染为 `text-green-500`（成功）
  - `✕` 或 `error` 开头：渲染为 `text-red-500`（错误）
  - 其他：渲染为 `text-muted`（普通输出）

### 3.8 `Endpoint`（API 路由语法糖）

用于展示 API 的 HTTP 方法和路由路径，会自动根据方法名匹配像素风格颜色。

    <Endpoint method="GET">/sandboxes/:id</Endpoint>
    <Endpoint method="POST">/sandboxes</Endpoint>

- **method**：支持 `GET` (绿色), `POST` (蓝色), `DELETE` (红色), `PUT`/`PATCH` (黄色)。
- **children**：API 路径文本。

### 3.9 Landing 页面组件

这些更偏向落地页/入口页的布局组件：

- `DocsHero`
- `CardGrid`
- `LinkCard`

示例：

    <DocsHero title="DOCUMENTATION">
      这里是一段简介。
    </DocsHero>
    
    <CardGrid>
      <LinkCard title="🚀 QUICK START" href="/docs/quickstart" cta="View Guide">
        Get your first sandbox running in under 5 minutes.
      </LinkCard>
    </CardGrid>

---

## 4. 推荐写法（减少 JSX 嵌套）

- **优先用 Markdown**：能用 `- 列表`、`[链接](...)`、fenced code 解决的就别写 JSX。
- **需要布局/组件时再用 MDX**：比如 `Tabs`、`Callout`、`LinkRow`、`ResourceList`。
- **避免手写容器 div**：常见场景优先用 `LinkRow` / `ResourceList` 等语法糖组件, 如果没有合适的语法糖, 允许创建新的语法糖。

