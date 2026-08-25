import { defineConfig } from "vitepress";

export default defineConfig({
  lang: "en-US",
  title: "Effect Agent",
  description: "An Effect-native runtime for typed, resource-safe autonomous agents.",
  cleanUrls: true,
  lastUpdated: true,
  // Contributor artifacts stay in the repository but out of the published site.
  srcExclude: ["TOOLCHAIN.md"],
  head: [
    ["link", { rel: "icon", type: "image/svg+xml", href: "/mark.svg" }],
    ["meta", { name: "theme-color", content: "#161714" }],
    ["meta", { property: "og:type", content: "website" }],
    ["meta", { property: "og:title", content: "Effect Agent" }],
    [
      "meta",
      {
        property: "og:description",
        content: "Typed agents. Explicit effects. Honest recovery.",
      },
    ],
  ],
  markdown: {
    // Code panels are always dark (--vp-code-block-bg), so pin dark tokens in both modes.
    theme: { light: "github-dark", dark: "github-dark" },
    lineNumbers: true,
    image: { lazyLoading: true },
    codeTransformers: [
      {
        name: "effect-agent-code-labels",
        code(node) {
          node.properties["data-effect-agent-code"] = "";
        },
      },
    ],
  },
  themeConfig: {
    logo: { src: "/mark.svg", alt: "Effect Agent" },
    siteTitle: "Effect Agent",
    nav: [
      { text: "Guide", link: "/guide/getting-started", activeMatch: "/guide/" },
      { text: "Architecture", link: "/concepts/effect-native", activeMatch: "/concepts/" },
      { text: "Reference", link: "/reference/packages", activeMatch: "/(reference|spec)/" },
    ],
    sidebar: [
      {
        text: "Start",
        items: [
          { text: "What is Effect Agent?", link: "/guide/introduction" },
          { text: "Getting started", link: "/guide/getting-started" },
        ],
      },
      {
        text: "Build agents",
        items: [
          { text: "Agent definitions", link: "/guide/agents" },
          { text: "Tools & Layers", link: "/guide/tools" },
          { text: "Run & stream", link: "/guide/run-agents" },
          { text: "Conversations", link: "/guide/conversations" },
          { text: "Context management", link: "/guide/context-management" },
          { text: "Deterministic testing", link: "/guide/testing" },
        ],
      },
      {
        text: "Operate",
        items: [
          { text: "Operations", link: "/guide/operations" },
          { text: "Certify storage adapters", link: "/guide/certify-adapters" },
        ],
      },
      {
        text: "Architecture",
        items: [
          { text: "Effect-native by construction", link: "/concepts/effect-native" },
          { text: "The runtime model", link: "/concepts/runtime-model" },
          { text: "Budgets & bounded autonomy", link: "/concepts/budgets" },
          { text: "Persistence & durability", link: "/concepts/durability" },
        ],
      },
      {
        text: "Reference",
        items: [{ text: "Package map", link: "/reference/packages" }],
      },
      {
        text: "Specifications",
        collapsed: true,
        items: [
          { text: "Authoring", link: "/spec/authoring" },
          { text: "Runtime", link: "/spec/runtime" },
          { text: "Providers", link: "/spec/providers" },
          { text: "Capabilities", link: "/spec/capabilities" },
          { text: "Subagents", link: "/spec/subagents" },
          { text: "Durability", link: "/spec/durability" },
          { text: "Persistence", link: "/spec/persistence" },
          { text: "Deployment", link: "/spec/deployment" },
          { text: "Security & operations", link: "/spec/security-operations" },
          { text: "PR review", link: "/spec/pr-review" },
          { text: "PR work orders", link: "/spec/pr-work-orders" },
          { text: "PR work-order ingress", link: "/spec/pr-work-order-ingress" },
          { text: "Testing", link: "/spec/testing" },
          { text: "Compatibility", link: "/spec/compatibility" },
        ],
      },
    ],
    search: {
      provider: "local",
      options: {
        detailedView: true,
        miniSearch: {
          searchOptions: { fuzzy: 0.2, prefix: true },
        },
      },
    },
    outline: { level: [2, 3], label: "On this page" },
    socialLinks: [{ icon: "github", link: "https://github.com/danieljvdm/effect-agent" }],
    editLink: {
      pattern: "https://github.com/danieljvdm/effect-agent/edit/main/docs/:path",
      text: "Edit this page",
    },
    lastUpdated: { text: "Last updated" },
    docFooter: { prev: "Previous", next: "Continue" },
    externalLinkIcon: true,
  },
});
