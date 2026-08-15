import { defineConfig } from "vitepress";

export default defineConfig({
  lang: "en-US",
  title: "Effect Agent",
  description: "An Effect-native runtime for typed, resource-safe autonomous agents.",
  cleanUrls: true,
  lastUpdated: true,
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
      { text: "Future", link: "/future/phases", activeMatch: "/future/" },
      { text: "Reference", link: "/reference/status", activeMatch: "/reference/" },
      {
        text: "Design source",
        items: [
          { text: "Product specification", link: "/PRODUCT" },
          { text: "Technical architecture", link: "/ARCHITECTURE" },
          { text: "Decision register", link: "/DECISIONS" },
          { text: "Implementation roadmap", link: "/ROADMAP" },
        ],
      },
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
          { text: "Observability", link: "/guide/observability" },
          { text: "Conversations", link: "/guide/conversations" },
          { text: "Deterministic testing", link: "/guide/testing" },
        ],
      },
      {
        text: "Architecture",
        items: [
          { text: "Effect-native by construction", link: "/concepts/effect-native" },
          { text: "The runtime model", link: "/concepts/runtime-model" },
          { text: "Persistence & durability", link: "/concepts/durability" },
        ],
      },
      {
        text: "Future target",
        items: [
          { text: "Phases", link: "/future/phases" },
          { text: "Durable execution", link: "/future/durable-execution" },
          { text: "Declared Subagents", link: "/future/subagents" },
        ],
      },
      {
        text: "Reference",
        items: [
          { text: "Implementation status", link: "/reference/status" },
          { text: "Package map", link: "/reference/packages" },
          { text: "Normative specifications", link: "/REQUIREMENTS" },
          { text: "Architecture decisions", link: "/adr/" },
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
