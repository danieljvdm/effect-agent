import { dirname, resolve } from "node:path";

import { transformerTwoslash } from "@shikijs/vitepress-twoslash";
import ts from "typescript-twoslash";
import { defineConfig } from "vitepress";

import tokyoNightLight from "./theme/tokyo-night-light.json";

export default defineConfig({
  lang: "en-US",
  title: "Effect Agent",
  description: "A TypeScript agent framework built on Effect and Effect AI.",
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
        content: "A TypeScript agent framework built on Effect and Effect AI.",
      },
    ],
  ],
  markdown: {
    theme: { light: { ...tokyoNightLight, type: "light" }, dark: "tokyo-night" },
    languages: ["js", "jsx", "ts", "tsx"],
    image: { lazyLoading: true },
    codeTransformers: [
      transformerTwoslash({
        throws: true,
        twoslashOptions: {
          // Twoslash uses the JS compiler API, which TypeScript 7 no longer exports.
          tsModule: ts,
          tsLibDirectory: dirname(ts.getDefaultLibFilePath({})),
          vfsRoot: resolve(import.meta.dirname, "../snippets/travel-planner"),
          // Re-read imported snippets when the dev server rebuilds a page.
          cache: false,
          fsCache: false,
          compilerOptions: {
            target: ts.ScriptTarget.ES2023,
            module: ts.ModuleKind.ESNext,
            moduleResolution: ts.ModuleResolutionKind.Bundler,
            allowImportingTsExtensions: true,
            noEmit: true,
            strict: true,
            types: [],
          },
        },
      }),
    ],
  },
  themeConfig: {
    logo: { src: "/mark.svg", alt: "Effect Agent" },
    siteTitle: "Effect Agent",
    nav: [
      { text: "Guide", link: "/guide/getting-started", activeMatch: "/guide/" },
      { text: "Architecture", link: "/concepts/effect-native", activeMatch: "/concepts/" },
      { text: "Reference", link: "/reference/packages", activeMatch: "/reference/" },
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
