import { dirname, resolve } from "node:path";

import { transformerTwoslash } from "@shikijs/vitepress-twoslash";
import ts from "typescript-twoslash";
import { defineConfig } from "vitepress";

import tokyoNightLight from "./theme/tokyo-night-light.json";

const siteUrl = "https://effect-agent.com";
const socialImage = `${siteUrl}/social-card.png`;

const socialImageAlt =
  "Effect Agent. An agent harness toolkit for TypeScript. Built on Effect and Effect AI.";

export default defineConfig({
  lang: "en-US",
  title: "Effect Agent",
  description: "An agent harness toolkit for TypeScript, built on Effect and Effect AI.",
  cleanUrls: true,
  lastUpdated: true,
  // Contributor artifacts stay in the repository but out of the published site.
  srcExclude: ["TOOLCHAIN.md"],
  head: [
    ["link", { rel: "icon", type: "image/svg+xml", href: "/mark.svg" }],
    ["link", { rel: "icon", type: "image/png", sizes: "32x32", href: "/favicon-32x32.png" }],
    ["link", { rel: "apple-touch-icon", sizes: "180x180", href: "/apple-touch-icon.png" }],
    ["meta", { name: "theme-color", content: "#161714" }],
    ["meta", { property: "og:type", content: "website" }],
    ["meta", { property: "og:site_name", content: "Effect Agent" }],
    ["meta", { property: "og:locale", content: "en_US" }],
    ["meta", { property: "og:image", content: socialImage }],
    ["meta", { property: "og:image:type", content: "image/png" }],
    ["meta", { property: "og:image:width", content: "1200" }],
    ["meta", { property: "og:image:height", content: "630" }],
    ["meta", { property: "og:image:alt", content: socialImageAlt }],
    ["meta", { name: "twitter:card", content: "summary_large_image" }],
    ["meta", { name: "twitter:image", content: socialImage }],
    ["meta", { name: "twitter:image:alt", content: socialImageAlt }],
  ],
  // Preview crawlers read the built HTML. Use VitePress's resolved title and
  // description so guide links keep their own metadata, including title templates.
  transformHead({ page, title, description }) {
    if (page === "404.md") return [];

    const path = page.replace(/(^|\/)index\.md$/, "$1").replace(/\.md$/, "");
    const url = new URL(path, `${siteUrl}/`).href;

    return [
      ["link", { rel: "canonical", href: url }],
      ["meta", { property: "og:url", content: url }],
      ["meta", { property: "og:title", content: title }],
      ["meta", { property: "og:description", content: description }],
      ["meta", { name: "twitter:title", content: title }],
      ["meta", { name: "twitter:description", content: description }],
    ];
  },
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
          // VitePress sets production mode before loading build configuration.
          // Reuse compiler state within a build, but re-read snippets in dev.
          cache: process.env.NODE_ENV === "production",
          fsCache: process.env.NODE_ENV === "production",
          compilerOptions: {
            target: ts.ScriptTarget.ES2023,
            module: ts.ModuleKind.ESNext,
            moduleResolution: ts.ModuleResolutionKind.Bundler,
            allowImportingTsExtensions: true,
            noEmit: true,
            strict: true,
            // Worker examples use the platform package's own ambient types.
            typeRoots: [
              resolve(import.meta.dirname, "../../packages/platform-cloudflare/node_modules"),
            ],
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
      { text: "Platforms", link: "/platforms/", activeMatch: "/platforms/" },
      { text: "Architecture", link: "/concepts/effect-native", activeMatch: "/concepts/" },
      { text: "Reference", link: "/reference/packages", activeMatch: "/reference/" },
    ],
    sidebar: [
      {
        text: "Start",
        items: [
          { text: "Getting started", link: "/guide/getting-started" },
          { text: "What is Effect Agent?", link: "/guide/introduction" },
        ],
      },
      {
        text: "Build agents",
        items: [
          { text: "Agent definitions", link: "/guide/agents" },
          { text: "Tools & layers", link: "/guide/tools" },
          { text: "Run & stream", link: "/guide/run-agents" },
          { text: "Threads", link: "/guide/threads" },
          { text: "Context management", link: "/guide/context-management" },
        ],
      },
      {
        text: "Extensions",
        items: [
          { text: "Subagents", link: "/guide/subagents" },
          { text: "Sandbox execution", link: "/guide/sandbox" },
          { text: "Code Mode", link: "/guide/code-mode" },
          { text: "Browser tools", link: "/guide/browser" },
        ],
      },
      {
        text: "Platforms",
        items: [
          { text: "Overview", link: "/platforms/" },
          { text: "Node.js", link: "/platforms/node" },
          { text: "Cloudflare", link: "/platforms/cloudflare" },
        ],
      },
      {
        text: "Test & operate",
        items: [
          { text: "Deterministic testing", link: "/guide/testing" },
          { text: "Operations", link: "/guide/operations" },
          { text: "Certify storage adapters", link: "/guide/certify-adapters" },
        ],
      },
      {
        text: "Architecture",
        items: [
          { text: "Built on Effect", link: "/concepts/effect-native" },
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
