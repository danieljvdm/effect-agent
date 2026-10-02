import starlight from "@astrojs/starlight";
import yieldedTheme from "@yielded/starlight-theme";
import { defineConfig } from "astro/config";
import starlightLinksValidator from "starlight-links-validator";

import { snippetPlugins } from "./integrations/snippets.ts";
import { socialImages } from "./integrations/social-images.ts";

export default defineConfig({
  site: "https://yielded.dev",
  base: "/agent",
  trailingSlash: "always",
  integrations: [
    starlight({
      title: "Yielded Agent",
      description: "An agent harness toolkit for TypeScript, built on Effect and Effect AI.",
      favicon: "/mark.svg",
      lastUpdated: true,
      plugins: [yieldedTheme({ library: "agent" }), starlightLinksValidator()],
      routeMiddleware: "./src/route-middleware.ts",
      tableOfContents: { minHeadingLevel: 2, maxHeadingLevel: 3 },
      expressiveCode: { plugins: snippetPlugins },
      head: [
        {
          tag: "link",
          attrs: {
            rel: "icon",
            type: "image/png",
            sizes: "32x32",
            href: "/agent/favicon-32x32.png",
          },
        },
        {
          tag: "link",
          attrs: { rel: "apple-touch-icon", sizes: "180x180", href: "/agent/apple-touch-icon.png" },
        },
        { tag: "meta", attrs: { name: "theme-color", content: "#161714" } },
        { tag: "meta", attrs: { property: "og:site_name", content: "Yielded Agent" } },
        { tag: "meta", attrs: { property: "og:locale", content: "en_US" } },
      ],
      sidebar: [
        {
          label: "Guide",
          items: [
            {
              label: "Start",
              items: [
                { label: "Overview", link: "/guide/" },
                { label: "Getting started", link: "/guide/getting-started/" },
                { label: "Migrate from Effect Agent", link: "/guide/migration/" },
                { label: "What is Yielded Agent?", link: "/guide/introduction/" },
              ],
            },
            {
              label: "Build agents",
              items: [
                { label: "Agent definitions", link: "/guide/agents/" },
                { label: "Tools & layers", link: "/guide/tools/" },
                { label: "Run & stream", link: "/guide/run-agents/" },
                { label: "Threads", link: "/guide/threads/" },
                { label: "Context management", link: "/guide/context-management/" },
              ],
            },
            {
              label: "Extensions",
              items: [
                {
                  label: "Subagents",
                  collapsed: false,
                  items: [
                    { label: "Overview", link: "/guide/subagents/" },
                    { label: "In-memory attached", link: "/guide/subagents/in-memory-attached/" },
                    { label: "Durable attached", link: "/guide/subagents/durable-attached/" },
                    { label: "Durable background", link: "/guide/subagents/background/" },
                  ],
                },
                { label: "Agent messaging", link: "/guide/messaging/" },
                { label: "Effect Workflows", link: "/guide/workflows/" },
                { label: "Sandbox execution", link: "/guide/sandbox/" },
                { label: "Code Mode", link: "/guide/code-mode/" },
                { label: "Browser tools", link: "/guide/browser/" },
              ],
            },
            {
              label: "Test & operate",
              items: [
                { label: "Deterministic testing", link: "/guide/testing/" },
                { label: "Operations", link: "/guide/operations/" },
                { label: "Certify storage adapters", link: "/guide/certify-adapters/" },
              ],
            },
          ],
        },
        {
          label: "Platforms",
          items: [
            { label: "Overview", link: "/platforms/" },
            { label: "Node.js", link: "/platforms/node/" },
            { label: "Cloudflare", link: "/platforms/cloudflare/" },
          ],
        },
        {
          label: "Storage",
          items: [
            { label: "Overview", link: "/storage/" },
            { label: "In-memory", link: "/storage/memory/" },
            { label: "SQLite", link: "/storage/sqlite/" },
            { label: "PostgreSQL", link: "/storage/postgres/" },
            { label: "Cloudflare", link: "/storage/cloudflare/" },
          ],
        },
        {
          label: "Architecture",
          items: [
            { label: "Overview", link: "/concepts/" },
            { label: "The runtime model", link: "/concepts/runtime-model/" },
            { label: "Budgets & bounded autonomy", link: "/concepts/budgets/" },
            { label: "Persistence & durability", link: "/concepts/durability/" },
          ],
        },
        {
          label: "Reference",
          items: [
            { label: "Overview", link: "/reference/" },
            { label: "Package map", link: "/reference/packages/" },
            { label: "Decision models", link: "/reference/decision-models/" },
            { label: "Subagent policies & recovery", link: "/reference/subagents/" },
          ],
        },
      ],
    }),
    socialImages(),
  ],
});
