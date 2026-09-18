import cloudflare from "@alchemy.run/cloudflare-runtime/vite";
import tailwindcss from "@tailwindcss/vite";
import { tanstackStart } from "@tanstack/react-start/plugin/vite";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite-plus";

export default defineConfig({
  define: { "globalThis.__ALCHEMY_RUNTIME__": "true" },
  plugins: process.env.VITEST
    ? [react()]
    : [
        process.env.ALCHEMY_CLOUDFLARE_VITE_INJECTED === "1"
          ? null
          : cloudflare({
              main: "src/worker.ts",
              compatibilityDate: "2026-07-01",
              compatibilityFlags: ["nodejs_compat"],
            }),
        {
          name: "travel-planner-alchemy-runtime",
          enforce: "pre",
          resolveId(source) {
            if (source === "effect-cf" || source.startsWith("effect-cf/"))
              this.error(`The travel planner uses Alchemy; unexpected runtime import: ${source}`);
          },
        },
        tanstackStart(),
        tailwindcss(),
        react(),
      ],
  resolve: { tsconfigPaths: true },
  test: {
    cache: false,
    silent: "passed-only",
    deps: {
      optimizer: {
        ssr: {
          enabled: true,
          include: ["@effect-agent/platform-alchemy-cloudflare/cloudflare-bindings"],
          exclude: ["effect", "effect-agent"],
          rolldownOptions: { external: [/^cloudflare:/] },
        },
      },
    },
  },
  run: {
    tasks: {
      deploy: {
        cache: false,
        command: "vp exec alchemy deploy alchemy.run.ts --stage production",
      },
      build: {
        command: "vp build",
        input: [
          { auto: true },
          // Track root inputs individually so a missing generated dist
          // directory does not invalidate the package directory listing.
          "*",
          { pattern: "!examples/travel-planner", base: "workspace" },
          "!dist",
          "!dist/**",
          { pattern: "bun.lock", base: "workspace" },
          { pattern: "!**/node_modules", base: "workspace" },
          { pattern: "!**/node_modules/.vite*", base: "workspace" },
          { pattern: "!**/node_modules/.vite*/**", base: "workspace" },
        ],
      },
      test: {
        command: "vp test",
        // Fresh runners do not have Vite's generated directories. Keep
        // dependency file hashes and the lockfile, but ignore directory listings.
        input: [
          { auto: true },
          { pattern: "bun.lock", base: "workspace" },
          { pattern: "!**/node_modules", base: "workspace" },
          { pattern: "!**/node_modules/.vite*", base: "workspace" },
          { pattern: "!**/node_modules/.vite*/**", base: "workspace" },
        ],
        output: [],
      },
      check: {
        command: "tsc --noEmit",
        input: [
          { auto: true },
          "src/**",
          "test/**",
          "tsconfig.json",
          "alchemy.run.ts",
          "!*.tsbuildinfo",
        ],
        output: [{ auto: true }, "!*.tsbuildinfo"],
      },
    },
  },
});
