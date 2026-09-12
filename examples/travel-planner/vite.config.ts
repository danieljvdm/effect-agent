import cloudflare from "@alchemy.run/cloudflare-runtime/vite";
import tailwindcss from "@tailwindcss/vite";
import { tanstackStart } from "@tanstack/react-start/plugin/vite";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite-plus";

export default defineConfig({
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
        tanstackStart(),
        tailwindcss(),
        react(),
      ],
  resolve: { tsconfigPaths: true },
  test: { cache: false, silent: "passed-only" },
  run: {
    tasks: {
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
