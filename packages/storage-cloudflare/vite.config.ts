import { cloudflareTest } from "@cloudflare/vitest-pool-workers";
import { defineConfig } from "vite-plus";

// Run this package's tests inside workerd with real SQLite-backed Durable Object namespaces.
// `@cloudflare/vitest-pool-workers` 0.21.x (the vitest 4 line) replaced `defineWorkersConfig`
// with the `cloudflareTest` Vite plugin, which installs the workers pool runner on the
// project during `configureVitest`. Note the 0.21.x pool has no `isolatedStorage`: Durable
// Object storage is SHARED across tests within a run, so every suite mints a unique Durable
// Object name per case.
export default defineConfig({
  pack: {
    dts: true,
    format: ["esm"],
    sourcemap: true,
  },
  plugins: [
    cloudflareTest({
      main: "./test/worker.ts",
      miniflare: {
        compatibilityDate: "2025-05-01",
        compatibilityFlags: ["nodejs_compat"],
        durableObjects: {
          PROBE: { className: "ProbeDurableObject", useSQLite: true },
          CONVERSATIONS: { className: "ConversationStorageObject", useSQLite: true },
        },
      },
    }),
  ],
});
