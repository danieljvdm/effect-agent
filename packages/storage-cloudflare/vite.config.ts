import { cloudflareTest } from "@cloudflare/vitest-pool-workers";
import { defineConfig, type UserConfig } from "vite-plus";

// Ignore generated Vite files and node_modules directory listings, while
// retaining dependency file hashes. The lockfile covers dependency additions.
const run: NonNullable<UserConfig["run"]> = {
  tasks: {
    test: {
      command: "vitest run",
      input: [
        { auto: true },
        { pattern: "bun.lock", base: "workspace" },
        { pattern: "!**/node_modules", base: "workspace" },
        { pattern: "!**/node_modules/.vite*", base: "workspace" },
        { pattern: "!**/node_modules/.vite*/**", base: "workspace" },
      ],
      output: [],
    },
  },
};

// Run this package's tests inside workerd with real SQLite-backed Durable Object namespaces.
// `@cloudflare/vitest-pool-workers` 0.21.x (the vitest 4 line) replaced `defineWorkersConfig`
// with the `cloudflareTest` Vite plugin, which installs the workers pool runner on the
// project during `configureVitest`. Note the 0.21.x pool has no `isolatedStorage`: Durable
// Object storage is SHARED across tests within a run, so every suite mints a unique Durable
// Object name per case.
export default defineConfig({
  run,
  // A package-level Vite config suppresses `vp pack`'s zero-config library
  // defaults, so the published artifact's declarations and sourcemap are
  // pinned explicitly here.
  pack: {
    entry: ["src/index.ts", "src/testing.ts"],
    dts: true,
    sourcemap: true,
  },
  test: {
    // Leave result caching to Vite Task, without a mutable results.json input.
    cache: false,
    silent: "passed-only",
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
          SCHEDULES: { className: "ScheduleStorageObject", useSQLite: true },
        },
      },
    }),
  ],
});
