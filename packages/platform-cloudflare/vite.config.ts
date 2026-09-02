import { cloudflareTest } from "@cloudflare/vitest-pool-workers";
import { defineConfig, type UserConfig } from "vite-plus";

// Ignore generated Vite files and node_modules directory listings, while
// retaining dependency file hashes. The lockfile covers dependency additions.
const run: NonNullable<UserConfig["run"]> = {
  tasks: {
    test: {
      command: "vitest run",
      env: ["BROWSER_TEST_EXECUTABLE"],
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

// Two lanes, one runner (WP0 probe contract, D-P6-7 Fallback A):
//
// - `workerd` — tests execute inside workerd against real SQLite-backed Thread Durable
//   Objects (`@cloudflare/vitest-pool-workers` 0.21.x via its `cloudflareTest` Vite plugin;
//   `defineWorkersConfig` no longer exists on the vitest 4 line). The 0.21.x pool has no
//   `isolatedStorage`: Durable Object storage is SHARED across tests within a run, so every
//   suite mints a unique Thread name per case.
// - `restart` — Node-side Miniflare programmatic runtimes for restart-persistence evidence
//   (dispose/reopen over one persist directory); these spawn real runtimes and HTTP
//   listeners and cannot run inside workerd.
export default defineConfig({
  run,
  // A package-level Vite config suppresses `vp pack`'s zero-config library
  // defaults, so the published artifact's declarations and sourcemap are
  // pinned explicitly here.
  pack: {
    entry: [
      "src/index.ts",
      "src/scheduling.ts",
      "src/subscriptions.ts",
      "src/browser-quick-action.ts",
      "src/interactive-browser.ts",
      "src/browser-rest-capture.ts",
      "src/browser-rest-crawl.ts",
    ],
    dts: true,
    sourcemap: true,
  },
  test: {
    // Vite Task owns result caching; Vitest's results.json is read and
    // rewritten by every run, which makes the entire task uncacheable.
    cache: false,
    silent: "passed-only",
    projects: [
      {
        plugins: [
          cloudflareTest({
            main: "./test/worker.ts",
            miniflare: {
              compatibilityDate: "2025-05-01",
              compatibilityFlags: ["nodejs_compat"],
              durableObjects: {
                THREADS: { className: "TestThreadObject", useSQLite: true },
                MEMORIES: { className: "TestMemoryObject", useSQLite: true },
                SCHEDULES: { className: "TestScheduleOwnerObject", useSQLite: true },
                SUBSCRIPTIONS: { className: "TestSubscriptionPartitionObject", useSQLite: true },
                LIMITED: { className: "LimitedThreadObject", useSQLite: true },
                TINYDB: { className: "TinyDatabaseThreadObject", useSQLite: true },
                DENIED: { className: "DeniedThreadObject", useSQLite: true },
                SUBAGENTS: { className: "SubagentThreadObject", useSQLite: true },
                DYNAMIC_BINDINGS: {
                  className: "DynamicBindingsThreadObject",
                  useSQLite: true,
                },
                TELEMETRY: {
                  className: "TelemetryThreadObject",
                  useSQLite: true,
                },
                CONTEXT_COMPACTOR: {
                  className: "ContextCompactorThreadObject",
                  useSQLite: true,
                },
              },
            },
          }),
        ],
        test: {
          name: "workerd",
          include: ["test/**/*.test.ts"],
          exclude: [
            "test/restart/**",
            "test/code-mode/**",
            "test/interactive-browser-actions.test.ts",
            "test/interactive-browser-native.test.ts",
            "test/travel-planner-dc.test.ts",
          ],
        },
      },
      {
        test: {
          name: "browser-actions",
          include: [
            "test/interactive-browser-actions.test.ts",
            "test/interactive-browser-native.test.ts",
          ],
        },
      },
      {
        // WP5's Travel Planner slice runs against its OWN worker entry (the phase-6 fixture
        // Bindings) in a separate workerd instance, so the eviction worker's registrations
        // and this one never interfere.
        plugins: [
          cloudflareTest({
            main: "./test/travel-planner-worker.ts",
            miniflare: {
              compatibilityDate: "2025-05-01",
              compatibilityFlags: ["nodejs_compat"],
              durableObjects: {
                THREADS: { className: "TravelPlannerThreadObject", useSQLite: true },
                LIMITED: { className: "TravelPlannerLimitedObject", useSQLite: true },
              },
            },
          }),
        ],
        test: {
          name: "travel-planner",
          include: ["test/travel-planner-dc.test.ts"],
        },
      },
      {
        // The Code Mode Dynamic Worker executor lane runs the real adapter
        // inside a bundled worker under programmatic Miniflare (like the
        // restart lane) so Worker Loader and cross-event RPC ownership use a
        // real workerd process rather than a Node substitute.
        test: {
          name: "code-mode",
          include: ["test/code-mode/**/*.test.ts"],
        },
      },
      {
        test: {
          name: "restart",
          include: ["test/restart/**/*.test.ts"],
        },
      },
    ],
  },
});
