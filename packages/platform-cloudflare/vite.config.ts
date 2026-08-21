import { cloudflareTest } from "@cloudflare/vitest-pool-workers";
import { defineConfig } from "vite-plus";

// Two lanes, one runner (WP0 probe contract, D-P6-7 Fallback A):
//
// - `workerd` — tests execute inside workerd against real SQLite-backed Conversation Durable
//   Objects (`@cloudflare/vitest-pool-workers` 0.21.x via its `cloudflareTest` Vite plugin;
//   `defineWorkersConfig` no longer exists on the vitest 4 line). The 0.21.x pool has no
//   `isolatedStorage`: Durable Object storage is SHARED across tests within a run, so every
//   suite mints a unique Conversation name per case.
// - `restart` — Node-side Miniflare programmatic runtimes for restart-persistence evidence
//   (dispose/reopen over one persist directory); these spawn real runtimes and HTTP
//   listeners and cannot run inside workerd.
export default defineConfig({
  // A package-level Vite config suppresses `vp pack`'s zero-config library
  // defaults, so the published artifact's declarations and sourcemap are
  // pinned explicitly here.
  pack: {
    entry: ["src/index.ts", "src/browser-quick-action.ts"],
    dts: true,
    sourcemap: true,
  },
  test: {
    projects: [
      {
        plugins: [
          cloudflareTest({
            main: "./test/worker.ts",
            miniflare: {
              compatibilityDate: "2025-05-01",
              compatibilityFlags: ["nodejs_compat"],
              durableObjects: {
                CONVERSATIONS: { className: "TestConversationObject", useSQLite: true },
                LIMITED: { className: "LimitedConversationObject", useSQLite: true },
                TINYDB: { className: "TinyDatabaseConversationObject", useSQLite: true },
                DENIED: { className: "DeniedConversationObject", useSQLite: true },
                SUBAGENTS: { className: "SubagentConversationObject", useSQLite: true },
                DYNAMIC_BINDINGS: {
                  className: "DynamicBindingsConversationObject",
                  useSQLite: true,
                },
                TELEMETRY: {
                  className: "TelemetryConversationObject",
                  useSQLite: true,
                },
                CONTEXT_COMPACTOR: {
                  className: "ContextCompactorConversationObject",
                  useSQLite: true,
                },
              },
            },
          }),
        ],
        test: {
          name: "workerd",
          include: ["test/**/*.test.ts"],
          exclude: ["test/restart/**", "test/code-mode/**", "test/travel-planner-dc.test.ts"],
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
                CONVERSATIONS: { className: "TravelPlannerConversationObject", useSQLite: true },
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
