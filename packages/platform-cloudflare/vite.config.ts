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
  test: {
    // The eviction harness ABORTS Durable Objects mid-flight by design; the pool surfaces
    // each abort's orphaned in-flight promise as an "unhandled error" (durableObjectReset)
    // even when every test passes. Convergence is asserted explicitly by every row, so
    // these by-design rejections must not fail the run. Root-level only (non-project option).
    dangerouslyIgnoreUnhandledErrors: true,
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
                SUBAGENTS: { className: "SubagentConversationObject", useSQLite: true },
                DYNAMIC_BINDINGS: {
                  className: "DynamicBindingsConversationObject",
                  useSQLite: true,
                },
                ARRAY_BINDINGS: {
                  className: "ArrayBindingsConversationObject",
                  useSQLite: true,
                },
                EFFECT_BINDINGS: {
                  className: "EffectBindingsConversationObject",
                  useSQLite: true,
                },
              },
            },
          }),
        ],
        test: {
          name: "workerd",
          include: ["test/**/*.test.ts"],
          exclude: ["test/restart/**", "test/travel-planner-dc.test.ts"],
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
        test: {
          name: "restart",
          include: ["test/restart/**/*.test.ts"],
        },
      },
    ],
  },
});
