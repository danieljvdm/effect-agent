import { cloudflareTest } from "@cloudflare/vitest-pool-workers";
import { defineConfig } from "vite-plus";

import { runtimeBundle } from "./test/runtime-bundle.ts";

export default defineConfig({
  pack: {
    entry: [
      "src/index.ts",
      "src/Alarm.ts",
      "src/CloudflareBindings.ts",
      "src/CloudflareBrowser.ts",
      "src/CloudflareThreadClient.ts",
      "src/ThreadObject.ts",
      "src/MemoryObject.ts",
      "src/Scheduling.ts",
      "src/Subscriptions.ts",
      "src/Rpc.ts",
      "src/Alarms.ts",
    ],
    dts: true,
    sourcemap: true,
  },
  run: {
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
  },
  test: {
    cache: false,
    silent: "passed-only",
    projects: [
      {
        plugins: [
          runtimeBundle(),
          cloudflareTest({
            main: "./test/worker.ts",
            miniflare: {
              compatibilityDate: "2026-08-18",
              compatibilityFlags: ["nodejs_compat"],
              bindings: { REGISTRATION_LABEL: "alchemy-config" },
              durableObjects: {
                THREADS: { className: "TestThread", useSQLite: true },
                PROBES: { className: "Probe", useSQLite: true },
                MEMORIES: { className: "TestMemory", useSQLite: true },
                SCHEDULES: { className: "TestScheduleOwner", useSQLite: true },
                SUBSCRIPTIONS: { className: "TestSubscriptionPartition", useSQLite: true },
              },
            },
          }),
        ],
        test: { name: "workerd", include: ["test/*.test.ts"] },
      },
      {
        test: { name: "restart", include: ["test/restart/*.test.ts"] },
      },
    ],
  },
});
