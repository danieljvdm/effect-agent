import { defineConfig } from "vite-plus";

export default defineConfig({
  run: {
    tasks: {
      test: {
        command: "vp test --passWithNoTests",
        // Fresh runners lack Vite's temporary directories. Ignore those and
        // dependency directory listings, while retaining dependency file hashes.
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
  pack: {
    entry: [
      "src/index.ts",
      "src/NodeDurableAgentRuntime.ts",
      "src/NodeDurableHost.ts",
      "src/NodeScheduling.ts",
      "src/NodeSubscriptions.ts",
      "src/NodeWakeScheduler.ts",
      "src/NodeWorkflow.ts",
    ],
    dts: true,
    sourcemap: true,
  },
  test: {
    cache: false,
    silent: "passed-only",
  },
});
