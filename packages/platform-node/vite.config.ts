import { defineConfig } from "vite-plus";

export default defineConfig({
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
