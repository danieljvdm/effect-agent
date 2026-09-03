import { defineConfig } from "vite-plus";

export default defineConfig({
  pack: {
    entry: [
      "src/index.ts",
      "src/AgentRuntime.ts",
      "src/Compaction.ts",
      "src/ContextCompactor.ts",
      "src/DurableStep.ts",
      "src/RunEventSink.ts",
      "src/RunOptions.ts",
      "src/ThreadHistory.ts",
      "src/ToolBroker.ts",
    ],
    dts: true,
    sourcemap: true,
  },
  test: { cache: false, silent: "passed-only" },
});
