import { defineConfig } from "vite-plus";

export default defineConfig({
  pack: {
    entry: [
      "src/index.ts",
      "src/AgentWorkflow.ts",
      "src/WorkflowAgentHost.ts",
      "src/WorkflowDispatch.ts",
      "src/WorkflowExecution.ts",
    ],
    dts: true,
    sourcemap: true,
  },
  test: { cache: false, silent: "passed-only" },
});
