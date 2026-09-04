import { defineConfig } from "vite-plus";

export default defineConfig({
  pack: {
    entry: [
      "src/index.ts",
      "src/RememberingStore.ts",
      "src/Agent.ts",
      "src/AgentError.ts",
      "src/AgentPolicy.ts",
      "src/Identifiers.ts",
      "src/IdGenerator.ts",
      "src/Memory.ts",
      "src/MemoryNamespace.ts",
      "src/MemoryReference.ts",
      "src/MemoryRevalidation.ts",
      "src/MemoryStore.ts",
      "src/RunEvent.ts",
      "src/RunPolicyUsage.ts",
      "src/SemanticMemoryIndex.ts",
      "src/SemanticMemoryRevalidation.ts",
      "src/SubagentContract.ts",
      "src/ToolResult.ts",
      "src/Usage.ts",
    ],
    dts: true,
    sourcemap: true,
  },
  test: { cache: false, silent: "passed-only" },
});
