import { defineConfig } from "vite-plus";

export default defineConfig({
  pack: {
    entry: [
      "src/index.ts",
      "src/ContextTools.ts",
      "src/MemoryNotes.ts",
      "src/Remembering.ts",
      "src/Approval.ts",
      "src/Budget.ts",
      "src/CodeMode.ts",
      "src/ToolDiscovery.ts",
      "src/Commands.ts",
      "src/EphemeralThreads.ts",
      "src/Mcp.ts",
      "src/McpClient.ts",
      "src/ModelContext.ts",
      "src/Redaction.ts",
      "src/RunHooks.ts",
      "src/SemanticMemory.ts",
      "src/Subagent.ts",
      "src/Messaging.ts",
      "src/SubagentReservations.ts",
      "src/WebCapture.ts",
      "src/WebSearch.ts",
    ],
    dts: true,
    sourcemap: true,
  },
  test: { cache: false, silent: "passed-only" },
});
