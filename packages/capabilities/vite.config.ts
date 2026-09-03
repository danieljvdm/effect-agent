import { defineConfig } from "vite-plus";

export default defineConfig({
  pack: {
    entry: [
      "src/index.ts",
      "src/Approval.ts",
      "src/Budget.ts",
      "src/CodeMode.ts",
      "src/Commands.ts",
      "src/EphemeralThreads.ts",
      "src/Mcp.ts",
      "src/McpClient.ts",
      "src/ModelContext.ts",
      "src/Redaction.ts",
      "src/RunHooks.ts",
      "src/SemanticMemory.ts",
      "src/Subagent.ts",
      "src/SubagentReservations.ts",
      "src/WebCapture.ts",
    ],
    dts: true,
    sourcemap: true,
  },
  test: { cache: false, silent: "passed-only" },
});
