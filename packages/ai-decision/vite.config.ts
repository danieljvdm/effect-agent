import { defineConfig } from "vite-plus";

export default defineConfig({
  pack: {
    entry: [
      "src/index.ts",
      "src/AutoModel.ts",
      "src/DecisionModel.ts",
      "src/DecisionQuery.ts",
      "src/DecisionSchema.ts",
      "src/DecisionSet.ts",
    ],
    dts: true,
    sourcemap: true,
  },
  test: { cache: false, silent: "passed-only" },
});
