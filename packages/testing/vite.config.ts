import { defineConfig } from "vite-plus";

export default defineConfig({
  pack: {
    entry: [
      "src/index.ts",
      "src/Certification.ts",
      "src/Chaos.ts",
      "src/CodeExecutorConformance.ts",
      "src/CodeExecutorSubstitute.ts",
      "src/DocsResearcher.ts",
      "src/ScriptedModel.ts",
      "src/TravelPlanner.ts",
    ],
    dts: true,
    sourcemap: true,
  },
  test: {
    cache: false,
    silent: "passed-only",
  },
});
