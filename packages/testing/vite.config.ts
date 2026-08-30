import { defineConfig } from "vite-plus";

export default defineConfig({
  pack: {
    entry: [
      "src/index.ts",
      "src/certification.ts",
      "src/chaos.ts",
      "src/code-executor.ts",
      "src/docs-researcher.ts",
      "src/travel-planner.ts",
    ],
    dts: true,
    sourcemap: true,
  },
  test: {
    cache: false,
    silent: "passed-only",
  },
});
