import { defineConfig } from "vite-plus";

// Generic thread contracts, adapter testing, and GitHub integration have separate public entries.
export default defineConfig({
  pack: {
    entry: [
      "src/index.ts",
      "src/history.ts",
      "src/durability.ts",
      "src/testing.ts",
      "src/github-workflow-source.ts",
      "src/sql-memory-store.ts",
    ],
    dts: true,
    sourcemap: true,
  },
  test: {
    cache: false,
    silent: "passed-only",
  },
});
