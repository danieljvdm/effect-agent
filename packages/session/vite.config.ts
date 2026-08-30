import { defineConfig } from "vite-plus";

// Generic session contracts, adapter testing, and GitHub integration have separate public entries.
export default defineConfig({
  pack: {
    entry: ["src/index.ts", "src/testing.ts", "src/github-workflow-source.ts"],
    dts: true,
    sourcemap: true,
  },
  test: {
    cache: false,
    silent: "passed-only",
  },
});
