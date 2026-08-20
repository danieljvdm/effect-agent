import { defineConfig } from "vite-plus";

// The production session surface and adapter-development harnesses are separate public entries.
// Keep both buildable while preserving the source and distribution export-map boundary.
export default defineConfig({
  pack: {
    entry: ["src/index.ts", "src/testing.ts"],
    dts: true,
    sourcemap: true,
  },
});
