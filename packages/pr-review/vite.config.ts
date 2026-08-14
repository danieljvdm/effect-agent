import { defineConfig } from "vite-plus";

// This package ships four public entries: the review library at ".", the
// deterministic test helpers at "./testing", and the two host entrypoints at
// "./action" and "./cli". `vp pack` builds only the default entry without
// this config, which would leave the subpaths broken in the published
// artifact.
export default defineConfig({
  pack: {
    entry: ["src/index.ts", "src/testing.ts", "src/action.ts", "src/cli.ts"],
    dts: true,
    sourcemap: true,
  },
});
