import { defineConfig } from "vite-plus";

// This package ships two public entries: the storage adapters at "." and the
// deterministic test helpers at "./testing". `vp pack` builds only the default
// entry without this config, which would leave the "./testing" subpath broken
// in the published artifact.
export default defineConfig({
  pack: {
    entry: ["src/index.ts", "src/testing.ts"],
    dts: true,
    sourcemap: true,
  },
});
