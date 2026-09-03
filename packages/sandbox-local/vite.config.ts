import { defineConfig } from "vite-plus";

export default defineConfig({
  pack: { entry: ["src/index.ts", "src/LocalSandbox.ts"], dts: true, sourcemap: true },
  test: { cache: false, silent: "passed-only" },
});
