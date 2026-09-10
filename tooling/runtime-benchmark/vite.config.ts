import { defineConfig } from "vite-plus";

export default defineConfig({
  test: { cache: false, silent: "passed-only" },
});
