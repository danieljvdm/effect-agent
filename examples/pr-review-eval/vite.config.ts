import { defineConfig } from "vite-plus";

export default defineConfig({
  test: {
    // Private replay experiments under results/ use their own frozen configurations.
    include: ["test/**/*.test.ts"],
    cache: false,
    silent: "passed-only",
  },
});
