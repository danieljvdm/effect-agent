import { defineConfig } from "vite-plus";

export default defineConfig({
  pack: {
    entry: ["src/index.ts", "src/Review.ts", "src/ReviewRepository.ts"],
    dts: true,
    sourcemap: true,
  },
  test: { cache: false, silent: "passed-only" },
});
