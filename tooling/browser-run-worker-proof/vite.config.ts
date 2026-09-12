import { defineConfig } from "vite-plus";

export default defineConfig({
  run: {
    tasks: {
      build: {
        command: "wrangler deploy --dry-run",
        // Wrangler reads its own temporary bundle during validation.
        input: [{ auto: true }, "!.wrangler", "!.wrangler/**"],
        output: [],
      },
    },
  },
  test: { cache: false, silent: "passed-only" },
});
