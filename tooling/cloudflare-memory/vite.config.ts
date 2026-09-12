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
      test: {
        command: "vp test --passWithNoTests",
        input: [
          { auto: true },
          { pattern: "bun.lock", base: "workspace" },
          { pattern: "!**/node_modules", base: "workspace" },
          { pattern: "!**/node_modules/.vite*", base: "workspace" },
          { pattern: "!**/node_modules/.vite*/**", base: "workspace" },
        ],
        output: [],
      },
    },
  },
  test: { cache: false, silent: "passed-only" },
});
