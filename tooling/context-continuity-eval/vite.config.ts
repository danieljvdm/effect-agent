import { defineConfig } from "vite-plus";

export default defineConfig({
  run: {
    tasks: {
      "compaction:eval": {
        command: "node --experimental-transform-types src/selective-eval-main.ts",
        cache: false,
      },
      "compaction:spike": {
        command: "node --experimental-transform-types src/selective-main.ts",
        // Live probes must inherit provider credentials and always execute.
        cache: false,
      },
      // Embed the actual checkout identity on every build, including clean/dirty state.
      build: { command: "bun src/build-cloudflare.ts", cache: false },
      // Like the neighboring workerd examples, drive Miniflare through a Vite task.
      test: {
        command: "vitest run",
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
  test: { cache: false, silent: "passed-only", maxWorkers: 1 },
});
