import { defineConfig } from "vite-plus";

export default defineConfig({
  run: {
    tasks: {
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
