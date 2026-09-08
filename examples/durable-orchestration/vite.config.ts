import { defineConfig } from "vite-plus";

export default defineConfig({
  run: {
    tasks: {
      node: { command: "node --experimental-transform-types src/node-main.ts", cache: false },
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
  test: { cache: false, silent: "passed-only" },
});
