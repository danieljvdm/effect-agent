import { defineConfig, type UserConfig } from "vite-plus";

// Ignore generated Vite files and node_modules directory listings, while
// retaining dependency file hashes. The lockfile covers dependency additions.
const run: NonNullable<UserConfig["run"]> = {
  tasks: {
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
};

export default defineConfig({
  run,
  test: {
    cache: false,
    silent: "passed-only",
  },
});
