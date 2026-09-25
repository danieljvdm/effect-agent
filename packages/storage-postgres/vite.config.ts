import { defineConfig } from "vite-plus";

export default defineConfig({
  run: {
    tasks: {
      test: {
        // Live database state is not a cache input. Disabling task caching also
        // passes the configured EFFECT_AGENT_TEST_POSTGRES_URL to the test process.
        cache: false,
        command: "vp test --passWithNoTests",
      },
    },
  },
  pack: {
    entry: [
      "src/index.ts",
      "src/PostgresStorage.ts",
      "src/PostgresStorageClient.ts",
      "src/PostgresStorageError.ts",
    ],
    dts: true,
    sourcemap: true,
  },
  test: {
    cache: false,
    silent: "passed-only",
    // Every case creates and drops its own database, so each one is dominated by server round
    // trips rather than by the assertion under test. The default budget is too tight when
    // suites run in parallel against one server.
    testTimeout: 60_000,
  },
});
