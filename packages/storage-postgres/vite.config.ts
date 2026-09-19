import { defineConfig } from "vite-plus";

export default defineConfig({
  pack: {
    entry: [
      "src/index.ts",
      "src/PostgresActivityStore.ts",
      "src/PostgresMessageDeliveryStore.ts",
      "src/PostgresScheduleStore.ts",
      "src/PostgresStorageClient.ts",
      "src/PostgresStorageConfig.ts",
      "src/PostgresStorageError.ts",
      "src/PostgresStorageFailpoint.ts",
      "src/PostgresStorageVersion.ts",
      "src/PostgresSubmissionLedger.ts",
      "src/PostgresSubscriptionStore.ts",
      "src/PostgresThreadStore.ts",
      "src/PostgresStorageFailpointTesting.ts",
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
