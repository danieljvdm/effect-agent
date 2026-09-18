import { defineConfig } from "vite-plus";

export default defineConfig({
  pack: {
    entry: [
      "src/index.ts",
      "src/PostgresMessageDeliveryStore.ts",
      "src/PostgresActivityStore.ts",
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
  },
});
