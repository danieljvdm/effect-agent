import { defineConfig } from "vite-plus";

export default defineConfig({
  pack: {
    entry: [
      "src/index.ts",
      "src/SqliteActivityStore.ts",
      "src/SqliteScheduleStore.ts",
      "src/SqliteStorageConfig.ts",
      "src/SqliteStorageError.ts",
      "src/SqliteStorageFailpoint.ts",
      "src/SqliteStorageVersion.ts",
      "src/SqliteSubmissionLedger.ts",
      "src/SqliteSubscriptionStore.ts",
      "src/SqliteThreadStore.ts",
      "src/SqliteStorageFailpointTesting.ts",
    ],
    dts: true,
    sourcemap: true,
  },
  test: {
    cache: false,
    silent: "passed-only",
  },
});
