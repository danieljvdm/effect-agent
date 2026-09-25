import { defineConfig } from "vite-plus";

export default defineConfig({
  pack: {
    entry: [
      "src/index.ts",
      "src/SqlStorage.ts",
      "src/SqlStorageFailpoint.ts",
      "src/SqlJournal.ts",
      "src/SqlThreadStore.ts",
      "src/SqlSubmissionLedger.ts",
      "src/SqlActivityStore.ts",
      "src/SqlScheduleStore.ts",
      "src/SqlStorageSchema.ts",
      "src/SqlThreadNativeReads.ts",
      "src/SqlMessageDeliveryStore.ts",
      "src/SqlLifecyclePublication.ts",
      "src/SqlSubscriptionStore.ts",
      "src/SqlStorageV2Upgrade.ts",
    ],
    dts: true,
    sourcemap: true,
  },
  test: { cache: false, silent: "passed-only" },
});
