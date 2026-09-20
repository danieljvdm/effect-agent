import { defineConfig } from "vite-plus";

export default defineConfig({
  test: {
    include: ["live/checkout.test.ts"],
    cache: false,
    retry: 0,
    maxWorkers: 1,
    disableConsoleIntercept: true,
    testTimeout: 7_200_000,
    hookTimeout: 300_000,
  },
});
