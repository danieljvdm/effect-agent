import { defineConfig } from "vite-plus";

export default defineConfig({
  pack: {
    entry: [
      "src/index.ts",
      "src/CodeExecutor.ts",
      "src/InteractiveBrowser.ts",
      "src/PageCapture.ts",
      "src/PageCrawl.ts",
      "src/PageScreenshot.ts",
      "src/ProtectedBrowser.ts",
      "src/Sandbox.ts",
    ],
    dts: true,
    sourcemap: true,
  },
  test: { cache: false, silent: "passed-only" },
});
