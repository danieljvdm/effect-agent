import react from "@vitejs/plugin-react";
import { defineConfig } from "vite-plus";

export default defineConfig({
  plugins: [react()],
  server: {
    host: "127.0.0.1",
    port: 5191,
    strictPort: true,
    proxy: {
      "/api": {
        target: "http://127.0.0.1:8791",
        changeOrigin: true,
        configure: (proxy) => {
          proxy.on("proxyReq", (outgoing, incoming) => {
            if (incoming.headers.origin === "http://127.0.0.1:5191")
              outgoing.setHeader("origin", "http://127.0.0.1:8791");
          });
        },
      },
    },
  },
  test: { include: ["test/**/*.test.ts"], cache: false, silent: "passed-only", maxWorkers: 1 },
  run: {
    tasks: {
      check: {
        command: "tsc --noEmit -p tsconfig.json",
        input: [{ auto: true }, "src/**", "test/**", "tsconfig.json"],
        output: [],
      },
      test: {
        command: "vp test",
        env: ["BROWSER_TEST_EXECUTABLE"],
        input: [{ auto: true }, "src/**", "test/**"],
        output: [],
      },
      build: {
        command: "vp build && wrangler deploy --dry-run",
        env: ["VITE_BROWSER_BENCHMARK_REPORT_URL"],
        input: [{ auto: true }, "src/**", "index.html", "!.wrangler/**", "!dist/**"],
        untrackedEnv: ["WRANGLER_LOG_PATH"],
        output: ["dist/**"],
      },
      worker: {
        command: "wrangler dev --port 8791",
        cache: false,
      },
      deploy: {
        command: "vp build && wrangler deploy",
        cache: false,
      },
    },
  },
});
