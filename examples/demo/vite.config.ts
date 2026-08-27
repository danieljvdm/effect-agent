import tailwindcss from "@tailwindcss/vite";
import { tanstackStart } from "@tanstack/react-start/plugin/vite";
import viteReact from "@vitejs/plugin-react";
import { defineConfig, loadEnv } from "vite-plus";

export default defineConfig(({ mode }) => {
  const demoEnvironment = loadEnv(mode, import.meta.dirname, "");
  const openAiApiKey = demoEnvironment.OPENAI_API_KEY;

  // Vite loads .env values for import.meta.env, while Effect Config reads the
  // server process environment. Copy only this server credential across that boundary.
  if (openAiApiKey !== undefined && process.env.OPENAI_API_KEY === undefined) {
    process.env.OPENAI_API_KEY = openAiApiKey;
  }

  return {
    resolve: {
      tsconfigPaths: true,
    },
    plugins: [tailwindcss(), tanstackStart(), viteReact()],
    test: {
      cache: false,
      silent: "passed-only",
    },
    run: {
      tasks: {
        check: {
          command: "tsc --noEmit",
          input: [{ auto: true }, "!*.tsbuildinfo"],
          output: [{ auto: true }, "!*.tsbuildinfo"],
        },
      },
    },
  };
});
