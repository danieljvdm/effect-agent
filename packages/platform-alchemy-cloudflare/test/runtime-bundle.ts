import { builtinModules } from "node:module";

import { build } from "esbuild";
import { type Plugin } from "vite-plus";

/** Apply Alchemy's runtime define and tree shaking just as its deployed bundle does. */
export const runtimeBundle = (): Plugin => ({
  name: "alchemy-runtime-fixtures",
  enforce: "pre",
  async load(id) {
    if (!id.endsWith("/test/worker.ts") && !id.endsWith("/test/ancillary-fixtures.ts")) return;

    const result = await build({
      entryPoints: [id],
      bundle: true,
      write: false,
      format: "esm",
      platform: "browser",
      target: "es2022",
      conditions: ["worker", "browser"],
      define: { "globalThis.__ALCHEMY_RUNTIME__": "true" },
      external: [
        "cloudflare:*",
        "node:*",
        "effect",
        "effect/*",
        "@effect/*",
        "effect-agent",
        "effect-agent/*",
        "@effect-agent/*",
        "../src/*",
        "./fixtures.ts",
        "./ancillary-fixtures.ts",
      ],
      logLevel: "silent",
      plugins: [
        {
          name: "external-runtime-dependencies",
          setup(builder) {
            builder.onResolve({ filter: /^[^./]|^@/ }, async (args) => {
              if (args.pluginData || args.path.startsWith("alchemy/")) return;
              if (args.path.startsWith("cloudflare:") || args.path.startsWith("node:"))
                return { path: args.path, external: true };
              if (builtinModules.includes(args.path))
                return { path: `node:${args.path}`, external: true };

              const resolved = await builder.resolve(args.path, {
                kind: args.kind,
                resolveDir: args.resolveDir,
                pluginData: { resolved: true },
              });

              return { path: resolved.path, external: true, sideEffects: false };
            });
          },
        },
      ],
    });

    const output = result.outputFiles[0];

    if (!output) throw new Error("Alchemy test worker produced no bundle");

    return output.text;
  },
});
