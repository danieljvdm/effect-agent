import { builtinModules } from "node:module";

import type { BuildOptions } from "esbuild";

/** Match Alchemy's runtime branch while retaining literal imports used by its native bridges. */
export const alchemyRuntimeBundle = {
  define: { "globalThis.__ALCHEMY_RUNTIME__": "true" },
  plugins: [
    {
      name: "alchemy-workerd-runtime",
      setup(builder) {
        builder.onResolve({ filter: /^[^./]/ }, (args) =>
          builtinModules.includes(args.path)
            ? { path: `node:${args.path}`, external: true }
            : undefined,
        );
        builder.onEnd((result) => {
          // Filesystem module loaders are unreachable in workerd, whose module graph is closed.
          for (const output of result.outputFiles ?? []) {
            const text = output.text.replaceAll(
              /\bimport\s*\((?!\s*["'])/g,
              "__disabledDynamicImport(",
            );

            output.contents = new TextEncoder().encode(
              `const __disabledDynamicImport = () => Promise.reject(new Error("Dynamic filesystem imports are unavailable in this fixture"));\n${text}`,
            );
          }
        });
      },
    },
  ],
} satisfies BuildOptions;
