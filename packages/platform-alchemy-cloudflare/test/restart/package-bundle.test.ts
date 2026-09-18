import { builtinModules } from "node:module";

import { NodeFileSystem } from "@effect/platform-node";
import { it } from "@effect/vitest";
import { Effect, FileSystem, Schema } from "effect";
import { build } from "esbuild";
import { expect } from "vite-plus/test";

const Package = Schema.Struct({
  name: Schema.String,
  exports: Schema.Record(Schema.String, Schema.String),
});

class BundleError extends Schema.TaggedError<BundleError>()("BundleError", {
  cause: Schema.Defect(),
}) {}

it.live("bundles every public Alchemy entry point without the effect-cf runtime", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;

    const manifest = yield* fs
      .readFileString(`${import.meta.dirname}/../../package.json`)
      .pipe(Effect.flatMap(Schema.decodeEffect(Schema.fromJsonString(Package))));

    // Keep every public value reachable. Externalizing shared framework packages here would
    // conceal an accidental runtime import in a host or a reexported service.
    const contents = Object.keys(manifest.exports)
      .map((path, index) => {
        const specifier = path === "." ? manifest.name : `${manifest.name}${path.slice(1)}`;

        return `export * as entry${index} from ${JSON.stringify(specifier)};`;
      })
      .join("\n");

    const result = yield* Effect.tryPromise({
      try: () =>
        build({
          stdin: { contents, resolveDir: `${import.meta.dirname}/../..`, loader: "ts" },
          bundle: true,
          write: false,
          metafile: true,
          format: "esm",
          platform: "browser",
          target: "es2022",
          conditions: ["workerd", "worker", "browser"],
          define: { "globalThis.__ALCHEMY_RUNTIME__": "true" },
          external: ["cloudflare:*", "node:*"],
          logLevel: "silent",
          plugins: [
            {
              name: "alchemy-runtime-boundary",
              setup(builder) {
                builder.onResolve({ filter: /^effect-cf(?:\/|$)/ }, (args) => ({
                  errors: [{ text: `Alchemy's public runtime must not import ${args.path}` }],
                }));
                builder.onResolve({ filter: /^[^./]/ }, (args) =>
                  builtinModules.includes(args.path)
                    ? { path: `node:${args.path}`, external: true }
                    : undefined,
                );
              },
            },
          ],
        }),
      catch: (cause) => BundleError.make({ cause }),
    });

    expect(result.outputFiles).toHaveLength(1);
    expect(
      Object.keys(result.metafile.inputs).filter((path) => /(?:^|\/)effect-cf(?:@|\/)/.test(path)),
    ).toEqual([]);
  }).pipe(Effect.provide(NodeFileSystem.layer)),
);
