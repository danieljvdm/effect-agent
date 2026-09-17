import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Console, Effect, FileSystem, Path, Schema } from "effect";

import { ProofUnavailable, readCommand } from "./release-ci.ts";
import { withPublishManifests } from "./release-publish.ts";

const Manifest = Schema.Struct({
  name: Schema.String,
  version: Schema.String,
  exports: Schema.Record(
    Schema.String,
    Schema.Struct({ types: Schema.String, default: Schema.String }),
  ),
});

const Packs = Schema.Array(
  Schema.Struct({
    name: Schema.String,
    version: Schema.String,
    files: Schema.Array(Schema.Struct({ path: Schema.String })),
  }),
);

/** Inspect the npm file list against the actual versioned, built export map. */
export const verifyPackedFiles = (manifest: typeof Manifest.Type, packs: typeof Packs.Type) => {
  const pack = packs[0];

  const required = [
    "package.json",
    ...Object.values(manifest.exports).flatMap((entry) => [
      entry.types.slice(2),
      entry.default.slice(2),
    ]),
  ];

  return packs.length === 1 &&
    pack?.name === manifest.name &&
    pack.version === manifest.version &&
    required.every((path) => pack.files.some((file) => file.path === path))
    ? Effect.void
    : Effect.fail(
        new ProofUnavailable({
          message: "npm package identity or exported files do not match the candidate",
        }),
      );
};

/** No registry calls, lifecycle scripts, model calls, publication or tags. */
export const checkReleasePackages = Effect.fn("checkReleasePackages")(function* (root: string) {
  const fs = yield* FileSystem.FileSystem;
  const cache = yield* fs.makeTempDirectoryScoped({ prefix: "release-package-check-" });

  yield* withPublishManifests(root, (directories) =>
    Effect.forEach(
      directories,
      Effect.fn(function* (directory) {
        const manifest = yield* Schema.decodeEffect(Schema.fromJsonString(Manifest))(
          yield* fs.readFileString(`${directory}/package.json`),
        );

        const packs = yield* Schema.decodeEffect(Schema.fromJsonString(Packs))(
          yield* readCommand(directory, "npm", [
            "pack",
            "--dry-run",
            "--ignore-scripts",
            "--json",
            "--cache",
            cache,
          ]),
        );

        yield* verifyPackedFiles(manifest, packs);
        yield* Console.log(`Inspected ${manifest.name}@${manifest.version}`);
      }),
      { discard: true },
    ),
  );
}, Effect.scoped);

const program = Effect.gen(function* () {
  const path = yield* Path.Path;
  const root = path.resolve(path.dirname(yield* path.fromFileUrl(new URL(import.meta.url))), "..");

  yield* checkReleasePackages(root);
});

if (import.meta.main) NodeRuntime.runMain(program.pipe(Effect.provide(NodeServices.layer)));
