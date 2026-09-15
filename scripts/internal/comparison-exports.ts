import { Effect, FileSystem, Path, Schema } from "effect";

import { PublishManifest } from "../release-publish.ts";

/** Add renamed paths only to disposable comparison manifests; published APIs stay canonical. */
export const comparisonExports = (original: Readonly<Record<string, string>> = {}) => {
  const exports = { ...original };

  for (const [key, target] of Object.entries(original)) {
    const canonical = key
      .replace(/([A-Z]+)([A-Z][a-z])/g, "$1-$2")
      .replace(/([a-z0-9])([A-Z])/g, "$1-$2")
      .toLowerCase();

    if (exports[canonical] === undefined) exports[canonical] = target;
  }

  return exports;
};

/** Let identical current fixtures reach the original implementations in older built releases. */
export const stageComparisonModules = Effect.fn("comparison.stageModules")(function* (
  stage: string,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const packageRoot = path.join(stage, "packages/effect-agent");
  const manifestPath = path.join(packageRoot, "package.json");
  const added: Array<string> = [];

  if (!(yield* fs.exists(manifestPath))) return added;

  const decode = Schema.decodeEffect(Schema.fromJsonString(PublishManifest));
  const manifest = yield* decode(yield* fs.readFileString(manifestPath));
  const exports = { ...manifest.exports };

  const forward = Effect.fn("comparison.forward")(function* (key: string, source: string) {
    if (exports[key] !== undefined) return;
    const name = `Module${added.length}`;
    const directory = path.join(packageRoot, "dist/comparison");

    yield* fs.makeDirectory(directory, { recursive: true });
    yield* fs.writeFileString(path.join(directory, `${name}.mjs`), source);
    yield* fs.writeFileString(path.join(directory, `${name}.d.mts`), source);
    exports[key] = `./src/comparison/${name}.ts`;
    added.push(`effect-agent${key.slice(1)}`);
  });

  const threadManifest = path.join(stage, "packages/thread/package.json");

  if (yield* fs.exists(threadManifest)) {
    const thread = yield* decode(yield* fs.readFileString(threadManifest));

    for (const key of Object.keys(thread.exports ?? {})) {
      if (key === ".") continue;
      yield* forward(
        key,
        `export * from ${JSON.stringify(`@effect-agent/thread${key.slice(1)}`)};\n`,
      );
    }
  }

  if (exports["./ephemeral-threads"] !== undefined) {
    yield* forward(
      "./thread",
      'export * from "effect-agent/ephemeral-threads";\n' +
        'export { ThreadSnapshot as Thread, EphemeralThreads as Store, EphemeralThreadsLive as layerMemory, threadPrompt as toPrompt } from "effect-agent/ephemeral-threads";\n',
    );
  }
  if (exports["./in-memory"] === undefined && exports["./ephemeral"] !== undefined) {
    yield* forward("./in-memory", 'export * from "effect-agent/ephemeral";\n');

    // Older releases expose the same assembly under the Ephemeral root namespace.
    // Update only the staged root so both current consumer fixtures reach it.
    for (const extension of ["mjs", "d.mts"]) {
      const root = path.join(packageRoot, "dist", `index.${extension}`);

      yield* fs.writeFileString(
        root,
        (yield* fs.readFileString(root)) +
          '\nexport * as InMemory from "effect-agent/in-memory";\n',
      );
    }
  }
  if (added.length > 0) {
    yield* fs.writeFileString(manifestPath, JSON.stringify({ ...manifest, exports }));
  }

  return added;
});
