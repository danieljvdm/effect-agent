import { digestJson } from "@effect-agent/thread/Digest";
import { NodeCrypto, NodeRuntime, NodeServices } from "@effect/platform-node";
import { Console, Effect, FileSystem, Layer, Path, Schema } from "effect";
import { Command, Flag } from "effect/unstable/cli";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { build } from "esbuild";

import { EvaluationError } from "./contracts.ts";
import { PERFORMANCE_FIXTURE } from "./performance-contracts.ts";

export const PerformanceBuild = Schema.Struct({
  sourceCommit: Schema.String,
  dirtyWorkingTree: Schema.Boolean,
  fixture: Schema.Literal(PERFORMANCE_FIXTURE),
  fixtureDigest: Schema.String,
  lockfileDigest: Schema.String,
  bundleDigest: Schema.String,
  sourceRoot: Schema.String,
  directory: Schema.String,
});

/** The fixture is identical for both builds; every bare import resolves in the selected checkout. */
export const buildPerformanceCloudflare = Effect.fn("Performance.build")(function* (
  sourceRoot: string,
  directory: string,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;

  sourceRoot = path.resolve(sourceRoot);
  directory = path.resolve(directory);
  const fixtureRoot = path.dirname(yield* path.fromFileUrl(new URL(import.meta.url)));

  const sourceCommit = (yield* spawner.string(
    ChildProcess.make("git", ["rev-parse", "HEAD"], { cwd: sourceRoot }),
  )).trim();

  const dirtyWorkingTree =
    (yield* spawner.string(
      ChildProcess.make("git", ["status", "--porcelain", "--untracked-files=normal"], {
        cwd: sourceRoot,
      }),
    )).trim().length > 0;

  const fixtureFiles = [
    "performance-worker.ts",
    "performance-contracts.ts",
    "live-model.ts",
    "request-audit.ts",
    "host-evidence.ts",
    "contracts.ts",
    "profiles.ts",
    "build-performance-cloudflare.ts",
    "performance-evaluate.ts",
    "performance-deployment.ts",
    "performance-main.ts",
  ];

  const fixtureDigest = yield* digestJson(
    yield* Effect.forEach(fixtureFiles, (file) =>
      fs
        .readFileString(path.join(fixtureRoot, file))
        .pipe(Effect.map((contents) => ({ file, contents }))),
    ),
  );

  const lockfileDigest = yield* digestJson(
    yield* fs.readFileString(path.join(sourceRoot, "bun.lock")),
  );

  yield* fs.makeDirectory(directory, { recursive: true });
  yield* Effect.tryPromise({
    try: () =>
      build({
        entryPoints: [path.join(fixtureRoot, "performance-worker.ts")],
        outfile: path.join(directory, "worker.mjs"),
        bundle: true,
        minify: true,
        format: "esm",
        platform: "browser",
        target: "es2022",
        conditions: ["workerd", "worker", "browser"],
        external: ["cloudflare:*", "node:*"],
        sourcemap: true,
        plugins: [
          {
            name: "selected-checkout-dependencies",
            setup(builder) {
              builder.onResolve({ filter: /^[^./]/ }, (args) => {
                if (args.path.startsWith("cloudflare:") || args.path.startsWith("node:"))
                  return { path: args.path, external: true };
                if (!args.importer.startsWith(`${fixtureRoot}/`)) return;

                return builder.resolve(args.path, {
                  resolveDir: path.join(sourceRoot, "examples/context-continuity-eval"),
                  kind: args.kind,
                });
              });
            },
          },
        ],
        define: {
          PERFORMANCE_SOURCE_COMMIT: JSON.stringify(sourceCommit),
          PERFORMANCE_DIRTY: JSON.stringify(dirtyWorkingTree),
          PERFORMANCE_FIXTURE_DIGEST: JSON.stringify(fixtureDigest),
        },
      }),
    catch: () =>
      EvaluationError.make({
        stage: "build",
        message: "Performance bundle failed; reference must support this fixture's public APIs",
      }),
  });

  const bundleDigest = yield* digestJson(
    yield* fs.readFileString(path.join(directory, "worker.mjs")),
  );

  const finalCommit = (yield* spawner.string(
    ChildProcess.make("git", ["rev-parse", "HEAD"], { cwd: sourceRoot }),
  )).trim();

  const finalDirty =
    (yield* spawner.string(
      ChildProcess.make("git", ["status", "--porcelain", "--untracked-files=normal"], {
        cwd: sourceRoot,
      }),
    )).trim().length > 0;

  if (finalCommit !== sourceCommit || finalDirty !== dirtyWorkingTree)
    return yield* EvaluationError.make({
      stage: "source",
      message: "Source identity changed during the build",
    });

  const result: typeof PerformanceBuild.Type = {
    sourceCommit,
    dirtyWorkingTree,
    fixture: PERFORMANCE_FIXTURE,
    fixtureDigest,
    lockfileDigest,
    bundleDigest,
    sourceRoot,
    directory,
  };

  yield* fs.writeFileString(
    path.join(directory, "build.json"),
    yield* Schema.encodeEffect(Schema.fromJsonString(PerformanceBuild))(result),
  );

  return result;
});

const command = Command.make(
  "perf:cloudflare:build",
  {
    sourceRoot: Flag.directory("source-root").pipe(Flag.withDefault(".")),
    output: Flag.directory("output-dir").pipe(
      Flag.withDefault(".context-continuity-eval/performance-build"),
    ),
  },
  Effect.fn(function* ({ sourceRoot, output }) {
    yield* Console.log(yield* buildPerformanceCloudflare(sourceRoot, output));
  }),
).pipe(
  Command.withDescription(
    "Build the shared performance fixture against an exact checkout. No deployment or model calls.",
  ),
);

if (import.meta.main)
  NodeRuntime.runMain(
    Command.run(command, { version: "1.0.0" }).pipe(
      Effect.scoped,
      Effect.provide(Layer.merge(NodeServices.layer, NodeCrypto.layer)),
    ),
  );
