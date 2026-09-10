import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Effect, FileSystem, Path } from "effect";
import { ChildProcessSpawner, ChildProcess } from "effect/unstable/process";
import { build } from "esbuild";

import { EvaluationError } from "./contracts.ts";

/** Bundle only; deployment remains an explicit operation against a selected account. */
export const buildCloudflare = Effect.fn("ContextContinuity.buildCloudflare")(function* () {
  const path = yield* Path.Path;
  const fs = yield* FileSystem.FileSystem;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;

  const root = path.resolve(
    path.dirname(yield* path.fromFileUrl(new URL(import.meta.url))),
    "../../..",
  );

  const sourceCommit = (yield* spawner.string(
    ChildProcess.make("git", ["rev-parse", "HEAD"], { cwd: root }),
  )).trim();

  const dirty =
    (yield* spawner.string(
      ChildProcess.make("git", ["status", "--porcelain", "--untracked-files=normal"], {
        cwd: root,
      }),
    )).trim().length > 0;

  const directory = path.join(root, ".context-continuity-eval", "cloudflare");

  yield* fs.makeDirectory(directory, { recursive: true });
  yield* Effect.tryPromise({
    try: () =>
      build({
        entryPoints: [path.join(root, "tooling/context-continuity-eval/src/cloudflare-worker.ts")],
        outfile: path.join(directory, "worker.mjs"),
        bundle: true,
        format: "esm",
        platform: "browser",
        target: "es2022",
        external: ["cloudflare:*", "node:*"],
        sourcemap: true,
        define: {
          CONTEXT_EVAL_SOURCE_COMMIT: JSON.stringify(sourceCommit),
          CONTEXT_EVAL_DIRTY: JSON.stringify(dirty),
        },
      }),
    catch: () => EvaluationError.make({ stage: "build", message: "Cloudflare bundle failed" }),
  });
  yield* fs.writeFileString(
    path.join(directory, "identity.json"),
    JSON.stringify({ sourceCommit, dirtyWorkingTree: dirty }),
  );
});

if (import.meta.main)
  NodeRuntime.runMain(buildCloudflare().pipe(Effect.scoped, Effect.provide(NodeServices.layer)));
