import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Console, Effect, FileSystem, Schema, Stream } from "effect";
import { Command as CliCommand, Flag } from "effect/unstable/cli";
import { ChildProcess } from "effect/unstable/process";

// ---------------------------------------------------------------------------
// Bundle both JavaScript Action entrypoints into committed dist files. A
// node-runtime Action must run directly from an immutable checkout without an
// install step. `--check` rebuilds to scratch paths and rejects stale bundles.
// ---------------------------------------------------------------------------

const bundles = [
  {
    entry: "packages/pr-review/src/internal/action-entry.ts",
    bundle: "action/dist/index.mjs",
    scratch: "node_modules/.tmp/action-dist-check/review-index.mjs",
  },
  {
    entry: "examples/pr-work-order-ingress/src/action-entry.ts",
    bundle: "work-order-action/dist/index.mjs",
    scratch: "node_modules/.tmp/action-dist-check/work-order-index.mjs",
  },
] as const;

class CommandError extends Schema.TaggedError<CommandError>()("CommandError", {
  command: Schema.String,
  exitCode: Schema.Int,
  output: Schema.String,
}) {
  override get message() {
    return this.output.length > 0
      ? `${this.command} exited with code ${this.exitCode}: ${this.output}`
      : `${this.command} exited with code ${this.exitCode}`;
  }
}

class StaleBundleError extends Schema.TaggedError<StaleBundleError>()("StaleBundleError", {
  bundle: Schema.String,
}) {
  override get message() {
    return `${this.bundle} is stale: rebuild it with \`vp run action:build\` and commit the result.`;
  }
}

const runCommand = Effect.fn("runCommand")(function* (
  command: string,
  args: ReadonlyArray<string>,
) {
  const formatted = [command, ...args].join(" ");
  const child = yield* ChildProcess.make(command, args, { stderr: "pipe", stdout: "pipe" });
  const [output, exitCode] = yield* Effect.all([
    Stream.mkString(Stream.decodeText(child.all)),
    child.exitCode,
  ]);
  const trimmed = output.trim();
  if (exitCode !== 0) {
    return yield* CommandError.make({ command: formatted, exitCode, output: trimmed });
  }
  return trimmed;
});

const bundleTo = Effect.fn("bundleTo")(function* (entry: string, outfile: string) {
  yield* runCommand("bun", [
    "build",
    entry,
    "--target=node",
    "--format=esm",
    `--outfile=${outfile}`,
  ]);
});

const checkFlag = Flag.boolean("check").pipe(
  Flag.withDefault(false),
  Flag.withDescription("Rebuild to a scratch path and fail if the committed bundle is stale."),
);

const command = CliCommand.make("build-action", { check: checkFlag }, ({ check }) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    if (!check) {
      for (const item of bundles) {
        yield* bundleTo(item.entry, item.bundle);
        const stat = yield* fs.stat(item.bundle);
        yield* Console.log(`Bundled ${item.entry} -> ${item.bundle} (${stat.size} bytes).`);
      }
      return;
    }
    for (const item of bundles) {
      yield* bundleTo(item.entry, item.scratch);
      const committed = yield* fs
        .readFileString(item.bundle)
        .pipe(Effect.orElseSucceed(() => undefined));
      const fresh = yield* fs.readFileString(item.scratch);
      if (committed !== fresh) {
        return yield* StaleBundleError.make({ bundle: item.bundle });
      }
      yield* Console.log(`${item.bundle} is up to date.`);
    }
  }),
).pipe(
  CliCommand.withDescription(
    "Bundle the committed PR-review and PR-work-order GitHub Action entrypoints.",
  ),
);

const program = CliCommand.run(command, { version: "1.0.0" }).pipe(
  Effect.scoped,
  Effect.provide(NodeServices.layer),
);

NodeRuntime.runMain(program, { disableErrorReporting: true });
