import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Console, Effect, FileSystem, Path, Schema, Stream } from "effect";
import { Command as CliCommand, Flag } from "effect/unstable/cli";
import { ChildProcess } from "effect/unstable/process";

// ---------------------------------------------------------------------------
// Bundle the PR-review Action entrypoint into its committed dist file. A
// node-runtime Action must run directly from an immutable checkout without an
// install step. esbuild produces reproducible output, so `--check` compares
// rebuilt JavaScript directly. Do not fingerprint the workspace lockfile or
// source graph: unrelated dependencies and tree-shaken code need no rebuild.
// ---------------------------------------------------------------------------

const entry = "packages/pr-review-action/src/action-entry.ts";
const bundle = "action/dist/index.mjs";

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
  yield* runCommand("esbuild", [
    entry,
    "--bundle",
    "--platform=node",
    "--target=node24",
    "--format=esm",
    // Bundled CommonJS dependencies still require Node built-ins at runtime.
    '--banner:js=import { createRequire } from "node:module"; const require = createRequire(import.meta.url);',
    `--outfile=${outfile}`,
  ]);
});

const checkFlag = Flag.boolean("check").pipe(
  Flag.withDefault(false),
  Flag.withDescription("Rebuild to a scratch path and fail if the committed bundle is stale."),
);

export const command = CliCommand.make("build-action", { check: checkFlag }, ({ check }) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const scratch = path.join(yield* fs.makeTempDirectoryScoped(), "index.mjs");

    yield* bundleTo(entry, scratch);
    const fresh = yield* fs.readFileString(scratch);

    if (check) {
      const committed = yield* fs.readFileString(bundle);

      if (committed !== fresh) {
        return yield* StaleBundleError.make({ bundle });
      }
      yield* Console.log(`${bundle} is up to date.`);
    } else {
      yield* fs.makeDirectory(path.dirname(bundle), { recursive: true });
      yield* fs.writeFileString(bundle, fresh);
      const stat = yield* fs.stat(bundle);

      yield* Console.log(`Bundled ${entry} -> ${bundle} (${stat.size} bytes).`);
    }
  }),
).pipe(CliCommand.withDescription("Bundle the committed PR-review GitHub Action entrypoint."));

const program = CliCommand.run(command, { version: "1.0.0" }).pipe(
  Effect.scoped,
  Effect.provide(NodeServices.layer),
);

if (import.meta.main) NodeRuntime.runMain(program);
