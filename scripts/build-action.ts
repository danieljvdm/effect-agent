import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Console, Effect, FileSystem, Path, Schema, Stream } from "effect";
import { Command as CliCommand } from "effect/unstable/cli";
import { ChildProcess } from "effect/unstable/process";

// ---------------------------------------------------------------------------
// Build the PR-review Action for local use and CI distribution. Generated
// output is ignored on source branches; CI adds it only to release commits.
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
    '--banner:js=import { createRequire as __actionCreateRequire } from "node:module"; const require = __actionCreateRequire(import.meta.url);',
    `--outfile=${outfile}`,
  ]);
});

export const command = CliCommand.make("build-action", {}, () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const scratch = path.join(yield* fs.makeTempDirectoryScoped(), "index.mjs");
    yield* bundleTo(entry, scratch);
    yield* runCommand("node", ["--check", scratch]);
    const fresh = yield* fs.readFileString(scratch);
    yield* fs.makeDirectory(path.dirname(bundle), { recursive: true });
    yield* fs.writeFileString(bundle, fresh);
    const stat = yield* fs.stat(bundle);
    yield* Console.log(`Bundled ${entry} -> ${bundle} (${stat.size} bytes).`);
  }),
).pipe(CliCommand.withDescription("Build the PR-review GitHub Action for distribution."));

const program = CliCommand.run(command, { version: "1.0.0" }).pipe(
  Effect.scoped,
  Effect.provide(NodeServices.layer),
);

if (import.meta.main) NodeRuntime.runMain(program);
