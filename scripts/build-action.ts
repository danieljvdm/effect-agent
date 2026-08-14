import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Console, Effect, FileSystem, Schema, Stream } from "effect";
import { Command as CliCommand, Flag } from "effect/unstable/cli";
import { ChildProcess } from "effect/unstable/process";

// ---------------------------------------------------------------------------
// Bundle the @effect-agent/pr-review GitHub Action entrypoint into the
// committed `action/dist/index.mjs`. The committed bundle is a deliberate
// exception to the dist-is-gitignored convention (see the pr-review ADR): a
// node-runtime action must be runnable straight from a checkout, with no
// install step. `--check` rebuilds to a scratch path and fails when the
// committed bundle is stale, so `bun run check` keeps the two in sync.
// ---------------------------------------------------------------------------

const ENTRY = "packages/pr-review/src/internal/action-entry.ts";
const BUNDLE = "action/dist/index.mjs";
const SCRATCH = "node_modules/.tmp/action-dist-check/index.mjs";

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
    return `${this.bundle} is stale: rebuild it with \`bun run action:build\` and commit the result.`;
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

const bundleTo = Effect.fn("bundleTo")(function* (outfile: string) {
  yield* runCommand("bun", [
    "build",
    ENTRY,
    "--target=node",
    "--format=esm",
    `--outfile=${outfile}`,
  ]);
});

const checkFlag = Flag.boolean("check").pipe(
  Flag.withDescription("Rebuild to a scratch path and fail if the committed bundle is stale."),
);

const command = CliCommand.make("build-action", { check: checkFlag }, ({ check }) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    if (!check) {
      yield* bundleTo(BUNDLE);
      const stat = yield* fs.stat(BUNDLE);
      yield* Console.log(`Bundled ${ENTRY} -> ${BUNDLE} (${stat.size} bytes).`);
      return;
    }
    yield* bundleTo(SCRATCH);
    const committed = yield* fs.readFileString(BUNDLE).pipe(Effect.orElseSucceed(() => undefined));
    const fresh = yield* fs.readFileString(SCRATCH);
    if (committed !== fresh) {
      return yield* StaleBundleError.make({ bundle: BUNDLE });
    }
    yield* Console.log(`${BUNDLE} is up to date.`);
  }),
).pipe(
  CliCommand.withDescription(
    "Bundle the pr-review GitHub Action entrypoint into action/dist/index.mjs.",
  ),
);

const program = CliCommand.run(command, { version: "1.0.0" }).pipe(
  Effect.scoped,
  Effect.provide(NodeServices.layer),
);

NodeRuntime.runMain(program, { disableErrorReporting: true });
