import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Console, Effect, Path, Schema as S, Stream } from "effect";
import { Command as CliCommand } from "effect/unstable/cli";
import { ChildProcess } from "effect/unstable/process";

class CommandError extends S.TaggedError<CommandError>()("CommandError", {
  command: S.String,
  exitCode: S.Int,
  output: S.String,
}) {
  override get message() {
    return this.output.length > 0
      ? `${this.command} exited with code ${this.exitCode}: ${this.output}`
      : `${this.command} exited with code ${this.exitCode}`;
  }
}

const resolvePaths = Effect.fn("resolvePaths")(function* () {
  const path = yield* Path.Path;
  const scriptPath = yield* path.fromFileUrl(new URL(import.meta.url));
  const rootDir = path.resolve(path.dirname(scriptPath), "..");
  const binDir = path.join(rootDir, "node_modules", ".bin");

  return {
    effectTsgoBin: path.join(binDir, "effect-tsgo"),
    rootDir,
    syncEffectScript: path.join(rootDir, "scripts", "sync-effect-submodule.ts"),
    tsxBin: path.join(binDir, "tsx"),
    vpBin: path.join(binDir, "vp"),
  };
});

const runCommand = Effect.fn("runCommand")(function* (
  cwd: string,
  command: string,
  args: ReadonlyArray<string>,
) {
  const formatted = [command, ...args].join(" ");
  const child = yield* ChildProcess.make(command, args, { cwd, stderr: "pipe", stdout: "pipe" });
  const [output, exitCode] = yield* Effect.all([
    Stream.mkString(Stream.decodeText(child.all)),
    child.exitCode,
  ]);
  const trimmed = output.trim();

  if (trimmed.length > 0) {
    yield* Console.error(trimmed);
  }

  if (exitCode !== 0) {
    return yield* CommandError.make({
      command: formatted,
      exitCode,
      output: trimmed,
    });
  }
});

const runStep = Effect.fn("runStep")(function* (
  label: string,
  cwd: string,
  command: string,
  args: ReadonlyArray<string>,
) {
  yield* Console.error(`${label}...`);
  yield* runCommand(cwd, command, args);
});

const postinstall = Effect.fn("postinstall")(function* () {
  const paths = yield* resolvePaths();

  yield* runStep("Syncing Effect submodule", paths.rootDir, paths.tsxBin, [paths.syncEffectScript]);
  yield* runStep("Configuring Vite+", paths.rootDir, paths.vpBin, ["config"]);
  yield* runStep("Patching effect-tsgo", paths.rootDir, paths.effectTsgoBin, ["patch"]);
});

const postinstallCommand = CliCommand.make("postinstall", {}, () => postinstall()).pipe(
  CliCommand.withDescription("Run repository postinstall setup."),
);

const program = CliCommand.run(postinstallCommand, { version: "1.0.0" }).pipe(
  Effect.scoped,
  Effect.provide(NodeServices.layer),
);

NodeRuntime.runMain(program, { disableErrorReporting: true });
