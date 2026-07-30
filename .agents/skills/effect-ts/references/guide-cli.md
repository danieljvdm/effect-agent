# Effect CLI

## Overview

Build scripts as small Effect programs with typed errors, platform services, and `effect/unstable/cli`. Read nearby scripts first and follow local conventions for imports, package scripts, and validation commands.

Use the matching general and local guides from `effect-ts` when the CLI also
includes domain models, reusable services, layers, typed domain errors,
structured logging, tests, or HTTP/runtime boundary code.

## Workflow

1. Read nearby scripts before editing. Prefer the repo's current imports, error classes, command helper names, and package-script style.
2. Use `effect/unstable/cli` for command shape, descriptions, help, and arguments/options.
3. Use platform services for effects:
   - `@effect/platform-node`: `NodeRuntime`, `NodeServices`
   - `@effect/platform-bun`: Bun equivalent only when the project already uses it
   - `FileSystem`, `Path`, `Console`, `Terminal`, `ChildProcess`, `Stream` from Effect/platform packages where available
4. Keep direct `node:*`, Bun globals, `process.argv`, `process.env`, `console.*`, `fs`, `path`, and `child_process` calls inside explicit runtime adapters. Use Effect platform services throughout script logic.
5. Model expected failures as `Schema.TaggedErrorClass` with useful `message` overrides.
6. Keep shell execution deterministic: pass command and args as arrays to `ChildProcess.make`, set `cwd` explicitly, capture output, and fail on non-zero exit.
7. Make interactive flows explicit and pleasant: show a short title, summarize detected state, prompt before ambiguous choices, and print the final command/action before running it.
8. Wire the command with `CliCommand.run(command, { version: "1.0.0" }).pipe(Effect.scoped, Effect.provide(NodeServices.layer))`, then `NodeRuntime.runMain(program, { disableErrorReporting: true })`.
9. Update `package.json` scripts to call the TypeScript script through the repo's established runner (`tsx`/`bun`) and keep runner choice consistent across scripts.
10. Include every executable TypeScript script in one checked TypeScript project. Validate with `bun run check` or the narrow package check plus a direct `--help`/dry-run path for the new command.

## Script Skeleton

Use this shape unless local scripts have a better established variant:

```ts
import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Console, Effect, Path, Schema as S, Stream } from "effect";
import { Command as CliCommand } from "effect/unstable/cli";
import { ChildProcess } from "effect/unstable/process";

class CommandError extends S.TaggedErrorClass<CommandError>()("CommandError", {
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

  if (exitCode !== 0) {
    return yield* new CommandError({ command: formatted, exitCode, output: trimmed });
  }

  return trimmed;
});

const main = Effect.gen(function* () {
  yield* Console.log("Doing work...");
});

const command = CliCommand.make("script-name", {}, () => main).pipe(
  CliCommand.withDescription("Describe what the script does."),
);

const program = CliCommand.run(command, { version: "1.0.0" }).pipe(
  Effect.scoped,
  Effect.provide(NodeServices.layer),
);

NodeRuntime.runMain(program, { disableErrorReporting: true });
```

## CLI Design Rules

- Prefer named options over environment variables for normal user input. Keep environment variables only for CI or stable machine-local overrides.
- Prefer deterministic auto-detection when there is exactly one valid candidate; prompt or fail clearly when there are zero or many.
- Print normal interactive UI/status lines to stdout with `Console.log`; use stderr for real errors or for commands that intentionally reserve stdout for machine-readable output.
- For visually nicer output, use simple ASCII structure that degrades well in CI: title lines, aligned labels, and short bullet lists. Avoid decorative Unicode unless the file already uses it.
- Use `CliCommand.withDescription` and ensure `--help` explains the happy path and escape hatches.
- Add a dry-run or help path for scripts that would start long-running servers or device builds.
- Decode structured JSON input with `Schema.fromJsonString(Model)` so parsing
  and validation share one schema-owned boundary.

## Validation

Run the narrowest meaningful checks:

- Confirm that every TypeScript entrypoint named by `package.json` belongs to
  one checked TypeScript project.
- `bun run check:scripts` for root scripts.
- Package-level `bun run check` when the script lives under an app/package and is included in that TS config.
- The command's `--help` path.
- A dry-run or harmless invalid-input path that loads the entrypoint and
  exercises parsing and environment discovery.
