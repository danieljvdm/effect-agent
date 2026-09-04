# Entrypoints and testing

Separate command definition from execution. Importing a command module in a test
or another program must not parse `process.argv`, start fibers, or terminate the
process.

For a fixed CI or automation script with no public command syntax, export its
Effect workflow and provide platform Layers only in a thin executable module;
it does not need an artificial `Command` tree. Use the same `runMain` boundary,
typed failures, platform services, and import-safety rules shown below.

```ts
// src/cli/command.ts
export const command = root.pipe(Command.withSubcommands([deploy, status]));

// src/bin/acme.ts
import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Effect } from "effect";
import { Command } from "effect/unstable/cli";
import { command } from "../cli/command";

const program = Command.run(command, { version: VERSION }).pipe(
  Effect.scoped,
  Effect.provide(ApplicationLive),
  Effect.provide(NodeServices.layer),
);

NodeRuntime.runMain(program, { disableErrorReporting: true });
```

Use `BunRuntime` and `BunServices` together when Bun owns the executable. Let
`runMain` own signals, interruption, and process completion. Do not call
`Effect.runPromise`, `process.exit`, or runtime globals inside command handlers.
If expected errors need custom presentation, catch and render them immediately
before `runMain` while preserving CLI control-flow errors such as help output.

## Choose a verification boundary

Follow repository testing policy before adding a committed test. Use existing
checks or direct command execution when sufficient. The options below are
alternatives selected by the changed behavior, not a required three-part suite.

1. Test application services directly with deterministic Layers. Cover domain
   success, expected failure, interruption, and plan/apply separation without
   involving argument parsing.
2. Test thin command handlers through their services when CLI input mapping or
   output-mode selection contains meaningful logic.
3. Spawn the real executable for boundary behavior. Select relevant cases:
   - root and changed-command `--help`;
   - representative valid arguments and flags;
   - missing or invalid input and a non-zero exit;
   - one expected operational failure with an actionable message;
   - exact JSON output with no prose contamination;
   - dry-run proving writes did not occur;
   - confirmation behavior in both interactive and non-interactive modes.

Run executable tests through Effect's child-process APIs with an explicit `cwd`,
captured stdout/stderr, and controlled environment. Use the repository's normal
runtime launcher and command authority; a test that bypasses the packaged or
declared entrypoint does not prove the CLI works for users.

## Completion checks

- Ensure every executable TypeScript entrypoint belongs to a checked project.
- Run the repository's formatter, linter, typechecker, and tests.
- Execute `--help` through the real entrypoint.
- Exercise a harmless dry-run or read-only command outside the source module.
- Verify non-zero exits for parsing and expected operational failures.
