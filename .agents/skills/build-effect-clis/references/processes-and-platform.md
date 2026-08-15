# Processes and platform services

Use Effect platform services inside CLI workflows. Keep direct `node:*`, Bun
globals, `process`, filesystem calls, and shell execution inside explicit
boundary adapters. The executable boundary itself should use Effect platform
runtime and service APIs.

## Own subprocess behavior in a service

```ts
import { Context, Effect, Layer, Schema, String } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

export class ToolError extends Schema.TaggedError<ToolError>()("ToolError", {
  command: Schema.String,
  cause: Schema.Defect(),
}) {}

export class Tools extends Context.Service<
  Tools,
  {
    readonly gitVersion: Effect.Effect<string, ToolError>;
    changedFiles(baseRef: string): Effect.Effect<ReadonlyArray<string>, ToolError>;
  }
>()("app/Tools") {
  static readonly layer = Layer.effect(
    Tools,
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const gitVersion = spawner.string(ChildProcess.make("git", ["--version"])).pipe(
        Effect.map(String.trim),
        Effect.mapError((cause) => new ToolError({ command: "git --version", cause })),
      );
      const changedFiles = Effect.fn("Tools.changedFiles")(function* (baseRef: string) {
        return yield* spawner
          .lines(ChildProcess.make("git", ["diff", "--name-only", `${baseRef}...HEAD`]))
          .pipe(
            Effect.mapError(
              (cause) =>
                new ToolError({ command: `git diff --name-only ${baseRef}...HEAD`, cause }),
            ),
          );
      });

      return Tools.of({ changedFiles, gitVersion });
    }),
  );
}
```

Pass the executable and arguments separately to `ChildProcess.make`; never
construct a shell command from user input. Set `cwd` explicitly when repository
identity matters. Provide `env` narrowly and choose `extendEnv` deliberately so
tests and CI do not inherit accidental machine state.

Use the smallest spawner operation that matches the contract:

- `string` for bounded complete output;
- `lines` for bounded line-oriented output;
- `spawn` plus a scoped process handle for streaming, interactive, or
  exit-code-sensitive work;
- `ChildProcess.pipeTo` for a real pipeline without invoking a shell.

When using `spawn`, consume stdout/stderr without deadlocking, inspect
`handle.exitCode`, and wrap the whole process lifetime in `Effect.scoped`.
Map platform failures once into an operation-specific error; do not expose a
generic subprocess error throughout the application.

## Keep the runtime choice at the edge

Application services may require `FileSystem`, `Path`, `Terminal`, or
`ChildProcessSpawner`, but they should not import `NodeServices` or `BunServices`.
Provide the matching platform Layer only in the executable entrypoint. This
keeps workflows reusable and lets tests provide deterministic substitutes.
