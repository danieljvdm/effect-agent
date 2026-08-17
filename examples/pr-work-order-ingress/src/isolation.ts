import { fileURLToPath } from "node:url";

import { Context, Effect, FileSystem, Layer, Path, Schema, type Scope, Stream } from "effect";
import { ChildProcess, type ChildProcessSpawner } from "effect/unstable/process";

import { type IsolatedCheckRequest, IsolatedEnvironment, IsolationViolation } from "./contracts.ts";

const workerPath = fileURLToPath(new URL("./isolation-worker.mjs", import.meta.url));

const IsolatedCheckOutcome = Schema.Union([
  Schema.Struct({
    _tag: Schema.Literal("checked"),
    environment: IsolatedEnvironment,
    results: Schema.Array(
      Schema.Struct({
        name: Schema.NonEmptyString,
        status: Schema.Literals(["passed", "failed"]),
        summary: Schema.NonEmptyString,
      }),
    ),
  }),
  IsolationViolation,
]);

export const spawnIsolatedWorker = Effect.fn("spawnIsolatedWorker")(function* (input: {
  readonly role: "check" | "publish";
  readonly request: unknown;
  readonly env?: Record<string, string> | undefined;
}) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: "ingress-isolate-" });
  const requestPath = path.join(directory, "request.json");
  yield* fs.writeFileString(requestPath, JSON.stringify(input.request));
  const child = yield* ChildProcess.make(process.execPath, [workerPath, input.role, requestPath], {
    env: { PATH: "/usr/bin:/bin", ...input.env },
    extendEnv: false,
    stdin: "ignore",
    stderr: "pipe",
    stdout: "pipe",
  }).pipe(
    Effect.mapError((cause) =>
      IsolationViolation.make({
        process: input.role,
        reason: String(cause).slice(0, 2_048),
      }),
    ),
  );
  const [output, exitCode] = yield* Effect.all([
    child.all.pipe(Stream.decodeText(), Stream.mkString),
    child.exitCode,
  ]);
  if (Number(exitCode) !== 0) {
    return yield* IsolationViolation.make({
      process: input.role,
      reason: output.slice(0, 2_048) || `isolation worker exited ${String(exitCode)}`,
    });
  }
  return yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown))(output).pipe(
    Effect.mapError(() =>
      IsolationViolation.make({
        process: input.role,
        reason: "isolation worker returned a non-JSON report",
      }),
    ),
  );
});

export class IsolatedChecks extends Context.Service<
  IsolatedChecks,
  {
    readonly run: (request: IsolatedCheckRequest) => Effect.Effect<
      {
        readonly environment: IsolatedEnvironment;
        readonly results: ReadonlyArray<{
          readonly name: string;
          readonly status: "passed" | "failed";
          readonly summary: string;
        }>;
      },
      IsolationViolation,
      FileSystem.FileSystem | Path.Path | ChildProcessSpawner.ChildProcessSpawner | Scope.Scope
    >;
  }
>()("@effect-agent/example-pr-work-order-ingress/IsolatedChecks") {
  static readonly layer = Layer.succeed(
    IsolatedChecks,
    IsolatedChecks.of({
      run: (request) =>
        Effect.gen(function* () {
          const decoded = yield* spawnIsolatedWorker({ role: "check", request }).pipe(
            Effect.flatMap(Schema.decodeUnknownEffect(IsolatedCheckOutcome)),
            Effect.mapError((error) =>
              error._tag === "IsolationViolation"
                ? error
                : IsolationViolation.make({
                    process: "check",
                    reason: "isolation worker returned an invalid check report",
                  }),
            ),
          );
          if (decoded._tag === "IsolationViolation") return yield* decoded;
          return { environment: decoded.environment, results: decoded.results };
        }),
    }),
  );
}
