import { Effect, Schema, Stream } from "effect";
import { ChildProcess } from "effect/unstable/process";

import {
  GITHUB_WRITE_TOKEN_ENV,
  IsolatedEnvironment,
  IsolationViolation,
  MODEL_SECRET_ENV,
} from "./contracts.ts";

const PATH_ONLY = { PATH: "/usr/bin:/bin" } as const;

const inspectScript = [
  "const env = process.env;",
  `const hasWriteToken = Object.prototype.hasOwnProperty.call(env, ${JSON.stringify(GITHUB_WRITE_TOKEN_ENV)});`,
  `const hasModelSecret = Object.prototype.hasOwnProperty.call(env, ${JSON.stringify(MODEL_SECRET_ENV)});`,
  "process.stdout.write(JSON.stringify({ hasWriteToken, hasModelSecret }));",
].join("");

const IsolatedEnvReport = Schema.Struct({
  hasWriteToken: Schema.Boolean,
  hasModelSecret: Schema.Boolean,
});

export const inspectIsolatedCheckEnvironment = Effect.fn("inspectIsolatedCheckEnvironment")(
  function* () {
    const child = yield* ChildProcess.make(process.execPath, ["-e", inspectScript], {
      env: PATH_ONLY,
      extendEnv: false,
      stdin: "ignore",
      stderr: "pipe",
      stdout: "pipe",
    });
    const [output, exitCode] = yield* Effect.all([
      child.all.pipe(Stream.decodeText(), Stream.mkString),
      child.exitCode,
    ]);
    if (Number(exitCode) !== 0) {
      return yield* IsolationViolation.make({
        process: "check",
        reason: output.slice(0, 2_048) || `check process exited ${String(exitCode)}`,
      });
    }
    const report = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(IsolatedEnvReport))(
      output,
    ).pipe(
      Effect.mapError(() =>
        IsolationViolation.make({
          process: "check",
          reason: "check process did not report an isolated environment",
        }),
      ),
    );
    if (report.hasWriteToken || report.hasModelSecret) {
      return yield* IsolationViolation.make({
        process: "check",
        reason: "check process inherited a GitHub write token or model-provider secret",
      });
    }
    return IsolatedEnvironment.make({
      process: "check",
      hasWriteToken: report.hasWriteToken,
      hasModelSecret: report.hasModelSecret,
    });
  },
);
