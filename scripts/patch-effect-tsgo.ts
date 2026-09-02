import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Console, Effect, FileSystem, Path, Schema, Stream } from "effect";
import { ChildProcess } from "effect/unstable/process";

// ---------------------------------------------------------------------------
// Patch the installed `typescript` binary with the Effect TypeScript-Go
// integration, replacing the `dev-kit tsgo patch` lifecycle task after
// ejecting Dev Kit's v1 managed model. `effect-tsgo patch` selects its
// replacement by installed package version and is safe to re-run, so this
// script only adds the version-pin check that made the pairing intentional.
// ---------------------------------------------------------------------------

const EXPECTED_TSGO_VERSION = "0.33.0";
const EXPECTED_TYPESCRIPT_VERSION = "7.0.2";

class EffectTsgoDependencyError extends Schema.TaggedError<EffectTsgoDependencyError>()(
  "EffectTsgoDependencyError",
  { packageName: Schema.String, expectedVersion: Schema.String, actualVersion: Schema.String },
) {
  override get message() {
    return `${this.packageName}@${this.actualVersion} is installed; the Effect TypeScript-Go patch requires ${this.packageName}@${this.expectedVersion}`;
  }
}

class EffectTsgoPatchCommandError extends Schema.TaggedError<EffectTsgoPatchCommandError>()(
  "EffectTsgoPatchCommandError",
  { command: Schema.String, exitCode: Schema.Int, output: Schema.String },
) {
  override get message() {
    return this.output.length > 0
      ? `${this.command} exited with code ${this.exitCode}: ${this.output}`
      : `${this.command} exited with code ${this.exitCode}`;
  }
}

const PackageVersionSchema = Schema.fromJsonString(Schema.Struct({ version: Schema.String }));

const requireExactVersion = Effect.fn("requireExactPackageVersion")(function* (
  repositoryRoot: string,
  packageName: string,
  expectedVersion: string,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;

  const manifestPath = path.join(
    repositoryRoot,
    "node_modules",
    ...packageName.split("/"),
    "package.json",
  );

  const contents = yield* fs.readFileString(manifestPath).pipe(
    Effect.mapError(() =>
      EffectTsgoDependencyError.make({
        packageName,
        expectedVersion,
        actualVersion: "(not installed)",
      }),
    ),
  );

  const manifest = yield* Schema.decodeEffect(PackageVersionSchema)(contents).pipe(
    Effect.mapError(() =>
      EffectTsgoDependencyError.make({
        packageName,
        expectedVersion,
        actualVersion: "(unreadable)",
      }),
    ),
  );

  if (manifest.version !== expectedVersion) {
    return yield* EffectTsgoDependencyError.make({
      packageName,
      expectedVersion,
      actualVersion: manifest.version,
    });
  }
});

const program = Effect.gen(function* () {
  const path = yield* Path.Path;
  const scriptPath = yield* path.fromFileUrl(new URL(import.meta.url));
  const repositoryRoot = path.resolve(path.dirname(scriptPath), "..");

  yield* requireExactVersion(repositoryRoot, "@effect/tsgo", EXPECTED_TSGO_VERSION);
  yield* requireExactVersion(repositoryRoot, "typescript", EXPECTED_TYPESCRIPT_VERSION);

  const executable = path.join(repositoryRoot, "node_modules", ".bin", "effect-tsgo");

  const child = yield* ChildProcess.make(executable, ["patch", "--typescript"], {
    cwd: repositoryRoot,
    stderr: "pipe",
    stdout: "pipe",
  });

  const [output, exitCode] = yield* Effect.all([
    Stream.mkString(Stream.decodeText(child.all)),
    child.exitCode,
  ]);

  if (exitCode !== 0) {
    return yield* EffectTsgoPatchCommandError.make({
      command: `${executable} patch --typescript`,
      exitCode,
      output: output.trim(),
    });
  }
  yield* Console.log(
    `Patched typescript@${EXPECTED_TYPESCRIPT_VERSION} with @effect/tsgo@${EXPECTED_TSGO_VERSION}.`,
  );
}).pipe(Effect.scoped, Effect.provide(NodeServices.layer));

NodeRuntime.runMain(program, { disableErrorReporting: true });
