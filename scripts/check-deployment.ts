import { BunRuntime, BunServices } from "@effect/platform-bun";
import { Effect, FileSystem, Path, Schema } from "effect";
import { ChildProcess } from "effect/unstable/process";

class DeploymentCheckError extends Schema.TaggedError<DeploymentCheckError>()(
  "DeploymentCheckError",
  { command: Schema.String, exitCode: Schema.Number },
) {}

export const checkDeployment = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const root = path.dirname(path.dirname(yield* path.fromFileUrl(new URL(import.meta.url))));

  // Alchemy initializes profiles even for --help. Keep the check independent of
  // developer credentials and remove its temporary profile when the scope closes.
  const profileDirectory = yield* fs.makeTempDirectoryScoped({
    prefix: "effect-agent-deploy-check-",
  });

  for (const args of [
    ["run", "docs:deploy", "--help"],
    ["run", "-F", "@effect-agent/example-travel-planner", "deploy", "--help"],
    [
      "exec",
      "bun",
      "--eval",
      'await import("./alchemy.run.ts"); await import("./examples/travel-planner/alchemy.run.ts");',
    ],
  ]) {
    const child = yield* ChildProcess.make("vp", args, {
      cwd: root,
      env: { ALCHEMY_HOME: profileDirectory, NO_TRACK: "1", CI: "true" },
      extendEnv: true,
      stdout: "inherit",
      stderr: "inherit",
    });

    const exitCode = yield* child.exitCode;

    if (exitCode !== 0) {
      return yield* new DeploymentCheckError({ command: ["vp", ...args].join(" "), exitCode });
    }
  }
});

if (import.meta.main) {
  BunRuntime.runMain(checkDeployment.pipe(Effect.scoped, Effect.provide(BunServices.layer)));
}
