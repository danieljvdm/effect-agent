import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Config, Console, Effect, FileSystem, Path, Schema as S, Stream } from "effect";
import { Command as CliCommand } from "effect/unstable/cli";
import { ChildProcess } from "effect/unstable/process";

const SUBMODULE_PATH = "repos/effect";
const SENTINEL_PATH = `${SUBMODULE_PATH}/packages/effect/package.json`;

const WorkspaceCatalog = S.Struct({
  catalog: S.optionalKey(S.Record(S.String, S.String)),
});

class RootPackageJson extends S.Class<RootPackageJson>("RootPackageJson")({
  catalog: S.optionalKey(S.Record(S.String, S.String)),
  workspaces: S.optionalKey(S.Union([S.Array(S.String), WorkspaceCatalog])),
}) {}

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

class CatalogMissingPackageError extends S.TaggedError<CatalogMissingPackageError>()(
  "CatalogMissingPackageError",
  {
    packageName: S.String,
  },
) {
  override get message() {
    return `package "${this.packageName}" missing from root catalog`;
  }
}

class PackageJsonError extends S.TaggedError<PackageJsonError>()("PackageJsonError", {
  cause: S.Defect(),
  path: S.String,
  message: S.String,
}) {}

class SubmoduleSentinelMissingError extends S.TaggedError<SubmoduleSentinelMissingError>()(
  "SubmoduleSentinelMissingError",
  {
    sentinel: S.String,
  },
) {
  override get message() {
    return `expected ${this.sentinel} after submodule init`;
  }
}

const resolveDirs = Effect.fn("resolveDirs")(function* () {
  const path = yield* Path.Path;
  const scriptPath = yield* path.fromFileUrl(new URL(import.meta.url));
  const rootDir = path.resolve(path.dirname(scriptPath), "..");

  return {
    rootDir,
    sentinel: path.resolve(rootDir, SENTINEL_PATH),
    submoduleDir: path.resolve(rootDir, SUBMODULE_PATH),
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
  if (exitCode !== 0) {
    return yield* CommandError.make({
      command: formatted,
      exitCode,
      output: trimmed,
    });
  }

  return trimmed;
});

const runGit = (cwd: string, args: ReadonlyArray<string>) => runCommand(cwd, "git", args);

const readEffectVersion = Effect.fn("readEffectVersion")(function* (rootDir: string) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const packageJsonPath = path.resolve(rootDir, "package.json");
  const raw = yield* fs.readFileString(packageJsonPath);
  const packageJson = yield* S.decodeUnknownEffect(S.fromJsonString(RootPackageJson))(raw).pipe(
    Effect.mapError((cause) =>
      PackageJsonError.make({
        cause,
        path: packageJsonPath,
        message: cause.message,
      }),
    ),
  );
  const workspaceCatalog =
    packageJson.workspaces !== undefined && S.is(WorkspaceCatalog)(packageJson.workspaces)
      ? packageJson.workspaces.catalog
      : undefined;
  const version = packageJson.catalog?.effect ?? workspaceCatalog?.effect;

  if (!version) {
    return yield* CatalogMissingPackageError.make({ packageName: "effect" });
  }

  return version;
});

const ensureSubmoduleInitialized = Effect.fn("ensureSubmoduleInitialized")(function* (
  rootDir: string,
  sentinel: string,
) {
  const fs = yield* FileSystem.FileSystem;

  if (yield* fs.exists(sentinel)) {
    return;
  }

  yield* runGit(rootDir, ["submodule", "sync", "--", SUBMODULE_PATH]);
  yield* runGit(rootDir, [
    "submodule",
    "update",
    "--init",
    "--checkout",
    "--depth",
    "1",
    "--",
    SUBMODULE_PATH,
  ]);

  if (!(yield* fs.exists(sentinel))) {
    return yield* SubmoduleSentinelMissingError.make({ sentinel: SENTINEL_PATH });
  }
});

const syncEffectSubmodule = Effect.fn("syncEffectSubmodule")(function* () {
  const isCi = yield* Config.boolean("CI").pipe(Config.withDefault(false));
  if (isCi) {
    yield* Console.error("Effect submodule skipped: CI=true");
    return;
  }

  const { rootDir, sentinel, submoduleDir } = yield* resolveDirs();
  const version = yield* readEffectVersion(rootDir);
  const tag = `effect@${version}`;

  yield* Console.error(`Effect submodule: syncing ${tag}`);
  yield* ensureSubmoduleInitialized(rootDir, sentinel);
  yield* runGit(submoduleDir, [
    "fetch",
    "--depth",
    "1",
    "--force",
    "--quiet",
    "origin",
    `refs/tags/${tag}:refs/tags/${tag}`,
  ]);

  const target = yield* runGit(submoduleDir, ["rev-parse", "-q", "--verify", `${tag}^{commit}`]);
  const current = yield* runGit(submoduleDir, ["rev-parse", "HEAD"]);

  if (current !== target) {
    yield* runGit(submoduleDir, ["checkout", "--detach", target]);
    yield* Console.error(`Effect submodule: ${tag} -> ${target.slice(0, 12)}`);
    return;
  }

  yield* Console.error(`Effect submodule: ${tag} already current`);
});

const syncCommand = CliCommand.make("sync-effect-submodule", {}, () => syncEffectSubmodule()).pipe(
  CliCommand.withDescription("Sync repos/effect to the root catalog's effect version."),
);

const program = CliCommand.run(syncCommand, { version: "1.0.0" }).pipe(
  Effect.scoped,
  Effect.provide(NodeServices.layer),
);

NodeRuntime.runMain(program, { disableErrorReporting: true });
