import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Cause, Console, Effect, Exit, FileSystem, Option, Path, Schema } from "effect";
import { Command as CliCommand, Flag } from "effect/unstable/cli";
import { FetchHttpClient, HttpClient, HttpClientResponse } from "effect/unstable/http";
import { ChildProcess } from "effect/unstable/process";

// Changesets owns final registry checks, publishing, prerelease tags, and Git tags.
// npm needs resolved Bun dependency ranges and built exports in its input manifests.
class ReleaseError extends Schema.TaggedError<ReleaseError>()("ReleaseError", {
  package: Schema.String,
  reason: Schema.String,
}) {
  override get message() {
    return this.package + ": " + this.reason;
  }
}

class CommandError extends Schema.TaggedError<CommandError>()("CommandError", {
  command: Schema.String,
  exitCode: Schema.Int,
}) {
  override get message() {
    return this.command + " exited with code " + this.exitCode;
  }
}

export class ReleaseManifestSwapError extends Schema.TaggedError<ReleaseManifestSwapError>()(
  "ReleaseManifestSwapError",
  {
    cause: Schema.optionalKey(Schema.Defect()),
    manifestPath: Schema.String,
    message: Schema.String,
    operation: Schema.Literals(["install", "restore"]),
  },
) {}

const DependencyMap = Schema.Record(Schema.String, Schema.String);

const manifestFields = {
  name: Schema.String,
  version: Schema.String,
  private: Schema.optionalKey(Schema.Boolean),
  dependencies: Schema.optionalKey(DependencyMap),
  devDependencies: Schema.optionalKey(DependencyMap),
  optionalDependencies: Schema.optionalKey(DependencyMap),
  peerDependencies: Schema.optionalKey(DependencyMap),
};

export const PublishManifest = Schema.StructWithRest(
  Schema.Struct({
    ...manifestFields,
    exports: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
  }),
  [Schema.Record(Schema.String, Schema.Unknown)],
);

const PackedManifest = Schema.StructWithRest(
  Schema.Struct({
    ...manifestFields,
    exports: Schema.Record(
      Schema.String,
      Schema.Struct({
        types: Schema.String,
        default: Schema.String,
      }),
    ),
  }),
  [Schema.Record(Schema.String, Schema.Unknown)],
);

const RootManifest = Schema.Struct({ catalog: DependencyMap });

const PrereleaseState = Schema.StructWithRest(
  Schema.Struct({ mode: Schema.Literals(["pre", "exit"]), tag: Schema.Literal("beta") }),
  [Schema.Record(Schema.String, Schema.Unknown)],
);

const decodeManifest = Schema.decodeUnknownEffect(Schema.fromJsonString(PublishManifest));

const readWorkspacePackages = Effect.fn("releasePublish.readWorkspacePackages")(function* (
  root: string,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;

  return yield* Effect.forEach(
    (yield* fs.readDirectory(path.join(root, "packages")))
      .filter((name) => !name.startsWith("."))
      .sort(),
    Effect.fn(function* (name) {
      const directory = path.join(root, "packages", name);
      const manifestPath = path.join(directory, "package.json");
      const originalBytes = yield* fs.readFileString(manifestPath);

      return {
        directory,
        manifestPath,
        originalBytes,
        manifest: yield* decodeManifest(originalBytes),
      };
    }),
  );
});

const RegistryVersion = Schema.Struct({ name: Schema.String, version: Schema.String });

/** Changesets invokes its publish hook even when nothing remains to publish. */
export const withUnpublishedRelease = <E, R>(
  packages: ReadonlyArray<typeof PublishManifest.Type>,
  release: Effect.Effect<void, E, R>,
) =>
  Effect.gen(function* () {
    const client = yield* HttpClient.HttpClient;

    const pending = yield* Effect.forEach(
      packages.filter((pkg) => pkg.private !== true),
      Effect.fn(function* (pkg) {
        const response = yield* client.get(
          `https://registry.npmjs.org/${encodeURIComponent(pkg.name)}/${encodeURIComponent(pkg.version)}`,
        );

        if (response.status === 404) return true;
        if (response.status !== 200)
          return yield* ReleaseError.make({
            package: pkg.name,
            reason: `Cannot determine release status: npm returned ${response.status}`,
          });
        const published = yield* HttpClientResponse.schemaBodyJson(RegistryVersion)(response);

        if (published.name !== pkg.name || published.version !== pkg.version)
          return yield* ReleaseError.make({
            package: pkg.name,
            reason: "npm returned a different package or version",
          });

        return false;
      }),
      { concurrency: 3 },
    ).pipe(Effect.timeout("30 seconds"));

    if (!pending.some(Boolean)) {
      yield* Console.log(
        "All public versions are already published; skipping the live gate and publication.",
      );

      return;
    }
    yield* release;
  });

export const withTemporaryManifest = <A, E, R>(
  manifestPath: string,
  originalBytes: string,
  publishBytes: string,
  use: Effect.Effect<A, E, R>,
) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;

    const manifestError = (operation: ReleaseManifestSwapError["operation"]) => (cause: unknown) =>
      ReleaseManifestSwapError.make({
        cause,
        manifestPath,
        message: `Could not ${operation} temporary publish manifest ${manifestPath}: ${String(cause)}`,
        operation,
      });

    return yield* Effect.uninterruptibleMask((restore) =>
      Effect.gen(function* () {
        const installExit = yield* fs
          .writeFileString(manifestPath, publishBytes)
          .pipe(Effect.mapError(manifestError("install")), Effect.exit);

        if (Exit.isFailure(installExit)) {
          const restoreExit = yield* fs
            .writeFileString(manifestPath, originalBytes)
            .pipe(Effect.mapError(manifestError("restore")), Effect.exit);

          return yield* Effect.failCause(
            Exit.isFailure(restoreExit)
              ? Cause.combine(installExit.cause, restoreExit.cause)
              : installExit.cause,
          );
        }
        const useExit = yield* restore(use).pipe(Effect.exit);

        const restoreExit = yield* fs
          .writeFileString(manifestPath, originalBytes)
          .pipe(Effect.mapError(manifestError("restore")), Effect.exit);

        if (Exit.isFailure(useExit)) {
          return yield* Effect.failCause(
            Exit.isFailure(restoreExit)
              ? Cause.combine(useExit.cause, restoreExit.cause)
              : useExit.cause,
          );
        }
        if (Exit.isFailure(restoreExit)) return yield* Effect.failCause(restoreExit.cause);

        return useExit.value;
      }),
    );
  });

/** Install npm-ready manifests only while publishing; restore every source file on all exits. */
export const withPublishManifests = <A, E, R>(
  root: string,
  use: (directories: ReadonlyArray<string>) => Effect.Effect<A, E, R>,
) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;

    const rootManifest = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(RootManifest))(
      yield* fs.readFileString(path.join(root, "package.json")),
    );

    const packages = yield* readWorkspacePackages(root);

    const versions = new Map(packages.map(({ manifest }) => [manifest.name, manifest.version]));
    const publicPackages = packages.filter(({ manifest }) => manifest.private !== true);

    // Validate every package before any manifest is replaced or any publish starts.
    const prepared = yield* Effect.forEach(
      publicPackages,
      Effect.fn(function* (pkg) {
        const mutable = { ...pkg.manifest };

        for (const section of [
          "dependencies",
          "devDependencies",
          "optionalDependencies",
          "peerDependencies",
        ] as const) {
          const dependencies = mutable[section];

          if (dependencies === undefined) continue;
          const resolved = { ...dependencies };

          for (const [name, range] of Object.entries(dependencies)) {
            if (range === "workspace:*") {
              const version = versions.get(name);

              if (version === undefined) {
                return yield* ReleaseError.make({
                  package: pkg.manifest.name,
                  reason: "Unknown workspace dependency: " + name,
                });
              }
              resolved[name] = version;
            } else if (range === "catalog:") {
              const version = rootManifest.catalog[name];

              if (version === undefined) {
                return yield* ReleaseError.make({
                  package: pkg.manifest.name,
                  reason: "Unknown catalog dependency: " + name,
                });
              }
              resolved[name] = version;
            } else if (range.startsWith("workspace:") || range.startsWith("catalog:")) {
              return yield* ReleaseError.make({
                package: pkg.manifest.name,
                reason: "Unsupported dependency range: " + name + "@" + range,
              });
            }
          }
          mutable[section] = resolved;
        }
        const exports: Record<string, { types: string; default: string }> = {};

        for (const [key, source] of Object.entries(pkg.manifest.exports ?? {})) {
          const match = /^\.\/src\/([A-Za-z0-9_-]+)\.ts$/.exec(source);

          if (match === null) {
            return yield* ReleaseError.make({
              package: pkg.manifest.name,
              reason: "Unsupported source export: " + source,
            });
          }

          const entry = {
            types: "./dist/" + match[1] + ".d.mts",
            default: "./dist/" + match[1] + ".mjs",
          };

          for (const artifact of [entry.types, entry.default]) {
            if (!(yield* fs.exists(path.join(pkg.directory, artifact)))) {
              return yield* ReleaseError.make({
                package: pkg.manifest.name,
                reason: "Missing built artifact: " + artifact,
              });
            }
          }
          exports[key] = entry;
        }

        const publishBytes = yield* Schema.encodeEffect(Schema.fromJsonString(PackedManifest))({
          ...mutable,
          exports,
        });

        return { ...pkg, publishBytes };
      }),
    );

    let publish: Effect.Effect<A, E | ReleaseManifestSwapError, R | FileSystem.FileSystem> = use(
      publicPackages.map(({ directory }) => directory),
    );

    // Changesets disallows --tag in pre mode and otherwise sends beta-only packages
    // to latest. Disable its prerelease tag selection only during publication;
    // versions and the repository's prerelease state remain unchanged afterward.
    const prePath = path.join(root, ".changeset", "pre.json");

    if (yield* fs.exists(prePath)) {
      const original = yield* fs.readFileString(prePath);

      const state = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(PrereleaseState))(
        original,
      );

      const temporary = yield* Schema.encodeEffect(Schema.fromJsonString(PrereleaseState))({
        ...state,
        mode: "exit",
      });

      publish = withTemporaryManifest(prePath, original, temporary, publish);
    }

    for (const pkg of prepared.toReversed()) {
      publish = withTemporaryManifest(
        pkg.manifestPath,
        pkg.originalBytes,
        pkg.publishBytes,
        publish,
      );
    }

    return yield* publish;
  });

const runCommand = Effect.fn("releasePublish.runCommand")(function* (
  cwd: string,
  command: string,
  args: ReadonlyArray<string>,
) {
  const child = yield* ChildProcess.make(command, args, {
    cwd,
    stdin: "inherit",
    stdout: "inherit",
    stderr: "inherit",
  });

  const exitCode = yield* child.exitCode;

  if (exitCode !== 0) {
    return yield* CommandError.make({ command, exitCode });
  }
}, Effect.scoped);

export const command = CliCommand.make(
  "release-publish",
  {
    dryRun: Flag.boolean("dry-run").pipe(
      Flag.withDescription("Build and inspect npm packages without publishing or creating tags."),
      Flag.withDefault(false),
    ),
    checkContinuity: Flag.boolean("check-continuity").pipe(
      Flag.withDescription(
        "Run the live continuity gate only when a public version needs publishing.",
      ),
      Flag.withDefault(false),
    ),
    otp: Flag.string("otp").pipe(
      Flag.optional,
      Flag.withDescription("npm one-time password for an authenticated manual release."),
    ),
  },
  Effect.fn("releasePublish.command")(function* ({ dryRun, otp, checkContinuity }) {
    const path = yield* Path.Path;

    const root = path.resolve(
      path.dirname(yield* path.fromFileUrl(new URL(import.meta.url))),
      "..",
    );

    const publish = Effect.gen(function* () {
      if (checkContinuity && !dryRun)
        yield* runCommand(root, "vp", [
          "run",
          "--no-cache",
          "context-continuity-eval",
          "--require-clean",
          "--profile",
          "explicit-rollover-sqlite-v1",
          "--max-cost-usd",
          "10",
        ]);
      yield* runCommand(root, "vp", ["run", "build"]);
      yield* withPublishManifests(root, (directories) =>
        dryRun
          ? Effect.forEach(
              directories,
              (directory) =>
                runCommand(directory, "npm", ["pack", "--dry-run", "--ignore-scripts"]),
              { discard: true },
            )
          : runCommand(root, path.join(root, "node_modules", ".bin", "changeset"), [
              "publish",
              "--tag",
              "beta",
              ...(Option.isSome(otp) ? ["--otp", otp.value] : []),
            ]),
      );
    });

    if (checkContinuity && !dryRun) {
      const packages = yield* readWorkspacePackages(root);

      yield* withUnpublishedRelease(
        packages.map((pkg) => pkg.manifest),
        publish,
      );
    } else yield* publish;
    if (dryRun) yield* Console.log("Dry run complete. Nothing published or tagged.");
  }),
).pipe(
  CliCommand.withDescription(
    "Build and publish with Changesets using npm-ready package manifests.",
  ),
);

const program = CliCommand.run(command, { version: "1.0.0" }).pipe(
  Effect.scoped,
  Effect.provide([NodeServices.layer, FetchHttpClient.layer]),
);

if (import.meta.main) NodeRuntime.runMain(program);
