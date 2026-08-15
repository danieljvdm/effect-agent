import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Cause, Console, Effect, Exit, FileSystem, Layer, Path, Schema, Stream } from "effect";
import { Command as CliCommand, Flag } from "effect/unstable/cli";
import { FetchHttpClient, HttpClient } from "effect/unstable/http";
import { ChildProcess } from "effect/unstable/process";

import { prepareReleaseArtifactDirectory } from "./release-artifact-directory.ts";

// ---------------------------------------------------------------------------
// Publish the public `@effect-agent/*` workspaces to npm with `bun publish`.
//
// Why not `changeset publish`: changesets shells out to `npm publish` in a
// non-pnpm repository, and npm ships `workspace:*` and `catalog:` protocol
// ranges verbatim — broken manifests. `bun publish` resolves both protocols
// at publish time (verified against Bun 1.3.14).
//
// The repository keeps package export maps pointing at TypeScript source for
// development (TOOLCHAIN.md); published artifacts must point at `dist`. This
// script therefore rewrites each manifest's exports to the built `dist`
// entries for the duration of one `bun publish`, then restores the original
// bytes exactly — on success, failure, or interrupt.
//
// Manual flow: build the dist artifacts, cut versions, run
// `release:publish`, then create and push Changesets tags. CI instead uses
// `--pack-directory` without OIDC authority, transfers the resulting manifest
// and tarballs as an immutable artifact, and lets an action-free publisher
// upload them. Manual publishing skips existing versions. Pack mode still
// produces their tarballs so the isolated publisher can validate an exact
// registry match and safely resume a partial release.
// ---------------------------------------------------------------------------

const REGISTRY = "https://registry.npmjs.org";

class ReleaseError extends Schema.TaggedError<ReleaseError>()("ReleaseError", {
  package: Schema.String,
  reason: Schema.String,
}) {
  override get message() {
    return `${this.package}: ${this.reason}`;
  }
}

class CommandError extends Schema.TaggedError<CommandError>()("CommandError", {
  command: Schema.String,
  exitCode: Schema.Int,
  output: Schema.String,
}) {
  override get message() {
    return this.output.length > 0
      ? `${this.command} exited with code ${this.exitCode}: ${this.output}`
      : `${this.command} exited with code ${this.exitCode}`;
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
export const PublishManifest = Schema.StructWithRest(
  Schema.Struct({
    name: Schema.String,
    version: Schema.String,
    private: Schema.optionalKey(Schema.Boolean),
    exports: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
    dependencies: Schema.optionalKey(DependencyMap),
    optionalDependencies: Schema.optionalKey(DependencyMap),
    peerDependencies: Schema.optionalKey(DependencyMap),
  }),
  [Schema.Record(Schema.String, Schema.Unknown)],
);

export const PreparedTarballPath = Schema.String.pipe(
  Schema.refine((value): value is string => /^packages\/[A-Za-z0-9._-]+[.]tgz$/.test(value), {
    expected: "a safe packages/<basename>.tgz relative path",
  }),
);

export const PreparedReleasePackage = Schema.Struct({
  name: Schema.String,
  version: Schema.String,
  distTag: Schema.String,
  tarball: Schema.NullOr(PreparedTarballPath),
});

export const PreparedReleaseManifest = Schema.Struct({
  version: Schema.Literal(1),
  packages: Schema.Array(PreparedReleasePackage),
});

const decodeManifest = Schema.decodeUnknownEffect(Schema.fromJsonString(PublishManifest));

const runCommand = Effect.fn("runCommand")(function* (
  cwd: string,
  command: string,
  args: ReadonlyArray<string>,
  env?: Record<string, string>,
) {
  const formatted = [command, ...args].join(" ");
  const child = yield* ChildProcess.make(command, args, {
    cwd,
    stderr: "pipe",
    stdout: "pipe",
    ...(env !== undefined ? { env, extendEnv: false } : {}),
  });
  const [output, exitCode] = yield* Effect.all([
    Stream.mkString(Stream.decodeText(child.all)),
    child.exitCode,
  ]);
  const trimmed = output.trim();
  if (exitCode !== 0) {
    return yield* CommandError.make({ command: formatted, exitCode, output: trimmed });
  }
  return trimmed;
});

/** True when `name@version` already exists on the public registry. */
const alreadyPublished = Effect.fn("alreadyPublished")(function* (name: string, version: string) {
  const response = yield* HttpClient.get(`${REGISTRY}/${name.replace("/", "%2f")}/${version}`).pipe(
    Effect.mapError((cause) =>
      ReleaseError.make({
        package: name,
        reason: `Registry lookup failed: ${String(cause)}`,
      }),
    ),
  );
  if (response.status === 200) return true;
  if (response.status === 404) return false;
  return yield* ReleaseError.make({
    package: name,
    reason: `Unexpected registry status ${response.status} for version lookup.`,
  });
});

/**
 * Bun resolves the global npmrc from `$XDG_CONFIG_HOME/.npmrc` whenever that
 * variable is set, ignoring `~/.npmrc` — where `npm login` writes the token.
 * When no npmrc exists at the XDG location, drop the variable from the
 * publish environment so bun falls back to `~/.npmrc` (verified on Bun
 * 1.3.14: with it set, `bun publish` reports "missing authentication"
 * despite a valid `npm whoami`).
 */
const publishEnvironment = Effect.fn("publishEnvironment")(function* () {
  const fs = yield* FileSystem.FileSystem;
  const xdgConfigHome = globalThis.process.env["XDG_CONFIG_HOME"];
  const dropXdg = xdgConfigHome !== undefined && !(yield* fs.exists(`${xdgConfigHome}/.npmrc`));
  const env: Record<string, string> = {};
  for (const [key, value] of Object.entries(globalThis.process.env)) {
    if (value !== undefined && (!dropXdg || key !== "XDG_CONFIG_HOME")) env[key] = value;
  }
  // CI mode keeps bun publish non-interactive: an expired OTP then fails
  // typed instead of prompting on the piped stdin — which hangs forever.
  env["CI"] = "1";
  return env;
});

/**
 * The npm dist-tag one version belongs to: the first prerelease identifier
 * (`0.0.1-beta.0` → `beta`), or `latest` for stable versions. `bun publish`
 * would otherwise tag PRERELEASES as `latest` — the channel must be explicit.
 */
const distTagFor = (version: string): string => {
  const prerelease = /^\d+\.\d+\.\d+-([0-9A-Za-z-]+)(?:\.|$)/.exec(version);
  return prerelease?.[1] ?? "latest";
};

/**
 * Map one source export path to its built dist entry, mirroring the `vp pack`
 * (tsdown) output naming: `./src/<entry>.ts` → `./dist/<entry>.mjs` with
 * `./dist/<entry>.d.mts` types.
 */
const distExport = (sourcePath: string): { types: string; default: string } | undefined => {
  const match = /^\.\/src\/([A-Za-z0-9_-]+)\.ts$/.exec(sourcePath);
  if (match === null) return undefined;
  return { types: `./dist/${match[1]}.d.mts`, default: `./dist/${match[1]}.mjs` };
};

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

const publishOne = Effect.fn("publishOne")(function* (options: {
  readonly directory: string;
  readonly dryRun: boolean;
  readonly packDirectory: string | undefined;
  readonly otp: string | undefined;
  readonly workspaceVersions: ReadonlyMap<string, string>;
}) {
  const fs = yield* FileSystem.FileSystem;
  const manifestPath = `${options.directory}/package.json`;
  const originalBytes = yield* fs.readFileString(manifestPath);
  const manifest = yield* decodeManifest(originalBytes);

  if (manifest.private === true) {
    yield* Console.log(`- ${manifest.name}: private, skipped`);
    return { _tag: "Private" as const };
  }
  const distTag = distTagFor(manifest.version);
  const versionAlreadyPublished =
    options.packDirectory === undefined
      ? yield* alreadyPublished(manifest.name, manifest.version)
      : false;
  if (versionAlreadyPublished && options.packDirectory === undefined) {
    yield* Console.log(`- ${manifest.name}@${manifest.version}: already on the registry, skipped`);
    return {
      _tag: "Skipped" as const,
      release: PreparedReleasePackage.make({
        name: manifest.name,
        version: manifest.version,
        distTag,
        tarball: null,
      }),
    };
  }
  // Rewrite every source export to its dist entry, fail-closed on both an
  // unrecognized export shape and a missing built artifact.
  const mutable = { ...manifest };

  // Pin internal `workspace:*` ranges to the exact workspace versions
  // ourselves: `bun publish` resolves them from the lockfile, which does not
  // pick up changeset version bumps ("no changes" install), and 0.0.1-beta.0
  // shipped with dependencies on unpublished internal versions as a result.
  for (const section of ["dependencies", "optionalDependencies", "peerDependencies"] as const) {
    const dependencies = manifest[section];
    if (dependencies === undefined) continue;
    const rewrittenDependencies = { ...dependencies };
    for (const [dependency, range] of Object.entries(rewrittenDependencies)) {
      if (!range.startsWith("workspace:")) continue;
      const version = options.workspaceVersions.get(dependency);
      if (version === undefined) {
        return yield* ReleaseError.make({
          package: manifest.name,
          reason: `Workspace dependency '${dependency}' has no known version to pin.`,
        });
      }
      rewrittenDependencies[dependency] = version;
    }
    mutable[section] = rewrittenDependencies;
  }
  const exportsMap = manifest.exports ?? {};
  const rewritten: Record<string, { types: string; default: string }> = {};
  for (const [key, sourcePath] of Object.entries(exportsMap)) {
    const dist = distExport(sourcePath);
    if (dist === undefined) {
      return yield* ReleaseError.make({
        package: manifest.name,
        reason: `Export '${key}' ('${sourcePath}') is not a recognized ./src/<entry>.ts path.`,
      });
    }
    for (const artifact of [dist.types, dist.default]) {
      if (!(yield* fs.exists(`${options.directory}/${artifact.slice(2)}`))) {
        return yield* ReleaseError.make({
          package: manifest.name,
          reason: `Built artifact '${artifact}' is missing; run the build first.`,
        });
      }
    }
    rewritten[key] = dist;
  }
  const publishManifest = { ...mutable, exports: rewritten };

  // Dry runs validate the packed artifact without registry credentials;
  // manual publishes require an authenticated npm session. CI stops at the
  // pack-directory branch and transfers the tarballs to its isolated OIDC job.
  yield* Console.log(
    `- ${manifest.name}@${manifest.version}: ${
      options.packDirectory !== undefined
        ? "preparing release tarball"
        : options.dryRun
          ? "packing (dry run)"
          : "publishing"
    }...`,
  );

  return yield* withTemporaryManifest(
    manifestPath,
    originalBytes,
    JSON.stringify(publishManifest, null, 2),
    Effect.gen(function* () {
      const environment = yield* publishEnvironment();
      const publishFailure = (error: { readonly _tag: string; readonly message: string }) =>
        error._tag === "CommandError"
          ? ReleaseError.make({
              package: manifest.name,
              reason: `${error.message} (an expired --otp is the usual cause locally; re-run with a fresh code — published versions are skipped)`,
            })
          : ReleaseError.make({
              package: manifest.name,
              reason: `Process execution failed (${error._tag}): ${error.message}`,
            });
      if (options.packDirectory !== undefined) {
        const before = new Set(yield* fs.readDirectory(options.packDirectory));
        yield* runCommand(
          options.directory,
          "bun",
          ["pm", "pack", "--destination", options.packDirectory],
          environment,
        ).pipe(Effect.mapError(publishFailure));
        const tarballs = (yield* fs.readDirectory(options.packDirectory)).filter(
          (entry) => entry.endsWith(".tgz") && !before.has(entry),
        );
        const tarball = tarballs[0];
        if (
          tarball === undefined ||
          tarballs.length !== 1 ||
          !/^[A-Za-z0-9._-]+\.tgz$/.test(tarball)
        ) {
          return yield* ReleaseError.make({
            package: manifest.name,
            reason: `Expected one safely named tarball in ${options.packDirectory}, found ${tarballs.length}.`,
          });
        }
        yield* Console.log(`- ${manifest.name}@${manifest.version}: prepared packages/${tarball}`);
        return {
          _tag: "Prepared" as const,
          release: PreparedReleasePackage.make({
            name: manifest.name,
            version: manifest.version,
            distTag,
            tarball: `packages/${tarball}`,
          }),
        };
      } else if (options.dryRun) {
        yield* runCommand(options.directory, "bun", ["pm", "pack", "--dry-run"], environment).pipe(
          Effect.mapError(publishFailure),
        );
      } else {
        yield* runCommand(
          options.directory,
          "bun",
          [
            "publish",
            "--access",
            "public",
            "--tag",
            distTag,
            ...(options.otp !== undefined ? ["--otp", options.otp] : []),
          ],
          environment,
        ).pipe(Effect.mapError(publishFailure));
      }
      yield* Console.log(
        `- ${manifest.name}@${manifest.version}: ${
          options.dryRun ? `dry-run ok (tag: ${distTag})` : `published (tag: ${distTag})`
        }`,
      );
      return {
        _tag: "Published" as const,
        release: PreparedReleasePackage.make({
          name: manifest.name,
          version: manifest.version,
          distTag,
          tarball: null,
        }),
      };
    }),
  );
});

const dryRunFlag = Flag.boolean("dry-run").pipe(
  Flag.withDescription("Pack and validate everything without uploading to the registry."),
);
const otpFlag = Flag.string("otp").pipe(
  Flag.optional,
  Flag.withDescription("npm one-time password forwarded to every bun publish."),
);
const packDirectoryFlag = Flag.string("pack-directory").pipe(
  Flag.optional,
  Flag.withDescription(
    "Prepare public package tarballs and a release-manifest.json in a new directory without publishing.",
  ),
);

const command = CliCommand.make(
  "release-publish",
  { dryRun: dryRunFlag, otp: otpFlag, packDirectory: packDirectoryFlag },
  ({ dryRun, otp, packDirectory }) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const packRoot =
        packDirectory._tag === "Some" ? path.resolve(packDirectory.value) : undefined;
      const selectedModes = Number(dryRun) + Number(packRoot !== undefined);
      if (selectedModes > 1 || (packRoot !== undefined && otp._tag === "Some")) {
        return yield* ReleaseError.make({
          package: "release-publish",
          reason: "--dry-run, --pack-directory, and --otp cannot select conflicting modes.",
        });
      }
      if (packRoot !== undefined) {
        if (yield* fs.exists(packRoot)) {
          return yield* ReleaseError.make({
            package: "release-publish",
            reason: `Pack directory already exists: ${packRoot}`,
          });
        }
      }
      const directories = (yield* fs.readDirectory("packages"))
        .filter((entry) => !entry.startsWith("."))
        .sort()
        .map((entry) => `packages/${entry}`);
      yield* Console.log(
        packRoot !== undefined
          ? `Preparing ${directories.length} workspaces for isolated trusted publishing...`
          : `Publishing ${directories.length} workspaces to ${REGISTRY}${dryRun ? " (dry run)" : ""}...`,
      );
      const workspaceVersions = new Map<string, string>();
      for (const directory of directories) {
        const manifest = yield* decodeManifest(
          yield* fs.readFileString(`${directory}/package.json`),
        );
        workspaceVersions.set(manifest.name, manifest.version);
      }
      const publishAll = (stagingRoot: string | undefined) =>
        Effect.gen(function* () {
          if (stagingRoot !== undefined) {
            yield* fs.makeDirectory(path.join(stagingRoot, "packages"), { recursive: true });
          }
          let published = 0;
          const releases: Array<typeof PreparedReleasePackage.Type> = [];
          for (const directory of directories) {
            const outcome = yield* Effect.scoped(
              publishOne({
                directory,
                dryRun,
                packDirectory:
                  stagingRoot === undefined ? undefined : path.join(stagingRoot, "packages"),
                otp: otp._tag === "Some" ? otp.value : undefined,
                workspaceVersions,
              }),
            );
            if (outcome._tag !== "Private") releases.push(outcome.release);
            if (outcome._tag === "Published" || outcome._tag === "Prepared") published += 1;
          }
          if (stagingRoot !== undefined) {
            const encoded = yield* Schema.encodeEffect(
              Schema.fromJsonString(PreparedReleaseManifest),
            )(
              PreparedReleaseManifest.make({
                version: 1,
                packages: releases,
              }),
            );
            yield* fs.writeFileString(path.join(stagingRoot, "release-manifest.json"), encoded);
          }
          return published;
        });
      const published =
        packRoot === undefined
          ? yield* publishAll(undefined)
          : yield* prepareReleaseArtifactDirectory(packRoot, publishAll);
      yield* Console.log(
        packRoot !== undefined
          ? `Release preparation complete: ${published} package(s) packed in ${packRoot}.`
          : dryRun
            ? `Dry run complete: ${published} package(s) would publish.`
            : `Published ${published} package(s). Now run: bun x changeset tag && git push --follow-tags`,
      );
    }),
).pipe(
  CliCommand.withDescription(
    "Prepare or publish framework workspaces with resolved workspace:/catalog: protocols and dist export maps.",
  ),
);

const program = CliCommand.run(command, { version: "1.0.0" }).pipe(
  Effect.scoped,
  Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer)),
  Effect.tapError((error) => Console.error(String(error))),
);

if (process.argv[1] !== undefined && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  NodeRuntime.runMain(program);
}
