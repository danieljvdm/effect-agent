import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Console, Effect, FileSystem, Schema, Stream } from "effect";
import { Command as CliCommand, Flag } from "effect/unstable/cli";
import { ChildProcess } from "effect/unstable/process";

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
// Flow: `bun run build` (dist artifacts) → `bun x changeset version` (cut the
// versions) → `bun run release:publish -- --otp <code>` → `bun x changeset
// tag` and push tags. Idempotent: versions already on the registry are
// skipped, so a partial publish can simply be re-run.
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

const PublishManifest = Schema.Struct({
  name: Schema.String,
  version: Schema.String,
  private: Schema.optionalKey(Schema.Boolean),
  exports: Schema.optionalKey(Schema.Record(Schema.String, Schema.String)),
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
  const response = yield* Effect.tryPromise({
    try: () => fetch(`${REGISTRY}/${name.replace("/", "%2f")}/${version}`),
    catch: (cause) =>
      ReleaseError.make({
        package: name,
        reason: `Registry lookup failed: ${String(cause)}`,
      }),
  });
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

const publishOne = Effect.fn("publishOne")(function* (options: {
  readonly directory: string;
  readonly dryRun: boolean;
  readonly ci: boolean;
  readonly otp: string | undefined;
  readonly workspaceVersions: ReadonlyMap<string, string>;
}) {
  const fs = yield* FileSystem.FileSystem;
  const manifestPath = `${options.directory}/package.json`;
  const originalBytes = yield* fs.readFileString(manifestPath);
  const manifest = yield* decodeManifest(originalBytes);

  if (manifest.private === true) {
    yield* Console.log(`- ${manifest.name}: private, skipped`);
    return "skipped" as const;
  }
  if (yield* alreadyPublished(manifest.name, manifest.version)) {
    yield* Console.log(`- ${manifest.name}@${manifest.version}: already on the registry, skipped`);
    return "skipped" as const;
  }

  // Rewrite every source export to its dist entry, fail-closed on both an
  // unrecognized export shape and a missing built artifact.
  const parsed: unknown = JSON.parse(originalBytes);
  const mutable = parsed as {
    exports?: Record<string, unknown>;
    dependencies?: Record<string, string>;
    optionalDependencies?: Record<string, string>;
    peerDependencies?: Record<string, string>;
  };

  // Pin internal `workspace:*` ranges to the exact workspace versions
  // ourselves: `bun publish` resolves them from the lockfile, which does not
  // pick up changeset version bumps ("no changes" install), and 0.0.1-beta.0
  // shipped with dependencies on unpublished internal versions as a result.
  for (const section of ["dependencies", "optionalDependencies", "peerDependencies"] as const) {
    const dependencies = mutable[section];
    if (dependencies === undefined) continue;
    for (const [dependency, range] of Object.entries(dependencies)) {
      if (!range.startsWith("workspace:")) continue;
      const version = options.workspaceVersions.get(dependency);
      if (version === undefined) {
        return yield* ReleaseError.make({
          package: manifest.name,
          reason: `Workspace dependency '${dependency}' has no known version to pin.`,
        });
      }
      dependencies[dependency] = version;
    }
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
  mutable.exports = rewritten;

  // Dry runs validate the packed artifact without registry credentials;
  // real publishes require an authenticated npm session (`bunx npm login`)
  // or, in CI mode, npm's OIDC trusted-publisher exchange.
  const distTag = distTagFor(manifest.version);

  yield* Console.log(
    `- ${manifest.name}@${manifest.version}: ${options.dryRun ? "packing (dry run)" : "publishing"}...`,
  );

  // The manifest swap is scoped: acquireRelease restores the original bytes
  // even when the publish fails or the fiber is interrupted.
  yield* Effect.acquireRelease(
    fs.writeFileString(manifestPath, JSON.stringify(parsed, null, 2)),
    () => fs.writeFileString(manifestPath, originalBytes).pipe(Effect.orDie),
  );
  const environment = yield* publishEnvironment();
  const publishFailure = (error: { readonly _tag: string; readonly message: string }) =>
    error._tag === "CommandError"
      ? ReleaseError.make({
          package: manifest.name,
          reason: `${error.message} (an expired --otp is the usual cause locally; re-run with a fresh code — published versions are skipped)`,
        })
      : (error as ReleaseError);
  if (options.dryRun) {
    yield* runCommand(options.directory, "bun", ["pm", "pack", "--dry-run"], environment).pipe(
      Effect.mapError(publishFailure),
    );
  } else if (options.ci) {
    // Trusted publishing: bun packs (resolving workspace:/catalog: protocols
    // into the tarball), and the npm CLI uploads it — only npm implements the
    // OIDC trusted-publisher exchange and provenance attestation.
    const packDirectory = `${options.directory}/dist/.release-pack`;
    yield* fs.makeDirectory(packDirectory, { recursive: true });
    yield* runCommand(
      options.directory,
      "bun",
      ["pm", "pack", "--destination", "dist/.release-pack"],
      environment,
    ).pipe(Effect.mapError(publishFailure));
    const tarballs = (yield* fs.readDirectory(packDirectory).pipe(Effect.orDie)).filter((entry) =>
      entry.endsWith(".tgz"),
    );
    const tarball = tarballs[0];
    if (tarball === undefined || tarballs.length !== 1) {
      return yield* ReleaseError.make({
        package: manifest.name,
        reason: `Expected exactly one packed tarball in ${packDirectory}, found ${tarballs.length}.`,
      });
    }
    yield* runCommand(
      options.directory,
      "npm",
      [
        "publish",
        `dist/.release-pack/${tarball}`,
        "--access",
        "public",
        "--tag",
        distTag,
        "--provenance",
      ],
      environment,
    ).pipe(
      Effect.mapError(publishFailure),
      Effect.ensuring(fs.remove(packDirectory, { recursive: true }).pipe(Effect.ignore)),
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
  return "published" as const;
});

const dryRunFlag = Flag.boolean("dry-run").pipe(
  Flag.withDescription("Pack and validate everything without uploading to the registry."),
);
const otpFlag = Flag.string("otp").pipe(
  Flag.optional,
  Flag.withDescription("npm one-time password forwarded to every bun publish."),
);
const ciFlag = Flag.boolean("ci").pipe(
  Flag.withDescription(
    "Trusted-publishing mode: bun packs the tarball, the npm CLI uploads it via the OIDC trusted-publisher exchange with provenance.",
  ),
);

const command = CliCommand.make(
  "release-publish",
  { dryRun: dryRunFlag, ci: ciFlag, otp: otpFlag },
  ({ ci, dryRun, otp }) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directories = (yield* fs.readDirectory("packages"))
        .filter((entry) => !entry.startsWith("."))
        .sort()
        .map((entry) => `packages/${entry}`);
      yield* Console.log(
        `Publishing ${directories.length} workspaces to ${REGISTRY}${dryRun ? " (dry run)" : ""}...`,
      );
      const workspaceVersions = new Map<string, string>();
      for (const directory of directories) {
        const manifest = yield* decodeManifest(
          yield* fs.readFileString(`${directory}/package.json`),
        );
        workspaceVersions.set(manifest.name, manifest.version);
      }
      let published = 0;
      for (const directory of directories) {
        const outcome = yield* Effect.scoped(
          publishOne({
            directory,
            dryRun,
            ci,
            otp: otp._tag === "Some" ? otp.value : undefined,
            workspaceVersions,
          }),
        );
        if (outcome === "published") published += 1;
      }
      yield* Console.log(
        dryRun
          ? `Dry run complete: ${published} package(s) would publish.`
          : `Published ${published} package(s). Now run: bun x changeset tag && git push --follow-tags`,
      );
    }),
).pipe(
  CliCommand.withDescription(
    "Publish public @effect-agent/* workspaces with bun publish (resolves workspace:/catalog: protocols) using dist export maps.",
  ),
);

const program = CliCommand.run(command, { version: "1.0.0" }).pipe(
  Effect.tapError((error) => Console.error(String(error))),
  Effect.scoped,
  Effect.provide(NodeServices.layer),
);

NodeRuntime.runMain(program, { disableErrorReporting: true });
