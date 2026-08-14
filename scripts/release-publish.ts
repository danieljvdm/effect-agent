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

class ReleaseError extends Schema.TaggedErrorClass<ReleaseError>()("ReleaseError", {
  package: Schema.String,
  reason: Schema.String,
}) {
  override get message() {
    return `${this.package}: ${this.reason}`;
  }
}

class CommandError extends Schema.TaggedErrorClass<CommandError>()("CommandError", {
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
  if (xdgConfigHome === undefined || (yield* fs.exists(`${xdgConfigHome}/.npmrc`))) {
    return undefined;
  }
  const env: Record<string, string> = {};
  for (const [key, value] of Object.entries(globalThis.process.env)) {
    if (key !== "XDG_CONFIG_HOME" && value !== undefined) env[key] = value;
  }
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
  readonly otp: string | undefined;
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
  const mutable = parsed as { exports?: Record<string, unknown> };
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
  // real publishes require an authenticated npm session (`bunx npm login`).
  const distTag = distTagFor(manifest.version);
  const args = options.dryRun
    ? ["pm", "pack", "--dry-run"]
    : [
        "publish",
        "--access",
        "public",
        "--tag",
        distTag,
        ...(options.otp !== undefined ? ["--otp", options.otp] : []),
      ];

  // The manifest swap is scoped: acquireRelease restores the original bytes
  // even when the publish fails or the fiber is interrupted.
  yield* Effect.acquireRelease(
    fs.writeFileString(manifestPath, JSON.stringify(parsed, null, 2)),
    () => fs.writeFileString(manifestPath, originalBytes).pipe(Effect.orDie),
  );
  yield* runCommand(options.directory, "bun", args, yield* publishEnvironment());
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

const command = CliCommand.make(
  "release-publish",
  { dryRun: dryRunFlag, otp: otpFlag },
  ({ dryRun, otp }) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directories = (yield* fs.readDirectory("packages"))
        .filter((entry) => !entry.startsWith("."))
        .sort()
        .map((entry) => `packages/${entry}`);
      yield* Console.log(
        `Publishing ${directories.length} workspaces to ${REGISTRY}${dryRun ? " (dry run)" : ""}...`,
      );
      let published = 0;
      for (const directory of directories) {
        const outcome = yield* Effect.scoped(
          publishOne({ directory, dryRun, otp: otp._tag === "Some" ? otp.value : undefined }),
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
