import { NodeRuntime, NodeServices } from "@effect/platform-node";
import {
  Config,
  Console,
  Effect,
  FileSystem,
  Path,
  type PlatformError,
  Schema,
  Stream,
} from "effect";
import { Command as CliCommand, Flag } from "effect/unstable/cli";
import { ChildProcess } from "effect/unstable/process";

// ---------------------------------------------------------------------------
// Keep the `repos/effect` Git submodule checked out at the exact tag of the
// installed `effect` npm package, so agents can read past the installed
// declaration files. This replaces the `dev-kit effect sync` lifecycle task
// after ejecting Dev Kit's v1 managed model.
//
// `repos/effect` is a real Git submodule (see `.gitmodules`): a fresh linked
// worktree starts with it as an empty tracked directory, so this script
// first ensures it is initialized (`git submodule update --init`, which
// checks out the tree's currently *committed* gitlink) and only then moves
// the working checkout to the installed npm version's tag if that differs.
// That move intentionally leaves the submodule "dirty" relative to the
// committed gitlink until a maintainer deliberately bumps and commits it
// (see docs/TOOLCHAIN.md's "To upgrade Effect" steps).
// ---------------------------------------------------------------------------

const REPOSITORY = "https://github.com/Effect-TS/effect.git";
const CHECKOUT_PATH = "repos/effect";

class EffectSourceCheckoutError extends Schema.TaggedError<EffectSourceCheckoutError>()(
  "EffectSourceCheckoutError",
  { message: Schema.String },
) {}

class EffectSourceCommandError extends Schema.TaggedError<EffectSourceCommandError>()(
  "EffectSourceCommandError",
  { command: Schema.String, exitCode: Schema.Int, output: Schema.String },
) {
  override get message() {
    return this.output.length > 0
      ? `${this.command} exited with code ${this.exitCode}: ${this.output}`
      : `${this.command} exited with code ${this.exitCode}`;
  }
}

const PackageVersionSchema = Schema.fromJsonString(Schema.Struct({ version: Schema.String }));

const runGit = Effect.fn("runGit")(function* (cwd: string, args: ReadonlyArray<string>) {
  const child = yield* ChildProcess.make("git", args, { cwd, stderr: "pipe", stdout: "pipe" });
  const [output, exitCode] = yield* Effect.all([
    Stream.mkString(Stream.decodeText(child.all)),
    child.exitCode,
  ]);
  const trimmed = output.trim();

  if (exitCode !== 0) {
    return yield* EffectSourceCommandError.make({
      command: ["git", ...args].join(" "),
      exitCode,
      output: trimmed,
    });
  }

  return trimmed;
});

const readEffectVersion = Effect.fn("readEffectVersion")(function* (repositoryRoot: string) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const manifestPath = path.join(repositoryRoot, "node_modules", "effect", "package.json");
  const contents = yield* fs.readFileString(manifestPath).pipe(
    Effect.mapError(() =>
      EffectSourceCheckoutError.make({
        message: "effect must be installed (run `bun install`) before syncing its source checkout",
      }),
    ),
  );

  return yield* Schema.decodeEffect(PackageVersionSchema)(contents).pipe(
    Effect.mapError(() =>
      EffectSourceCheckoutError.make({ message: `could not read a version from ${manifestPath}` }),
    ),
    Effect.map((manifest) => manifest.version),
  );
});

/** Effect's Node FileSystem maps readlink(2) EINVAL (not a symlink) to Unknown. */
const isNotSymbolicLinkError = (error: PlatformError.PlatformError): boolean => {
  if (error.reason._tag !== "Unknown") return false;
  const cause = error.reason.cause;

  return cause instanceof Error && "code" in cause && cause.code === "EINVAL";
};

const isSymbolicLink = Effect.fn("isSymbolicLink")(function* (absolutePath: string) {
  const fs = yield* FileSystem.FileSystem;

  return yield* fs.readLink(absolutePath).pipe(
    Effect.map(() => true),
    Effect.catch((error) => {
      if (error.reason._tag === "NotFound") return Effect.succeed(false);
      if (isNotSymbolicLinkError(error)) return Effect.succeed(false);

      return Effect.fail(error);
    }),
  );
});

/** A submodule starts as an empty tracked directory until `git submodule update --init` runs. */
const isInitialized = Effect.fn("isEffectSourceInitialized")(function* (checkoutDir: string) {
  const fs = yield* FileSystem.FileSystem;

  return yield* fs
    .exists(checkoutDir)
    .pipe(
      Effect.flatMap((exists) =>
        exists ? fs.exists(`${checkoutDir}/.git`) : Effect.succeed(false),
      ),
    );
});

const initializeSubmodule = Effect.fn("initializeEffectSourceSubmodule")(function* (
  repositoryRoot: string,
) {
  yield* runGit(repositoryRoot, ["submodule", "update", "--init", "--", CHECKOUT_PATH]);
});

const verifyCheckout = Effect.fn("verifyEffectSourceCheckout")(function* (checkoutDir: string) {
  const fs = yield* FileSystem.FileSystem;

  if (yield* isSymbolicLink(checkoutDir)) {
    return yield* EffectSourceCheckoutError.make({
      message: `Effect source destination is a symlink: ${checkoutDir}`,
    });
  }
  const actualRoot = yield* runGit(checkoutDir, ["rev-parse", "--show-toplevel"]).pipe(
    Effect.mapError(() =>
      EffectSourceCheckoutError.make({
        message: `Effect source destination exists but is not a Git checkout: ${checkoutDir}`,
      }),
    ),
  );
  const expectedRoot = yield* fs.realPath(checkoutDir);

  if ((yield* fs.realPath(actualRoot)) !== expectedRoot) {
    return yield* EffectSourceCheckoutError.make({
      message: `Effect source destination is nested inside another Git checkout: ${checkoutDir}`,
    });
  }
  const remote = yield* runGit(checkoutDir, ["remote", "get-url", "origin"]);

  if (remote !== REPOSITORY) {
    return yield* EffectSourceCheckoutError.make({
      message: `Effect source origin is ${remote}; expected ${REPOSITORY}`,
    });
  }
});

/** "move" when the checkout needs fetching and moving to `tag`; "unchanged" otherwise. */
const inspectTag = Effect.fn("inspectEffectSourceTag")(function* (
  checkoutDir: string,
  tag: string,
) {
  const target = yield* runGit(checkoutDir, [
    "rev-parse",
    "-q",
    "--verify",
    `${tag}^{commit}`,
  ]).pipe(Effect.catchTag("EffectSourceCommandError", () => Effect.succeed(undefined)));

  if (target !== undefined) {
    const current = yield* runGit(checkoutDir, ["rev-parse", "HEAD"]);

    if (current === target) return "unchanged" as const;
  }
  const dirty = yield* runGit(checkoutDir, ["status", "--porcelain", "--untracked-files=all"]);

  if (dirty.length > 0) {
    return yield* EffectSourceCheckoutError.make({
      message: `Effect source checkout has local changes; refusing to move ${checkoutDir} to ${tag}`,
    });
  }

  return "move" as const;
});

const moveCheckout = Effect.fn("moveEffectSourceCheckout")(function* (
  checkoutDir: string,
  tag: string,
) {
  yield* runGit(checkoutDir, [
    "fetch",
    "--depth",
    "1",
    "--force",
    "--quiet",
    "origin",
    `refs/tags/${tag}:refs/tags/${tag}`,
  ]);
  const target = yield* runGit(checkoutDir, ["rev-parse", "-q", "--verify", `${tag}^{commit}`]);

  yield* runGit(checkoutDir, ["checkout", "--detach", "--quiet", target]);
});

const dryRunFlag = Flag.boolean("dry-run").pipe(
  Flag.withDefault(false),
  Flag.withDescription("Report what would change without touching the checkout."),
);

const command = CliCommand.make("sync-effect-source", { dryRun: dryRunFlag }, ({ dryRun }) =>
  Effect.gen(function* () {
    const path = yield* Path.Path;
    const scriptPath = yield* path.fromFileUrl(new URL(import.meta.url));
    const repositoryRoot = path.resolve(path.dirname(scriptPath), "..");
    const checkoutDir = path.join(repositoryRoot, CHECKOUT_PATH);
    const ci = yield* Config.string("CI").pipe(Config.withDefault(""));

    if (ci === "true" || ci === "1") {
      yield* Console.log("Effect source sync skipped (CI).");
      return;
    }

    const version = yield* readEffectVersion(repositoryRoot);
    const tag = `effect@${version}`;

    if (!(yield* isInitialized(checkoutDir))) {
      if (dryRun) {
        yield* Console.log(`Would initialize the repos/effect submodule, then move it to ${tag}.`);
        return;
      }
      yield* initializeSubmodule(repositoryRoot);
    }
    yield* verifyCheckout(checkoutDir);
    const action = yield* inspectTag(checkoutDir, tag);

    if (action === "unchanged") {
      yield* Console.log(`Effect source up to date at ${tag}.`);
      return;
    }
    if (dryRun) {
      yield* Console.log(`Would move Effect source to ${tag} -> ${CHECKOUT_PATH}.`);
      return;
    }
    yield* moveCheckout(checkoutDir, tag);
    yield* Console.log(`Synced Effect source to ${tag} -> ${CHECKOUT_PATH}.`);
  }),
).pipe(
  CliCommand.withDescription(
    "Initialize the repos/effect submodule and move it to the installed effect version's tag.",
  ),
);

const program = CliCommand.run(command, { version: "1.0.0" }).pipe(
  Effect.scoped,
  Effect.provide(NodeServices.layer),
);

NodeRuntime.runMain(program, { disableErrorReporting: true });
