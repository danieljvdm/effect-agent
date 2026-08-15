import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Cause, Console, Effect, Exit, FileSystem, Path, Schema, Stream } from "effect";
import { Command as CliCommand, Flag } from "effect/unstable/cli";
import { ChildProcess } from "effect/unstable/process";

const CommitSha = Schema.String.pipe(
  Schema.refine((value): value is string => /^[0-9a-f]{40}$/.test(value), {
    expected: "a full lowercase 40-character Git commit SHA",
  }),
);

export class InvalidCommitSha extends Schema.TaggedError<InvalidCommitSha>()("InvalidCommitSha", {
  label: Schema.String,
  value: Schema.String,
}) {
  override get message() {
    return `${this.label} must be a full lowercase 40-character Git commit SHA.`;
  }
}

export class VerificationCommandError extends Schema.TaggedError<VerificationCommandError>()(
  "VerificationCommandError",
  {
    command: Schema.String,
    exitCode: Schema.Int,
    output: Schema.String,
  },
) {
  override get message() {
    return this.output.length > 0
      ? `${this.command} exited with code ${this.exitCode}:\n${this.output}`
      : `${this.command} exited with code ${this.exitCode}`;
  }
}

export class ReleaseTreeMismatch extends Schema.TaggedError<ReleaseTreeMismatch>()(
  "ReleaseTreeMismatch",
  {
    expectedTree: Schema.String,
    actualTree: Schema.String,
    changedPaths: Schema.String,
  },
) {
  override get message() {
    return [
      "The Changesets PR tree does not match a clean regeneration from its trusted base.",
      `Regenerated tree: ${this.expectedTree}`,
      `PR head tree:    ${this.actualTree}`,
      this.changedPaths.length > 0 ? `Unexpected differences:\n${this.changedPaths}` : "",
    ]
      .filter((line) => line.length > 0)
      .join("\n");
  }
}

export class VerificationCleanupError extends Schema.TaggedError<VerificationCleanupError>()(
  "VerificationCleanupError",
  {
    cause: Schema.optionalKey(Schema.Defect()),
    message: Schema.String,
    operation: Schema.String,
  },
) {}

const decodeCommitSha = Effect.fn("verifyChangesetsRelease.decodeCommitSha")(function* (
  label: string,
  value: string,
) {
  return yield* Schema.decodeUnknownEffect(CommitSha)(value).pipe(
    Effect.mapError(() => InvalidCommitSha.make({ label, value })),
  );
});

const runCommand = Effect.fn("verifyChangesetsRelease.runCommand")(function* (
  cwd: string,
  command: string,
  args: ReadonlyArray<string>,
) {
  const rendered = [command, ...args].join(" ");
  const child = yield* ChildProcess.make(command, args, {
    cwd,
    stderr: "pipe",
    stdout: "pipe",
  });
  const [output, exitCode] = yield* Effect.all([
    Stream.mkString(Stream.decodeText(child.all)),
    child.exitCode,
  ]);
  const trimmed = output.trim();
  if (exitCode !== 0) {
    return yield* VerificationCommandError.make({
      command: rendered,
      exitCode,
      output: trimmed,
    });
  }
  return trimmed;
});

const findRepositoryRoot = Effect.fn("verifyChangesetsRelease.findRepositoryRoot")(function* () {
  const path = yield* Path.Path;
  const scriptPath = yield* path.fromFileUrl(new URL(import.meta.url));
  return path.resolve(path.dirname(scriptPath), "..");
});

const cleanupError = (operation: string) => (cause: unknown) =>
  VerificationCleanupError.make({
    cause,
    message: `Verification cleanup failed during ${operation}: ${String(cause)}`,
    operation,
  });

const cleanupExpectedWorktree = Effect.fn("verifyChangesetsRelease.cleanupExpectedWorktree")(
  function* (root: string, temporaryRoot: string, worktree: string) {
    const fs = yield* FileSystem.FileSystem;
    const worktreeExistsExit = yield* fs
      .exists(worktree)
      .pipe(Effect.mapError(cleanupError("worktree existence check")), Effect.exit);
    const cleanupEffects = [
      ...(Exit.isSuccess(worktreeExistsExit) && worktreeExistsExit.value
        ? [
            Effect.scoped(
              runCommand(root, "git", ["worktree", "remove", "--force", worktree]),
            ).pipe(Effect.asVoid, Effect.mapError(cleanupError("git worktree remove"))),
          ]
        : []),
      fs
        .remove(temporaryRoot, { force: true, recursive: true })
        .pipe(Effect.mapError(cleanupError("temporary directory removal"))),
      Effect.scoped(runCommand(root, "git", ["worktree", "prune"])).pipe(
        Effect.asVoid,
        Effect.mapError(cleanupError("git worktree prune")),
      ),
    ];
    const exits = yield* Effect.forEach(cleanupEffects, (cleanup) => cleanup.pipe(Effect.exit), {
      concurrency: 1,
    });
    let combined: Cause.Cause<VerificationCleanupError> | undefined = Exit.isFailure(
      worktreeExistsExit,
    )
      ? worktreeExistsExit.cause
      : undefined;
    for (const exit of exits) {
      if (Exit.isSuccess(exit)) continue;
      combined = combined === undefined ? exit.cause : Cause.combine(combined, exit.cause);
    }
    if (combined !== undefined) return yield* Effect.failCause(combined);
  },
);

const withExpectedWorktree = <A, E, R>(
  root: string,
  baseSha: string,
  use: (worktree: string) => Effect.Effect<A, E, R>,
) =>
  Effect.uninterruptibleMask((restore) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const temporaryRoot = yield* fs.makeTempDirectory({
        prefix: "effect-agent-changesets-release-",
      });
      const worktree = path.join(temporaryRoot, "expected");
      const acquisitionExit = yield* restore(
        runCommand(root, "git", ["worktree", "add", "--detach", worktree, baseSha]),
      ).pipe(Effect.exit);
      if (Exit.isFailure(acquisitionExit)) {
        const cleanupExit = yield* cleanupExpectedWorktree(root, temporaryRoot, worktree).pipe(
          Effect.exit,
        );
        return yield* Effect.failCause(
          Exit.isFailure(cleanupExit)
            ? Cause.combine(acquisitionExit.cause, cleanupExit.cause)
            : acquisitionExit.cause,
        );
      }

      const useExit = yield* restore(use(worktree)).pipe(Effect.exit);
      const cleanupExit = yield* cleanupExpectedWorktree(root, temporaryRoot, worktree).pipe(
        Effect.exit,
      );
      if (Exit.isFailure(useExit)) {
        return yield* Effect.failCause(
          Exit.isFailure(cleanupExit)
            ? Cause.combine(useExit.cause, cleanupExit.cause)
            : useExit.cause,
        );
      }
      if (Exit.isFailure(cleanupExit)) return yield* Effect.failCause(cleanupExit.cause);
      return useExit.value;
    }),
  );

export const verifyChangesetsRelease = Effect.fn("verifyChangesetsRelease")(function* (options: {
  readonly baseSha: string;
  readonly headSha: string;
  readonly repositoryRoot?: string;
  readonly changesetBinary?: string;
  readonly bunBinary?: string;
}) {
  const baseSha = yield* decodeCommitSha("--base-sha", options.baseSha);
  const headSha = yield* decodeCommitSha("--head-sha", options.headSha);
  const path = yield* Path.Path;
  const root = options.repositoryRoot ?? (yield* findRepositoryRoot());

  yield* runCommand(root, "git", ["cat-file", "-e", `${baseSha}^{commit}`]);
  yield* runCommand(root, "git", ["cat-file", "-e", `${headSha}^{commit}`]);
  const actualTree = yield* runCommand(root, "git", ["rev-parse", `${headSha}^{tree}`]);

  const expectedTree = yield* withExpectedWorktree(root, baseSha, (expectedWorktree) =>
    Effect.gen(function* () {
      const bunBinary = options.bunBinary ?? "bun";
      if (options.changesetBinary === undefined) {
        yield* runCommand(expectedWorktree, bunBinary, [
          "install",
          "--frozen-lockfile",
          "--ignore-scripts",
        ]);
      }
      const changesetBinary =
        options.changesetBinary ?? path.join(expectedWorktree, "node_modules", ".bin", "changeset");
      yield* runCommand(expectedWorktree, changesetBinary, ["version"]);
      yield* runCommand(expectedWorktree, bunBinary, ["install", "--ignore-scripts"]);
      yield* runCommand(expectedWorktree, "git", ["add", "--all"]);
      return yield* runCommand(expectedWorktree, "git", ["write-tree"]);
    }),
  );

  if (expectedTree !== actualTree) {
    const changedPaths = yield* runCommand(root, "git", [
      "diff",
      "--name-status",
      expectedTree,
      actualTree,
    ]);
    return yield* ReleaseTreeMismatch.make({ expectedTree, actualTree, changedPaths });
  }

  yield* Console.log(
    `Verified Changesets release tree ${actualTree} from base ${baseSha} at head ${headSha}.`,
  );
});

const baseSha = Flag.string("base-sha").pipe(
  Flag.withDescription("Trusted pull-request base commit SHA."),
);
const headSha = Flag.string("head-sha").pipe(
  Flag.withDescription("Generated Changesets pull-request head commit SHA."),
);

const command = CliCommand.make(
  "verify-changesets-release",
  { baseSha, headSha },
  Effect.fn("verifyChangesetsRelease.command")(function* ({ baseSha, headSha }) {
    yield* verifyChangesetsRelease({ baseSha, headSha });
  }),
).pipe(
  CliCommand.withDescription(
    "Regenerate Changesets output from a trusted base and require the PR head tree to match exactly.",
  ),
);

const program = CliCommand.run(command, { version: "1.0.0" }).pipe(
  Effect.tapError((error) => Console.error(String(error))),
  Effect.scoped,
  Effect.provide(NodeServices.layer),
);

if (process.argv[1] !== undefined && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  NodeRuntime.runMain(program, { disableErrorReporting: true });
}
