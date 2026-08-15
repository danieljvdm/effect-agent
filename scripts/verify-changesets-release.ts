import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Console, Effect, FileSystem, Path, Schema, Stream } from "effect";
import { Command as CliCommand, Flag } from "effect/unstable/cli";
import { ChildProcess } from "effect/unstable/process";

const CommitSha = Schema.String.pipe(
  Schema.refine((value): value is string => /^[0-9a-f]{40}$/.test(value), {
    expected: "a full lowercase 40-character Git commit SHA",
  }),
);

class InvalidCommitSha extends Schema.TaggedError<InvalidCommitSha>()("InvalidCommitSha", {
  label: Schema.String,
  value: Schema.String,
}) {
  override get message() {
    return `${this.label} must be a full lowercase 40-character Git commit SHA.`;
  }
}

class VerificationCommandError extends Schema.TaggedError<VerificationCommandError>()(
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

class ReleaseTreeMismatch extends Schema.TaggedError<ReleaseTreeMismatch>()("ReleaseTreeMismatch", {
  expectedTree: Schema.String,
  actualTree: Schema.String,
  changedPaths: Schema.String,
}) {
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

const repositoryRoot = Effect.fn("verifyChangesetsRelease.repositoryRoot")(function* () {
  const path = yield* Path.Path;
  const scriptPath = yield* path.fromFileUrl(new URL(import.meta.url));
  return path.resolve(path.dirname(scriptPath), "..");
});

const acquireExpectedWorktree = Effect.fn("verifyChangesetsRelease.acquireExpectedWorktree")(
  function* (root: string, baseSha: string) {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    return yield* Effect.acquireRelease(
      Effect.gen(function* () {
        const temporaryRoot = yield* fs.makeTempDirectory({
          prefix: "effect-agent-changesets-release-",
        });
        const worktree = path.join(temporaryRoot, "expected");
        yield* runCommand(root, "git", ["worktree", "add", "--detach", worktree, baseSha]).pipe(
          Effect.tapError(() =>
            fs.remove(temporaryRoot, { force: true, recursive: true }).pipe(Effect.ignore),
          ),
        );
        return { temporaryRoot, worktree } as const;
      }),
      ({ temporaryRoot, worktree }) =>
        Effect.gen(function* () {
          yield* Effect.scoped(
            runCommand(root, "git", ["worktree", "remove", "--force", worktree]),
          ).pipe(
            Effect.catch((error) =>
              Console.error(`Failed to remove temporary verification worktree: ${String(error)}`),
            ),
          );
          yield* fs
            .remove(temporaryRoot, { force: true, recursive: true })
            .pipe(
              Effect.catch((error) =>
                Console.error(
                  `Failed to remove temporary verification directory: ${String(error)}`,
                ),
              ),
            );
          yield* Effect.scoped(runCommand(root, "git", ["worktree", "prune"])).pipe(
            Effect.catch((error) =>
              Console.error(`Failed to prune temporary Git worktree metadata: ${String(error)}`),
            ),
          );
        }),
    );
  },
);

export const verifyChangesetsRelease = Effect.fn("verifyChangesetsRelease")(function* (options: {
  readonly baseSha: string;
  readonly headSha: string;
}) {
  const baseSha = yield* decodeCommitSha("--base-sha", options.baseSha);
  const headSha = yield* decodeCommitSha("--head-sha", options.headSha);
  const path = yield* Path.Path;
  const root = yield* repositoryRoot();

  yield* runCommand(root, "git", ["cat-file", "-e", `${baseSha}^{commit}`]);
  yield* runCommand(root, "git", ["cat-file", "-e", `${headSha}^{commit}`]);
  const actualTree = yield* runCommand(root, "git", ["rev-parse", `${headSha}^{tree}`]);

  const { worktree: expectedWorktree } = yield* acquireExpectedWorktree(root, baseSha);

  const changesetBinary = path.join(root, "node_modules", ".bin", "changeset");
  yield* runCommand(expectedWorktree, changesetBinary, ["version"]);
  yield* runCommand(expectedWorktree, "bun", ["install", "--ignore-scripts"]);
  yield* runCommand(expectedWorktree, "git", ["add", "--all"]);
  const expectedTree = yield* runCommand(expectedWorktree, "git", ["write-tree"]);

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

NodeRuntime.runMain(program, { disableErrorReporting: true });
