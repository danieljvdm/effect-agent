import {
  Context,
  Crypto,
  Effect,
  Encoding,
  FileSystem,
  Layer,
  Path,
  Ref,
  Schema,
  Stream,
} from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import {
  GitCommitSha,
  normalizeWorkspacePath,
  PatchDigest,
  PatchSnapshot,
  PublishedWorkOrder,
  type PullRequestWorkOrder,
  type WorkOrderCheckResult,
  StalePullRequestHead,
  WorkOrderRejected,
  WorkspaceOperationFailure,
  WorkspaceViolation,
} from "./contracts.ts";
import {
  type AcquiredWorktree,
  type HostCheck,
  type ImplementationWorkspace,
  WorkOrderHost,
  WorkspaceSearchHit,
} from "./workspace.ts";

const MAX_FILE_CHARS = 200_000;
const MAX_PATCH_CHARS = 1_000_000;
const MAX_GIT_OUTPUT_BYTES = 4_000_000;
const MAX_SEARCH_RESULTS = 100;

export interface LocalGitWorkOrderConfig {
  readonly repositoryPath: string;
  readonly repository: string;
  readonly pullRequestNumber: number;
  readonly headRef: string;
  readonly authorizedActorIds: ReadonlyArray<string>;
  readonly checks: ReadonlyArray<HostCheck>;
  readonly requiredChecks: ReadonlyArray<string>;
  readonly allowedSupportPaths?: ReadonlyArray<string> | undefined;
  readonly commitMessage?: string | undefined;
  readonly gitExecutable?: string | undefined;
  readonly onWorktreeAcquired?: ((path: string) => Effect.Effect<void>) | undefined;
  readonly onBeforeWorktreeRelease?: ((path: string) => Effect.Effect<void>) | undefined;
  readonly onWorktreeReleased?: ((path: string) => Effect.Effect<void>) | undefined;
}

class LocalGit extends Context.Service<
  LocalGit,
  {
    readonly run: (
      cwd: string,
      args: ReadonlyArray<string>,
      operation: string,
    ) => Effect.Effect<string, WorkspaceOperationFailure>;
  }
>()("@effect-agent/example-pr-work-orders/LocalGit") {}

const localGitLayer = (gitExecutable: string) =>
  Layer.effect(
    LocalGit,
    Effect.gen(function* () {
      const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
      const run = Effect.fn("LocalGit.run")(function* (
        cwd: string,
        args: ReadonlyArray<string>,
        operation: string,
      ) {
        const command = ChildProcess.make(
          gitExecutable,
          ["-c", "core.hooksPath=/dev/null", ...args],
          {
            cwd,
            env: {
              GIT_CONFIG_GLOBAL: "/dev/null",
              GIT_CONFIG_NOSYSTEM: "1",
              GIT_TERMINAL_PROMPT: "0",
              PATH: "/usr/bin:/bin",
            },
            extendEnv: false,
            stderr: "pipe",
            stdout: "pipe",
          },
        );
        return yield* Effect.scoped(
          Effect.gen(function* () {
            const handle = yield* spawner.spawn(command);
            const [collected, exitCode] = yield* Effect.all([
              Stream.runFoldEffect(
                handle.all,
                () => ({ size: 0, chunks: [] as Array<Uint8Array> }),
                (state, chunk) => {
                  const size = state.size + chunk.length;
                  if (size > MAX_GIT_OUTPUT_BYTES) {
                    return WorkspaceOperationFailure.make({
                      operation,
                      reason: `git output exceeds the ${MAX_GIT_OUTPUT_BYTES}-byte bound`,
                    });
                  }
                  return Effect.succeed({ size, chunks: [...state.chunks, chunk] });
                },
              ),
              handle.exitCode,
            ]);
            const bytes = new Uint8Array(collected.size);
            let offset = 0;
            for (const chunk of collected.chunks) {
              bytes.set(chunk, offset);
              offset += chunk.length;
            }
            const output = yield* Effect.try({
              try: () => new TextDecoder("utf-8", { fatal: true }).decode(bytes),
              catch: () =>
                WorkspaceOperationFailure.make({
                  operation,
                  reason: "git returned output that is not valid UTF-8 text",
                }),
            });
            if (Number(exitCode) !== 0) {
              return yield* WorkspaceOperationFailure.make({
                operation,
                reason: output.slice(0, 4_096) || `git exited with status ${String(exitCode)}`,
              });
            }
            return output;
          }),
        ).pipe(
          Effect.mapError((error) =>
            error._tag === "WorkspaceOperationFailure"
              ? error
              : WorkspaceOperationFailure.make({
                  operation,
                  reason: String(error).slice(0, 4_096),
                }),
          ),
        );
      });
      return LocalGit.of({ run });
    }),
  );

const sha256 = (
  crypto: Crypto.Crypto,
  text: string,
): Effect.Effect<PatchDigest, WorkspaceOperationFailure> =>
  crypto.digest("SHA-256", new TextEncoder().encode(text)).pipe(
    Effect.map(Encoding.encodeHex),
    Effect.flatMap(Schema.decodeUnknownEffect(PatchDigest)),
    Effect.mapError((cause) =>
      WorkspaceOperationFailure.make({
        operation: "digest work-order patch",
        reason: String(cause).slice(0, 4_096),
      }),
    ),
  );

const parseNulFields = (output: string): ReadonlyArray<string> | undefined => {
  if (output.length === 0) return [];
  if (!output.endsWith("\0")) return undefined;
  const fields = output.split("\0");
  fields.pop();
  return fields;
};

const decodeSha = (
  operation: string,
  output: string,
): Effect.Effect<GitCommitSha, WorkspaceOperationFailure> =>
  Schema.decodeUnknownEffect(GitCommitSha)(output.trim()).pipe(
    Effect.mapError(() =>
      WorkspaceOperationFailure.make({
        operation,
        reason: "git returned an invalid commit SHA",
      }),
    ),
  );

const makeWorkspace = Effect.fn("LocalGitWorkOrderHost.makeWorkspace")(function* (input: {
  readonly root: string;
  readonly allowedPaths: ReadonlySet<string>;
  readonly checks: ReadonlyMap<string, HostCheck>;
  readonly fs: FileSystem.FileSystem;
  readonly pathService: Path.Path;
  readonly git: LocalGit["Service"];
  readonly crypto: Crypto.Crypto;
}) {
  const { crypto, fs, git, pathService } = input;
  const observedChecks = yield* Ref.make<ReadonlyArray<WorkOrderCheckResult>>([]);
  const realRoot = yield* fs
    .realPath(input.root)
    .pipe(
      Effect.mapError((error) =>
        WorkspaceOperationFailure.make({ operation: "resolve worktree", reason: String(error) }),
      ),
    );
  const resolve = Effect.fn("ImplementationWorkspace.resolve")(function* (requested: string) {
    const relative = yield* normalizeWorkspacePath(requested);
    if (!input.allowedPaths.has(relative)) {
      return yield* WorkspaceViolation.make({
        path: requested,
        reason: "path is outside the host-configured work-order allowlist",
      });
    }
    const full = pathService.resolve(realRoot, relative);
    const canonical = yield* fs.realPath(full).pipe(
      Effect.mapError((error) =>
        WorkspaceOperationFailure.make({
          operation: `resolve ${relative}`,
          reason: String(error).slice(0, 4_096),
        }),
      ),
    );
    if (canonical !== full || !canonical.startsWith(`${realRoot}${pathService.sep}`)) {
      return yield* WorkspaceViolation.make({
        path: requested,
        reason: "symbolic links and paths outside the acquired worktree are forbidden",
      });
    }
    return { relative, full };
  });
  const readFile = Effect.fn("ImplementationWorkspace.readFile")(function* (requested: string) {
    const target = yield* resolve(requested);
    const contents = yield* fs.readFileString(target.full).pipe(
      Effect.mapError((error) =>
        WorkspaceOperationFailure.make({
          operation: `read ${target.relative}`,
          reason: String(error).slice(0, 4_096),
        }),
      ),
    );
    if (contents.length > MAX_FILE_CHARS) {
      return yield* WorkspaceOperationFailure.make({
        operation: `read ${target.relative}`,
        reason: `file exceeds the ${MAX_FILE_CHARS}-character bound`,
      });
    }
    return contents;
  });
  const search = Effect.fn("ImplementationWorkspace.search")(function* (query: string) {
    if (query.length === 0 || query.length > 500) {
      return yield* WorkspaceViolation.make({
        path: "<search>",
        reason: "search query length is out of bounds",
      });
    }
    const hits: Array<WorkspaceSearchHit> = [];
    for (const allowed of [...input.allowedPaths].sort()) {
      const contents = yield* readFile(allowed);
      for (const [index, line] of contents.split("\n").entries()) {
        if (line.includes(query) && hits.length < MAX_SEARCH_RESULTS) {
          hits.push(
            WorkspaceSearchHit.make({ path: allowed, line: index + 1, text: line.slice(0, 1_000) }),
          );
        }
      }
    }
    return hits;
  });
  const applyEdit = Effect.fn("ImplementationWorkspace.applyEdit")(function* (edit: {
    readonly path: string;
    readonly expected: string;
    readonly replacement: string;
  }) {
    if (
      edit.expected.length === 0 ||
      edit.expected.length > 100_000 ||
      edit.replacement.length > 100_000
    ) {
      return yield* WorkspaceViolation.make({
        path: edit.path,
        reason: "edit input is empty or exceeds the 100,000-character bound",
      });
    }
    const target = yield* resolve(edit.path);
    const contents = yield* readFile(target.relative);
    const first = contents.indexOf(edit.expected);
    if (first < 0 || contents.indexOf(edit.expected, first + edit.expected.length) >= 0) {
      return yield* WorkspaceOperationFailure.make({
        operation: `edit ${target.relative}`,
        reason: "expected text must occur exactly once",
      });
    }
    const updated = `${contents.slice(0, first)}${edit.replacement}${contents.slice(first + edit.expected.length)}`;
    if (updated.length > MAX_FILE_CHARS) {
      return yield* WorkspaceOperationFailure.make({
        operation: `edit ${target.relative}`,
        reason: `edited file exceeds the ${MAX_FILE_CHARS}-character bound`,
      });
    }
    yield* fs.writeFileString(target.full, updated).pipe(
      Effect.mapError((error) =>
        WorkspaceOperationFailure.make({
          operation: `edit ${target.relative}`,
          reason: String(error).slice(0, 4_096),
        }),
      ),
    );
  });
  const collectPatch = Effect.gen(function* () {
    const changed = yield* git.run(
      realRoot,
      ["diff", "--name-only", "-z", "--no-ext-diff", "HEAD", "--"],
      "collect changed paths",
    );
    const fields = parseNulFields(changed);
    if (fields === undefined) {
      return yield* WorkspaceOperationFailure.make({
        operation: "collect changed paths",
        reason: "git returned a path list without NUL termination",
      });
    }
    const changedPaths = yield* Effect.forEach([...fields].sort(), normalizeWorkspacePath).pipe(
      Effect.mapError((error) =>
        WorkspaceOperationFailure.make({
          operation: "collect changed paths",
          reason: error.reason,
        }),
      ),
    );
    const patch = yield* git.run(
      realRoot,
      ["diff", "--binary", "--full-index", "--no-ext-diff", "HEAD", "--"],
      "collect patch",
    );
    if (patch.length > MAX_PATCH_CHARS) {
      return yield* WorkspaceOperationFailure.make({
        operation: "collect patch",
        reason: `patch exceeds the ${MAX_PATCH_CHARS}-character host bound`,
      });
    }
    const snapshot = PatchSnapshot.make({
      digest: yield* sha256(crypto, patch),
      changedPaths,
      preview: patch.slice(0, 20_000),
      truncated: patch.length > 20_000,
    });
    return { snapshot, patch } as const;
  });
  const inspectPatch = collectPatch.pipe(Effect.map(({ snapshot }) => snapshot));
  const requestCheck = Effect.fn("ImplementationWorkspace.requestCheck")(function* (name: string) {
    const check = input.checks.get(name);
    if (check === undefined) {
      return yield* WorkspaceOperationFailure.make({
        operation: "request check",
        reason: `unknown host-configured check '${name}'`,
      });
    }
    const result = yield* check.run(realRoot);
    yield* Ref.update(observedChecks, (previous) => [...previous, result]);
    return result;
  });
  const modelWorkspace: ImplementationWorkspace = {
    readFile,
    search,
    applyEdit,
    inspectPatch,
    requestCheck,
  };
  return {
    modelWorkspace,
    inspectPatch,
    collectPatch,
    requestCheck,
    observedChecks: Ref.get(observedChecks),
    git,
    realRoot,
  };
});

export const localGitWorkOrderHostLayer = (
  config: LocalGitWorkOrderConfig,
): Layer.Layer<
  WorkOrderHost,
  never,
  Crypto.Crypto | FileSystem.FileSystem | Path.Path | ChildProcessSpawner.ChildProcessSpawner
> =>
  Layer.effect(
    WorkOrderHost,
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const pathService = yield* Path.Path;
      const crypto = yield* Crypto.Crypto;
      const git = yield* LocalGit;
      const checks = new Map(config.checks.map((check) => [check.name, check] as const));
      const configuredSupportPaths = Effect.forEach(config.allowedSupportPaths ?? [], (path) =>
        normalizeWorkspacePath(path).pipe(
          Effect.mapError((violation) =>
            WorkOrderRejected.make({
              reason: `support path rejected: ${violation.reason}`,
            }),
          ),
        ),
      );
      const currentHead = git
        .run(config.repositoryPath, ["rev-parse", config.headRef], "resolve pull-request head")
        .pipe(Effect.flatMap((output) => decodeSha("resolve pull-request head", output)));
      const authorizeDispatch = Effect.fn("WorkOrderHost.authorizeDispatch")(function* (
        order: PullRequestWorkOrder,
      ) {
        if (
          order.repository !== config.repository ||
          order.pullRequestNumber !== config.pullRequestNumber
        ) {
          return yield* WorkOrderRejected.make({
            reason: "work order is not an authorized same-repository request",
          });
        }
        if (!config.authorizedActorIds.includes(order.dispatch.actorId)) {
          return yield* WorkOrderRejected.make({
            reason: "dispatch actor is not authorized",
          });
        }
        if (order.source.commitSha !== order.headSha) {
          return yield* WorkOrderRejected.make({
            reason: "source comment is not anchored to the current pull-request head",
          });
        }
        yield* normalizeWorkspacePath(order.source.path).pipe(
          Effect.mapError((violation) =>
            WorkOrderRejected.make({
              reason: `source path rejected: ${violation.reason}`,
            }),
          ),
        );
        yield* configuredSupportPaths;
      });
      const requireCurrentHead = Effect.fn("WorkOrderHost.requireCurrentHead")(function* (
        order: PullRequestWorkOrder,
      ) {
        const actual = yield* currentHead;
        if (actual !== order.headSha) {
          return yield* StalePullRequestHead.make({ expected: order.headSha, actual });
        }
      });
      const withWorktree: WorkOrderHost["Service"]["withWorktree"] = (order, use) =>
        Effect.acquireUseRelease(
          fs.makeTempDirectory({ prefix: "effect-agent-work-order-" }).pipe(
            Effect.mapError((error) =>
              WorkspaceOperationFailure.make({
                operation: "create work-order worktree parent",
                reason: String(error).slice(0, 4_096),
              }),
            ),
          ),
          (parent) => {
            const root = pathService.join(parent, "worktree");
            return Effect.acquireUseRelease(
              git
                .run(
                  config.repositoryPath,
                  ["worktree", "add", "--detach", root, order.headSha],
                  "acquire work-order worktree",
                )
                .pipe(Effect.as(root)),
              () =>
                Effect.gen(function* () {
                  yield* config.onWorktreeAcquired?.(root) ?? Effect.void;
                  const allowedPaths = new Set<string>([
                    order.source.path,
                    ...(yield* configuredSupportPaths),
                  ]);
                  const workspace = yield* makeWorkspace({
                    root,
                    allowedPaths,
                    checks,
                    fs,
                    pathService,
                    git,
                    crypto,
                  });
                  const commitAndPublish: AcquiredWorktree["commitAndPublish"] = Effect.fn(
                    "WorkOrderHost.commitAndPublish",
                  )(function* ({ order: expected, report, patch, checks: checkResults }) {
                    const beforeCommit = yield* currentHead;
                    if (beforeCommit !== expected.headSha) {
                      return yield* StalePullRequestHead.make({
                        expected: expected.headSha,
                        actual: beforeCommit,
                      });
                    }
                    yield* workspace.git.run(
                      workspace.realRoot,
                      ["add", "--", ...patch.changedPaths],
                      "stage work-order patch",
                    );
                    const stagedPatch = yield* workspace.git.run(
                      workspace.realRoot,
                      [
                        "diff",
                        "--cached",
                        "--binary",
                        "--full-index",
                        "--no-ext-diff",
                        "HEAD",
                        "--",
                      ],
                      "collect staged work-order patch",
                    );
                    const stagedDigest = yield* sha256(crypto, stagedPatch);
                    if (stagedDigest !== patch.digest) {
                      return yield* WorkspaceOperationFailure.make({
                        operation: "stage work-order patch",
                        reason: "staged patch digest differs from the host-validated patch",
                      });
                    }
                    yield* workspace.git.run(
                      workspace.realRoot,
                      [
                        "-c",
                        "user.name=effect-agent-implementer",
                        "-c",
                        "user.email=effect-agent-implementer@localhost",
                        "-c",
                        "commit.gpgSign=false",
                        "commit",
                        "-m",
                        config.commitMessage ?? `fix: implement work order ${expected.workOrderId}`,
                      ],
                      "commit work-order patch",
                    );
                    const publishedHeadSha = yield* workspace.git
                      .run(workspace.realRoot, ["rev-parse", "HEAD"], "resolve work-order commit")
                      .pipe(
                        Effect.flatMap((output) => decodeSha("resolve work-order commit", output)),
                      );
                    const beforePublish = yield* currentHead;
                    if (beforePublish !== expected.headSha) {
                      return yield* StalePullRequestHead.make({
                        expected: expected.headSha,
                        actual: beforePublish,
                      });
                    }
                    yield* git
                      .run(
                        config.repositoryPath,
                        ["update-ref", config.headRef, publishedHeadSha, expected.headSha],
                        "publish work-order commit",
                      )
                      .pipe(
                        Effect.catchTag("WorkspaceOperationFailure", (failure) =>
                          Effect.gen(function* () {
                            const actual = yield* currentHead;
                            if (actual === expected.headSha) return yield* failure;
                            return yield* StalePullRequestHead.make({
                              expected: expected.headSha,
                              actual,
                            });
                          }),
                        ),
                      );
                    return PublishedWorkOrder.make({
                      workOrderId: expected.workOrderId,
                      workOrderDigest: report.workOrderDigest,
                      previousHeadSha: expected.headSha,
                      publishedHeadSha,
                      patchDigest: patch.digest,
                      changedPaths: patch.changedPaths,
                      checks: checkResults,
                    });
                  });
                  return yield* use({
                    allowedPaths,
                    modelWorkspace: workspace.modelWorkspace,
                    inspectPatch: workspace.inspectPatch,
                    collectPatch: workspace.collectPatch,
                    runCheck: workspace.requestCheck,
                    observedChecks: workspace.observedChecks,
                    commitAndPublish,
                  });
                }),
              (rootPath) =>
                Effect.gen(function* () {
                  yield* config.onBeforeWorktreeRelease?.(rootPath) ?? Effect.void;
                  yield* git.run(
                    config.repositoryPath,
                    ["worktree", "remove", "--force", rootPath],
                    "release work-order worktree",
                  );
                }).pipe(Effect.ensuring(config.onWorktreeReleased?.(rootPath) ?? Effect.void)),
            );
          },
          (parent) =>
            fs.remove(parent, { recursive: true, force: true }).pipe(
              Effect.mapError((error) =>
                WorkspaceOperationFailure.make({
                  operation: "release work-order worktree parent",
                  reason: String(error).slice(0, 4_096),
                }),
              ),
            ),
        );
      return WorkOrderHost.of({
        requiredChecks: [...config.requiredChecks],
        currentHead,
        authorizeDispatch,
        requireCurrentHead,
        withWorktree,
      });
    }),
  ).pipe(Layer.provide(localGitLayer(config.gitExecutable ?? "/usr/bin/git")));
