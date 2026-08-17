import {
  ChangedFile,
  GitCommitSha,
  normalizeRepoRelativePath,
  PullRequestMetadata,
  PullRequestSource,
  PullRequestSourceFailure,
  ReviewInputViolation,
} from "@effect-agent/pr-review";
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
  normalizeWorkspacePath,
  PatchDigest,
  PatchSnapshot,
  PublishedRemediation,
  type RemediationCheckResult,
  type RemediationTrigger,
  RemediationTriggerRejected,
  StalePullRequestHead,
  WorkspaceOperationFailure,
  WorkspaceViolation,
} from "./contracts.ts";
import {
  type AcquiredRemediationWorktree,
  type HostCheck,
  type ImplementationWorkspace,
  RemediationHost,
  WorkspaceSearchHit,
} from "./workspace.ts";

const MAX_FILE_CHARS = 200_000;
const MAX_PATCH_CHARS = 1_000_000;
const MAX_GIT_OUTPUT_BYTES = 4_000_000;
const MAX_SEARCH_RESULTS = 100;

export interface LocalGitPullRequestConfig {
  readonly repositoryPath: string;
  readonly repository: string;
  readonly pullRequestNumber: number;
  readonly title: string;
  readonly body?: string | undefined;
  readonly baseRef: string;
  readonly headRef: string;
  readonly baseRefName?: string | undefined;
  readonly headRefName?: string | undefined;
  readonly gitExecutable?: string | undefined;
}

export interface LocalGitRemediationConfig extends LocalGitPullRequestConfig {
  readonly checks: ReadonlyArray<HostCheck>;
  readonly requiredChecks: ReadonlyArray<string>;
  readonly allowedSupportPaths?: ReadonlyArray<string> | undefined;
  readonly commitMessage?: string | undefined;
  readonly onWorktreeAcquired?: ((path: string) => Effect.Effect<void>) | undefined;
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
>()("@effect-agent/example-pr-remediation/LocalGit") {}

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
        operation: "digest remediation patch",
        reason: String(cause).slice(0, 4_096),
      }),
    ),
  );

interface GitChange {
  readonly path: string;
  readonly status: ChangedFile["status"];
  readonly previousPath?: string | undefined;
}

const parseNulFields = (output: string): ReadonlyArray<string> | undefined => {
  if (output.length === 0) return [];
  if (!output.endsWith("\0")) return undefined;
  const fields = output.split("\0");
  fields.pop();
  return fields;
};

const invalidGitChange = (reason: string) =>
  WorkspaceOperationFailure.make({ operation: "parse changed files", reason });

const parseGitChanges = Effect.fn("LocalGit.parseGitChanges")(function* (output: string) {
  const fields = parseNulFields(output);
  if (fields === undefined) {
    return yield* invalidGitChange("git returned a change list without NUL termination");
  }
  const changes: Array<GitChange> = [];
  let index = 0;
  while (index < fields.length) {
    const code = fields[index++];
    if (code === undefined || code.length === 0) {
      return yield* invalidGitChange("git returned an empty change status");
    }
    if (code.startsWith("R") || code.startsWith("C")) {
      const previousRaw = fields[index++];
      const pathRaw = fields[index++];
      if (previousRaw === undefined || pathRaw === undefined) {
        return yield* invalidGitChange(`git returned an incomplete ${code} change record`);
      }
      const previousPath = yield* normalizeWorkspacePath(previousRaw).pipe(
        Effect.mapError((failure) => invalidGitChange(failure.reason)),
      );
      const path = yield* normalizeWorkspacePath(pathRaw).pipe(
        Effect.mapError((failure) => invalidGitChange(failure.reason)),
      );
      changes.push({
        path,
        previousPath,
        status: code.startsWith("R") ? "renamed" : "copied",
      });
      continue;
    }
    const pathRaw = fields[index++];
    if (pathRaw === undefined) {
      return yield* invalidGitChange(`git returned an incomplete ${code} change record`);
    }
    const status =
      code === "A"
        ? "added"
        : code === "D"
          ? "removed"
          : code === "M"
            ? "modified"
            : code === "T"
              ? "changed"
              : undefined;
    if (status === undefined) {
      return yield* invalidGitChange(`unsupported git change status '${code}'`);
    }
    const path = yield* normalizeWorkspacePath(pathRaw).pipe(
      Effect.mapError((failure) => invalidGitChange(failure.reason)),
    );
    changes.push({ path, status });
  }
  return changes.sort((left, right) =>
    left.path < right.path ? -1 : left.path > right.path ? 1 : 0,
  );
});

const decodeSha = (
  operation: string,
  output: string,
): Effect.Effect<typeof GitCommitSha.Type, WorkspaceOperationFailure> =>
  Schema.decodeUnknownEffect(GitCommitSha)(output.trim()).pipe(
    Effect.mapError(() =>
      WorkspaceOperationFailure.make({
        operation,
        reason: "git returned an invalid commit SHA",
      }),
    ),
  );

const makeGitAccess = (config: LocalGitPullRequestConfig) =>
  Effect.gen(function* () {
    const git = yield* LocalGit;
    const currentHead = git
      .run(config.repositoryPath, ["rev-parse", config.headRef], "resolve pull-request head")
      .pipe(Effect.flatMap((output) => decodeSha("resolve pull-request head", output)));
    const baseHead = git
      .run(config.repositoryPath, ["rev-parse", config.baseRef], "resolve pull-request base")
      .pipe(Effect.flatMap((output) => decodeSha("resolve pull-request base", output)));
    const changedFiles = Effect.fn("LocalGit.changedFiles")(function* (
      baseSha: string,
      headSha: string,
    ) {
      const output = yield* git.run(
        config.repositoryPath,
        [
          "diff",
          "--name-status",
          "-z",
          "--find-renames",
          "--no-ext-diff",
          `${baseSha}...${headSha}`,
          "--",
        ],
        "list changed files",
      );
      return yield* parseGitChanges(output);
    });
    return { git, currentHead, baseHead, changedFiles };
  });

export const localGitPullRequestSourceLayer = (
  config: LocalGitPullRequestConfig,
): Layer.Layer<PullRequestSource, never, ChildProcessSpawner.ChildProcessSpawner> =>
  Layer.effect(
    PullRequestSource,
    Effect.gen(function* () {
      const access = yield* makeGitAccess(config);
      const load = Effect.fn("LocalGitPullRequestSource.load")(function* () {
        const [baseSha, headSha] = yield* Effect.all([access.baseHead, access.currentHead]);
        const changes = yield* access.changedFiles(baseSha, headSha);
        const files = yield* Effect.forEach(changes, (change) =>
          access.git
            .run(
              config.repositoryPath,
              [
                "diff",
                "--unified=80",
                "--no-ext-diff",
                `${baseSha}...${headSha}`,
                "--",
                ...(change.previousPath === undefined
                  ? [change.path]
                  : [change.previousPath, change.path]),
              ],
              `read diff for ${change.path}`,
            )
            .pipe(
              Effect.flatMap((patch) => {
                const textual =
                  patch.length <= MAX_PATCH_CHARS &&
                  !patch.includes("GIT binary patch") &&
                  !/^Binary files .* differ$/m.test(patch);
                return Schema.decodeUnknownEffect(ChangedFile)({
                  path: change.path,
                  status: change.status,
                  additions: patch
                    .split("\n")
                    .filter((line) => line.startsWith("+") && !line.startsWith("+++")).length,
                  deletions: patch
                    .split("\n")
                    .filter((line) => line.startsWith("-") && !line.startsWith("---")).length,
                  ...(change.previousPath === undefined
                    ? {}
                    : { previousPath: change.previousPath }),
                  ...(textual ? { patch } : {}),
                }).pipe(
                  Effect.mapError(() =>
                    WorkspaceOperationFailure.make({
                      operation: `decode changed file ${change.path}`,
                      reason: "git returned changed-file evidence outside the supported schema",
                    }),
                  ),
                );
              }),
            ),
        );
        return { baseSha, headSha, files };
      });
      const loadSnapshot = yield* Effect.cached(load());
      const metadata = loadSnapshot.pipe(
        Effect.map(({ baseSha, headSha, files }) =>
          PullRequestMetadata.make({
            repository: config.repository,
            number: config.pullRequestNumber,
            title: config.title,
            body: config.body ?? "",
            baseRef: config.baseRefName ?? config.baseRef,
            baseSha,
            headRef: config.headRefName ?? config.headRef,
            headSha,
            totalChangedFiles: files.length,
          }),
        ),
        Effect.mapError((error) =>
          PullRequestSourceFailure.make({
            operation: error.operation,
            reason: error.reason,
          }),
        ),
      );
      const files = loadSnapshot.pipe(
        Effect.map((snapshot) => snapshot.files),
        Effect.mapError((error) =>
          PullRequestSourceFailure.make({
            operation: error.operation,
            reason: error.reason,
          }),
        ),
      );
      return PullRequestSource.of({
        metadata,
        changedFiles: files,
        anchorFiles: files,
        readFile: (requestedPath: string) =>
          Effect.gen(function* () {
            const path = yield* normalizeRepoRelativePath(requestedPath);
            const snapshot = yield* loadSnapshot.pipe(
              Effect.mapError((error) =>
                PullRequestSourceFailure.make({
                  operation: error.operation,
                  reason: error.reason,
                }),
              ),
            );
            if (!snapshot.files.some((file) => file.path === path)) {
              return yield* ReviewInputViolation.make({
                input: path,
                reason: "Path is not part of this pull request's changeset.",
              });
            }
            return yield* access.git
              .run(
                config.repositoryPath,
                ["show", `${snapshot.headSha}:${path}`],
                `read head file ${path}`,
              )
              .pipe(
                Effect.mapError((error) =>
                  PullRequestSourceFailure.make({
                    operation: error.operation,
                    reason: error.reason,
                  }),
                ),
              );
          }),
      });
    }),
  ).pipe(Layer.provide(localGitLayer(config.gitExecutable ?? "/usr/bin/git")));

const makeWorkspace = Effect.fn("LocalGitRemediationHost.makeWorkspace")(function* (input: {
  readonly root: string;
  readonly allowedPaths: ReadonlySet<string>;
  readonly checks: ReadonlyMap<string, HostCheck>;
  readonly fs: FileSystem.FileSystem;
  readonly pathService: Path.Path;
  readonly git: LocalGit["Service"];
  readonly crypto: Crypto.Crypto;
}) {
  const { crypto, fs, git, pathService } = input;
  const observedChecks = yield* Ref.make<ReadonlyArray<RemediationCheckResult>>([]);
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
        reason: "path is outside the host-configured remediation allowlist",
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
  const inspectPatch = Effect.gen(function* () {
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
    return PatchSnapshot.make({
      digest: yield* sha256(crypto, patch),
      changedPaths,
      preview: patch.slice(0, 20_000),
      truncated: patch.length > 20_000,
    });
  });
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
    requestCheck,
    observedChecks: Ref.get(observedChecks),
    git,
    realRoot,
  };
});

export const localGitRemediationHostLayer = (
  config: LocalGitRemediationConfig,
): Layer.Layer<
  RemediationHost,
  never,
  Crypto.Crypto | FileSystem.FileSystem | Path.Path | ChildProcessSpawner.ChildProcessSpawner
> =>
  Layer.effect(
    RemediationHost,
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const pathService = yield* Path.Path;
      const crypto = yield* Crypto.Crypto;
      const access = yield* makeGitAccess(config);
      const checks = new Map(config.checks.map((check) => [check.name, check] as const));
      const authorizeTrigger = Effect.fn("RemediationHost.authorizeTrigger")(function* (
        trigger: RemediationTrigger,
      ) {
        if (
          trigger.label !== "pr-remediate" ||
          trigger.trust !== "same-repository" ||
          trigger.repository !== config.repository ||
          trigger.pullRequestNumber !== config.pullRequestNumber
        ) {
          return yield* RemediationTriggerRejected.make({
            reason: "trigger is not an authorized same-repository pr-remediate request",
          });
        }
        const actual = yield* access.currentHead;
        if (actual !== trigger.requestedHeadSha) {
          return yield* StalePullRequestHead.make({ expected: trigger.requestedHeadSha, actual });
        }
      });
      const withWorktree: RemediationHost["Service"]["withWorktree"] = (handoff, use) =>
        Effect.acquireUseRelease(
          fs.makeTempDirectory({ prefix: "effect-agent-remediation-" }).pipe(
            Effect.mapError((error) =>
              WorkspaceOperationFailure.make({
                operation: "create remediation worktree parent",
                reason: String(error).slice(0, 4_096),
              }),
            ),
          ),
          (parent) => {
            const root = pathService.join(parent, "worktree");
            return Effect.acquireUseRelease(
              access.git
                .run(
                  config.repositoryPath,
                  ["worktree", "add", "--detach", root, handoff.reviewedHeadSha],
                  "acquire remediation worktree",
                )
                .pipe(Effect.as(root)),
              () =>
                Effect.gen(function* () {
                  yield* config.onWorktreeAcquired?.(root) ?? Effect.void;
                  const allowedPaths = new Set<string>([
                    ...handoff.findings.map((finding) => finding.path),
                    ...(config.allowedSupportPaths ?? []),
                  ]);
                  const workspace = yield* makeWorkspace({
                    root,
                    allowedPaths,
                    checks,
                    fs,
                    pathService,
                    git: access.git,
                    crypto,
                  });
                  const commitAndPublish: AcquiredRemediationWorktree["commitAndPublish"] =
                    Effect.fn("RemediationHost.commitAndPublish")(function* ({
                      handoff: expected,
                      patch,
                      checks: checkResults,
                    }) {
                      const beforeCommit = yield* access.currentHead;
                      if (beforeCommit !== expected.reviewedHeadSha) {
                        return yield* StalePullRequestHead.make({
                          expected: expected.reviewedHeadSha,
                          actual: beforeCommit,
                        });
                      }
                      yield* workspace.git.run(
                        workspace.realRoot,
                        ["add", "--", ...patch.changedPaths],
                        "stage remediation patch",
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
                        "collect staged remediation patch",
                      );
                      const stagedDigest = yield* sha256(crypto, stagedPatch);
                      if (stagedDigest !== patch.digest) {
                        return yield* WorkspaceOperationFailure.make({
                          operation: "stage remediation patch",
                          reason: "staged patch digest differs from the host-validated patch",
                        });
                      }
                      yield* workspace.git.run(
                        workspace.realRoot,
                        [
                          "-c",
                          "user.name=effect-agent-remediator",
                          "-c",
                          "user.email=effect-agent-remediator@localhost",
                          "-c",
                          "commit.gpgSign=false",
                          "commit",
                          "-m",
                          config.commitMessage ?? "fix: remediate reviewed findings",
                        ],
                        "commit remediation patch",
                      );
                      const publishedHeadSha = yield* workspace.git
                        .run(
                          workspace.realRoot,
                          ["rev-parse", "HEAD"],
                          "resolve remediation commit",
                        )
                        .pipe(
                          Effect.flatMap((output) =>
                            decodeSha("resolve remediation commit", output),
                          ),
                        );
                      const beforePublish = yield* access.currentHead;
                      if (beforePublish !== expected.reviewedHeadSha) {
                        return yield* StalePullRequestHead.make({
                          expected: expected.reviewedHeadSha,
                          actual: beforePublish,
                        });
                      }
                      yield* access.git
                        .run(
                          config.repositoryPath,
                          [
                            "update-ref",
                            config.headRef,
                            publishedHeadSha,
                            expected.reviewedHeadSha,
                          ],
                          "publish remediation commit",
                        )
                        .pipe(
                          Effect.catchTag("WorkspaceOperationFailure", (failure) =>
                            Effect.gen(function* () {
                              const actual = yield* access.currentHead;
                              if (actual === expected.reviewedHeadSha) return yield* failure;
                              return yield* StalePullRequestHead.make({
                                expected: expected.reviewedHeadSha,
                                actual,
                              });
                            }),
                          ),
                        );
                      return PublishedRemediation.make({
                        previousHeadSha: expected.reviewedHeadSha,
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
                    runCheck: workspace.requestCheck,
                    observedChecks: workspace.observedChecks,
                    commitAndPublish,
                  });
                }),
              () =>
                access.git
                  .run(
                    config.repositoryPath,
                    ["worktree", "remove", "--force", root],
                    "release remediation worktree",
                  )
                  .pipe(Effect.ensuring(config.onWorktreeReleased?.(root) ?? Effect.void)),
            );
          },
          (parent) =>
            fs.remove(parent, { recursive: true, force: true }).pipe(
              Effect.mapError((error) =>
                WorkspaceOperationFailure.make({
                  operation: "release remediation worktree parent",
                  reason: String(error).slice(0, 4_096),
                }),
              ),
            ),
        );
      return RemediationHost.of({
        requiredChecks: [...config.requiredChecks],
        authorizeTrigger,
        withWorktree,
      });
    }),
  ).pipe(Layer.provide(localGitLayer(config.gitExecutable ?? "/usr/bin/git")));
