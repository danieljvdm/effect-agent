import {
  GitCommitSha,
  normalizeWorkspacePath,
  PatchDigest,
  type ProposedWorkOrder,
  RequiredCheckFailed,
  WorkOrderCheckResult,
  WorkOrderValidationFailure,
  WorkspaceOperationFailure,
} from "@effect-agent/example-pr-work-orders";
import {
  Config,
  Crypto,
  Duration,
  Effect,
  Encoding,
  FileSystem,
  Path,
  Schema,
  Stream,
} from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import type { ActionCheckSpec, WorkOrderAdmission } from "./action-contracts.ts";
import { CheckedFile, CheckedWorkOrder } from "./action-contracts.ts";

const MAX_OUTPUT_BYTES = 4_000_000;
const TRUSTED_VITE_PLUS_BINARY = "/home/vp/.vite-plus/bin/vp";
const TRUSTED_INSTALL_PATH =
  "/runtime/.vite-plus/bin:/home/vp/.vite-plus/bin:/usr/local/bin:/usr/bin:/bin";
const CHECK_PATH =
  "/workspace/node_modules/.bin:/runtime/.vite-plus/bin:/home/vp/.vite-plus/bin:/usr/local/bin:/usr/bin:/bin";
const ContainerImage = Schema.NonEmptyString.check(
  Schema.isMaxLength(512),
  Schema.isPattern(/^[^\s]+@sha256:[0-9a-f]{64}$/),
);

const runProcess = Effect.fn("workOrderAction.runProcess")(function* (input: {
  readonly command: string;
  readonly args: ReadonlyArray<string>;
  readonly cwd: string;
  readonly operation: string;
  readonly timeoutSeconds?: number | undefined;
}) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const configuredPath = yield* Config.string("PATH").pipe(Config.withDefault("/usr/bin:/bin"));
  const process = Effect.scoped(
    Effect.gen(function* () {
      const handle = yield* spawner.spawn(
        ChildProcess.make(input.command, input.args, {
          cwd: input.cwd,
          env: { PATH: configuredPath },
          extendEnv: false,
          stdin: "ignore",
          stderr: "pipe",
          stdout: "pipe",
        }),
      );
      const [collected, exitCode] = yield* Effect.all([
        Stream.runFoldEffect(
          handle.all,
          () => ({ size: 0, chunks: [] as Array<Uint8Array> }),
          (state, chunk) => {
            const size = state.size + chunk.length;
            if (size > MAX_OUTPUT_BYTES) {
              return WorkspaceOperationFailure.make({
                operation: input.operation,
                reason: `process output exceeds ${String(MAX_OUTPUT_BYTES)} bytes`,
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
            operation: input.operation,
            reason: "process output is not valid UTF-8",
          }),
      });
      return { exitCode: Number(exitCode), output } as const;
    }),
  ).pipe(
    Effect.mapError((cause) =>
      Schema.is(WorkspaceOperationFailure)(cause)
        ? cause
        : WorkspaceOperationFailure.make({
            operation: input.operation,
            reason: String(cause).slice(0, 4_096),
          }),
    ),
  );
  return yield* input.timeoutSeconds === undefined
    ? process
    : process.pipe(
        Effect.timeoutOrElse({
          duration: Duration.seconds(input.timeoutSeconds),
          orElse: () =>
            WorkspaceOperationFailure.make({
              operation: input.operation,
              reason: `process exceeded ${String(input.timeoutSeconds)} seconds`,
            }),
        }),
      );
});

const runGit = (root: string, args: ReadonlyArray<string>, operation: string) =>
  runProcess({
    command: "/usr/bin/git",
    args: [
      "-c",
      "core.hooksPath=/dev/null",
      "-c",
      "core.autocrlf=false",
      "-c",
      "core.safecrlf=true",
      ...args,
    ],
    cwd: root,
    operation,
  }).pipe(
    Effect.flatMap(({ exitCode, output }) =>
      exitCode === 0
        ? Effect.succeed(output)
        : WorkspaceOperationFailure.make({
            operation,
            reason: output.slice(0, 4_096) || `git exited with ${String(exitCode)}`,
          }),
    ),
  );

const copyTree = Effect.fn("workOrderAction.copyTree")(function* (input: {
  readonly source: string;
  readonly target: string;
  readonly operation: string;
}) {
  const copied = yield* runProcess({
    command: "/bin/cp",
    args: ["-a", `${input.source}/.`, input.target],
    cwd: input.source,
    operation: input.operation,
    timeoutSeconds: 900,
  });
  if (copied.exitCode !== 0) {
    return yield* WorkspaceOperationFailure.make({
      operation: input.operation,
      reason: copied.output.slice(0, 4_096) || `cp exited with ${String(copied.exitCode)}`,
    });
  }
});

export const restoreFreshCheckWorkspace = Effect.fn("workOrderAction.restoreFreshCheckWorkspace")(
  function* (input: {
    readonly repositoryPath: string;
    readonly runtimeRoot: string;
    readonly checkName: string;
    readonly index: number;
  }) {
    const fs = yield* FileSystem.FileSystem;
    const repositoryPath = yield* fs
      .makeTempDirectoryScoped({ prefix: `work-order-check-${String(input.index)}-` })
      .pipe(
        Effect.mapError((cause) =>
          WorkspaceOperationFailure.make({
            operation: `create fresh checkout for required check ${input.checkName}`,
            reason: String(cause).slice(0, 4_096),
          }),
        ),
      );
    const runtimeRoot = yield* fs
      .makeTempDirectoryScoped({ prefix: `work-order-check-runtime-${String(input.index)}-` })
      .pipe(
        Effect.mapError((cause) =>
          WorkspaceOperationFailure.make({
            operation: `create fresh runtime for required check ${input.checkName}`,
            reason: String(cause).slice(0, 4_096),
          }),
        ),
      );
    yield* copyTree({
      source: input.repositoryPath,
      target: repositoryPath,
      operation: `restore fresh checkout for required check ${input.checkName}`,
    });
    yield* copyTree({
      source: input.runtimeRoot,
      target: runtimeRoot,
      operation: `restore fresh runtime for required check ${input.checkName}`,
    });
    return { repositoryPath, runtimeRoot } as const;
  },
);

export const isolatedCheckContainerArguments = (input: {
  readonly args: ReadonlyArray<string>;
  readonly command: string;
  readonly containerImage: string;
  readonly containerName: string;
  readonly network: "bridge" | "none";
  readonly root: string;
  readonly runnerUser: string;
  readonly runtimeRoot: string;
}): ReadonlyArray<string> => [
  "run",
  "--rm",
  "--name",
  input.containerName,
  "--network",
  input.network,
  "--user",
  input.runnerUser,
  "--read-only",
  "--cap-drop",
  "ALL",
  "--security-opt",
  "no-new-privileges",
  "--pids-limit",
  "256",
  "--memory",
  "4g",
  "--cpus",
  "2",
  "--tmpfs",
  "/tmp:rw,nosuid,nodev,size=268435456",
  "--mount",
  `type=bind,src=${input.root},dst=/workspace`,
  "--mount",
  `type=bind,src=${input.runtimeRoot},dst=/runtime`,
  "--workdir",
  "/workspace",
  "--env",
  "CI=true",
  "--env",
  "HOME=/runtime",
  "--env",
  "VP_HOME=/runtime/.vite-plus",
  "--env",
  `PATH=${input.network === "bridge" ? TRUSTED_INSTALL_PATH : CHECK_PATH}`,
  input.containerImage,
  input.command,
  ...input.args,
];

const runIsolatedContainer = Effect.fn("workOrderAction.runIsolatedContainer")(function* (input: {
  readonly args: ReadonlyArray<string>;
  readonly command: string;
  readonly containerImage: string;
  readonly containerName: string;
  readonly network: "bridge" | "none";
  readonly operation: string;
  readonly repositoryPath: string;
  readonly runnerUser: string;
  readonly runtimeRoot: string;
  readonly timeoutSeconds: number;
}) {
  const fs = yield* FileSystem.FileSystem;
  const root = yield* fs.realPath(input.repositoryPath).pipe(
    Effect.mapError((cause) =>
      WorkspaceOperationFailure.make({
        operation: input.operation,
        reason: String(cause).slice(0, 4_096),
      }),
    ),
  );
  const runtimeRoot = yield* fs
    .realPath(input.runtimeRoot)
    .pipe(
      Effect.mapError((cause) =>
        WorkspaceOperationFailure.make({ operation: input.operation, reason: String(cause) }),
      ),
    );
  if (
    [root, runtimeRoot].some(
      (value) =>
        value.includes(",") || [...value].some((character) => character.charCodeAt(0) <= 31),
    )
  ) {
    return yield* WorkspaceOperationFailure.make({
      operation: input.operation,
      reason: "worktree path cannot be expressed as a Docker bind mount",
    });
  }
  const run = runProcess({
    command: "docker",
    args: isolatedCheckContainerArguments({
      args: input.args,
      command: input.command,
      containerImage: input.containerImage,
      containerName: input.containerName,
      network: input.network,
      root,
      runnerUser: input.runnerUser,
      runtimeRoot,
    }),
    cwd: root,
    operation: input.operation,
    timeoutSeconds: input.timeoutSeconds,
  });
  return yield* run.pipe(
    Effect.ensuring(
      runProcess({
        command: "docker",
        args: ["rm", "--force", input.containerName],
        cwd: root,
        operation: `release ${input.operation}`,
        timeoutSeconds: 30,
      }).pipe(Effect.ignore),
    ),
  );
});

const decodeSha = (operation: string, output: string) =>
  Schema.decodeUnknownEffect(GitCommitSha)(output.trim()).pipe(
    Effect.mapError(() =>
      WorkspaceOperationFailure.make({ operation, reason: "git returned an invalid SHA" }),
    ),
  );

const patchDigest = Effect.fn("workOrderAction.patchDigest")(function* (patch: string) {
  const crypto = yield* Crypto.Crypto;
  return yield* crypto.digest("SHA-256", new TextEncoder().encode(patch)).pipe(
    Effect.map(Encoding.encodeHex),
    Effect.flatMap(Schema.decodeUnknownEffect(PatchDigest)),
    Effect.mapError((cause) =>
      WorkspaceOperationFailure.make({
        operation: "digest checked patch",
        reason: String(cause).slice(0, 4_096),
      }),
    ),
  );
});

const collectPatch = Effect.fn("workOrderAction.collectPatch")(function* (root: string) {
  const pathsOutput = yield* runGit(
    root,
    ["diff", "--name-only", "-z", "--no-ext-diff", "HEAD", "--"],
    "collect checked paths",
  );
  if (pathsOutput.length > 0 && !pathsOutput.endsWith("\0")) {
    return yield* WorkspaceOperationFailure.make({
      operation: "collect checked paths",
      reason: "git path output is not NUL terminated",
    });
  }
  const rawPaths = pathsOutput.length === 0 ? [] : pathsOutput.slice(0, -1).split("\0");
  const changedPaths = yield* Effect.forEach(rawPaths.sort(), normalizeWorkspacePath).pipe(
    Effect.mapError((cause) =>
      WorkspaceOperationFailure.make({
        operation: "collect checked paths",
        reason: cause.reason,
      }),
    ),
  );
  const patch = yield* runGit(
    root,
    ["diff", "--binary", "--full-index", "--no-ext-diff", "HEAD", "--"],
    "collect checked patch",
  );
  if (patch.length > 1_000_000) {
    return yield* WorkspaceOperationFailure.make({
      operation: "collect checked patch",
      reason: "checked patch exceeds 1,000,000 characters",
    });
  }
  return { patch, changedPaths, digest: yield* patchDigest(patch) } as const;
});

const exactStrings = (left: ReadonlyArray<string>, right: ReadonlyArray<string>): boolean => {
  const a = [...left].sort();
  const b = [...right].sort();
  return a.length === b.length && a.every((value, index) => value === b[index]);
};

export const validateProposedWorkOrder = Effect.fn("validateProposedWorkOrder")(function* (input: {
  readonly admission: WorkOrderAdmission;
  readonly proposal: ProposedWorkOrder;
  readonly repositoryPath: string;
  readonly checks: ReadonlyArray<ActionCheckSpec>;
  readonly containerImage: string;
  readonly runnerUser: string;
}) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const { admission, checks, proposal, repositoryPath } = input;
  const containerImage = yield* Schema.decodeUnknownEffect(ContainerImage)(
    input.containerImage,
  ).pipe(
    Effect.mapError(() =>
      WorkspaceOperationFailure.make({
        operation: "configure isolated checks",
        reason: "check container image must use an immutable sha256 digest",
      }),
    ),
  );
  if (
    proposal.order.workOrderId !== admission.order.workOrderId ||
    proposal.workOrderDigest !== admission.workOrderDigest ||
    proposal.order.headSha !== admission.order.headSha
  ) {
    return yield* WorkOrderValidationFailure.make({
      reason: "work-order-mismatch",
      detail: "proposal does not match the admitted work order",
    });
  }
  if (
    !exactStrings(
      checks.map((check) => check.name),
      proposal.requiredChecks,
    )
  ) {
    return yield* WorkOrderValidationFailure.make({
      reason: "check-results-mismatch",
      detail: "configured check names differ from the proposal trust envelope",
    });
  }
  const head = yield* runGit(repositoryPath, ["rev-parse", "HEAD"], "resolve checked head").pipe(
    Effect.flatMap((output) => decodeSha("resolve checked head", output)),
  );
  if (head !== proposal.order.headSha) {
    return yield* WorkOrderValidationFailure.make({
      reason: "work-order-mismatch",
      detail: "check checkout is not the admitted pull-request head",
    });
  }
  const runtimeRoot = yield* fs.makeTempDirectoryScoped({ prefix: "work-order-runtime-" }).pipe(
    Effect.mapError((cause) =>
      WorkspaceOperationFailure.make({
        operation: "create isolated check runtime",
        reason: String(cause).slice(0, 4_096),
      }),
    ),
  );
  const containerPrefix = `effect-agent-${admission.order.workOrderId.replace(/[^A-Za-z0-9_.-]/g, "-").slice(0, 40)}`;
  const install = yield* runIsolatedContainer({
    args: ["install", "--frozen-lockfile", "--ignore-scripts"],
    command: TRUSTED_VITE_PLUS_BINARY,
    containerImage,
    containerName: `${containerPrefix}-install`,
    network: "bridge",
    operation: "install check dependencies in isolated container",
    repositoryPath,
    runnerUser: input.runnerUser,
    runtimeRoot,
    timeoutSeconds: 900,
  });
  if (install.exitCode !== 0) {
    return yield* WorkspaceOperationFailure.make({
      operation: "install check dependencies in isolated container",
      reason:
        install.output.slice(0, 4_096) || `vp install exited with ${String(install.exitCode)}`,
    });
  }
  const before = yield* collectPatch(repositoryPath);
  if (before.changedPaths.length > 0) {
    return yield* WorkOrderValidationFailure.make({
      reason: "check-mutated-patch",
      detail: "check checkout was dirty before applying the validated proposal",
    });
  }
  const patchDirectory = yield* fs.makeTempDirectoryScoped({ prefix: "work-order-check-" }).pipe(
    Effect.mapError((cause) =>
      WorkspaceOperationFailure.make({
        operation: "create proposal patch directory",
        reason: String(cause).slice(0, 4_096),
      }),
    ),
  );
  const patchFile = path.join(patchDirectory, "proposal.patch");
  yield* fs.writeFileString(patchFile, proposal.patch).pipe(
    Effect.mapError((cause) =>
      WorkspaceOperationFailure.make({
        operation: "write proposal patch",
        reason: String(cause).slice(0, 4_096),
      }),
    ),
  );
  yield* runGit(
    repositoryPath,
    ["apply", "--check", "--whitespace=nowarn", patchFile],
    "check proposed patch",
  );
  yield* runGit(
    repositoryPath,
    ["apply", "--whitespace=nowarn", patchFile],
    "apply proposed patch",
  );
  const applied = yield* collectPatch(repositoryPath);
  if (
    applied.digest !== proposal.patchDigest ||
    applied.patch !== proposal.patch ||
    !exactStrings(applied.changedPaths, proposal.changedPaths)
  ) {
    return yield* WorkOrderValidationFailure.make({
      reason: "patch-digest-mismatch",
      detail: "isolated checkout did not reproduce the proposed patch exactly",
    });
  }
  const results = yield* Effect.forEach(checks, (check, index) =>
    Effect.gen(function* () {
      const fresh = yield* restoreFreshCheckWorkspace({
        repositoryPath,
        runtimeRoot,
        checkName: check.name,
        index,
      });
      const checkRepositoryPath = fresh.repositoryPath;
      const checkRuntimeRoot = fresh.runtimeRoot;
      const restored = yield* collectPatch(checkRepositoryPath);
      if (
        restored.patch !== proposal.patch ||
        restored.digest !== proposal.patchDigest ||
        !exactStrings(restored.changedPaths, proposal.changedPaths)
      ) {
        return yield* WorkOrderValidationFailure.make({
          reason: "check-mutated-patch",
          detail: `fresh checkout for required check ${check.name} did not reproduce the validated patch`,
        });
      }
      const result = yield* runIsolatedContainer({
        args: check.args,
        command: check.command,
        containerImage,
        containerName: `${containerPrefix}-${String(index)}`,
        network: "none",
        operation: `run isolated required check ${check.name}`,
        repositoryPath: checkRepositoryPath,
        runnerUser: input.runnerUser,
        runtimeRoot: checkRuntimeRoot,
        timeoutSeconds: check.timeoutSeconds,
      });
      const afterCheck = yield* collectPatch(checkRepositoryPath);
      if (
        afterCheck.patch !== proposal.patch ||
        afterCheck.digest !== proposal.patchDigest ||
        !exactStrings(afterCheck.changedPaths, proposal.changedPaths)
      ) {
        return yield* WorkOrderValidationFailure.make({
          reason: "check-mutated-patch",
          detail: `required check ${check.name} mutated the validated patch`,
        });
      }
      return WorkOrderCheckResult.make({
        name: check.name,
        status: result.exitCode === 0 ? "passed" : "failed",
        summary: result.output.slice(0, 2_000) || (result.exitCode === 0 ? "passed" : "failed"),
      });
    }),
  );
  const failed = results.find((result) => result.status === "failed");
  if (failed !== undefined) {
    return yield* RequiredCheckFailed.make({ check: failed.name, summary: failed.summary });
  }
  const after = yield* collectPatch(repositoryPath);
  if (
    after.patch !== proposal.patch ||
    after.digest !== proposal.patchDigest ||
    !exactStrings(after.changedPaths, proposal.changedPaths)
  ) {
    return yield* WorkOrderValidationFailure.make({
      reason: "check-mutated-patch",
      detail: "a required check mutated the validated proposal",
    });
  }
  const files = yield* Effect.forEach(after.changedPaths, (relative) =>
    runGit(
      repositoryPath,
      ["ls-files", "--stage", "-z", "--", relative],
      `verify checked file mode ${relative}`,
    ).pipe(
      Effect.flatMap((entry) => {
        const withoutTerminator = entry.endsWith("\0") ? entry.slice(0, -1) : undefined;
        return withoutTerminator !== undefined &&
          !withoutTerminator.includes("\0") &&
          /^100644 [0-9a-f]{40,64} 0\t.+$/s.test(withoutTerminator)
          ? Effect.void
          : WorkspaceOperationFailure.make({
              operation: `verify checked file mode ${relative}`,
              reason: "publisher supports only existing regular tracked files",
            });
      }),
      Effect.andThen(fs.readFileString(path.join(repositoryPath, relative))),
      Effect.flatMap((content) =>
        content.length <= 200_000
          ? Effect.succeed(CheckedFile.make({ path: relative, content }))
          : WorkspaceOperationFailure.make({
              operation: `read checked file ${relative}`,
              reason: "checked file exceeds 200,000 characters",
            }),
      ),
      Effect.mapError((cause) =>
        Schema.is(WorkspaceOperationFailure)(cause)
          ? cause
          : WorkspaceOperationFailure.make({
              operation: `read checked file ${relative}`,
              reason: String(cause).slice(0, 4_096),
            }),
      ),
    ),
  );
  return CheckedWorkOrder.make({
    version: 1,
    admission,
    proposal,
    checks: results,
    files,
  });
});

export const reproduceCheckedPatch = Effect.fn("reproduceCheckedPatch")(function* (input: {
  readonly checked: CheckedWorkOrder;
  readonly expectedHeadFiles: ReadonlyMap<string, string>;
}) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const root = yield* fs.makeTempDirectoryScoped({ prefix: "work-order-publisher-" });
  const changedPaths = input.checked.proposal.changedPaths;
  yield* runGit(root, ["init", "--quiet"], "initialize publisher patch verifier");
  for (const relative of changedPaths) {
    const content = input.expectedHeadFiles.get(relative);
    if (content === undefined) {
      return yield* WorkOrderValidationFailure.make({
        reason: "patch-digest-mismatch",
        detail: `publisher did not load expected-head content for ${relative}`,
      });
    }
    const target = path.join(root, relative);
    yield* fs.makeDirectory(path.dirname(target), { recursive: true });
    yield* fs.writeFileString(target, content);
  }
  yield* runGit(root, ["add", "--", ...changedPaths], "stage publisher base files");
  yield* runGit(
    root,
    [
      "-c",
      "user.name=effect-agent-publisher",
      "-c",
      "user.email=effect-agent-publisher@localhost",
      "-c",
      "commit.gpgSign=false",
      "commit",
      "--quiet",
      "-m",
      "expected head",
    ],
    "commit publisher base files",
  );
  const patchFile = path.join(root, ".effect-agent-proposal.patch");
  yield* fs.writeFileString(patchFile, input.checked.proposal.patch);
  yield* runGit(
    root,
    ["apply", "--check", "--whitespace=nowarn", patchFile],
    "verify publisher patch",
  );
  yield* runGit(root, ["apply", "--whitespace=nowarn", patchFile], "reproduce publisher patch");
  const reproduced = yield* collectPatch(root);
  if (
    reproduced.patch !== input.checked.proposal.patch ||
    reproduced.digest !== input.checked.proposal.patchDigest ||
    !exactStrings(reproduced.changedPaths, changedPaths)
  ) {
    return yield* WorkOrderValidationFailure.make({
      reason: "patch-digest-mismatch",
      detail: "publisher could not reproduce the checked patch from expected-head file content",
    });
  }
  for (const file of input.checked.files) {
    const reproducedContent = yield* fs.readFileString(path.join(root, file.path));
    if (reproducedContent !== file.content) {
      return yield* WorkOrderValidationFailure.make({
        reason: "patch-digest-mismatch",
        detail: `publisher file content differs from the reproduced patch for ${file.path}`,
      });
    }
  }
});
