import {
  CodeReview,
  GitCommitSha,
  PrReview,
  PullRequestSource,
  ReviewFinding,
  ReviewHandoff,
  ReviewPublicationPlan,
  ReviewRunOutcome,
  webCryptoReviewHandoffAuthenticatorLayer,
} from "@effect-agent/pr-review";
import {
  collectingReviewPublisherLayer,
  makePromptKeyedModel,
  OFFLINE_DIFF_CALL_ID,
  OFFLINE_LIST_CALL_ID,
  OFFLINE_READ_CALL_ID,
  scriptedFinalParts,
  scriptedToolTurn,
} from "@effect-agent/pr-review/testing";
import { NodeServices } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import {
  Context,
  Deferred,
  Effect,
  Fiber,
  FileSystem,
  Layer,
  Redacted,
  Ref,
  Schema,
  Stream,
} from "effect";
import { LanguageModel, Model } from "effect/unstable/ai";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import {
  RemediationCheckResult,
  RemediationReport,
  RemediationTrigger,
  type PatchSnapshot,
  type WorkspaceOperationFailure,
} from "../src/contracts.ts";
import { makeImplementationAgent, type RemediationMission } from "../src/implementation-agent.ts";
import {
  localGitPullRequestSourceLayer,
  localGitRemediationHostLayer,
  type LocalGitRemediationConfig,
} from "../src/local-git.ts";
import { runPrRemediationLoop } from "../src/loop.ts";
import {
  type HostCheck,
  type ImplementationWorkspace,
  type ImplementationWorkspaceService,
  RemediationAttemptPolicy,
  RemediationHost,
} from "../src/workspace.ts";

const FILE_PATH = "src/value.ts";
const REQUIRED_CHECK = "fixture-check";

const decodeGitSha = (value: string) => Schema.decodeUnknownEffect(GitCommitSha)(value.trim());

type Equal<Left, Right> =
  (<Value>() => Value extends Left ? 1 : 2) extends <Value>() => Value extends Right ? 1 : 2
    ? true
    : false;
type Assert<Value extends true> = Value;

class ImplementationModelRequirement extends Context.Service<
  ImplementationModelRequirement,
  { readonly marker: "model-requirement" }
>()("@effect-agent/example-pr-remediation/test/ImplementationModelRequirement") {}

const typedImplementationModel = Model.make(
  "scripted",
  "pr-remediation-type-proof",
  Layer.effect(
    LanguageModel.LanguageModel,
    Effect.gen(function* () {
      yield* ImplementationModelRequirement;
      return yield* LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: () => Stream.empty,
      });
    }),
  ),
);
const typedImplementationRun = makeImplementationAgent(typedImplementationModel).run;
type TypedImplementationServices = Effect.Services<ReturnType<typeof typedImplementationRun>>;
type TypedImplementationError = Effect.Error<ReturnType<typeof typedImplementationRun>>;
type ModelRequirementProof = Assert<
  Equal<
    Extract<TypedImplementationServices, ImplementationModelRequirement>,
    ImplementationModelRequirement
  >
>;
type WorkspaceExcludedProof = Assert<
  Equal<Extract<TypedImplementationServices, ImplementationWorkspaceService>, never>
>;
type UnknownErrorExcludedProof = Assert<
  Equal<unknown extends TypedImplementationError ? true : false, false>
>;
type WorkspaceFailureExcludedProof = Assert<
  Equal<Extract<TypedImplementationError, WorkspaceOperationFailure>, never>
>;

describe("implementation Agent type proofs", () => {
  it("keeps model requirements and errors typed while hiding workspace internals", () => {
    const modelRequirementProof: ModelRequirementProof = true;
    const workspaceExcludedProof: WorkspaceExcludedProof = true;
    const unknownErrorExcludedProof: UnknownErrorExcludedProof = true;
    const workspaceFailureExcludedProof: WorkspaceFailureExcludedProof = true;
    expect({
      modelRequirementProof,
      workspaceExcludedProof,
      unknownErrorExcludedProof,
      workspaceFailureExcludedProof,
    }).toEqual({
      modelRequirementProof: true,
      workspaceExcludedProof: true,
      unknownErrorExcludedProof: true,
      workspaceFailureExcludedProof: true,
    });
  });
});

const runGit = Effect.fn("prRemediationTest.runGit")(function* (
  cwd: string,
  args: ReadonlyArray<string>,
) {
  const child = yield* ChildProcess.make(
    "/usr/bin/git",
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
  const [output, exitCode] = yield* Effect.all([
    child.all.pipe(Stream.decodeText(), Stream.mkString),
    child.exitCode,
  ]);
  if (Number(exitCode) !== 0) return yield* Effect.die(new Error(output));
  return output;
});

interface FixtureRepository {
  readonly root: string;
  readonly h0: typeof GitCommitSha.Type;
  readonly config: Omit<LocalGitRemediationConfig, "checks" | "requiredChecks">;
}

const withFixtureRepository = <A, E, R>(
  use: (fixture: FixtureRepository) => Effect.Effect<A, E, R>,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const parent = yield* fs.makeTempDirectoryScoped({ prefix: "pr-remediation-loop-test-" });
      const root = `${parent}/repository`;
      yield* fs.makeDirectory(`${root}/src`, { recursive: true });
      yield* runGit(parent, ["init", "repository"]);
      yield* fs.writeFileString(`${root}/${FILE_PATH}`, "export const answer = 40;\n");
      yield* runGit(root, ["add", "--", FILE_PATH]);
      yield* runGit(root, [
        "-c",
        "user.name=fixture",
        "-c",
        "user.email=fixture@localhost",
        "commit",
        "-m",
        "initial",
      ]);
      yield* runGit(root, ["branch", "-M", "main"]);
      yield* runGit(root, ["checkout", "-b", "feature"]);
      yield* fs.writeFileString(`${root}/${FILE_PATH}`, "export const answer = 41;\n");
      yield* runGit(root, ["add", "--", FILE_PATH]);
      yield* runGit(root, [
        "-c",
        "user.name=fixture",
        "-c",
        "user.email=fixture@localhost",
        "commit",
        "-m",
        "introduce reviewed change",
      ]);
      const h0 = yield* decodeGitSha(yield* runGit(root, ["rev-parse", "HEAD"]));
      return yield* use({
        root,
        h0,
        config: {
          repositoryPath: root,
          repository: "acme/widgets",
          pullRequestNumber: 17,
          title: "Update the exported answer",
          body: "",
          baseRef: "refs/heads/main",
          headRef: "refs/heads/feature",
          baseRefName: "main",
          headRefName: "feature",
        },
      });
    }),
  );

interface SourceFixtureRepository {
  readonly root: string;
  readonly config: FixtureRepository["config"];
}

const withSourceFixtureRepository = <A, E, R>(
  use: (fixture: SourceFixtureRepository) => Effect.Effect<A, E, R>,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const parent = yield* fs.makeTempDirectoryScoped({ prefix: "pr-source-test-" });
      const root = `${parent}/repository`;
      yield* fs.makeDirectory(`${root}/src`, { recursive: true });
      yield* runGit(parent, ["init", "repository"]);
      yield* fs.writeFileString(`${root}/src/old-name.ts`, "export const renamed = true;\n");
      yield* fs.writeFileString(`${root}/src/removed.ts`, "export const removed = true;\n");
      yield* runGit(root, ["add", "--", "src/old-name.ts", "src/removed.ts"]);
      yield* runGit(root, [
        "-c",
        "user.name=fixture",
        "-c",
        "user.email=fixture@localhost",
        "commit",
        "-m",
        "source base",
      ]);
      yield* runGit(root, ["branch", "-M", "main"]);
      yield* runGit(root, ["checkout", "-b", "feature"]);
      return yield* use({
        root,
        config: {
          repositoryPath: root,
          repository: "acme/source-fixture",
          pullRequestNumber: 23,
          title: "Exercise local Git status parsing",
          body: "",
          baseRef: "refs/heads/main",
          headRef: "refs/heads/feature",
          baseRefName: "main",
          headRefName: "feature",
        },
      });
    }),
  );

const finding = ReviewFinding.make({
  path: FILE_PATH,
  startLine: 1,
  endLine: 1,
  severity: "important",
  category: "correctness",
  title: "The answer is off by one",
  body: "The exported answer must be 42.",
  suggestion: "export const answer = 42;",
});

const secondFinding = ReviewFinding.make({
  path: FILE_PATH,
  startLine: 1,
  endLine: 1,
  severity: "important",
  category: "correctness",
  title: "The answer also violates the fixture contract",
  body: "The fixture contract independently requires the exported answer to be 42.",
});

const findingReview = CodeReview.make({
  summary: "The exported value is wrong.",
  verdict: "comment",
  findings: [finding],
});

const cleanReview = CodeReview.make({
  summary: "The exported answer is now correct.",
  verdict: "approve",
  findings: [],
});

const makeReviewer = Effect.fn("prRemediationTest.makeReviewer")(function* (
  config: FixtureRepository["config"],
) {
  const scripted = yield* makePromptKeyedModel("pr-remediation-reviewer", (prompt) => {
    if (prompt.includes(OFFLINE_READ_CALL_ID)) {
      const review = prompt.includes("answer = 42") ? cleanReview : findingReview;
      return scriptedFinalParts(JSON.stringify(Schema.encodeSync(CodeReview)(review)));
    }
    if (prompt.includes(OFFLINE_DIFF_CALL_ID)) {
      return scriptedToolTurn({
        type: "tool-call",
        id: OFFLINE_READ_CALL_ID,
        name: "read_file",
        params: { path: FILE_PATH },
        providerExecuted: false,
      });
    }
    if (prompt.includes(OFFLINE_LIST_CALL_ID)) {
      return scriptedToolTurn({
        type: "tool-call",
        id: OFFLINE_DIFF_CALL_ID,
        name: "read_file_diff",
        params: { path: FILE_PATH },
        providerExecuted: false,
      });
    }
    return scriptedToolTurn({
      type: "tool-call",
      id: OFFLINE_LIST_CALL_ID,
      name: "list_changed_files",
      params: { scope: "all" },
      providerExecuted: false,
    });
  });
  const reviewer = PrReview.make({ model: scripted.model, modelLabel: "scripted/offline" });
  const source = localGitPullRequestSourceLayer(config);
  const published = yield* Ref.make<ReadonlyArray<ReviewPublicationPlan>>([]);
  const reviewLayer = Layer.merge(source, collectingReviewPublisherLayer(published));
  const review = () =>
    Effect.gen(function* () {
      const outcome = yield* reviewer.run();
      const snapshot = yield* reviewer.snapshot;
      const profileFingerprint = yield* reviewer.profileFingerprint;
      return {
        outcome,
        metadata: snapshot.metadata,
        files: snapshot.files,
        profileFingerprint,
      };
    }).pipe(Effect.provide(reviewLayer));
  return { review, calls: scripted.calls };
});

const makeScriptedImplementer = Effect.fn("prRemediationTest.makeScriptedImplementer")(function* (
  mode: "fix" | "escape",
) {
  let mission: RemediationMission | undefined;
  let patch: PatchSnapshot | undefined;
  const observedChecks: Array<RemediationCheckResult> = [];
  const scripted = yield* makePromptKeyedModel(`pr-remediation-${mode}`, (prompt) => {
    if (prompt.includes("impl-inspect-1")) {
      if (mission === undefined || patch === undefined) throw new Error("missing scripted state");
      const status = mode === "fix" ? "fixed" : "needs-human";
      return scriptedFinalParts(
        JSON.stringify(
          Schema.encodeSync(RemediationReport)(
            RemediationReport.make({
              handoffDigest: mission.handoffDigest,
              reviewedHeadSha: mission.handoff.reviewedHeadSha,
              resolutions: mission.handoff.findings.map((item) => ({
                findingId: item.id,
                status,
                rationale:
                  mode === "fix"
                    ? "Changed the exported value after inspecting the file."
                    : "The requested path was outside the worktree jail.",
              })),
              changedPaths: patch.changedPaths,
              checks: observedChecks,
              patchDigest: patch.digest,
              summary:
                mode === "fix"
                  ? "Corrected the reviewed value."
                  : "No patch was applied because the path was invalid.",
            }),
          ),
        ),
      );
    }
    if (mode === "escape") {
      if (prompt.includes("impl-escape-1")) {
        return scriptedToolTurn({
          type: "tool-call",
          id: "impl-inspect-1",
          name: "inspect_workspace_patch",
          params: { scope: "all" },
          providerExecuted: false,
        });
      }
      return scriptedToolTurn({
        type: "tool-call",
        id: "impl-escape-1",
        name: "apply_workspace_edit",
        params: { path: "../outside.txt", expected: "safe", replacement: "escaped" },
        providerExecuted: false,
      });
    }
    if (prompt.includes("impl-check-1")) {
      return scriptedToolTurn({
        type: "tool-call",
        id: "impl-inspect-1",
        name: "inspect_workspace_patch",
        params: { scope: "all" },
        providerExecuted: false,
      });
    }
    if (prompt.includes("impl-edit-1")) {
      return scriptedToolTurn({
        type: "tool-call",
        id: "impl-check-1",
        name: "request_named_check",
        params: { name: REQUIRED_CHECK },
        providerExecuted: false,
      });
    }
    if (prompt.includes("impl-read-1")) {
      return scriptedToolTurn({
        type: "tool-call",
        id: "impl-edit-1",
        name: "apply_workspace_edit",
        params: {
          path: FILE_PATH,
          expected: "export const answer = 41;",
          replacement: "export const answer = 42;",
        },
        providerExecuted: false,
      });
    }
    return scriptedToolTurn({
      type: "tool-call",
      id: "impl-read-1",
      name: "read_workspace_file",
      params: { path: FILE_PATH },
      providerExecuted: false,
    });
  });
  const implementer = makeImplementationAgent(scripted.model);
  const implement = (input: RemediationMission, workspace: ImplementationWorkspace) => {
    mission = input;
    const observed: ImplementationWorkspace = {
      ...workspace,
      inspectPatch: workspace.inspectPatch.pipe(
        Effect.tap((snapshot) => Effect.sync(() => (patch = snapshot))),
      ),
      requestCheck: (name) =>
        workspace
          .requestCheck(name)
          .pipe(Effect.tap((result) => Effect.sync(() => observedChecks.push(result)))),
    };
    return implementer.run(input, observed);
  };
  return { implement, prompts: scripted.prompts };
});

const passCheck: HostCheck = {
  name: REQUIRED_CHECK,
  run: () =>
    Effect.succeed(
      RemediationCheckResult.make({
        name: REQUIRED_CHECK,
        status: "passed",
        summary: "fixture validation passed",
      }),
    ),
};

const triggerFor = (fixture: FixtureRepository) =>
  RemediationTrigger.make({
    version: 1,
    triggerId: `label:${fixture.h0}`,
    label: "pr-remediate",
    repository: fixture.config.repository,
    pullRequestNumber: fixture.config.pullRequestNumber,
    requestedHeadSha: fixture.h0,
    trust: "same-repository",
  });

const hostServices = (config: LocalGitRemediationConfig) =>
  Layer.mergeAll(
    localGitRemediationHostLayer(config),
    RemediationAttemptPolicy.layerMemory,
    webCryptoReviewHandoffAuthenticatorLayer(Redacted.make("offline-handoff-secret")),
  );

describe("local Git pull-request source", () => {
  it.effect(
    "uses one immutable snapshot per layer acquisition and refreshes the next acquisition",
    () =>
      withSourceFixtureRepository((fixture) =>
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          yield* fs.writeFileString(`${fixture.root}/src/first.ts`, "export const first = 1;\n");
          yield* runGit(fixture.root, ["add", "--", "src/first.ts"]);
          yield* runGit(fixture.root, [
            "-c",
            "user.name=fixture",
            "-c",
            "user.email=fixture@localhost",
            "commit",
            "-m",
            "first source head",
          ]);
          const sourceLayer = localGitPullRequestSourceLayer(fixture.config);
          const firstSnapshot = yield* Effect.gen(function* () {
            const source = yield* PullRequestSource;
            const metadata = yield* source.metadata;
            yield* fs.writeFileString(
              `${fixture.root}/src/second.ts`,
              "export const second = 2;\n",
            );
            yield* runGit(fixture.root, ["add", "--", "src/second.ts"]);
            yield* runGit(fixture.root, [
              "-c",
              "user.name=fixture",
              "-c",
              "user.email=fixture@localhost",
              "commit",
              "-m",
              "second source head",
            ]);
            return { metadata, files: yield* source.changedFiles };
          }).pipe(Effect.provide(sourceLayer));
          const freshSnapshot = yield* Effect.gen(function* () {
            const source = yield* PullRequestSource;
            return {
              metadata: yield* source.metadata,
              files: yield* source.changedFiles,
            };
          }).pipe(Effect.provide(sourceLayer));

          expect(firstSnapshot.files.map((file) => file.path)).toEqual(["src/first.ts"]);
          expect(freshSnapshot.metadata.headSha).not.toBe(firstSnapshot.metadata.headSha);
          expect(freshSnapshot.files.map((file) => file.path)).toEqual([
            "src/first.ts",
            "src/second.ts",
          ]);
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
    60_000,
  );

  it.effect(
    "preserves added, removed, renamed, and binary change semantics",
    () =>
      withSourceFixtureRepository((fixture) =>
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          yield* fs.rename(`${fixture.root}/src/old-name.ts`, `${fixture.root}/src/new-name.ts`);
          yield* fs.remove(`${fixture.root}/src/removed.ts`);
          yield* fs.writeFileString(`${fixture.root}/src/added.ts`, "export const added = true;\n");
          yield* fs.writeFile(`${fixture.root}/src/binary.dat`, new Uint8Array([0, 1, 2, 3]));
          yield* runGit(fixture.root, ["add", "--all", "--", "src"]);
          yield* runGit(fixture.root, [
            "-c",
            "user.name=fixture",
            "-c",
            "user.email=fixture@localhost",
            "commit",
            "-m",
            "exercise statuses",
          ]);
          const files = yield* Effect.gen(function* () {
            const source = yield* PullRequestSource;
            return yield* source.changedFiles;
          }).pipe(Effect.provide(localGitPullRequestSourceLayer(fixture.config)));

          expect(
            files.map(({ path, previousPath, status, patch }) => ({
              path,
              previousPath,
              status,
              hasPatch: patch !== undefined,
            })),
          ).toEqual([
            {
              path: "src/added.ts",
              previousPath: undefined,
              status: "added",
              hasPatch: true,
            },
            {
              path: "src/binary.dat",
              previousPath: undefined,
              status: "added",
              hasPatch: false,
            },
            {
              path: "src/new-name.ts",
              previousPath: "src/old-name.ts",
              status: "renamed",
              hasPatch: true,
            },
            {
              path: "src/removed.ts",
              previousPath: undefined,
              status: "removed",
              hasPatch: true,
            },
          ]);
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
    60_000,
  );

  it.effect(
    "fails closed on a Git path containing control characters",
    () =>
      withSourceFixtureRepository((fixture) =>
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const invalidPath = "src/bad\nname.ts";
          yield* fs.writeFileString(`${fixture.root}/${invalidPath}`, "export const bad = true;\n");
          yield* runGit(fixture.root, ["add", "--", invalidPath]);
          yield* runGit(fixture.root, [
            "-c",
            "user.name=fixture",
            "-c",
            "user.email=fixture@localhost",
            "commit",
            "-m",
            "add invalid path",
          ]);
          const failure = yield* Effect.gen(function* () {
            const source = yield* PullRequestSource;
            return yield* source.changedFiles;
          }).pipe(Effect.provide(localGitPullRequestSourceLayer(fixture.config)), Effect.flip);
          expect(failure._tag).toBe("PullRequestSourceFailure");
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
    60_000,
  );
});

describe("PR review -> remediation -> re-review loop", () => {
  it.effect(
    "publishes one host-validated commit at H1 and invokes a fresh reviewer",
    () =>
      withFixtureRepository((fixture) =>
        Effect.gen(function* () {
          const reviewer = yield* makeReviewer(fixture.config);
          const implementer = yield* makeScriptedImplementer("fix");
          const outcome = yield* runPrRemediationLoop({
            trigger: triggerFor(fixture),
            review: reviewer.review,
            implement: implementer.implement,
          }).pipe(
            Effect.provide(
              hostServices({
                ...fixture.config,
                checks: [passCheck],
                requiredChecks: [REQUIRED_CHECK],
              }),
            ),
          );

          expect(outcome.publication.previousHeadSha).toBe(fixture.h0);
          expect(outcome.publication.publishedHeadSha).not.toBe(fixture.h0);
          expect(outcome.publication.changedPaths).toEqual([FILE_PATH]);
          expect(outcome.remediation.patchDigest).toBe(outcome.publication.patchDigest);
          expect(outcome.freshReview.review.findings).toEqual([]);
          expect(outcome.freshReview.review.verdict).toBe("approve");
          expect(yield* reviewer.calls).toBe(8);
          expect(
            (yield* runGit(fixture.root, [
              "show",
              `${outcome.publication.publishedHeadSha}:${FILE_PATH}`,
            ])).trim(),
          ).toBe("export const answer = 42;");
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
    60_000,
  );

  it.effect(
    "rejects a fresh review whose publication plan names a different head",
    () =>
      withFixtureRepository((fixture) =>
        Effect.gen(function* () {
          const reviewer = yield* makeReviewer(fixture.config);
          const implementer = yield* makeScriptedImplementer("fix");
          const reviewNumber = yield* Ref.make(0);
          const failure = yield* runPrRemediationLoop({
            trigger: triggerFor(fixture),
            review: () =>
              Ref.updateAndGet(reviewNumber, (count) => count + 1).pipe(
                Effect.flatMap((count) =>
                  reviewer.review().pipe(Effect.map((evidence) => [count, evidence] as const)),
                ),
                Effect.map(([count, evidence]) =>
                  count === 2
                    ? {
                        ...evidence,
                        outcome: ReviewRunOutcome.make({
                          ...evidence.outcome,
                          plan: ReviewPublicationPlan.make({
                            ...evidence.outcome.plan,
                            commitSha: fixture.h0,
                          }),
                        }),
                      }
                    : evidence,
                ),
              ),
            implement: implementer.implement,
          }).pipe(
            Effect.provide(
              hostServices({
                ...fixture.config,
                checks: [passCheck],
                requiredChecks: [REQUIRED_CHECK],
              }),
            ),
            Effect.flip,
          );
          expect(failure._tag).toBe("RemediationValidationFailure");
          if (failure._tag === "RemediationValidationFailure") {
            expect(failure.reason).toBe("reviewed-head-mismatch");
          }
          expect(
            yield* decodeGitSha(yield* runGit(fixture.root, ["rev-parse", "feature"])),
          ).not.toBe(fixture.h0);
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
    60_000,
  );

  it.effect(
    "rejects a path-escaping edit without publishing",
    () =>
      withFixtureRepository((fixture) =>
        Effect.gen(function* () {
          const reviewer = yield* makeReviewer(fixture.config);
          const implementer = yield* makeScriptedImplementer("escape");
          const failure = yield* runPrRemediationLoop({
            trigger: triggerFor(fixture),
            review: reviewer.review,
            implement: implementer.implement,
          }).pipe(
            Effect.provide(
              hostServices({
                ...fixture.config,
                checks: [passCheck],
                requiredChecks: [REQUIRED_CHECK],
              }),
            ),
            Effect.flip,
          );
          expect(failure._tag).toBe("RemediationValidationFailure");
          if (failure._tag === "RemediationValidationFailure") {
            expect(failure.reason).toBe("finding-needs-human");
          }
          expect((yield* implementer.prompts).join("\n")).toContain("WorkspaceViolation");
          expect(yield* decodeGitSha(yield* runGit(fixture.root, ["rev-parse", "feature"]))).toBe(
            fixture.h0,
          );
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
    60_000,
  );

  it.effect(
    "rejects model-reported check results that differ from host observations",
    () =>
      withFixtureRepository((fixture) =>
        Effect.gen(function* () {
          const reviewer = yield* makeReviewer(fixture.config);
          const implementer = yield* makeScriptedImplementer("fix");
          const failure = yield* runPrRemediationLoop({
            trigger: triggerFor(fixture),
            review: reviewer.review,
            implement: (mission, workspace) =>
              implementer.implement(mission, workspace).pipe(
                Effect.map((report) =>
                  RemediationReport.make({
                    ...report,
                    checks: report.checks.map((check) =>
                      RemediationCheckResult.make({
                        ...check,
                        summary: "model supplied a different check result",
                      }),
                    ),
                  }),
                ),
              ),
          }).pipe(
            Effect.provide(
              hostServices({
                ...fixture.config,
                checks: [passCheck],
                requiredChecks: [REQUIRED_CHECK],
              }),
            ),
            Effect.flip,
          );
          expect(failure._tag).toBe("RemediationValidationFailure");
          if (failure._tag === "RemediationValidationFailure") {
            expect(failure.reason).toBe("check-results-mismatch");
          }
          expect(yield* decodeGitSha(yield* runGit(fixture.root, ["rev-parse", "feature"]))).toBe(
            fixture.h0,
          );
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
    60_000,
  );

  it.effect(
    "rejects unequal finding-identity multisets",
    () =>
      withFixtureRepository((fixture) =>
        Effect.gen(function* () {
          const reviewer = yield* makeReviewer(fixture.config);
          const implementer = yield* makeScriptedImplementer("fix");
          const failure = yield* runPrRemediationLoop({
            trigger: triggerFor(fixture),
            review: () =>
              reviewer.review().pipe(
                Effect.map((evidence) => ({
                  ...evidence,
                  outcome: ReviewRunOutcome.make({
                    ...evidence.outcome,
                    activeFindings: [finding, finding, secondFinding],
                  }),
                })),
              ),
            implement: (mission, workspace) =>
              implementer.implement(mission, workspace).pipe(
                Effect.map((report) =>
                  RemediationReport.make({
                    ...report,
                    resolutions: report.resolutions.map((resolution, index) =>
                      index === 1 ? (report.resolutions[2] ?? resolution) : resolution,
                    ),
                  }),
                ),
              ),
          }).pipe(
            Effect.provide(
              hostServices({
                ...fixture.config,
                checks: [passCheck],
                requiredChecks: [REQUIRED_CHECK],
              }),
            ),
            Effect.flip,
          );
          expect(failure._tag).toBe("RemediationValidationFailure");
          if (failure._tag === "RemediationValidationFailure") {
            expect(failure.reason).toBe("finding-accounting-mismatch");
          }
          expect(yield* decodeGitSha(yield* runGit(fixture.root, ["rev-parse", "feature"]))).toBe(
            fixture.h0,
          );
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
    60_000,
  );

  it.effect(
    "rejects a failed host re-check and consumes the reviewed head's only attempt",
    () =>
      withFixtureRepository((fixture) =>
        Effect.gen(function* () {
          const reviewer = yield* makeReviewer(fixture.config);
          const implementer = yield* makeScriptedImplementer("fix");
          const invocations = yield* Ref.make(0);
          const check: HostCheck = {
            name: REQUIRED_CHECK,
            run: () =>
              Ref.updateAndGet(invocations, (count) => count + 1).pipe(
                Effect.map((count) =>
                  RemediationCheckResult.make({
                    name: REQUIRED_CHECK,
                    status: count === 1 ? "passed" : "failed",
                    summary:
                      count === 1
                        ? "model-requested check passed"
                        : "independent host re-check failed",
                  }),
                ),
              ),
          };
          const services = hostServices({
            ...fixture.config,
            checks: [check],
            requiredChecks: [REQUIRED_CHECK],
          });
          const run = runPrRemediationLoop({
            trigger: triggerFor(fixture),
            review: reviewer.review,
            implement: implementer.implement,
          });
          const [first, second] = yield* Effect.gen(function* () {
            const first = yield* run.pipe(Effect.flip);
            const second = yield* run.pipe(Effect.flip);
            return [first, second] as const;
          }).pipe(Effect.provide(services));
          expect(first._tag).toBe("RequiredCheckFailed");
          expect(second._tag).toBe("RemediationAttemptAlreadyClaimed");
          expect(yield* Ref.get(invocations)).toBe(2);
          expect(yield* decodeGitSha(yield* runGit(fixture.root, ["rev-parse", "feature"]))).toBe(
            fixture.h0,
          );
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
    60_000,
  );

  it.effect(
    "rejects publication when the PR head moves during implementation",
    () =>
      withFixtureRepository((fixture) =>
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
          const reviewer = yield* makeReviewer(fixture.config);
          const implementer = yield* makeScriptedImplementer("fix");
          const moved = yield* Ref.make(false);
          const movingCheck: HostCheck = {
            name: REQUIRED_CHECK,
            run: () =>
              Effect.gen(function* () {
                const shouldMove = yield* Ref.modify(moved, (value) => [!value, true] as const);
                if (shouldMove) {
                  yield* fs.writeFileString(`${fixture.root}/external.txt`, "concurrent update\n");
                  yield* Effect.scoped(runGit(fixture.root, ["add", "--", "external.txt"]));
                  yield* Effect.scoped(
                    runGit(fixture.root, [
                      "-c",
                      "user.name=fixture",
                      "-c",
                      "user.email=fixture@localhost",
                      "commit",
                      "-m",
                      "move head concurrently",
                    ]),
                  );
                }
                return RemediationCheckResult.make({
                  name: REQUIRED_CHECK,
                  status: "passed",
                  summary: "fixture check passed",
                });
              }).pipe(
                Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
                Effect.orDie,
              ),
          };
          const failure = yield* runPrRemediationLoop({
            trigger: triggerFor(fixture),
            review: reviewer.review,
            implement: implementer.implement,
          }).pipe(
            Effect.provide(
              hostServices({
                ...fixture.config,
                checks: [movingCheck],
                requiredChecks: [REQUIRED_CHECK],
              }),
            ),
            Effect.flip,
          );
          expect(failure._tag).toBe("StalePullRequestHead");
          if (failure._tag === "StalePullRequestHead") {
            expect(failure.expected).toBe(fixture.h0);
            expect(failure.actual).not.toBe(fixture.h0);
          }
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
    60_000,
  );

  it.effect(
    "removes the scoped worktree when implementation is interrupted",
    () =>
      withFixtureRepository((fixture) =>
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const reviewer = yield* makeReviewer(fixture.config);
          const acquired = yield* Deferred.make<string>();
          const released = yield* Deferred.make<string>();
          const hangingModel = Model.make(
            "scripted",
            "hanging-implementer",
            Layer.effect(
              LanguageModel.LanguageModel,
              LanguageModel.make({
                generateText: () => Effect.succeed([]),
                streamText: () => Stream.never,
              }),
            ),
          );
          const implementer = makeImplementationAgent(hangingModel);
          const loop = runPrRemediationLoop({
            trigger: triggerFor(fixture),
            review: reviewer.review,
            implement: implementer.run,
          }).pipe(
            Effect.provide(
              hostServices({
                ...fixture.config,
                checks: [passCheck],
                requiredChecks: [REQUIRED_CHECK],
                onWorktreeAcquired: (path) => Deferred.succeed(acquired, path).pipe(Effect.asVoid),
                onWorktreeReleased: (path) => Deferred.succeed(released, path).pipe(Effect.asVoid),
              }),
            ),
          );
          const fiber = yield* Effect.forkChild(loop);
          const worktree = yield* Deferred.await(acquired);
          yield* Fiber.interrupt(fiber);
          expect(yield* Deferred.await(released)).toBe(worktree);
          expect(yield* fs.exists(worktree)).toBe(false);
          expect(yield* decodeGitSha(yield* runGit(fixture.root, ["rev-parse", "feature"]))).toBe(
            fixture.h0,
          );
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
    60_000,
  );

  it.effect(
    "returns worktree release failures in the typed error channel",
    () =>
      withFixtureRepository((fixture) =>
        Effect.gen(function* () {
          const acquired = yield* Deferred.make<string>();
          const handoff = ReviewHandoff.make({
            version: 1,
            repository: fixture.config.repository,
            pullRequestNumber: fixture.config.pullRequestNumber,
            baseRef: fixture.config.baseRefName ?? fixture.config.baseRef,
            headRef: fixture.config.headRefName ?? fixture.config.headRef,
            reviewedHeadSha: fixture.h0,
            profileFingerprint: "d".repeat(64),
            reviewFingerprint: "e".repeat(64),
            verdict: "comment",
            findings: [],
          });
          const failure = yield* Effect.gen(function* () {
            const host = yield* RemediationHost;
            return yield* host.withWorktree(handoff, () =>
              Effect.gen(function* () {
                const root = yield* Deferred.await(acquired);
                yield* runGit(fixture.root, ["worktree", "remove", "--force", root]);
              }),
            );
          }).pipe(
            Effect.provide(
              localGitRemediationHostLayer({
                ...fixture.config,
                checks: [],
                requiredChecks: [],
                onWorktreeAcquired: (path) => Deferred.succeed(acquired, path).pipe(Effect.asVoid),
              }),
            ),
            Effect.flip,
          );
          expect(failure).toMatchObject({
            _tag: "WorkspaceOperationFailure",
            operation: "release remediation worktree",
          });
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
    60_000,
  );
});
