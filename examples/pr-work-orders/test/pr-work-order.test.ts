import { NodeServices } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import {
  Context,
  type Crypto,
  Deferred,
  Duration,
  Effect,
  Fiber,
  FileSystem,
  Layer,
  Ref,
  Schema,
  Stream,
} from "effect";
import { LanguageModel, Model, type Response } from "effect/unstable/ai";
import { ChildProcess } from "effect/unstable/process";

import {
  createWorkOrder,
  GitCommitSha,
  PullRequestWorkOrder,
  WorkOrderCheckResult,
  WorkOrderDigest,
  WorkOrderIdentity,
  WorkOrderReport,
  workOrderDigest,
  type PatchSnapshot,
  type RequiredCheckFailed,
  type StalePullRequestHead,
  type WorkOrderReleaseFailure,
  type WorkOrderTimeout,
  type WorkOrderValidationFailure,
  type WorkspaceOperationFailure,
} from "../src/contracts.ts";
import { prepareWorkOrder, runWorkOrder } from "../src/host.ts";
import { makeImplementationAgent, type WorkOrderMission } from "../src/implementation-agent.ts";
import { localGitWorkOrderHostLayer, type LocalGitWorkOrderConfig } from "../src/local-git.ts";
import {
  type HostCheck,
  type ImplementationWorkspace,
  type ImplementationWorkspaceService,
  WorkOrderAttemptPolicy,
  WorkOrderHost,
} from "../src/workspace.ts";

const FILE_PATH = "src/value.ts";
const OTHER_PATH = "notes.md";
const REQUIRED_CHECK = "fixture-check";
const ACTOR_ID = "42";
const decodeGitSha = (value: string) => Schema.decodeUnknownEffect(GitCommitSha)(value.trim());

type Equal<Left, Right> =
  (<Value>() => Value extends Left ? 1 : 2) extends <Value>() => Value extends Right ? 1 : 2
    ? true
    : false;
type Assert<Value extends true> = Value;

class ImplementationModelRequirement extends Context.Service<
  ImplementationModelRequirement,
  { readonly marker: "model-requirement" }
>()("@effect-agent/example-pr-work-orders/test/ImplementationModelRequirement") {}

const typedImplementationModel = Model.make(
  "scripted",
  "pr-work-order-type-proof",
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

const typedHostRun = runWorkOrder({
  order: null as unknown as PullRequestWorkOrder,
  implement: () => Effect.succeed(null as unknown as WorkOrderReport),
});
type TypedHostServices = Effect.Services<typeof typedHostRun>;
type TypedHostError = Effect.Error<typeof typedHostRun>;
type HostRequiresHost = Assert<Equal<Extract<TypedHostServices, WorkOrderHost>, WorkOrderHost>>;
type HostRequiresAttempts = Assert<
  Equal<Extract<TypedHostServices, WorkOrderAttemptPolicy>, WorkOrderAttemptPolicy>
>;
type HostRequiresCrypto = Assert<Equal<Extract<TypedHostServices, Crypto.Crypto>, Crypto.Crypto>>;
type HostKeepsValidation = Assert<
  Equal<Extract<TypedHostError, WorkOrderValidationFailure>, WorkOrderValidationFailure>
>;
type HostKeepsStale = Assert<
  Equal<Extract<TypedHostError, StalePullRequestHead>, StalePullRequestHead>
>;
type HostKeepsRelease = Assert<
  Equal<Extract<TypedHostError, WorkOrderReleaseFailure>, WorkOrderReleaseFailure>
>;
type HostKeepsTimeout = Assert<Equal<Extract<TypedHostError, WorkOrderTimeout>, WorkOrderTimeout>>;
type HostKeepsCheck = Assert<
  Equal<Extract<TypedHostError, RequiredCheckFailed>, RequiredCheckFailed>
>;
type HostUnknownExcluded = Assert<Equal<unknown extends TypedHostError ? true : false, false>>;

const typedPreparation = prepareWorkOrder({
  order: null as unknown as PullRequestWorkOrder,
  implement: () => Effect.succeed(null as unknown as WorkOrderReport),
});
type TypedPreparationServices = Effect.Services<typeof typedPreparation>;
type TypedPreparationError = Effect.Error<typeof typedPreparation>;
type PreparationRequiresHost = Assert<
  Equal<Extract<TypedPreparationServices, WorkOrderHost>, WorkOrderHost>
>;
type PreparationRequiresCrypto = Assert<
  Equal<Extract<TypedPreparationServices, Crypto.Crypto>, Crypto.Crypto>
>;
type PreparationExcludesAttempts = Assert<
  Equal<Extract<TypedPreparationServices, WorkOrderAttemptPolicy>, never>
>;
type PreparationExcludesCheckFailure = Assert<
  Equal<Extract<TypedPreparationError, RequiredCheckFailed>, never>
>;
type PreparationExcludesReleaseFailure = Assert<
  Equal<Extract<TypedPreparationError, WorkOrderReleaseFailure>, never>
>;
type PreparationUnknownExcluded = Assert<
  Equal<unknown extends TypedPreparationError ? true : false, false>
>;

describe("implementation Agent type proofs", () => {
  it("WO-003 WO-012 keeps model requirements and host failures typed while hiding workspace internals", () => {
    const proofs = {
      modelRequirementProof: true as ModelRequirementProof,
      workspaceExcludedProof: true as WorkspaceExcludedProof,
      unknownErrorExcludedProof: true as UnknownErrorExcludedProof,
      workspaceFailureExcludedProof: true as WorkspaceFailureExcludedProof,
      hostRequiresHost: true as HostRequiresHost,
      hostRequiresAttempts: true as HostRequiresAttempts,
      hostRequiresCrypto: true as HostRequiresCrypto,
      hostKeepsValidation: true as HostKeepsValidation,
      hostKeepsStale: true as HostKeepsStale,
      hostKeepsRelease: true as HostKeepsRelease,
      hostKeepsTimeout: true as HostKeepsTimeout,
      hostKeepsCheck: true as HostKeepsCheck,
      hostUnknownExcluded: true as HostUnknownExcluded,
      preparationRequiresHost: true as PreparationRequiresHost,
      preparationRequiresCrypto: true as PreparationRequiresCrypto,
      preparationExcludesAttempts: true as PreparationExcludesAttempts,
      preparationExcludesCheckFailure: true as PreparationExcludesCheckFailure,
      preparationExcludesReleaseFailure: true as PreparationExcludesReleaseFailure,
      preparationUnknownExcluded: true as PreparationUnknownExcluded,
    };
    expect(proofs).toEqual({
      modelRequirementProof: true,
      workspaceExcludedProof: true,
      unknownErrorExcludedProof: true,
      workspaceFailureExcludedProof: true,
      hostRequiresHost: true,
      hostRequiresAttempts: true,
      hostRequiresCrypto: true,
      hostKeepsValidation: true,
      hostKeepsStale: true,
      hostKeepsRelease: true,
      hostKeepsTimeout: true,
      hostKeepsCheck: true,
      hostUnknownExcluded: true,
      preparationRequiresHost: true,
      preparationRequiresCrypto: true,
      preparationExcludesAttempts: true,
      preparationExcludesCheckFailure: true,
      preparationExcludesReleaseFailure: true,
      preparationUnknownExcluded: true,
    });
  });
});

const scriptedUsage = { inputTokens: { total: 64 }, outputTokens: { total: 48 } };

const scriptedToolTurn = (
  ...calls: ReadonlyArray<Response.StreamPartEncoded>
): ReadonlyArray<Response.StreamPartEncoded> => [
  ...calls,
  { type: "finish", reason: "tool-calls", usage: scriptedUsage },
];

const scriptedFinalParts = (text: string): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "work-order" },
  { type: "text-delta", id: "work-order", delta: text },
  { type: "text-end", id: "work-order" },
  { type: "finish", reason: "stop", usage: scriptedUsage },
];

const makePromptKeyedModel = (
  name: string,
  decide: (promptJson: string) => ReadonlyArray<Response.StreamPartEncoded>,
) => ({
  model: Model.make(
    "scripted",
    name,
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: (request) =>
          Stream.unwrap(
            Effect.sync(() => Stream.fromIterable(decide(JSON.stringify(request.prompt)))),
          ),
      }),
    ),
  ),
});

const runGit = Effect.fn("prWorkOrderTest.runGit")(function* (
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
  readonly h0: GitCommitSha;
  readonly config: Omit<
    LocalGitWorkOrderConfig,
    "checks" | "requiredChecks" | "authorizedActorIds"
  >;
}

const withFixtureRepository = <A, E, R>(
  use: (fixture: FixtureRepository) => Effect.Effect<A, E, R>,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const parent = yield* fs.makeTempDirectoryScoped({ prefix: "pr-work-order-test-" });
      const root = `${parent}/repository`;
      yield* fs.makeDirectory(`${root}/src`, { recursive: true });
      yield* runGit(parent, ["init", "repository"]);
      yield* fs.writeFileString(`${root}/${FILE_PATH}`, "export const answer = 40;\n");
      yield* fs.writeFileString(`${root}/${OTHER_PATH}`, "tracked notes\n");
      yield* runGit(root, ["add", "--", FILE_PATH, OTHER_PATH]);
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
          headRef: "refs/heads/feature",
        },
      });
    }),
  );

type ScriptedMode = "fix" | "escape" | "not-applicable" | "needs-human" | "needs-human-with-patch";

const makeScriptedImplementer = (mode: ScriptedMode) => {
  let mission: WorkOrderMission | undefined;
  let patch: PatchSnapshot | undefined;
  const observedChecks: Array<WorkOrderCheckResult> = [];
  let invocations = 0;
  const scripted = makePromptKeyedModel(`pr-work-order-${mode}`, (prompt) => {
    if (prompt.includes("impl-inspect-1")) {
      if (mission === undefined || patch === undefined) throw new Error("missing scripted state");
      const disposition =
        mode === "not-applicable"
          ? "not-applicable"
          : mode === "needs-human" || mode === "needs-human-with-patch"
            ? "needs-human"
            : "fixed";
      return scriptedFinalParts(
        JSON.stringify(
          Schema.encodeSync(WorkOrderReport)(
            WorkOrderReport.make({
              workOrderDigest: mission.workOrderDigest,
              headSha: mission.order.headSha,
              disposition,
              changedPaths:
                disposition === "fixed" || mode === "needs-human-with-patch"
                  ? patch.changedPaths
                  : [],
              checks: disposition === "fixed" ? observedChecks : [],
              ...(disposition === "fixed" ? { patchDigest: patch.digest } : {}),
              summary:
                disposition === "fixed"
                  ? "Corrected the exported value."
                  : disposition === "not-applicable"
                    ? "The instruction is already satisfied."
                    : "A human decision is required.",
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
    if (mode === "not-applicable" || mode === "needs-human") {
      if (prompt.includes("impl-read-1")) {
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
        id: "impl-read-1",
        name: "read_workspace_file",
        params: { path: FILE_PATH },
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
  const implement = (input: WorkOrderMission, workspace: ImplementationWorkspace) => {
    invocations += 1;
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
  return {
    implement,
    invocations: () => invocations,
  };
};

const fakeDigest = Schema.decodeUnknownSync(WorkOrderDigest)("ab".repeat(32));

const passCheck: HostCheck = {
  name: REQUIRED_CHECK,
  run: () =>
    Effect.succeed(
      WorkOrderCheckResult.make({
        name: REQUIRED_CHECK,
        status: "passed",
        summary: "fixture validation passed",
      }),
    ),
};

const identityFor = (
  fixture: FixtureRepository,
  overrides?: {
    readonly eventId?: string;
    readonly actorId?: string;
    readonly actorLogin?: string;
    readonly path?: string;
    readonly commitSha?: GitCommitSha;
    readonly body?: string;
  },
) =>
  WorkOrderIdentity.make({
    version: 1,
    repository: fixture.config.repository,
    pullRequestNumber: fixture.config.pullRequestNumber,
    headSha: fixture.h0,
    source: {
      commentId: "IC_1",
      threadId: "PRRT_1",
      authorId: "7",
      authorLogin: "reviewer",
      commitSha: overrides?.commitSha ?? fixture.h0,
      path: overrides?.path ?? FILE_PATH,
      lineRange: { startLine: 1, endLine: 1 },
      body: overrides?.body ?? "The exported answer must be 42.",
      suggestion: "export const answer = 42;",
    },
    dispatch: {
      kind: "mention",
      eventId: overrides?.eventId ?? "evt_1",
      actorId: overrides?.actorId ?? ACTOR_ID,
      actorLogin: overrides?.actorLogin ?? "dan",
    },
  });

const orderFor = (fixture: FixtureRepository, overrides?: Parameters<typeof identityFor>[1]) =>
  createWorkOrder(identityFor(fixture, overrides));

const hostServices = (config: LocalGitWorkOrderConfig) =>
  Layer.mergeAll(localGitWorkOrderHostLayer(config), WorkOrderAttemptPolicy.layerMemory);

const authorizedConfig = (
  fixture: FixtureRepository,
  rest: Pick<LocalGitWorkOrderConfig, "checks" | "requiredChecks"> &
    Partial<LocalGitWorkOrderConfig>,
): LocalGitWorkOrderConfig => ({
  ...fixture.config,
  authorizedActorIds: [ACTOR_ID],
  ...rest,
});

describe("PR work-order host", () => {
  it.effect(
    "WO-001 WO-005 WO-006 TEST-017 publishes one host-validated commit for a fixed work order",
    () =>
      withFixtureRepository((fixture) =>
        Effect.gen(function* () {
          const implementer = makeScriptedImplementer("fix");
          const order = yield* orderFor(fixture);
          const digest = yield* workOrderDigest(order);
          const publication = yield* runWorkOrder({
            order,
            implement: implementer.implement,
          }).pipe(
            Effect.provide(
              hostServices(
                authorizedConfig(fixture, {
                  checks: [passCheck],
                  requiredChecks: [REQUIRED_CHECK],
                }),
              ),
            ),
          );
          expect(publication._tag).toBe("published");
          if (publication._tag !== "published") return;
          expect(publication.workOrderDigest).toBe(digest);
          expect(publication.previousHeadSha).toBe(fixture.h0);
          expect(publication.publishedHeadSha).not.toBe(fixture.h0);
          expect(publication.changedPaths).toEqual([FILE_PATH]);
          expect(
            (yield* runGit(fixture.root, [
              "show",
              `${publication.publishedHeadSha}:${FILE_PATH}`,
            ])).trim(),
          ).toBe("export const answer = 42;");
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
    60_000,
  );

  it.effect(
    "WO-005 TEST-017 settles not-applicable and needs-human without a commit and rejects an accompanying patch",
    () =>
      withFixtureRepository((fixture) =>
        Effect.gen(function* () {
          const notApplicable = makeScriptedImplementer("not-applicable");
          const settled = yield* runWorkOrder({
            order: yield* orderFor(fixture, { eventId: "evt-na" }),
            implement: notApplicable.implement,
          }).pipe(
            Effect.provide(
              hostServices(
                authorizedConfig(fixture, {
                  checks: [passCheck],
                  requiredChecks: [REQUIRED_CHECK],
                }),
              ),
            ),
          );
          expect(settled).toMatchObject({ _tag: "settled", disposition: "not-applicable" });
          expect(yield* decodeGitSha(yield* runGit(fixture.root, ["rev-parse", "feature"]))).toBe(
            fixture.h0,
          );

          const needsHuman = makeScriptedImplementer("needs-human");
          const human = yield* runWorkOrder({
            order: yield* orderFor(fixture, { eventId: "evt-human" }),
            implement: needsHuman.implement,
          }).pipe(
            Effect.provide(
              hostServices(
                authorizedConfig(fixture, {
                  checks: [passCheck],
                  requiredChecks: [REQUIRED_CHECK],
                }),
              ),
            ),
          );
          expect(human).toMatchObject({ _tag: "settled", disposition: "needs-human" });

          const patched = makeScriptedImplementer("needs-human-with-patch");
          const rejected = yield* runWorkOrder({
            order: yield* orderFor(fixture, { eventId: "evt-human-patch" }),
            implement: patched.implement,
          }).pipe(
            Effect.provide(
              hostServices(
                authorizedConfig(fixture, {
                  checks: [passCheck],
                  requiredChecks: [REQUIRED_CHECK],
                }),
              ),
            ),
            Effect.flip,
          );
          expect(rejected).toMatchObject({
            _tag: "WorkOrderValidationFailure",
            reason: "unexpected-patch",
          });
          expect(yield* decodeGitSha(yield* runGit(fixture.root, ["rev-parse", "feature"]))).toBe(
            fixture.h0,
          );
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
    60_000,
  );

  it.effect(
    "WO-002 WO-006 TEST-017 never publishes malformed, escaping, or unallowlisted paths",
    () =>
      withFixtureRepository((fixture) =>
        Effect.gen(function* () {
          const services = hostServices(
            authorizedConfig(fixture, {
              checks: [passCheck],
              requiredChecks: [REQUIRED_CHECK],
            }),
          );
          const implementer = makeScriptedImplementer("fix");
          for (const path of ["../outside.ts", "/tmp/x.ts", "src/foo\nbar.ts", "C:\\windows.ts"]) {
            const failure = yield* runWorkOrder({
              order: yield* orderFor(fixture, { eventId: `evt-path-${path}`, path }),
              implement: implementer.implement,
            }).pipe(Effect.provide(services), Effect.flip);
            expect(failure._tag).toBe("WorkOrderRejected");
          }
          expect(implementer.invocations()).toBe(0);

          const escape = makeScriptedImplementer("escape");
          const escaped = yield* runWorkOrder({
            order: yield* orderFor(fixture, { eventId: "evt-escape" }),
            implement: escape.implement,
          }).pipe(Effect.provide(services), Effect.flip);
          expect(escaped).toMatchObject({
            _tag: "WorkOrderValidationFailure",
            reason: "empty-patch",
          });

          const outside = makeScriptedImplementer("fix");
          const leakingCheck: HostCheck = {
            name: REQUIRED_CHECK,
            run: (root) =>
              Effect.gen(function* () {
                const fs = yield* FileSystem.FileSystem;
                yield* fs.writeFileString(`${root}/${OTHER_PATH}`, "leaked\n");
                return WorkOrderCheckResult.make({
                  name: REQUIRED_CHECK,
                  status: "passed",
                  summary: "wrote outside the allowlist",
                });
              }).pipe(Effect.orDie, Effect.provide(NodeServices.layer)),
          };
          const leaked = yield* runWorkOrder({
            order: yield* orderFor(fixture, { eventId: "evt-outside" }),
            implement: outside.implement,
          }).pipe(
            Effect.provide(
              hostServices(
                authorizedConfig(fixture, {
                  checks: [leakingCheck],
                  requiredChecks: [REQUIRED_CHECK],
                }),
              ),
            ),
            Effect.flip,
          );
          expect(leaked).toMatchObject({
            _tag: "WorkOrderValidationFailure",
            reason: "path-not-allowed",
          });
          expect(yield* decodeGitSha(yield* runGit(fixture.root, ["rev-parse", "feature"]))).toBe(
            fixture.h0,
          );
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
    60_000,
  );

  it.effect(
    "WO-006 TEST-017 rejects a stale digest, false check claim, failed required check, and check-mutated patch",
    () =>
      withFixtureRepository((fixture) =>
        Effect.gen(function* () {
          const services = hostServices(
            authorizedConfig(fixture, {
              checks: [passCheck],
              requiredChecks: [REQUIRED_CHECK],
            }),
          );
          const stale = makeScriptedImplementer("fix");
          const staleDigest = yield* runWorkOrder({
            order: yield* orderFor(fixture, { eventId: "evt-stale-digest" }),
            implement: (mission, workspace) =>
              stale.implement(mission, workspace).pipe(
                Effect.map((report) =>
                  WorkOrderReport.make({
                    ...report,
                    workOrderDigest: fakeDigest,
                  }),
                ),
              ),
          }).pipe(Effect.provide(services), Effect.flip);
          expect(staleDigest).toMatchObject({
            _tag: "WorkOrderValidationFailure",
            reason: "work-order-mismatch",
          });

          const falseCheck = makeScriptedImplementer("fix");
          const invented = yield* runWorkOrder({
            order: yield* orderFor(fixture, { eventId: "evt-false-check" }),
            implement: (mission, workspace) =>
              falseCheck.implement(mission, workspace).pipe(
                Effect.map((report) =>
                  WorkOrderReport.make({
                    ...report,
                    checks: [
                      WorkOrderCheckResult.make({
                        name: REQUIRED_CHECK,
                        status: "passed",
                        summary: "invented",
                      }),
                    ],
                  }),
                ),
              ),
          }).pipe(Effect.provide(services), Effect.flip);
          expect(invented).toMatchObject({
            _tag: "WorkOrderValidationFailure",
            reason: "check-results-mismatch",
          });

          const failing = makeScriptedImplementer("fix");
          const invocations = yield* Ref.make(0);
          const failingCheck: HostCheck = {
            name: REQUIRED_CHECK,
            run: () =>
              Ref.updateAndGet(invocations, (count) => count + 1).pipe(
                Effect.map((count) =>
                  WorkOrderCheckResult.make({
                    name: REQUIRED_CHECK,
                    status: count === 1 ? "passed" : "failed",
                    summary: count === 1 ? "model check passed" : "host re-check failed",
                  }),
                ),
              ),
          };
          const failedCheck = yield* runWorkOrder({
            order: yield* orderFor(fixture, { eventId: "evt-failed-check" }),
            implement: failing.implement,
          }).pipe(
            Effect.provide(
              hostServices(
                authorizedConfig(fixture, {
                  checks: [failingCheck],
                  requiredChecks: [REQUIRED_CHECK],
                }),
              ),
            ),
            Effect.flip,
          );
          expect(failedCheck._tag).toBe("RequiredCheckFailed");

          const mutating = makeScriptedImplementer("fix");
          const mutationCount = yield* Ref.make(0);
          const mutatingCheck: HostCheck = {
            name: REQUIRED_CHECK,
            run: (root) =>
              Effect.gen(function* () {
                const count = yield* Ref.updateAndGet(mutationCount, (value) => value + 1);
                if (count > 1) {
                  const fs = yield* FileSystem.FileSystem;
                  yield* fs.writeFileString(`${root}/${FILE_PATH}`, "export const answer = 0;\n");
                }
                return WorkOrderCheckResult.make({
                  name: REQUIRED_CHECK,
                  status: "passed",
                  summary: "mutated after settlement",
                });
              }).pipe(Effect.orDie, Effect.provide(NodeServices.layer)),
          };
          const mutated = yield* runWorkOrder({
            order: yield* orderFor(fixture, { eventId: "evt-mutated" }),
            implement: mutating.implement,
          }).pipe(
            Effect.provide(
              hostServices(
                authorizedConfig(fixture, {
                  checks: [mutatingCheck],
                  requiredChecks: [REQUIRED_CHECK],
                }),
              ),
            ),
            Effect.flip,
          );
          expect(mutated).toMatchObject({
            _tag: "WorkOrderValidationFailure",
            reason: "check-mutated-patch",
          });
          expect(yield* decodeGitSha(yield* runGit(fixture.root, ["rev-parse", "feature"]))).toBe(
            fixture.h0,
          );
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
    60_000,
  );

  it.effect(
    "WO-001 WO-002 rejects an older source commit, unauthorized actor, and foreign repository",
    () =>
      withFixtureRepository((fixture) =>
        Effect.gen(function* () {
          const implementer = makeScriptedImplementer("fix");
          const services = hostServices(
            authorizedConfig(fixture, {
              checks: [passCheck],
              requiredChecks: [REQUIRED_CHECK],
            }),
          );
          const older = yield* decodeGitSha("ab".repeat(20));
          const staleSource = yield* runWorkOrder({
            order: yield* orderFor(fixture, { eventId: "evt-old-source", commitSha: older }),
            implement: implementer.implement,
          }).pipe(Effect.provide(services), Effect.flip);
          expect(staleSource).toMatchObject({
            _tag: "WorkOrderRejected",
            reason: "source comment is not anchored to the current pull-request head",
          });

          const unauthorized = yield* runWorkOrder({
            order: yield* orderFor(fixture, {
              eventId: "evt-unauth",
              actorId: "99",
              actorLogin: "dan",
            }),
            implement: implementer.implement,
          }).pipe(Effect.provide(services), Effect.flip);
          expect(unauthorized).toMatchObject({
            _tag: "WorkOrderRejected",
            reason: "dispatch actor is not authorized",
          });

          const foreign = yield* runWorkOrder({
            order: PullRequestWorkOrder.make({
              ...(yield* orderFor(fixture, { eventId: "evt-foreign" })),
              repository: "other/repo",
            }),
            implement: implementer.implement,
          }).pipe(Effect.provide(services), Effect.flip);
          expect(foreign._tag).toBe("WorkOrderRejected");

          const forged = yield* runWorkOrder({
            order: PullRequestWorkOrder.make({
              ...(yield* orderFor(fixture, { eventId: "evt-forged-id" })),
              workOrderId: "forged-work-order-id",
            }),
            implement: implementer.implement,
          }).pipe(Effect.provide(services), Effect.flip);
          expect(forged).toMatchObject({
            _tag: "WorkOrderRejected",
            reason: "work-order identity does not match the admitted snapshot",
          });

          const badSupport = yield* runWorkOrder({
            order: yield* orderFor(fixture, { eventId: "evt-bad-support" }),
            implement: implementer.implement,
          }).pipe(
            Effect.provide(
              hostServices(
                authorizedConfig(fixture, {
                  checks: [passCheck],
                  requiredChecks: [REQUIRED_CHECK],
                  allowedSupportPaths: ["../escape.ts"],
                }),
              ),
            ),
            Effect.flip,
          );
          expect(badSupport).toMatchObject({
            _tag: "WorkOrderRejected",
            reason: "support path rejected: path segments must not be empty, '.' or '..'",
          });
          expect(implementer.invocations()).toBe(0);
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
    60_000,
  );

  it.effect(
    "WO-004 TEST-017 makes duplicate delivery idempotent and gives a new dispatch a distinct identity",
    () =>
      withFixtureRepository((fixture) =>
        Effect.gen(function* () {
          const implementer = makeScriptedImplementer("not-applicable");
          const services = hostServices(
            authorizedConfig(fixture, {
              checks: [passCheck],
              requiredChecks: [REQUIRED_CHECK],
            }),
          );
          const firstOrder = yield* orderFor(fixture, { eventId: "evt-dup" });
          const replayedOrder = yield* orderFor(fixture, { eventId: "evt-dup" });
          expect(firstOrder.workOrderId).toBe(replayedOrder.workOrderId);
          yield* Effect.gen(function* () {
            const first = yield* runWorkOrder({
              order: firstOrder,
              implement: implementer.implement,
            });
            const duplicate = yield* runWorkOrder({
              order: replayedOrder,
              implement: implementer.implement,
            });
            expect(duplicate).toEqual(first);
            expect(implementer.invocations()).toBe(1);

            const nextOrder = yield* orderFor(fixture, { eventId: "evt-new" });
            expect(nextOrder.workOrderId).not.toBe(firstOrder.workOrderId);
            const second = yield* runWorkOrder({
              order: nextOrder,
              implement: implementer.implement,
            });
            expect(second._tag).toBe("settled");
            expect(implementer.invocations()).toBe(2);
          }).pipe(Effect.provide(services));

          const publisher = makeScriptedImplementer("fix");
          const publishServices = hostServices(
            authorizedConfig(fixture, {
              checks: [passCheck],
              requiredChecks: [REQUIRED_CHECK],
            }),
          );
          const publishedOrder = yield* orderFor(fixture, { eventId: "evt-pub-dup" });
          yield* Effect.gen(function* () {
            const published = yield* runWorkOrder({
              order: publishedOrder,
              implement: publisher.implement,
            });
            expect(published._tag).toBe("published");
            const replayed = yield* runWorkOrder({
              order: publishedOrder,
              implement: publisher.implement,
            });
            expect(replayed).toEqual(published);
            expect(publisher.invocations()).toBe(1);
          }).pipe(Effect.provide(publishServices));
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
    60_000,
  );

  it.effect(
    "WO-007 TEST-017 rejects publication when the PR head moves before compare-and-swap",
    () =>
      withFixtureRepository((fixture) =>
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const implementer = makeScriptedImplementer("fix");
          const moved = yield* Ref.make(false);
          const movingCheck: HostCheck = {
            name: REQUIRED_CHECK,
            run: () =>
              Effect.scoped(
                Effect.gen(function* () {
                  if (!(yield* Ref.get(moved))) {
                    yield* Ref.set(moved, true);
                    yield* fs.writeFileString(
                      `${fixture.root}/${FILE_PATH}`,
                      "export const answer = 99;\n",
                    );
                    yield* runGit(fixture.root, ["add", "--", FILE_PATH]);
                    yield* runGit(fixture.root, [
                      "-c",
                      "user.name=fixture",
                      "-c",
                      "user.email=fixture@localhost",
                      "commit",
                      "-m",
                      "move head",
                    ]);
                  }
                  return WorkOrderCheckResult.make({
                    name: REQUIRED_CHECK,
                    status: "passed",
                    summary: "moved",
                  });
                }),
              ).pipe(Effect.orDie, Effect.provide(NodeServices.layer)),
          };
          const failure = yield* runWorkOrder({
            order: yield* orderFor(fixture),
            implement: implementer.implement,
          }).pipe(
            Effect.provide(
              hostServices(
                authorizedConfig(fixture, {
                  checks: [movingCheck],
                  requiredChecks: [REQUIRED_CHECK],
                }),
              ),
            ),
            Effect.flip,
          );
          expect(failure._tag).toBe("StalePullRequestHead");
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
    60_000,
  );

  it.effect(
    "WO-004 WO-007 completes a claimed stale-head failure so duplicate delivery does not hang",
    () =>
      withFixtureRepository((fixture) =>
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          yield* fs.writeFileString(`${fixture.root}/${FILE_PATH}`, "export const answer = 99;\n");
          yield* runGit(fixture.root, ["add", "--", FILE_PATH]);
          yield* runGit(fixture.root, [
            "-c",
            "user.name=fixture",
            "-c",
            "user.email=fixture@localhost",
            "commit",
            "-m",
            "advance head before admission",
          ]);
          const implementer = makeScriptedImplementer("fix");
          const services = hostServices(
            authorizedConfig(fixture, {
              checks: [passCheck],
              requiredChecks: [REQUIRED_CHECK],
            }),
          );
          const order = yield* orderFor(fixture);
          yield* Effect.gen(function* () {
            const first = yield* runWorkOrder({
              order,
              implement: implementer.implement,
            }).pipe(Effect.flip);
            expect(first._tag).toBe("StalePullRequestHead");
            const duplicate = yield* runWorkOrder({
              order,
              implement: implementer.implement,
            }).pipe(Effect.flip);
            expect(duplicate._tag).toBe("StalePullRequestHead");
            expect(implementer.invocations()).toBe(0);
          }).pipe(Effect.provide(services));
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
    60_000,
  );

  it.effect(
    "WO-008 TEST-017 releases the scoped worktree when implementation is interrupted",
    () =>
      withFixtureRepository((fixture) =>
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const acquired = yield* Deferred.make<string>();
          const released = yield* Deferred.make<string>();
          const fiber = yield* Effect.forkChild(
            runWorkOrder({
              order: yield* orderFor(fixture, { eventId: "evt-interrupt" }),
              implement: () => Effect.never,
            }).pipe(
              Effect.provide(
                hostServices(
                  authorizedConfig(fixture, {
                    checks: [passCheck],
                    requiredChecks: [REQUIRED_CHECK],
                    onWorktreeAcquired: (path) =>
                      Deferred.succeed(acquired, path).pipe(Effect.asVoid),
                    onWorktreeReleased: (path) =>
                      Deferred.succeed(released, path).pipe(Effect.asVoid),
                  }),
                ),
              ),
            ),
          );
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
    "WO-008 TEST-017 releases the scoped worktree on timeout without publishing",
    () =>
      withFixtureRepository((fixture) =>
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const acquired = yield* Deferred.make<string>();
          const released = yield* Deferred.make<string>();
          const timeout = yield* runWorkOrder({
            order: yield* orderFor(fixture, { eventId: "evt-timeout" }),
            implement: () => Effect.never,
            timeout: Duration.zero,
          }).pipe(
            Effect.provide(
              hostServices(
                authorizedConfig(fixture, {
                  checks: [passCheck],
                  requiredChecks: [REQUIRED_CHECK],
                  onWorktreeAcquired: (path) =>
                    Deferred.succeed(acquired, path).pipe(Effect.asVoid),
                  onWorktreeReleased: (path) =>
                    Deferred.succeed(released, path).pipe(Effect.asVoid),
                }),
              ),
            ),
            Effect.flip,
          );
          expect(timeout._tag).toBe("WorkOrderTimeout");
          const worktree = yield* Deferred.await(acquired);
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
    "WO-007 WO-008 TEST-017 keeps release failures typed, including the post-publication case",
    () =>
      withFixtureRepository((fixture) =>
        Effect.gen(function* () {
          const acquired = yield* Deferred.make<string>();
          const prePublish = yield* Effect.gen(function* () {
            const host = yield* WorkOrderHost;
            return yield* host.withWorktree(yield* orderFor(fixture, { eventId: "evt-pre" }), () =>
              Effect.gen(function* () {
                const root = yield* Deferred.await(acquired);
                yield* runGit(fixture.root, ["worktree", "remove", "--force", root]);
              }),
            );
          }).pipe(
            Effect.provide(
              localGitWorkOrderHostLayer(
                authorizedConfig(fixture, {
                  checks: [],
                  requiredChecks: [],
                  onWorktreeAcquired: (path) =>
                    Deferred.succeed(acquired, path).pipe(Effect.asVoid),
                }),
              ),
            ),
            Effect.flip,
          );
          expect(prePublish).toMatchObject({
            _tag: "WorkspaceOperationFailure",
            operation: "release work-order worktree",
          });

          const implementer = makeScriptedImplementer("fix");
          const postPublish = yield* runWorkOrder({
            order: yield* orderFor(fixture, { eventId: "evt-post" }),
            implement: implementer.implement,
          }).pipe(
            Effect.provide(
              hostServices(
                authorizedConfig(fixture, {
                  checks: [passCheck],
                  requiredChecks: [REQUIRED_CHECK],
                  onBeforeWorktreeRelease: (path) =>
                    Effect.scoped(
                      runGit(fixture.root, ["worktree", "remove", "--force", path]),
                    ).pipe(Effect.asVoid, Effect.orDie, Effect.provide(NodeServices.layer)),
                }),
              ),
            ),
            Effect.flip,
          );
          expect(postPublish._tag).toBe("WorkOrderReleaseFailure");
          if (postPublish._tag === "WorkOrderReleaseFailure") {
            expect(postPublish.publication?._tag).toBe("published");
            expect(postPublish.observedHeadSha).toBe(postPublish.publication?.publishedHeadSha);
            expect(postPublish.observedHeadSha).not.toBe(fixture.h0);
          }
        }),
      ).pipe(Effect.provide(NodeServices.layer)),
    60_000,
  );
});
