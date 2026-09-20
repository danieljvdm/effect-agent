import { MemorySubmissionLedgerLive } from "@effect-agent/storage-memory/memory-submission-ledger";
import { MemoryThreadStoreLive } from "@effect-agent/storage-memory/memory-thread-store";
import { NodeCrypto } from "@effect/platform-node";
import { describe, expect, it, layer } from "@effect/vitest";
import {
  Cause,
  Context,
  DateTime,
  Deferred,
  Duration,
  Effect,
  Exit,
  Fiber,
  Layer,
  Option,
  Ref,
  Schema,
  Stream,
} from "effect";
import * as Agent from "effect-agent/agent";
import { AgentPolicy } from "effect-agent/agent-policy";
import { compileRegistrations, type AgentAttemptContext } from "effect-agent/agent-registration";
import {
  ApprovalApproved,
  ApprovalAuditMemoryLive,
  ApprovalResolver,
  ApprovalResolverError,
} from "effect-agent/approval";
import {
  ApprovalSuspensionError,
  DurableAgentRuntime,
  DurableApprovalResolver,
  DurableApprovalSuspension,
  DurableRuntimeConfig,
  type DurableSubmitOptions,
} from "effect-agent/durable-agent-runtime";
import {
  DurableRuntimeFailpointError,
  type DurableRuntimeFailpointLocation,
} from "effect-agent/durable-failpoint";
import { ThreadId, RunId, ToolCallId, TurnId, type SubmissionId } from "effect-agent/identifiers";
import {
  DefinitionDigestInput,
  DefinitionDigests,
  DeploymentId,
  Digest,
  ProducerId,
  type CanonicalRecordEnvelope,
} from "effect-agent/records";
import { StructuralRedactorLive } from "effect-agent/redaction";
import { toDurableRunApprovalHook } from "effect-agent/run-hooks";
import { runIdForSubmission } from "effect-agent/run-journal";
import {
  RunToolAuthorization,
  type RunApprovalDecision,
  type RunApprovalHook,
  type RunApprovalRequest,
} from "effect-agent/run-options";
import {
  AbortCommand,
  ApprovalDecisionCommand,
  IdempotencyKey,
  Principal,
  SubmissionLedger,
  SubmissionLookupById,
  ClaimRequest,
  DEFAULT_OWNERSHIP_LEASE_DURATION,
  RecoverySnapshotRequest,
} from "effect-agent/submission-ledger";
import { DurableRuntimeFailpointTestControl } from "effect-agent/testing/durable-failpoint-test-control";
import { ThreadRead, ThreadStore } from "effect-agent/thread-store";
import { ToolReconciler } from "effect-agent/tool-reconciler";
import { WakeScheduler } from "effect-agent/wake-scheduler";
import { TestClock } from "effect/testing";
import { LanguageModel, Model, Response, Tool, Toolkit, type Prompt } from "effect/unstable/ai";

const SHA_A = Schema.decodeSync(Digest)("a".repeat(64));
const PRINCIPAL = Schema.decodeSync(Principal)("principal-durable-approval");
const DIGESTS = DefinitionDigests.make({ agent: SHA_A, model: SHA_A, tools: SHA_A });
const decodeThreadId = Schema.decodeSync(ThreadId);
const decodeIdempotencyKey = Schema.decodeSync(IdempotencyKey);
const decodeToolCallId = Schema.decodeSync(ToolCallId);

const submitOptions = (threadId: string, idempotencyKey: string): DurableSubmitOptions => ({
  threadId: decodeThreadId(threadId),
  principal: PRINCIPAL,
  idempotencyKey: decodeIdempotencyKey(idempotencyKey),
  definitions: DIGESTS,
});

const usage = { inputTokens: {}, outputTokens: {} };

const finalParts = (text: string): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: text },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage },
];

const toolTurn = (
  ...calls: ReadonlyArray<Response.StreamPartEncoded>
): ReadonlyArray<Response.StreamPartEncoded> => [
  ...calls,
  { type: "finish", reason: "tool-calls", usage },
];

const toolCall = (id: string, name: string, params: unknown): Response.StreamPartEncoded => ({
  type: "tool-call",
  id,
  name,
  params,
  providerExecuted: false,
});

/**
 * Scripted model whose call counter and captured request prompts live OUTSIDE the Model Layer,
 * so they survive Layer rebuilds across Attempts (each Attempt provides the Model afresh).
 */
const makeScriptedModel = (
  script: (call: number) => ReadonlyArray<Response.StreamPartEncoded>,
  onRelease?: Effect.Effect<void>,
) =>
  Effect.gen(function* () {
    const calls = yield* Ref.make(0);
    const prompts: Array<Prompt.Prompt> = [];

    const model = Model.make(
      "scripted",
      "durable-approval-test",
      Layer.effect(
        LanguageModel.LanguageModel,
        Effect.gen(function* () {
          if (onRelease !== undefined) yield* Effect.addFinalizer(() => onRelease);

          return yield* LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: (request) =>
              Stream.unwrap(
                Ref.getAndUpdate(calls, (call) => call + 1).pipe(
                  Effect.map((call) => {
                    prompts.push(request.prompt);

                    return Stream.fromIterable(script(call));
                  }),
                ),
              ),
          });
        }),
      ),
    );

    return { model, prompts };
  });

const policy = AgentPolicy.make({
  maxTurns: 3,
  maxToolCalls: 4,
  maxDuration: "30 seconds",
  toolConcurrency: 2,
});

/** Approval-gated booking Tool; unannotated → fail-closed `uncertain` execution class. */
const BookApproval = Tool.make("book", {
  parameters: Schema.Struct({ ref: Schema.String }),
  success: Schema.Struct({ confirmation: Schema.String }),
  needsApproval: true,
});

const approvalTools = Toolkit.make(BookApproval);

const approvalDefinition = Agent.make("durable-approval-book", {
  input: Schema.Struct({ question: Schema.String }),
  output: Schema.Struct({ answer: Schema.String }),
  instructions: "Book after approval.",
  toolkit: approvalTools,
  policy,
});

/** Per-ref supplier call counters that survive Tool-Layer rebuilds across Attempts. */
const makeBookDesk = Effect.gen(function* () {
  const calls = yield* Ref.make<ReadonlyMap<string, number>>(new Map());

  const toolLayer = approvalTools.toLayer({
    book: ({ ref }) =>
      Ref.update(calls, (current) => new Map(current).set(ref, (current.get(ref) ?? 0) + 1)).pipe(
        Effect.as({ confirmation: `confirmed-${ref}` }),
      ),
  });

  const count = (ref: string) => Ref.get(calls).pipe(Effect.map((m) => m.get(ref) ?? 0));

  return { toolLayer, count };
});

const PRODUCER_ID = Schema.decodeSync(ProducerId)("producer-durable-approval");

const configLayer = DurableRuntimeConfig.layer({
  deploymentId: Schema.decodeSync(DeploymentId)("deployment-durable-approval"),
  producerId: PRODUCER_ID,
  settlementPollInterval: Duration.millis(100),
  leaseRenewalInterval: Duration.seconds(5),
  abortPollInterval: Duration.millis(100),
});

const baseLayer = Layer.mergeAll(
  MemorySubmissionLedgerLive,
  MemoryThreadStoreLive,
  WakeScheduler.layerNoop,
  DurableRuntimeFailpointTestControl.layer,
  ToolReconciler.uncertain,
  configLayer,
).pipe(Layer.provideMerge(NodeCrypto.layer));

/** Fail-closed default: no `DurableApprovalResolver` — undecided approvals suspend durably. */
const testLayer = DurableAgentRuntime.layer.pipe(Layer.provideMerge(baseLayer));

/** Test control replacing the policy-auto approval delegate per test. */
class ApprovalDelegateTestControl extends Context.Service<
  ApprovalDelegateTestControl,
  {
    readonly set: (
      handler: (request: RunApprovalRequest) => Effect.Effect<RunApprovalDecision>,
    ) => Effect.Effect<void>;
    readonly reset: Effect.Effect<void>;
  }
>()("@effect-agent/testing/ApprovalDelegateTestControl") {}

const unresolvedDelegate = (): Effect.Effect<RunApprovalDecision> =>
  Effect.succeed({ _tag: "unresolved" });

const approvalDelegateLayer = Layer.effectContext(
  Effect.gen(function* () {
    const handler =
      yield* Ref.make<(request: RunApprovalRequest) => Effect.Effect<RunApprovalDecision>>(
        unresolvedDelegate,
      );

    const hook: RunApprovalHook<never, never> = {
      request: (request) => Ref.get(handler).pipe(Effect.flatMap((current) => current(request))),
    };

    return Context.make(DurableApprovalResolver, hook).pipe(
      Context.add(
        ApprovalDelegateTestControl,
        ApprovalDelegateTestControl.of({
          set: (next) => Ref.set(handler, next),
          reset: Ref.set(handler, unresolvedDelegate),
        }),
      ),
    );
  }),
);

const delegateTestLayer = DurableAgentRuntime.layer.pipe(
  Layer.provideMerge(Layer.mergeAll(baseLayer, approvalDelegateLayer)),
);

const readLog = (threadId: string) =>
  Effect.gen(function* () {
    const store = yield* ThreadStore;

    return yield* Stream.runCollect(
      store.read(
        ThreadRead.make({
          threadId: decodeThreadId(threadId),
          limit: 1_024,
        }),
      ),
    );
  });

const logTags = (records: ReadonlyArray<CanonicalRecordEnvelope>): ReadonlyArray<string> =>
  records.map((envelope) => envelope.record.payload._tag);

const recordsById = (records: ReadonlyArray<CanonicalRecordEnvelope>) =>
  new Map(records.map((envelope) => [envelope.record.recordId as string, envelope]));

const lookupState = (submissionId: SubmissionId) =>
  Effect.gen(function* () {
    const ledger = yield* SubmissionLedger;
    const snapshot = yield* ledger.lookup(SubmissionLookupById.make({ submissionId }));

    expect(Option.isSome(snapshot)).toBe(true);
    if (Option.isNone(snapshot)) throw new Error("Expected the Submission to exist");

    return snapshot.value.state;
  });

const armFailpoint = (location: DurableRuntimeFailpointLocation) =>
  Effect.gen(function* () {
    const control = yield* DurableRuntimeFailpointTestControl;

    yield* control.setHandler((hitLocation) =>
      hitLocation === location
        ? Effect.fail(DurableRuntimeFailpointError.make({ location: hitLocation }))
        : Effect.void,
    );
  });

const clearFailpoint = Effect.gen(function* () {
  const control = yield* DurableRuntimeFailpointTestControl;

  yield* control.clear;
});

const failureTag = <A, E>(exit: Exit.Exit<A, E>): string => {
  expect(Exit.isFailure(exit)).toBe(true);
  if (Exit.isSuccess(exit)) throw new Error("Expected the Effect to fail");
  const failure = Cause.findErrorOption(exit.cause);

  expect(Option.isSome(failure)).toBe(true);
  if (Option.isNone(failure)) throw new Error("Expected a typed failure");
  const error: unknown = failure.value;

  return typeof error === "object" && error !== null && "_tag" in error
    ? String(error._tag)
    : "unknown";
};

const approveCommand = (
  submissionId: SubmissionId,
  decision: "approved" | "denied",
  reason: string,
): ApprovalDecisionCommand =>
  ApprovalDecisionCommand.make({
    submissionId,
    toolCallId: decodeToolCallId("book-1"),
    decision,
    resolver: "operator",
    reason,
  });

/** A retained external resource with a new, scoped attachment for each actual Attempt. */
const makeRetentionCase = (
  name: string,
  options: {
    readonly beforeRetain?: (
      submissionId: SubmissionId,
    ) => Effect.Effect<void, ApprovalSuspensionError>;
    readonly beforeOrdinaryResult?: Effect.Effect<void>;
    readonly modelFinalizer?: Effect.Effect<void>;
  } = {},
) =>
  Effect.gen(function* () {
    const ledger = yield* SubmissionLedger;
    const store = yield* ThreadStore;
    const entered = yield* Deferred.make<void>();
    const resource = { open: true };
    const attachments: Array<{ attemptId: string; active: boolean; retained: boolean }> = [];
    const uses: Array<{ tool: string; attemptId: string; resource: typeof resource }> = [];
    const lifecycle: Array<string> = [];

    const toolkit = Toolkit.make(
      Tool.make("use_resource", { parameters: Schema.Struct({}), success: Schema.String }),
      BookApproval,
    );

    const definition = Agent.make(`retained-approval-${name}`, {
      input: Schema.Struct({ question: Schema.String }),
      output: Schema.Struct({ answer: Schema.String }),
      instructions: "Use the resource, then book after approval.",
      toolkit,
      policy,
    });

    const scripted = yield* makeScriptedModel(
      (call) =>
        call === 0
          ? toolTurn(toolCall("use-1", "use_resource", {}))
          : call === 1
            ? toolTurn(toolCall("book-1", "book", { ref: "retained" }))
            : finalParts('{"answer":"booked"}'),
      options.modelFinalizer,
    );

    const thread = `thread-retained-approval-${name}`;

    const attemptLayer = ({ attemptId, submissionId }: AgentAttemptContext) =>
      Layer.unwrap(
        Effect.gen(function* () {
          const attachment = yield* Effect.acquireRelease(
            Effect.sync(() => {
              expect(attachments.every((previous) => !previous.active)).toBe(true);
              const current = { attemptId, active: true, retained: false };

              attachments.push(current);
              lifecycle.push("acquire");

              return current;
            }),
            (current) =>
              Effect.sync(() => {
                current.active = false;
                if (!current.retained) resource.open = false;
                lifecycle.push("release");
              }),
          );

          const recordUse = (tool: string) =>
            Effect.sync(() => {
              expect(resource.open && attachment.active && !attachment.retained).toBe(true);
              uses.push({ tool, attemptId, resource });
            });

          const retain = Effect.gen(function* () {
            expect(resource.open && attachment.active && !attachment.retained).toBe(true);

            const snapshot = yield* ledger
              .loadRecoverySnapshot(RecoverySnapshotRequest.make({ submissionId }))
              .pipe(Effect.orDie);

            expect(snapshot.ownership?.attemptId).toBe(attemptId);

            const records = yield* readLog(thread).pipe(
              Effect.provideService(ThreadStore, store),
              Effect.orDie,
            );

            expect(
              records.filter(({ record }) => record.payload._tag === "ToolApprovalRequested"),
            ).toHaveLength(1);
            expect(
              records.some(
                ({ record }) =>
                  record.payload._tag === "ToolCallSettled" &&
                  record.payload.toolCallId === "use-1",
              ),
            ).toBe(true);
            expect(uses.map(({ tool }) => tool)).toEqual(["use_resource"]);
            yield* Deferred.succeed(entered, undefined);
            if (options.beforeRetain !== undefined) yield* options.beforeRetain(submissionId);
            attachment.retained = true;
            lifecycle.push("retain");
          });

          return Layer.merge(
            Layer.succeed(DurableApprovalSuspension)(retain),
            toolkit.toLayer({
              use_resource: () =>
                recordUse("use_resource").pipe(
                  Effect.andThen(options.beforeOrdinaryResult ?? Effect.void),
                  Effect.as("ready"),
                ),
              book: () => recordUse("book").pipe(Effect.as({ confirmation: "retained-resource" })),
            }),
          );
        }),
      );

    const bindings = yield* compileRegistrations([
      {
        agent: Agent.withModel(definition, scripted.model),
        definitions: DefinitionDigestInput.make({
          agent: "retain-1",
          model: "retain-1",
          tools: ["retain-1"],
        }),
        attemptLayer,
      },
    ]);

    const runtime = yield* DurableAgentRuntime.pipe(
      Effect.provide(
        DurableAgentRuntime.layerWithBindings(bindings).pipe(
          Layer.provide(RunToolAuthorization.allowAll),
        ),
      ),
    );

    const receipt = yield* runtime.submit(
      { definition },
      { question: "book it" },
      { ...submitOptions(thread, name), definitions: bindings[0]!.digests },
    );

    const process = runtime.processThreadResolved(receipt.threadId);

    const snapshot = ledger.loadRecoverySnapshot(
      RecoverySnapshotRequest.make({ submissionId: receipt.submissionId }),
    );

    const waitForRetention = <A, E>(worker: Fiber.Fiber<A, E>) =>
      Effect.raceFirst(
        Deferred.await(entered),
        Fiber.join(worker).pipe(
          Effect.andThen(Effect.die(new Error("Attempt ended before retaining its live resource"))),
        ),
      );

    return {
      runtime,
      receipt,
      process,
      snapshot,
      waitForRetention,
      resource,
      attachments,
      uses,
      lifecycle,
      scripted,
    };
  });

layer(baseLayer)("approval suspension with attempt resources", (it) => {
  it.effect("retains live resources before suspension and resumes the same resource once", () =>
    Effect.gen(function* () {
      const fixture = yield* makeRetentionCase("suspended");

      expect(yield* fixture.process).toEqual([]);
      expect(yield* lookupState(fixture.receipt.submissionId)).toBe("suspended");
      expect(fixture.resource.open).toBe(true);
      expect(fixture.lifecycle).toEqual(["acquire", "retain", "release"]);
      expect((yield* fixture.snapshot).ownership).toBeUndefined();
      yield* fixture.runtime.resolveApproval(
        approveCommand(fixture.receipt.submissionId, "approved", "resume retained resource"),
      );
      expect((yield* fixture.process)[0]?.outcome).toBe("completed");
      expect(fixture.attachments).toHaveLength(2);
      expect(fixture.attachments[0]?.attemptId).not.toBe(fixture.attachments[1]?.attemptId);
      expect(fixture.uses.map(({ tool }) => tool)).toEqual(["use_resource", "book"]);
      expect(fixture.uses.every(({ resource }) => resource === fixture.resource)).toBe(true);
      expect(fixture.lifecycle).toEqual(["acquire", "retain", "release", "acquire", "release"]);
      expect(fixture.scripted.prompts).toHaveLength(3);
      const tags = logTags(yield* readLog(fixture.receipt.threadId));

      expect(tags.filter((tag) => tag === "ToolApprovalRequested")).toHaveLength(1);
      expect(tags.filter((tag) => tag === "ToolApprovalDecided")).toHaveLength(1);
      expect(tags.filter((tag) => tag === "RunStarted")).toHaveLength(1);
    }),
  );

  it.effect("a raced approval retires the retained attachment and claim before continuing", () =>
    Effect.gen(function* () {
      const ledger = yield* SubmissionLedger;

      const fixture = yield* makeRetentionCase("race", {
        beforeRetain: (submissionId) =>
          ledger
            .recordApprovalDecision(
              approveCommand(submissionId, "approved", "decision during retention"),
            )
            .pipe(
              Effect.asVoid,
              Effect.mapError((cause) => ApprovalSuspensionError.make({ cause })),
            ),
      });

      expect((yield* fixture.process)[0]?.outcome).toBe("completed");
      expect(fixture.attachments).toHaveLength(2);
      expect(fixture.attachments[0]?.attemptId).not.toBe(fixture.attachments[1]?.attemptId);
      expect(fixture.lifecycle).toEqual(["acquire", "retain", "release", "acquire", "release"]);
      expect(fixture.uses.map(({ tool }) => tool)).toEqual(["use_resource", "book"]);
      expect(fixture.uses[0]?.attemptId).not.toBe(fixture.uses[1]?.attemptId);
      expect(fixture.uses.every(({ resource }) => resource === fixture.resource)).toBe(true);
      expect(fixture.scripted.prompts).toHaveLength(3);
      const tags = logTags(yield* readLog(fixture.receipt.threadId));

      expect(tags.filter((tag) => tag === "ToolApprovalRequested")).toHaveLength(1);
      expect(tags.filter((tag) => tag === "ToolApprovalDecided")).toHaveLength(1);
      expect(tags.filter((tag) => tag === "RunStarted")).toHaveLength(1);
    }),
  );

  for (const failure of ["typed failure", "defect"] as const) {
    it.effect(`preserves the retention ${failure} cause without claiming suspension`, () =>
      Effect.gen(function* () {
        const cause = new Error(`retention ${failure}`);
        const error = ApprovalSuspensionError.make({ cause });

        const fixture = yield* makeRetentionCase(failure, {
          beforeRetain: () =>
            failure === "typed failure" ? Effect.fail(error) : Effect.die(cause),
        });

        const exit = yield* Effect.exit(fixture.process);

        expect(Exit.isFailure(exit)).toBe(true);
        if (Exit.isSuccess(exit)) throw new Error("Expected retention to fail");
        if (failure === "typed failure") {
          expect(Cause.findErrorOption(exit.cause)).toEqual(Option.some(error));
          expect(error.cause).toBe(cause);
        } else {
          expect(
            exit.cause.reasons.some((reason) => reason._tag === "Die" && reason.defect === cause),
          ).toBe(true);
        }
        expect(yield* lookupState(fixture.receipt.submissionId)).toBe("input-applied");
        expect((yield* fixture.snapshot).ownership).toBeUndefined();
        expect(fixture.lifecycle).toEqual(["acquire", "release"]);
        expect(fixture.resource.open).toBe(false);
        expect(fixture.uses.map(({ tool }) => tool)).toEqual(["use_resource"]);
        expect(logTags(yield* readLog(fixture.receipt.threadId))).not.toContain(
          "SubmissionSettled",
        );
      }),
    );
  }

  for (const extra of ["defect", "interruption"] as const) {
    it.effect(`preserves a retention failure combined with ${extra}`, () =>
      Effect.gen(function* () {
        const defect = new Error("retention finalizer defect");
        const error = ApprovalSuspensionError.make({ cause: new Error("retention failed") });

        const cause = Cause.combine(
          Cause.fail(error),
          extra === "defect" ? Cause.die(defect) : Cause.interrupt(123),
        );

        const fixture = yield* makeRetentionCase(`mixed-${extra}`, {
          beforeRetain: () => Effect.failCause(cause),
        });

        const exit = yield* Effect.exit(fixture.process);

        expect(Exit.isFailure(exit)).toBe(true);
        if (Exit.isSuccess(exit)) throw new Error("Expected mixed retention failure");
        expect(Cause.findErrorOption(exit.cause)).toEqual(Option.some(error));
        if (extra === "defect") {
          expect(
            exit.cause.reasons.some((reason) => reason._tag === "Die" && reason.defect === defect),
          ).toBe(true);
        } else {
          expect(
            exit.cause.reasons.some(
              (reason) => reason._tag === "Interrupt" && reason.fiberId === 123,
            ),
          ).toBe(true);
        }
        expect(yield* lookupState(fixture.receipt.submissionId)).toBe("input-applied");
        expect(fixture.lifecycle).toEqual(["acquire", "release"]);
        expect(fixture.resource.open).toBe(false);
        expect(fixture.uses.map(({ tool }) => tool)).toEqual(["use_resource"]);
      }),
    );
  }

  for (const extra of ["defect", "interruption"] as const) {
    it.effect(
      `a pending approval combined with a model finalizer ${extra} cannot retain or suspend`,
      () =>
        Effect.gen(function* () {
          const defect = new Error("model finalizer defect");

          const fixture = yield* makeRetentionCase(`mixed-pending-${extra}`, {
            modelFinalizer: Effect.failCause(
              extra === "defect" ? Cause.die(defect) : Cause.interrupt(456),
            ),
          });

          const exit = yield* Effect.exit(fixture.process);

          expect(Exit.isFailure(exit)).toBe(true);
          if (Exit.isSuccess(exit)) throw new Error("Expected mixed approval failure");
          expect(Cause.hasFails(exit.cause)).toBe(false);
          if (extra === "defect") {
            expect(
              exit.cause.reasons.some(
                (reason) => reason._tag === "Die" && reason.defect === defect,
              ),
            ).toBe(true);
          } else {
            expect(Cause.hasDies(exit.cause)).toBe(false);
            expect(
              exit.cause.reasons.some(
                (reason) => reason._tag === "Interrupt" && reason.fiberId === 456,
              ),
            ).toBe(true);
          }
          expect(yield* lookupState(fixture.receipt.submissionId)).toBe("input-applied");
          expect(fixture.lifecycle).toEqual(["acquire", "release"]);
          expect(fixture.resource.open).toBe(false);
          expect(fixture.uses.map(({ tool }) => tool)).toEqual(["use_resource"]);
          const tags = logTags(yield* readLog(fixture.receipt.threadId));

          expect(tags).toContain("ToolApprovalRequested");
          expect(tags).not.toContain("SubmissionSettled");
        }),
    );
  }

  it.effect("keeps the existing claim renewal active throughout slow retention", () =>
    Effect.gen(function* () {
      const release = yield* Deferred.make<void>();

      const fixture = yield* makeRetentionCase("slow", {
        beforeRetain: () => Deferred.await(release),
      });

      const worker = yield* Effect.forkChild(fixture.process);

      yield* fixture.waitForRetention(worker);
      const initial = (yield* fixture.snapshot).ownership;

      expect(initial).toBeDefined();
      yield* TestClock.adjust(Duration.sum(DEFAULT_OWNERSHIP_LEASE_DURATION, Duration.seconds(1)));
      const renewed = (yield* fixture.snapshot).ownership;

      expect(renewed?.attemptId).toBe(initial?.attemptId);
      expect(
        renewed &&
          initial &&
          DateTime.toEpochMillis(renewed.leaseExpiresAt) >
            DateTime.toEpochMillis(initial.leaseExpiresAt),
      ).toBe(true);
      expect(fixture.attachments[0]?.active).toBe(true);
      yield* Deferred.succeed(release, undefined);
      expect(yield* Fiber.join(worker)).toEqual([]);
      expect(yield* lookupState(fixture.receipt.submissionId)).toBe("suspended");
      expect(fixture.lifecycle).toEqual(["acquire", "retain", "release"]);
      expect(fixture.resource.open).toBe(true);
    }),
  );

  it.effect("Stop interrupts pending retention through the existing abort watcher", () =>
    Effect.gen(function* () {
      const fixture = yield* makeRetentionCase("stop", { beforeRetain: () => Effect.never });
      const worker = yield* Effect.forkChild(fixture.process);

      yield* fixture.waitForRetention(worker);
      yield* fixture.runtime.abort(
        AbortCommand.make({
          submissionId: fixture.receipt.submissionId,
          author: "operator",
          reason: "Stop while retaining",
        }),
      );
      yield* TestClock.adjust(Duration.millis(100));
      expect((yield* Fiber.join(worker))[0]?.outcome).toBe("aborted");
      expect(fixture.lifecycle).toEqual(["acquire", "release"]);
      expect(fixture.resource.open).toBe(false);
      expect(fixture.uses.map(({ tool }) => tool)).toEqual(["use_resource"]);
      expect((yield* fixture.snapshot).ownership).toBeUndefined();
    }),
  );

  it.effect(
    "interruption during retention releases resources without reporting a safe suspension",
    () =>
      Effect.gen(function* () {
        const fixture = yield* makeRetentionCase("interrupted", {
          beforeRetain: () => Effect.never,
        });

        const worker = yield* Effect.forkChild(fixture.process);

        yield* fixture.waitForRetention(worker);
        yield* Fiber.interrupt(worker);
        const exit = yield* Fiber.await(worker);

        expect(Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause)).toBe(true);
        expect(yield* lookupState(fixture.receipt.submissionId)).toBe("input-applied");
        expect((yield* fixture.snapshot).ownership).toBeUndefined();
        expect(fixture.lifecycle).toEqual(["acquire", "release"]);
        expect(fixture.resource.open).toBe(false);
        expect(fixture.uses.map(({ tool }) => tool)).toEqual(["use_resource"]);
      }),
  );

  it.effect("an uncertain ordinary effect never invokes retention or replays the tool", () =>
    Effect.gen(function* () {
      const acted = yield* Deferred.make<void>();

      const fixture = yield* makeRetentionCase("uncertain", {
        beforeOrdinaryResult: Deferred.succeed(acted, undefined).pipe(Effect.andThen(Effect.never)),
      });

      const worker = yield* Effect.forkChild(fixture.process);

      yield* Deferred.await(acted);
      yield* Fiber.interrupt(worker);
      yield* fixture.runtime.runRecovery();
      expect(yield* fixture.process).toEqual([]);
      expect(yield* lookupState(fixture.receipt.submissionId)).toBe("unknown");
      expect(fixture.lifecycle).toEqual(["acquire", "release"]);
      expect(fixture.resource.open).toBe(false);
      expect(fixture.uses.map(({ tool }) => tool)).toEqual(["use_resource"]);
      const tags = logTags(yield* readLog(fixture.receipt.threadId));

      expect(tags).toContain("ToolCallUnknown");
      expect(tags).not.toContain("ToolApprovalRequested");
    }),
  );
});

layer(testLayer)("DUR P5 durable approval suspension (plan §2.6)", (it) => {
  it.effect("an unresolved approval suspends without a settlement and releases the lane", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const ledger = yield* SubmissionLedger;
      const desk = yield* makeBookDesk;

      const scripted = yield* makeScriptedModel((call) =>
        call === 0
          ? toolTurn(toolCall("book-1", "book", { ref: "r-suspend" }))
          : finalParts('{"answer":"never"}'),
      );

      const agent = Agent.withModel(approvalDefinition, scripted.model);
      const thread = "thread-approval-suspend";

      const receipt = yield* runtime.submit(
        agent,
        { question: "book it" },
        submitOptions(thread, "suspend-1"),
      );

      const settlements = yield* runtime
        .processThread(agent, decodeThreadId(thread))
        .pipe(Effect.provide(desk.toolLayer));

      // No settlement: the accepted-work obligation stays owed while the lane waits durably.
      expect(settlements).toHaveLength(0);
      expect(yield* lookupState(receipt.submissionId)).toBe("suspended");
      expect(yield* desk.count("r-suspend")).toBe(0);

      // Ownership ended with the suspension: the suspended head is never worker-claimable, so
      // the lane consumes no worker permit (durability §16).
      const claimed = yield* ledger.claim(
        ClaimRequest.make({
          threadId: decodeThreadId(thread),
          producerId: PRODUCER_ID,
        }),
      );

      expect(Option.isNone(claimed)).toBe(true);

      const runId = runIdForSubmission(receipt.submissionId);
      const records = yield* readLog(thread);

      // The response committed BEFORE approval preflight (the durable boundary), the request is
      // canonical (durability §8), and nothing was prepared or executed.
      expect(logTags(records)).toEqual([
        "ThreadCreated",
        "UserInputRecorded",
        "RunStarted",
        "ModelResponseRecorded",
        "ToolApprovalRequested",
      ]);
      const byId = recordsById(records);
      const request = byId.get(`approval-request:${runId}:1:book-1`);

      expect(request?.batchId).toBe(`turn-approvals:${runId}:1`);
      if (request?.record.payload._tag === "ToolApprovalRequested") {
        expect(request.record.payload.toolName).toBe("book");
      }
    }),
  );

  it.effect(
    "resolveApproval(approved) resumes the declared batch without model re-invocation",
    () =>
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const desk = yield* makeBookDesk;

        const scripted = yield* makeScriptedModel((call) =>
          call === 0
            ? toolTurn(toolCall("book-1", "book", { ref: "r-approve" }))
            : finalParts('{"answer":"booked"}'),
        );

        const agent = Agent.withModel(approvalDefinition, scripted.model);
        const thread = "thread-approval-approve";

        const receipt = yield* runtime.submit(
          agent,
          { question: "book it" },
          submitOptions(thread, "approve-1"),
        );

        const first = yield* runtime
          .processThread(agent, decodeThreadId(thread))
          .pipe(Effect.provide(desk.toolLayer));

        expect(first).toHaveLength(0);
        expect(yield* lookupState(receipt.submissionId)).toBe("suspended");

        const intent = yield* runtime.resolveApproval(
          approveCommand(receipt.submissionId, "approved", "reviewed and approved"),
        );

        expect(intent.decision).toBe("approved");
        // The covering decision wakes the lane: suspended → input-applied (plan §2.6).
        expect(yield* lookupState(receipt.submissionId)).toBe("input-applied");

        const settlements = yield* runtime
          .processThread(agent, decodeThreadId(thread))
          .pipe(Effect.provide(desk.toolLayer));

        expect(settlements).toHaveLength(1);
        expect(settlements[0]?.outcome).toBe("completed");
        expect(yield* desk.count("r-approve")).toBe(1);

        // Exactly two model requests ever: the declaring Turn and the continuation — the resumed
        // batch replayed the canonical declaration instead of re-invoking the model.
        expect(scripted.prompts).toHaveLength(2);

        const runId = runIdForSubmission(receipt.submissionId);
        const records = yield* readLog(thread);
        const byId = recordsById(records);
        // The resuming Attempt appended the canonical decision BEFORE honoring it.
        const decision = byId.get(`approval-decision:${runId}:1:book-1`);

        expect(decision?.batchId).toBe(`approval-decision:${receipt.submissionId}:book-1`);
        if (decision?.record.payload._tag === "ToolApprovalDecided") {
          expect(decision.record.payload.decision).toBe("approved");
          expect(decision.record.payload.resolver).toBe("operator");
        }
        // The approved call entered the ordinary uncertainty protocol and settled canonically.
        expect(byId.has(`tool-prepared:${runId}:1:book-1`)).toBe(true);
        expect(byId.has(`tool-settled:${runId}:1:book-1`)).toBe(true);
        // The request was appended exactly once across both Attempts.
        expect(
          records.filter((envelope) => envelope.record.payload._tag === "ToolApprovalRequested"),
        ).toHaveLength(1);
        expect(
          records.filter((envelope) => envelope.record.payload._tag === "ModelResponseInterrupted"),
        ).toHaveLength(0);
      }),
  );

  it.effect("resolveApproval(denied) settles failed with a canonical decision record", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const desk = yield* makeBookDesk;

      const scripted = yield* makeScriptedModel((call) =>
        call === 0
          ? toolTurn(toolCall("book-1", "book", { ref: "r-deny" }))
          : finalParts('{"answer":"never"}'),
      );

      const agent = Agent.withModel(approvalDefinition, scripted.model);
      const thread = "thread-approval-deny";

      const receipt = yield* runtime.submit(
        agent,
        { question: "book it" },
        submitOptions(thread, "deny-1"),
      );

      const first = yield* runtime
        .processThread(agent, decodeThreadId(thread))
        .pipe(Effect.provide(desk.toolLayer));

      expect(first).toHaveLength(0);

      yield* runtime.resolveApproval(
        approveCommand(receipt.submissionId, "denied", "policy forbids this booking"),
      );

      const settlements = yield* runtime
        .processThread(agent, decodeThreadId(thread))
        .pipe(Effect.provide(desk.toolLayer));

      expect(settlements).toHaveLength(1);
      // Denial-terminal (P2 policy default): the Run fails through `AgentApprovalDenied` with
      // the denial already canonical; the handler never started.
      expect(settlements[0]?.outcome).toBe("failed");
      expect(yield* desk.count("r-deny")).toBe(0);

      const runId = runIdForSubmission(receipt.submissionId);
      const records = yield* readLog(thread);
      const byId = recordsById(records);
      const decision = byId.get(`approval-decision:${runId}:1:book-1`);

      if (decision?.record.payload._tag === "ToolApprovalDecided") {
        expect(decision.record.payload.decision).toBe("denied");
      } else {
        throw new Error("Expected a canonical ToolApprovalDecided record");
      }
      expect(byId.has(`tool-prepared:${runId}:1:book-1`)).toBe(false);
      const settled = yield* runtime.awaitSettlement(receipt);

      expect(settled.outcome).toBe("failed");
    }),
  );

  it.effect("resolveApproval is idempotent and conflicts on a divergent re-decision", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const desk = yield* makeBookDesk;

      const scripted = yield* makeScriptedModel((call) =>
        call === 0
          ? toolTurn(toolCall("book-1", "book", { ref: "r-idem" }))
          : finalParts('{"answer":"never"}'),
      );

      const agent = Agent.withModel(approvalDefinition, scripted.model);
      const thread = "thread-approval-idempotent";

      const receipt = yield* runtime.submit(
        agent,
        { question: "book it" },
        submitOptions(thread, "idem-1"),
      );

      yield* runtime
        .processThread(agent, decodeThreadId(thread))
        .pipe(Effect.provide(desk.toolLayer));

      const original = yield* runtime.resolveApproval(
        approveCommand(receipt.submissionId, "approved", "first decision"),
      );

      // The replay returns the stored intent unchanged (original resolver/reason/decidedAt).
      const replayed = yield* runtime.resolveApproval(
        approveCommand(receipt.submissionId, "approved", "first decision"),
      );

      expect(replayed.reason).toBe(original.reason);
      expect(replayed.decidedAt).toStrictEqual(original.decidedAt);

      const divergent = yield* Effect.exit(
        runtime.resolveApproval(approveCommand(receipt.submissionId, "denied", "changed my mind")),
      );

      expect(failureTag(divergent)).toBe("ApprovalConflict");
    }),
  );

  it.effect("abort of a suspended Submission settles aborted", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const desk = yield* makeBookDesk;

      const scripted = yield* makeScriptedModel((call) =>
        call === 0
          ? toolTurn(toolCall("book-1", "book", { ref: "r-abort" }))
          : finalParts('{"answer":"never"}'),
      );

      const agent = Agent.withModel(approvalDefinition, scripted.model);
      const thread = "thread-approval-abort";

      const receipt = yield* runtime.submit(
        agent,
        { question: "book it" },
        submitOptions(thread, "abort-1"),
      );

      yield* runtime
        .processThread(agent, decodeThreadId(thread))
        .pipe(Effect.provide(desk.toolLayer));
      expect(yield* lookupState(receipt.submissionId)).toBe("suspended");

      yield* runtime.abort(
        AbortCommand.make({
          submissionId: receipt.submissionId,
          author: "operator",
          reason: "stop the suspended booking",
        }),
      );
      // A suspended head is never worker-claimable, so the durable abort settles through the
      // recovery pass (durability §13: inactive accepted work settles aborted).
      const reports = (yield* runtime.runRecovery()).reports;
      const report = reports.find((entry) => entry.submissionId === receipt.submissionId);

      expect(report?.decision._tag).toBe("SettleAborted");
      expect(report?.disposition).toBe("repaired");

      const settled = yield* runtime.awaitSettlement(receipt);

      expect(settled.outcome).toBe("aborted");
      expect(yield* lookupState(receipt.submissionId)).toBe("settled");
      expect(yield* desk.count("r-abort")).toBe(0);

      const records = yield* readLog(thread);
      const byId = recordsById(records);

      expect(byId.has(`abort:${receipt.submissionId}`)).toBe(true);
      // Nothing was prepared, so abort records no ToolCallUnknown audit for this Run.
      expect(logTags(records)).not.toContain("ToolCallPrepared");
      expect(logTags(records)).not.toContain("ToolCallUnknown");
    }),
  );

  it.effect("a kill at approval:after-request-append repairs the suspension from history", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const desk = yield* makeBookDesk;

      const scripted = yield* makeScriptedModel((call) =>
        call === 0
          ? toolTurn(toolCall("book-1", "book", { ref: "r-fp-request" }))
          : finalParts('{"answer":"booked"}'),
      );

      const agent = Agent.withModel(approvalDefinition, scripted.model);
      const thread = "thread-approval-fp-request";

      const receipt = yield* runtime.submit(
        agent,
        { question: "book it" },
        submitOptions(thread, "fp-request-1"),
      );

      yield* armFailpoint("approval:after-request-append");

      const killed = yield* Effect.exit(
        runtime.processThread(agent, decodeThreadId(thread)).pipe(Effect.provide(desk.toolLayer)),
      );

      expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
      yield* clearFailpoint;

      // The request is canonical but the ledger never suspended: recovery repairs the
      // suspension from history — no execution, no settlement (plan §4.3).
      expect(yield* lookupState(receipt.submissionId)).toBe("input-applied");
      const reports = (yield* runtime.runRecovery()).reports;
      const report = reports.find((entry) => entry.submissionId === receipt.submissionId);

      expect(report?.decision._tag).toBe("AwaitApprovalDecision");
      expect(report?.disposition).toBe("repaired");
      expect(yield* lookupState(receipt.submissionId)).toBe("suspended");
      expect(yield* desk.count("r-fp-request")).toBe(0);

      // The durable decision converges the lane to one settlement, executing exactly once.
      yield* runtime.resolveApproval(
        approveCommand(receipt.submissionId, "approved", "approved after repair"),
      );

      const settlements = yield* runtime
        .processThread(agent, decodeThreadId(thread))
        .pipe(Effect.provide(desk.toolLayer));

      expect(settlements[0]?.outcome).toBe("completed");
      expect(yield* desk.count("r-fp-request")).toBe(1);

      const records = yield* readLog(thread);

      expect(
        records.filter((envelope) => envelope.record.payload._tag === "ToolApprovalRequested"),
      ).toHaveLength(1);
    }),
  );

  it.effect("sequential approvals in one Turn suspend iteratively and converge once", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const desk = yield* makeBookDesk;

      const scripted = yield* makeScriptedModel((call) =>
        call === 0
          ? toolTurn(
              toolCall("book-1", "book", { ref: "r-multi-1" }),
              toolCall("book-2", "book", { ref: "r-multi-2" }),
            )
          : finalParts('{"answer":"booked both"}'),
      );

      const agent = Agent.withModel(approvalDefinition, scripted.model);
      const thread = "thread-approval-multi";

      const receipt = yield* runtime.submit(
        agent,
        { question: "book both" },
        submitOptions(thread, "multi-1"),
      );

      // The engine resolves approvals in declaration order and fails the batch on the FIRST
      // unresolved call, so each undecided approval suspends the lane one at a time.
      const first = yield* runtime
        .processThread(agent, decodeThreadId(thread))
        .pipe(Effect.provide(desk.toolLayer));

      expect(first).toHaveLength(0);
      expect(yield* lookupState(receipt.submissionId)).toBe("suspended");

      yield* runtime.resolveApproval(approveCommand(receipt.submissionId, "approved", "first ok"));

      const second = yield* runtime
        .processThread(agent, decodeThreadId(thread))
        .pipe(Effect.provide(desk.toolLayer));

      expect(second).toHaveLength(0);
      expect(yield* lookupState(receipt.submissionId)).toBe("suspended");
      // Nothing executed while any approval of the batch is undecided.
      expect(yield* desk.count("r-multi-1")).toBe(0);
      expect(yield* desk.count("r-multi-2")).toBe(0);

      yield* runtime.resolveApproval(
        ApprovalDecisionCommand.make({
          submissionId: receipt.submissionId,
          toolCallId: decodeToolCallId("book-2"),
          decision: "approved",
          resolver: "operator",
          reason: "second ok",
        }),
      );

      const settlements = yield* runtime
        .processThread(agent, decodeThreadId(thread))
        .pipe(Effect.provide(desk.toolLayer));

      expect(settlements).toHaveLength(1);
      expect(settlements[0]?.outcome).toBe("completed");
      expect(yield* desk.count("r-multi-1")).toBe(1);
      expect(yield* desk.count("r-multi-2")).toBe(1);
      // Three model-free resumptions of the same declaration: exactly two model requests ever.
      expect(scripted.prompts).toHaveLength(2);

      const runId = runIdForSubmission(receipt.submissionId);
      const records = yield* readLog(thread);
      const byId = recordsById(records);

      // Batch identity across suspension cycles: the Turn's FIRST canonical approval append
      // owns the shared turn-approvals batch; the later request of the same Turn commits under
      // its deterministic per-call batch so the committed batch is never contradicted.
      expect(byId.get(`approval-request:${runId}:1:book-1`)?.batchId).toBe(
        `turn-approvals:${runId}:1`,
      );
      expect(byId.get(`approval-request:${runId}:1:book-2`)?.batchId).toBe(
        `approval-request:${runId}:1:book-2`,
      );
      expect(byId.has(`tool-settled:${runId}:1:book-1`)).toBe(true);
      expect(byId.has(`tool-settled:${runId}:1:book-2`)).toBe(true);
    }),
  );

  it.effect(
    "a kill at approval:after-suspend leaves a durably suspended lane that resumes on resolveApproval",
    () =>
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const desk = yield* makeBookDesk;

        const scripted = yield* makeScriptedModel((call) =>
          call === 0
            ? toolTurn(toolCall("book-1", "book", { ref: "r-fp-suspend" }))
            : finalParts('{"answer":"booked"}'),
        );

        const agent = Agent.withModel(approvalDefinition, scripted.model);
        const thread = "thread-approval-fp-suspend";

        const receipt = yield* runtime.submit(
          agent,
          { question: "book it" },
          submitOptions(thread, "fp-suspend-1"),
        );

        yield* armFailpoint("approval:after-suspend");

        const killed = yield* Effect.exit(
          runtime.processThread(agent, decodeThreadId(thread)).pipe(Effect.provide(desk.toolLayer)),
        );

        expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
        yield* clearFailpoint;

        // The suspend transaction committed before the kill: the lane is durably suspended and
        // recovery has nothing to repair — it waits for the authorized decision path.
        expect(yield* lookupState(receipt.submissionId)).toBe("suspended");
        const reports = (yield* runtime.runRecovery()).reports;
        const report = reports.find((entry) => entry.submissionId === receipt.submissionId);

        expect(report?.decision._tag).toBe("AwaitApprovalDecision");
        expect(report?.disposition).toBe("deferred");
        expect(yield* lookupState(receipt.submissionId)).toBe("suspended");

        yield* runtime.resolveApproval(
          approveCommand(receipt.submissionId, "approved", "approved after crash"),
        );

        const settlements = yield* runtime
          .processThread(agent, decodeThreadId(thread))
          .pipe(Effect.provide(desk.toolLayer));

        expect(settlements).toHaveLength(1);
        expect(settlements[0]?.outcome).toBe("completed");
        expect(yield* desk.count("r-fp-suspend")).toBe(1);
      }),
  );
});

layer(delegateTestLayer)("DUR P5 policy-auto approval delegation (plan §2.6 step 2)", (it) => {
  it.effect("a policy-auto approval becomes canonical in one atomic batch and proceeds", () =>
    Effect.gen(function* () {
      const control = yield* ApprovalDelegateTestControl;

      yield* control.set(() =>
        Effect.succeed({ _tag: "approved", reason: "auto-approved by policy" }),
      );
      const runtime = yield* DurableAgentRuntime;
      const desk = yield* makeBookDesk;

      const scripted = yield* makeScriptedModel((call) =>
        call === 0
          ? toolTurn(toolCall("book-1", "book", { ref: "r-auto" }))
          : finalParts('{"answer":"booked"}'),
      );

      const agent = Agent.withModel(approvalDefinition, scripted.model);
      const thread = "thread-approval-auto";

      const receipt = yield* runtime.submit(
        agent,
        { question: "book it" },
        submitOptions(thread, "auto-1"),
      );

      const settlements = yield* runtime
        .processThread(agent, decodeThreadId(thread))
        .pipe(Effect.provide(desk.toolLayer));

      expect(settlements).toHaveLength(1);
      expect(settlements[0]?.outcome).toBe("completed");
      expect(yield* desk.count("r-auto")).toBe(1);

      const runId = runIdForSubmission(receipt.submissionId);
      const records = yield* readLog(thread);
      const byId = recordsById(records);
      // The immediate decision committed atomically WITH its request in the Turn batch.
      const request = byId.get(`approval-request:${runId}:1:book-1`);
      const decision = byId.get(`approval-decision:${runId}:1:book-1`);

      expect(request?.batchId).toBe(`turn-approvals:${runId}:1`);
      expect(decision?.batchId).toBe(`turn-approvals:${runId}:1`);
      if (decision?.record.payload._tag === "ToolApprovalDecided") {
        expect(decision.record.payload.decision).toBe("approved");
        expect(decision.record.payload.resolver).toBe("approval-policy");
      }
      yield* control.reset;
    }),
  );

  it.effect("a policy-auto denial settles failed with canonical request and decision", () =>
    Effect.gen(function* () {
      const control = yield* ApprovalDelegateTestControl;

      yield* control.set(() =>
        Effect.succeed({ _tag: "denied", reason: "policy denies this booking" }),
      );
      const runtime = yield* DurableAgentRuntime;
      const desk = yield* makeBookDesk;

      const scripted = yield* makeScriptedModel((call) =>
        call === 0
          ? toolTurn(toolCall("book-1", "book", { ref: "r-auto-deny" }))
          : finalParts('{"answer":"never"}'),
      );

      const agent = Agent.withModel(approvalDefinition, scripted.model);
      const thread = "thread-approval-auto-deny";

      const receipt = yield* runtime.submit(
        agent,
        { question: "book it" },
        submitOptions(thread, "auto-deny-1"),
      );

      const settlements = yield* runtime
        .processThread(agent, decodeThreadId(thread))
        .pipe(Effect.provide(desk.toolLayer));

      expect(settlements).toHaveLength(1);
      expect(settlements[0]?.outcome).toBe("failed");
      expect(yield* desk.count("r-auto-deny")).toBe(0);

      const runId = runIdForSubmission(receipt.submissionId);
      const records = yield* readLog(thread);
      const byId = recordsById(records);

      expect(byId.has(`approval-request:${runId}:1:book-1`)).toBe(true);
      const decision = byId.get(`approval-decision:${runId}:1:book-1`);

      if (decision?.record.payload._tag === "ToolApprovalDecided") {
        expect(decision.record.payload.decision).toBe("denied");
      } else {
        throw new Error("Expected a canonical ToolApprovalDecided record");
      }
      expect(byId.has(`tool-prepared:${runId}:1:book-1`)).toBe(false);
      yield* control.reset;
    }),
  );

  it.effect("a decision recorded before suspension resumes immediately", () =>
    Effect.gen(function* () {
      const control = yield* ApprovalDelegateTestControl;
      const runtime = yield* DurableAgentRuntime;
      const ledger = yield* SubmissionLedger;
      const desk = yield* makeBookDesk;

      const scripted = yield* makeScriptedModel((call) =>
        call === 0
          ? toolTurn(toolCall("book-1", "book", { ref: "r-race" }))
          : finalParts('{"answer":"booked"}'),
      );

      const agent = Agent.withModel(approvalDefinition, scripted.model);
      const thread = "thread-approval-race";

      const receipt = yield* runtime.submit(
        agent,
        { question: "book it" },
        submitOptions(thread, "race-1"),
      );

      // Deterministic construction of the plan §2.6 race: the delegate records the durable
      // decision intent AFTER the Attempt's snapshot read but reports unresolved, so the
      // decision lands strictly between the recorded-decision lookup and the suspend
      // transaction. `suspend` must observe the covering intent and resume immediately.
      yield* control.set(() =>
        ledger
          .recordApprovalDecision(
            ApprovalDecisionCommand.make({
              submissionId: receipt.submissionId,
              toolCallId: decodeToolCallId("book-1"),
              decision: "approved",
              resolver: "operator",
              reason: "raced ahead of the suspend transaction",
            }),
          )
          .pipe(Effect.orDie, Effect.as<RunApprovalDecision>({ _tag: "unresolved" })),
      );

      // One ownership period settles the Submission: no durable suspension ever happens.
      const settlements = yield* runtime
        .processThread(agent, decodeThreadId(thread))
        .pipe(Effect.provide(desk.toolLayer));

      expect(settlements).toHaveLength(1);
      expect(settlements[0]?.outcome).toBe("completed");
      expect(yield* desk.count("r-race")).toBe(1);
      // The declaring Turn was never re-invoked: the resumed batch replayed the declaration.
      expect(scripted.prompts).toHaveLength(2);

      const runId = runIdForSubmission(receipt.submissionId);
      const records = yield* readLog(thread);
      const byId = recordsById(records);
      const decision = byId.get(`approval-decision:${runId}:1:book-1`);

      if (decision?.record.payload._tag === "ToolApprovalDecided") {
        expect(decision.record.payload.resolver).toBe("operator");
      } else {
        throw new Error("Expected a canonical ToolApprovalDecided record");
      }
      expect(
        records.filter((envelope) => envelope.record.payload._tag === "ToolApprovalRequested"),
      ).toHaveLength(1);
      yield* control.reset;
    }),
  );
});

describe("toDurableRunApprovalHook (capabilities durable adapter)", () => {
  const adapterPolicy = {
    expiresInMillis: 60_000,
    risk: "high",
    denial: "terminal",
    actionSummary: () => "Place a booking hold",
    resourceTargets: () => ["booking:ref-1"],
  } as const;

  const adapterRequest: RunApprovalRequest = {
    request: Response.toolApprovalRequestPart({
      approvalId: "approval-adapter-1",
      toolCallId: "book-1",
    }),
    threadId: decodeThreadId("thread-adapter"),
    runId: Schema.decodeSync(RunId)("run-adapter-1"),
    turnId: Schema.decodeSync(TurnId)("turn-adapter-1"),
    toolCallId: decodeToolCallId("book-1"),
    toolName: "book",
    parameters: { ref: "r-1" },
  };

  it.effect("captures the P2 stack and returns policy decisions with never-typed errors", () =>
    Effect.gen(function* () {
      const hook = yield* toDurableRunApprovalHook(adapterPolicy);
      const decision = yield* hook.request(adapterRequest);

      expect(decision._tag).toBe("approved");
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          StructuralRedactorLive,
          ApprovalAuditMemoryLive,
          Layer.succeed(ApprovalResolver)({
            request: (request) =>
              Effect.succeed(
                ApprovalApproved.make({
                  requestId: request.requestId,
                  decidedAt: Schema.decodeSync(Schema.DateTimeUtcFromString)(
                    "2026-01-01T00:00:00.000Z",
                  ),
                  resolver: "test-resolver",
                }),
              ),
          }),
        ),
      ),
    ),
  );

  it.effect("fails closed to unresolved when the resolver fails", () =>
    Effect.gen(function* () {
      const hook = yield* toDurableRunApprovalHook(adapterPolicy);
      const decision = yield* hook.request(adapterRequest);

      // Fail-closed: a policy fault must never approve, deny, or crash the Attempt — the
      // decision defers to the durable suspension + resolveApproval path.
      expect(decision._tag).toBe("unresolved");
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          StructuralRedactorLive,
          ApprovalAuditMemoryLive,
          Layer.succeed(ApprovalResolver)({
            request: () =>
              Effect.fail(ApprovalResolverError.make({ message: "resolver backend is down" })),
          }),
        ),
      ),
    ),
  );
});
