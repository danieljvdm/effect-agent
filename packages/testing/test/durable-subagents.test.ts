import {
  MemorySubmissionLedgerLive,
  memorySubmissionLedgerLayer,
} from "@effect-agent/storage-memory/memory-submission-ledger";
import { MemoryThreadStoreLive } from "@effect-agent/storage-memory/memory-thread-store";
import { NodeCrypto } from "@effect/platform-node";
import { expect, layer } from "@effect/vitest";
import {
  Cause,
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
import { DurableWorkerBinding, type ResolvedBinding } from "effect-agent/agent-registration";
import { ContextRolloverRequest, ContextRolloverTool } from "effect-agent/context-window";
import {
  DurableAgentRuntime,
  DurableRuntimeConfig,
  type DurableSubmitOptions,
} from "effect-agent/durable-agent-runtime";
import {
  DurableRuntimeFailpointError,
  type DurableRuntimeFailpointLocation,
} from "effect-agent/durable-failpoint";
import { ToolExecutionClass } from "effect-agent/durable-step";
import { IdGenerator } from "effect-agent/id-generator";
import { ThreadId, RunId, ToolCallId, TurnId, type SubmissionId } from "effect-agent/identifiers";
import {
  DefinitionDigests,
  DeploymentId,
  Digest,
  ProducerId,
  RecordEnvelope,
  ToolCallPrepared,
  type CanonicalRecordEnvelope,
} from "effect-agent/records";
import { childThreadIdFor, runIdForSubmission } from "effect-agent/run-journal";
import { RunToolAuthorization } from "effect-agent/run-options";
import * as Subagent from "effect-agent/subagent";
import { SubagentPolicy } from "effect-agent/subagent";
import { SubagentReservationsMemoryLive } from "effect-agent/subagent-reservations";
import {
  AdmissionRequest,
  ParentLinkage,
  type AdmissionResult,
  IdempotencyKey,
  Principal,
  RecoverySnapshotRequest,
  ResolutionCompletedWithResult,
  SubmissionLedger,
  SubmissionLookupById,
  UnknownResolutionCommand,
} from "effect-agent/submission-ledger";
import { DurableRuntimeFailpointTestControl } from "effect-agent/testing/durable-failpoint-test-control";
import { ThreadRead, ThreadStore, ThreadStoreError } from "effect-agent/thread-store";
import { ToolReconciler } from "effect-agent/tool-reconciler";
import { WakeScheduler } from "effect-agent/wake-scheduler";
import { TestClock } from "effect/testing";
import {
  LanguageModel,
  Model,
  Tool,
  Toolkit,
  type Prompt,
  type Response,
} from "effect/unstable/ai";

const SHA_A = Schema.decodeSync(Digest)("a".repeat(64));
const PARENT_DIGESTS = DefinitionDigests.make({ agent: SHA_A, model: SHA_A, tools: SHA_A });

const CHILD_DIGEST_STRINGS = {
  agent: "b".repeat(64),
  model: "c".repeat(64),
  tools: "d".repeat(64),
} as const;

const CHILD_DIGESTS = DefinitionDigests.make({
  agent: Schema.decodeSync(Digest)(CHILD_DIGEST_STRINGS.agent),
  model: Schema.decodeSync(Digest)(CHILD_DIGEST_STRINGS.model),
  tools: Schema.decodeSync(Digest)(CHILD_DIGEST_STRINGS.tools),
});

const WRONG_CHILD_DIGESTS = DefinitionDigests.make({
  agent: Schema.decodeSync(Digest)("e".repeat(64)),
  model: Schema.decodeSync(Digest)("e".repeat(64)),
  tools: Schema.decodeSync(Digest)("e".repeat(64)),
});

const PRINCIPAL = Schema.decodeSync(Principal)("principal-durable-subagents");
const decodeThreadId = Schema.decodeSync(ThreadId);
const decodeIdempotencyKey = Schema.decodeSync(IdempotencyKey);
const decodeToolCallId = Schema.decodeSync(ToolCallId);

const submitOptions = (threadId: string, idempotencyKey: string): DurableSubmitOptions => ({
  threadId: decodeThreadId(threadId),
  principal: PRINCIPAL,
  idempotencyKey: decodeIdempotencyKey(idempotencyKey),
  definitions: PARENT_DIGESTS,
});

const usage = { inputTokens: {}, outputTokens: {} };

const finalParts = (text: string): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: text },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage },
];

const toolCall = (id: string, name: string, params: unknown): Response.StreamPartEncoded => ({
  type: "tool-call",
  id,
  name,
  params,
  providerExecuted: false,
});

const toolTurn = (
  ...calls: ReadonlyArray<Response.StreamPartEncoded>
): ReadonlyArray<Response.StreamPartEncoded> => [
  ...calls,
  { type: "finish", reason: "tool-calls", usage },
];

/** Scripted model whose call counter and captured prompts survive Layer rebuilds across Attempts. */
const makeScriptedModel = (script: (call: number) => ReadonlyArray<Response.StreamPartEncoded>) =>
  Effect.gen(function* () {
    const calls = yield* Ref.make(0);
    const prompts: Array<Prompt.Prompt> = [];

    const model = Model.make(
      "scripted",
      "durable-subagents-test",
      Layer.effect(
        LanguageModel.LanguageModel,
        LanguageModel.make({
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
        }),
      ),
    );

    return { model, prompts, calls: Ref.get(calls) };
  });

const ChildInput = Schema.Struct({ question: Schema.String });
const ChildOutput = Schema.Struct({ answer: Schema.String });

const childDefinition = Agent.make("research-child", {
  input: ChildInput,
  output: ChildOutput,
  instructions: "Answer as JSON.",
  toolkit: Toolkit.empty,
  policy: AgentPolicy.make({
    maxTurns: 2,
    maxToolCalls: 1,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  }),
});

class ResearchDelegationFailed extends Schema.TaggedError<ResearchDelegationFailed>()(
  "ResearchDelegationFailed",
  { childErrorTag: Schema.String },
) {}

const researchDelegation = Subagent.define("delegate_research", {
  description: "Research one bounded question and return findings.",
  target: childDefinition,
  parameters: Schema.Struct({ topic: Schema.String }),
  success: Schema.Struct({ summary: Schema.String }),
  failure: ResearchDelegationFailed,
  prepareInput: ({ topic }) => Effect.succeed({ question: `research:${topic}` }),
  projectResult: (output) => Effect.succeed({ summary: `finding:${output.answer}` }),
  policy: SubagentPolicy.make({
    maxChildren: 2,
    maxConcurrency: 2,
    maxTurns: 4,
    maxToolCalls: 4,
    maxDuration: "10 seconds",
  }),
});

/** Ordinary sibling Tool executed in the same batch as the delegation (uncertain class). */
const Lookup = Tool.make("lookup", {
  parameters: Schema.Struct({ key: Schema.String }),
  success: Schema.Struct({ value: Schema.String }),
});

const coordinatorDefinition = Agent.make("travel-coordinator", {
  input: Schema.Struct({ mission: Schema.String }),
  output: Schema.Struct({ report: Schema.String }),
  instructions: "Delegate, then answer as JSON.",
  toolkit: Toolkit.make(researchDelegation.tool),
  policy: AgentPolicy.make({
    maxTurns: 3,
    maxToolCalls: 2,
    maxDuration: "30 seconds",
    toolConcurrency: 2,
  }),
});

const mixedCoordinatorDefinition = Agent.make("travel-coordinator-mixed", {
  input: Schema.Struct({ mission: Schema.String }),
  output: Schema.Struct({ report: Schema.String }),
  instructions: "Delegate and look up, then answer as JSON.",
  toolkit: Toolkit.make(researchDelegation.tool, Lookup),
  policy: AgentPolicy.make({
    maxTurns: 3,
    maxToolCalls: 3,
    maxDuration: "30 seconds",
    toolConcurrency: 2,
  }),
});

const mapChildFailure = (failure: { readonly _tag: string }) =>
  ResearchDelegationFailed.make({ childErrorTag: failure._tag });

/** SUB-033 fixture: the same delegation under first-party containment. */
const containedResearchDelegation = Subagent.define("delegate_research_contained", {
  description: "Research one bounded question; failures are contained result data.",
  target: childDefinition,
  parameters: Schema.Struct({ topic: Schema.String }),
  success: Schema.Struct({ summary: Schema.String }),
  failure: ResearchDelegationFailed,
  failureMode: "return",
  prepareInput: ({ topic }) => Effect.succeed({ question: `research:${topic}` }),
  projectResult: (output) => Effect.succeed({ summary: `finding:${output.answer}` }),
  policy: SubagentPolicy.make({
    maxChildren: 2,
    maxConcurrency: 2,
    maxTurns: 4,
    maxToolCalls: 4,
    maxDuration: "10 seconds",
  }),
});

const containedCoordinatorDefinition = Agent.make("travel-coordinator-contained", {
  input: Schema.Struct({ mission: Schema.String }),
  output: Schema.Struct({ report: Schema.String }),
  instructions: "Delegate, then answer as JSON.",
  toolkit: Toolkit.make(containedResearchDelegation.tool),
  policy: AgentPolicy.make({
    maxTurns: 3,
    maxToolCalls: 2,
    maxDuration: "30 seconds",
    toolConcurrency: 2,
  }),
});

const configLayer = DurableRuntimeConfig.layer({
  deploymentId: Schema.decodeSync(DeploymentId)("deployment-durable-subagents"),
  producerId: Schema.decodeSync(ProducerId)("producer-durable-subagents"),
  settlementPollInterval: Duration.millis(100),
  leaseRenewalInterval: Duration.seconds(5),
  abortPollInterval: Duration.millis(100),
});

/** Test-only fault switch for the memory ledger's authoritative admission lookup (SUB-031). */
let admissionFault: string | undefined;

const baseLayer = (ledger: Layer.Layer<SubmissionLedger>) =>
  DurableAgentRuntime.layer.pipe(
    Layer.provideMerge(
      Layer.mergeAll(
        ledger,
        MemoryThreadStoreLive,
        WakeScheduler.layerNoop,
        DurableRuntimeFailpointTestControl.layer,
        ToolReconciler.uncertain,
        configLayer,
      ).pipe(Layer.provideMerge(NodeCrypto.layer)),
    ),
  );

const testLayer = baseLayer(MemorySubmissionLedgerLive);

const faultTestLayer = baseLayer(
  memorySubmissionLedgerLayer({
    resolveAdmissionFault: Effect.sync(() =>
      admissionFault === undefined ? Option.none() : Option.some(admissionFault),
    ),
  }),
);

const makeChildFixture = Effect.gen(function* () {
  const childScripted = yield* makeScriptedModel(() => finalParts('{"answer":"child-answer"}'));
  const childBinding = Agent.withModel(childDefinition, childScripted.model);

  return { childScripted, childBinding };
});

/** Fixture-only identity source consumed by the delegation Layer's ephemeral capture. */
const identifiers = Layer.effect(
  IdGenerator,
  Effect.gen(function* () {
    const counter = yield* Ref.make(0);

    const next = <A>(decode: (value: string) => A, prefix: string) =>
      Ref.getAndUpdate(counter, (value) => value + 1).pipe(
        Effect.map((value) => decode(`${prefix}-${value}`)),
      );

    return {
      nextThreadId: next(decodeThreadId, "fixture-thread"),
      nextRunId: next(Schema.decodeSync(RunId), "fixture-run"),
      nextTurnId: next(Schema.decodeSync(TurnId), "fixture-turn"),
    };
  }),
);

const delegationSupport = Layer.mergeAll(SubagentReservationsMemoryLive, identifiers);

const submitParentWith =
  (
    definition:
      | typeof coordinatorDefinition
      | typeof mixedCoordinatorDefinition
      | typeof containedCoordinatorDefinition,
  ) =>
  (thread: string, key: string) =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;

      const receipt = yield* runtime.submit(
        // The structural submit slice only needs identity + input schema.
        { definition: { id: definition.id, input: coordinatorDefinition.input } },
        { mission: "plan" },
        submitOptions(thread, key),
      );

      return { submissionId: receipt.submissionId, threadId: receipt.threadId };
    });

/**
 * One durable parent/child fixture: the parent coordinator delegates
 * `delegate_research` to the scripted child; both bindings register with the
 * binding array under their exact digests. The capability derives child digests
 * from registration without repeating durable setup in its handler Layer.
 */
const makeHarness = (options?: {
  readonly registration?: "missing" | "ambiguous" | "different-definition";
  readonly declaredDigests?: DefinitionDigests;
}) =>
  Effect.gen(function* () {
    const { childScripted, childBinding } = yield* makeChildFixture;

    const parentScripted = yield* makeScriptedModel((call) =>
      call === 0
        ? toolTurn(toolCall("delegate-1", "delegate_research", { topic: "paris" }))
        : finalParts('{"report":"done"}'),
    );

    const parentBinding = Agent.withModel(coordinatorDefinition, parentScripted.model);

    const delegationLayer = Subagent.layer(researchDelegation, childBinding, {
      mapChildFailure,
      ...(options?.declaredDigests === undefined
        ? {}
        : { durable: { targetDigests: options.declaredDigests } }),
    }).pipe(Layer.provide(delegationSupport));

    const parentResolved: ResolvedBinding = yield* DurableWorkerBinding.make(
      parentBinding,
      PARENT_DIGESTS,
    ).pipe(Effect.provide(delegationLayer));

    const childResolved: ResolvedBinding = yield* DurableWorkerBinding.make(
      childBinding,
      CHILD_DIGESTS,
    );

    const bindings = [
      parentResolved,
      ...(options?.registration === "missing"
        ? []
        : options?.registration === "ambiguous"
          ? [childResolved, childResolved]
          : options?.registration === "different-definition"
            ? [
                {
                  ...childResolved,
                  definition: Agent.make(childDefinition.id, {
                    input: childDefinition.input,
                    output: childDefinition.output,
                    instructions: childDefinition.instructions,
                    toolkit: childDefinition.toolkit,
                    policy: childDefinition.policy,
                  }),
                },
              ]
            : [childResolved]),
    ];

    return {
      bindings,
      runtime: yield* DurableAgentRuntime.pipe(
        Effect.provide(
          DurableAgentRuntime.layerWithBindings(bindings).pipe(
            Layer.provide(RunToolAuthorization.allowAll),
          ),
        ),
      ),
      childInvocations: childScripted.calls,
      parentPrompts: parentScripted.prompts,
      submitParent: submitParentWith(coordinatorDefinition),
      lookupInvocations: Effect.succeed(0),
    };
  });

/** The mixed fixture: the delegation call plus an ordinary uncertain sibling in one batch. */
const makeSiblingHarnessWith = (pendingSibling = false, retryableSibling = true) =>
  Effect.gen(function* () {
    const { childScripted, childBinding } = yield* makeChildFixture;

    const parentScripted = yield* makeScriptedModel((call) =>
      call === 0
        ? toolTurn(
            toolCall("delegate-1", "delegate_research", { topic: "paris" }),
            toolCall("lookup-1", "lookup", { key: "hotels" }),
          )
        : finalParts('{"report":"done"}'),
    );

    const lookupTool =
      pendingSibling && retryableSibling
        ? Lookup.annotate(ToolExecutionClass, "idempotent")
        : Lookup;

    const definition = Agent.make(mixedCoordinatorDefinition.id, {
      input: mixedCoordinatorDefinition.input,
      output: mixedCoordinatorDefinition.output,
      instructions: mixedCoordinatorDefinition.instructions,
      toolkit: Toolkit.make(researchDelegation.tool, lookupTool),
      policy: mixedCoordinatorDefinition.policy,
    });

    const parentBinding = Agent.withModel(definition, parentScripted.model);
    const lookupInvocations = yield* Ref.make(0);
    const lookupFinalizers = yield* Ref.make(0);

    const delegationLayer = Subagent.layer(researchDelegation, childBinding, {
      mapChildFailure,
      durable: { targetDigests: CHILD_DIGEST_STRINGS },
    }).pipe(Layer.provide(delegationSupport));

    const lookupLayer = Toolkit.make(lookupTool).toLayer({
      lookup: ({ key }) =>
        Ref.update(lookupInvocations, (count) => count + 1).pipe(
          Effect.andThen(pendingSibling ? Effect.never : Effect.succeed({ value: `found-${key}` })),
          Effect.ensuring(Ref.update(lookupFinalizers, (count) => count + 1)),
        ),
    });

    const parentResolved: ResolvedBinding = yield* DurableWorkerBinding.make(
      parentBinding,
      PARENT_DIGESTS,
    ).pipe(Effect.provide(Layer.mergeAll(delegationLayer, lookupLayer)));

    const childResolved: ResolvedBinding = yield* DurableWorkerBinding.make(
      childBinding,
      CHILD_DIGESTS,
    );

    return {
      bindings: [parentResolved, childResolved],
      runtime: yield* DurableAgentRuntime.pipe(
        Effect.provide(
          DurableAgentRuntime.layerWithBindings([parentResolved, childResolved]).pipe(
            Layer.provide(RunToolAuthorization.allowAll),
          ),
        ),
      ),
      childInvocations: childScripted.calls,
      parentPrompts: parentScripted.prompts,
      submitParent: submitParentWith(mixedCoordinatorDefinition),
      lookupInvocations: Ref.get(lookupInvocations),
      lookupFinalizers: Ref.get(lookupFinalizers),
    };
  });

const DELEGATE_CALL = decodeToolCallId("delegate-1");

const drive =
  (harness: { readonly runtime: DurableAgentRuntime["Service"] }) => (threadId: ThreadId) =>
    harness.runtime.processThreadResolved(threadId);

const readLog = (threadId: ThreadId) =>
  Effect.gen(function* () {
    const store = yield* ThreadStore;

    return yield* Stream.runCollect(store.read(ThreadRead.make({ threadId, limit: 1_024 })));
  });

const parentState = (submissionId: SubmissionId) =>
  Effect.gen(function* () {
    const ledger = yield* SubmissionLedger;
    const snapshot = yield* ledger.lookup(SubmissionLookupById.make({ submissionId }));

    expect(Option.isSome(snapshot)).toBe(true);
    if (Option.isNone(snapshot)) throw new Error("Expected the Submission to exist");

    return snapshot.value;
  });

const parentReservations = (submissionId: SubmissionId) =>
  Effect.gen(function* () {
    const ledger = yield* SubmissionLedger;

    const snapshot = yield* ledger.loadRecoverySnapshot(
      RecoverySnapshotRequest.make({ submissionId }),
    );

    return snapshot.childReservations;
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

const payloadsOf = <Tag extends string>(
  records: ReadonlyArray<CanonicalRecordEnvelope>,
  tag: Tag,
): ReadonlyArray<CanonicalRecordEnvelope> =>
  records.filter((envelope) => envelope.record.payload._tag === tag);

layer(testLayer)("S2 durable attached Subagents (WP4 coordinator)", (it) => {
  it.effect(
    "RUN-030: expired child cleanup preserves an uncertain ordinary call until operator resolution",
    () =>
      Effect.gen(function* () {
        yield* clearFailpoint;
        const runtime = yield* DurableAgentRuntime;
        const harness = yield* makeSiblingHarnessWith(true, false);
        const run = drive(harness);
        const parent = yield* harness.submitParent("expired-uncertain-mixed", "parent");
        const firstAttempt = yield* Effect.forkChild(run(parent.threadId));

        yield* TestClock.adjust(Duration.seconds(31));
        expect(yield* Fiber.join(firstAttempt)).toEqual([]);
        expect(yield* run(parent.threadId)).toEqual([]);
        yield* runtime.runRecovery();
        expect((yield* parentState(parent.submissionId)).state).not.toBe("unknown");
        const childThreadId = childThreadIdFor(parent.submissionId, DELEGATE_CALL);

        expect((yield* run(childThreadId)).map((entry) => entry.outcome)).toEqual(["completed"]);
        yield* runtime.runRecovery();
        expect((yield* parentState(parent.submissionId)).state).not.toBe("unknown");
        expect(yield* run(parent.threadId)).toEqual([]);
        expect((yield* parentState(parent.submissionId)).state).toBe("unknown");
        const records = yield* readLog(parent.threadId);

        expect(payloadsOf(records, "SubagentJoined")).toHaveLength(1);
        expect(
          payloadsOf(records, "ToolCallUnknown").map(({ record }) => record.payload),
        ).toMatchObject([{ toolCallId: "lookup-1" }]);
        expect(payloadsOf(records, "SubmissionSettled")).toHaveLength(0);
        expect((yield* parentReservations(parent.submissionId)).map((row) => row.status)).toEqual([
          "released",
        ]);
        yield* runtime.resolveUnknown(
          UnknownResolutionCommand.make({
            submissionId: parent.submissionId,
            toolCallId: decodeToolCallId("lookup-1"),
            author: "test-operator",
            reason: "The fixture has no external side effect.",
            resolution: { _tag: "NeverHappened" },
          }),
        );
        expect((yield* run(parent.threadId)).map((entry) => entry.outcome)).toEqual(["failed"]);
        expect(yield* harness.lookupInvocations).toBe(1);
        expect(yield* harness.lookupFinalizers).toBe(1);
        expect(harness.parentPrompts).toHaveLength(1);
      }),
  );

  it.effect(
    "a failed child history read blocks its parent without stopping independent recovery",
    () =>
      Effect.gen(function* () {
        yield* clearFailpoint;
        const ledger = yield* SubmissionLedger;
        const store = yield* ThreadStore;
        const harness = yield* makeHarness();
        const run = drive(harness);
        const parent = yield* harness.submitParent("child-history-failure", "parent");
        const childThread = childThreadIdFor(parent.submissionId, DELEGATE_CALL);

        yield* armFailpoint("subagent:after-child-ready");
        expect(failureTag(yield* Effect.exit(run(parent.threadId)))).toBe(
          "DurableRuntimeFailpointError",
        );
        yield* clearFailpoint;
        const independent = yield* harness.submitParent("independent-recovery", "independent");
        let childReads = 0;

        const unavailable = ThreadStore.of({
          ...store,
          read: (request) =>
            request.threadId !== childThread
              ? store.read(request)
              : Stream.suspend(() => {
                  childReads++;

                  return Stream.fail(
                    ThreadStoreError.make({
                      operation: "read child history",
                      message: "private child history is unavailable",
                    }),
                  );
                }),
        });

        const reports = yield* DurableAgentRuntime.pipe(
          Effect.flatMap((runtime) => runtime.runRecovery()),
          Effect.provide(Layer.fresh(DurableAgentRuntime.layer)),
          Effect.provideService(ThreadStore, unavailable),
        );

        expect(childReads).toBeGreaterThan(0);
        expect(reports.blocked.find((fault) => fault.threadId === parent.threadId)).toMatchObject({
          failure: {
            phase: "recovery",
            errorTag: "ThreadStoreError",
            operation: "read child history",
          },
        });
        expect(
          reports.reports.find((report) => report.submissionId === independent.submissionId),
        ).toMatchObject({
          disposition: "deferred",
          decision: { _tag: "ApplyInput" },
        });
        expect(JSON.stringify(reports)).not.toContain("private child");
        const after = yield* readLog(parent.threadId);

        expect(payloadsOf(after, "SubagentJoined")).toHaveLength(0);
        expect(payloadsOf(after, "SubmissionSettled")).toHaveLength(0);
        expect(
          (yield* ledger.loadRecoverySnapshot(
            RecoverySnapshotRequest.make({ submissionId: parent.submissionId }),
          )).ownership,
        ).toBeUndefined();
        expect(yield* harness.childInvocations).toBe(0);
        yield* harness.runtime.runRecovery();
        expect(payloadsOf(yield* readLog(parent.threadId), "SubagentStarted")).toHaveLength(1);
        expect((yield* run(childThread)).map((entry) => entry.outcome)).toEqual(["completed"]);
        expect((yield* run(parent.threadId)).map((entry) => entry.outcome)).toEqual(["completed"]);
        expect(yield* harness.childInvocations).toBe(1);
      }),
  );

  it.effect(
    "missing or conflicting preparation classification never grants delegation replay",
    () =>
      Effect.gen(function* () {
        const store = yield* ThreadStore;

        for (const classification of ["missing", "conflicting"] as const) {
          yield* clearFailpoint;
          const harness = yield* makeHarness();
          const parent = yield* harness.submitParent(`classification-${classification}`, "parent");

          yield* armFailpoint(
            classification === "missing"
              ? "tools:after-prepared-append"
              : "subagent:after-request-append",
          );
          expect(failureTag(yield* Effect.exit(drive(harness)(parent.threadId)))).toBe(
            "DurableRuntimeFailpointError",
          );
          yield* clearFailpoint;

          const corruptStore = ThreadStore.of({
            ...store,
            read: (request) =>
              store.read(request).pipe(
                Stream.map((envelope) => {
                  if (
                    request.threadId !== parent.threadId ||
                    envelope.record.payload._tag !== "ToolCallPrepared"
                  )
                    return envelope;
                  const { executionKind: _kind, ...prepared } = envelope.record.payload;

                  return {
                    ...envelope,
                    record: RecordEnvelope.make({
                      ...envelope.record,
                      payload: ToolCallPrepared.make({
                        ...prepared,
                        ...(classification === "missing" ? {} : { executionKind: "ordinary" }),
                      }),
                    }),
                  };
                }),
              ),
          });

          const hostileRuntime = yield* DurableAgentRuntime.pipe(
            Effect.provide(
              DurableAgentRuntime.layerWithBindings(harness.bindings).pipe(
                Layer.provide(RunToolAuthorization.allowAll),
              ),
            ),
            Effect.provideService(ThreadStore, corruptStore),
          );

          const before = yield* readLog(parent.threadId);
          const result = yield* Effect.exit(hostileRuntime.processThreadResolved(parent.threadId));

          // The original response still records the delegation kind; missing or contradictory
          // preparation evidence must not erase that contract or authorize child admission.
          expect(failureTag(result)).toBe("RunJournalError");
          expect(yield* readLog(parent.threadId)).toEqual(before);
          expect(yield* harness.childInvocations).toBe(0);
        }
      }),
  );

  it.effect(
    "retires a prepared delegation before child admission instead of invoking its ordinary replacement",
    () =>
      Effect.gen(function* () {
        yield* clearFailpoint;
        const runtime = yield* DurableAgentRuntime;
        const harness = yield* makeHarness();
        const parent = yield* harness.submitParent("changed-delegation-binding", "parent");

        yield* armFailpoint("tools:after-prepared-append");
        expect(failureTag(yield* Effect.exit(drive(harness)(parent.threadId)))).toBe(
          "DurableRuntimeFailpointError",
        );
        yield* clearFailpoint;

        const replacement = Tool.make("delegate_research", {
          parameters: Schema.Struct({ topic: Schema.String }),
          success: Schema.Struct({ summary: Schema.String }),
        });

        const toolkit = Toolkit.make(replacement);

        const definition = Agent.make(coordinatorDefinition.id, {
          input: coordinatorDefinition.input,
          output: coordinatorDefinition.output,
          instructions: "Research.",
          toolkit,
          policy: coordinatorDefinition.policy,
        });

        const scripted = yield* makeScriptedModel(() =>
          finalParts('{"report":"delegation retired"}'),
        );

        const calls = yield* Ref.make(0);

        const exit = yield* Effect.exit(
          runtime.processThread(Agent.withModel(definition, scripted.model), parent.threadId).pipe(
            Effect.provide(
              toolkit.toLayer({
                delegate_research: () =>
                  Ref.update(calls, (n) => n + 1).pipe(Effect.as({ summary: "unexpected" })),
              }),
            ),
          ),
        );

        expect(exit).toMatchObject({
          _tag: "Success",
          value: [{ submissionId: parent.submissionId, outcome: "completed" }],
        });
        expect(
          payloadsOf(yield* readLog(parent.threadId), "ToolCallSettled").map(
            ({ record }) => record.payload,
          ),
        ).toMatchObject([
          {
            toolCallId: "delegate-1",
            isFailure: true,
            result: { _tag: "ToolUnavailable", execution: "not-executed" },
          },
        ]);
        expect(yield* Ref.get(calls)).toBe(0);
        expect(payloadsOf(yield* readLog(parent.threadId), "SubagentRequested")).toHaveLength(0);
      }),
  );

  it.effect(
    "SUB-033 a kill around the contained join yields ONE non-failure settlement carrying the bounded child failure",
    () =>
      Effect.gen(function* () {
        {
          const location = "subagent:after-join-append" as const;

          yield* clearFailpoint;
          const childScripted = yield* makeScriptedModel(() => finalParts("not-json"));
          const childBinding = Agent.withModel(childDefinition, childScripted.model);

          const parentScripted = yield* makeScriptedModel((call) =>
            call === 0
              ? toolTurn(toolCall("delegate-1", "delegate_research_contained", { topic: "paris" }))
              : finalParts('{"report":"handled"}'),
          );

          const parentBinding = Agent.withModel(
            containedCoordinatorDefinition,
            parentScripted.model,
          );

          const delegationLayer = Subagent.layer(containedResearchDelegation, childBinding, {
            mapChildFailure,
            durable: { targetDigests: CHILD_DIGEST_STRINGS },
          }).pipe(Layer.provide(delegationSupport));

          const parentResolved: ResolvedBinding = yield* DurableWorkerBinding.make(
            parentBinding,
            PARENT_DIGESTS,
          ).pipe(Effect.provide(delegationLayer));

          const childResolved: ResolvedBinding = yield* DurableWorkerBinding.make(
            childBinding,
            CHILD_DIGESTS,
          );

          const harness = {
            bindings: [parentResolved, childResolved],
            runtime: yield* DurableAgentRuntime.pipe(
              Effect.provide(
                DurableAgentRuntime.layerWithBindings([parentResolved, childResolved]).pipe(
                  Layer.provide(RunToolAuthorization.allowAll),
                ),
              ),
            ),
            childInvocations: childScripted.calls,
            parentPrompts: parentScripted.prompts,
            submitParent: submitParentWith(containedCoordinatorDefinition),
            lookupInvocations: Effect.succeed(0),
          };

          const run = drive(harness);
          const thread = `thread-s2-contained-${location.replaceAll(":", "-")}`;
          const parent = yield* harness.submitParent(thread, `contained-${location}`);
          const parentRunId = runIdForSubmission(parent.submissionId);
          const childThreadId = childThreadIdFor(parent.submissionId, DELEGATE_CALL);

          yield* run(parent.threadId);
          const childSettlements = yield* run(childThreadId);

          expect(childSettlements.map((settlement) => settlement.outcome)).toEqual(["failed"]);
          expect(yield* harness.childInvocations).toBe(1);
          yield* armFailpoint(location);
          const exit = yield* Effect.exit(run(parent.threadId));

          expect(failureTag(exit)).toBe("DurableRuntimeFailpointError");
          yield* clearFailpoint;
          const settlements = yield* run(parent.threadId);

          expect(settlements.map((settlement) => settlement.outcome)).toEqual(["completed"]);
          expect(yield* harness.childInvocations).toBe(1);
          const log = yield* readLog(parent.threadId);

          expect(payloadsOf(log, "SubagentJoined")).toHaveLength(1);

          const joinSettles = log.filter(
            (envelope) =>
              envelope.record.payload._tag === "ToolCallSettled" &&
              envelope.record.recordId === `tool-settled:${parentRunId}:1:delegate-1`,
          );

          expect(joinSettles).toHaveLength(1);
          const settled = joinSettles[0]?.record.payload;

          if (settled?._tag === "ToolCallSettled") {
            expect(settled.isFailure).toBe(false);
            expect(settled.result).toMatchObject({
              _tag: "SubagentExecutionFailure",
              classification: "child-failed",
            });
          }
          const finalPrompt = JSON.stringify(harness.parentPrompts.at(-1));

          expect(finalPrompt).toContain("SubagentExecutionFailure");
        }
      }),
  );

  // https://github.com/danieljvdm/effect-agent/commit/c1a6e6a915be73a49b2c266e2df74256f44c25e2
  // A later suspension duplicated prior Turn results and broke rollover after recovery.
  // https://linear.app/reve-ai/issue/KOM-127
  // Real provider metadata and assistant content must also survive the resumed declaration.
  it.effect("preserves prior Turn results when a later delegation suspends before rollover", () =>
    Effect.gen(function* () {
      const { childBinding } = yield* makeChildFixture;
      const Failure = Schema.TaggedStruct("LookupFailure", { detail: Schema.String });

      const toolkit = Toolkit.make(
        researchDelegation.tool,
        Lookup,
        Tool.make("fail_lookup", {
          parameters: Schema.Struct({}),
          success: Schema.String,
          failure: Failure,
          failureMode: "return",
        }),
        Tool.make("new_context", {
          parameters: ContextRolloverRequest,
          success: ContextRolloverRequest,
        })
          .annotate(ToolExecutionClass, "readonly")
          .annotate(ContextRolloverTool, true),
      );

      const definition = Agent.make("suspension-after-prior-results", {
        input: coordinatorDefinition.input,
        output: coordinatorDefinition.output,
        instructions: "Preserve completed results, delegate, roll over, then finish.",
        toolkit,
        policy: AgentPolicy.make({
          maxTurns: 8,
          maxToolCalls: 8,
          maxDuration: "30 seconds",
          toolConcurrency: 2,
        }),
      });

      const model = yield* makeScriptedModel((index) => {
        if (index === 0) return finalParts('{"report":"prior task"}');
        const call = index - 1;

        if (call === 0) return toolTurn(toolCall("prior-failure", "fail_lookup", {}));
        if (call === 1) return toolTurn(toolCall("prior-success", "lookup", { key: "prior" }));
        if (call === 2)
          return toolTurn(
            { type: "text-start", id: "delegation-note" },
            { type: "text-delta", id: "delegation-note", delta: "Delegating the retained task." },
            { type: "text-end", id: "delegation-note" },
            {
              ...toolCall("delegate-1", "delegate_research", { topic: "paris" }),
              metadata: { openai: { itemId: "fc_provider_item" } },
            },
            toolCall("current-sibling", "lookup", { key: "current" }),
          );
        if (call === 3)
          return toolTurn(
            toolCall("reset", "new_context", { handoff: "Research and both lookups completed." }),
          );

        return finalParts('{"report":"done"}');
      });

      const lookups = yield* Ref.make(0);

      const handlers = Toolkit.make(
        toolkit.tools.lookup,
        toolkit.tools.fail_lookup,
        toolkit.tools.new_context,
      ).toLayer({
        lookup: ({ key }) => Ref.update(lookups, (n) => n + 1).pipe(Effect.as({ value: key })),
        fail_lookup: () => Effect.fail(Failure.make({ detail: "Original typed failure" })),
        new_context: Effect.succeed,
      });

      const delegation = Subagent.layer(researchDelegation, childBinding, {
        mapChildFailure,
      }).pipe(Layer.provide(delegationSupport));

      const parentBinding = Agent.withModel(definition, model.model);

      const bindings = [
        yield* DurableWorkerBinding.make(parentBinding, PARENT_DIGESTS).pipe(
          Effect.provide(Layer.merge(handlers, delegation)),
        ),
        yield* DurableWorkerBinding.make(childBinding, CHILD_DIGESTS),
      ];

      const runtime = yield* DurableAgentRuntime.pipe(
        Effect.provide(
          DurableAgentRuntime.layerWithBindings(bindings).pipe(
            Layer.provide(RunToolAuthorization.allowAll),
          ),
        ),
      );

      const prior = yield* runtime.submit(
        parentBinding,
        { mission: "prior task" },
        submitOptions("suspension-prior-results", "prior"),
      );

      const run = drive({ runtime });

      expect((yield* run(prior.threadId)).map((settlement) => settlement.outcome)).toEqual([
        "completed",
      ]);

      const receipt = yield* runtime.submit(
        parentBinding,
        { mission: "regression" },
        submitOptions("suspension-prior-results", "one"),
      );

      yield* run(receipt.threadId);
      expect((yield* parentState(receipt.submissionId)).state).toBe("suspended");
      const suspended = yield* readLog(receipt.threadId);

      const priorResults = suspended.filter(
        ({ record }) =>
          record.payload._tag === "ToolCallSettled" &&
          ["prior-failure", "prior-success"].includes(record.payload.toolCallId),
      );

      expect(priorResults).toHaveLength(2);
      expect(priorResults[0]?.record.payload).toMatchObject({
        result: { _tag: "LookupFailure", detail: "Original typed failure" },
      });
      yield* run(childThreadIdFor(receipt.submissionId, DELEGATE_CALL));
      const completed = yield* run(receipt.threadId);

      expect(completed.map((settlement) => settlement.outcome)).toEqual(["completed"]);

      const resumedAssistant = model.prompts[4]?.content.find(
        (message) =>
          message.role === "assistant" &&
          message.content.some((part) => part.type === "tool-call" && part.id === "delegate-1"),
      );

      expect(resumedAssistant).toMatchObject({
        role: "assistant",
        content: [
          { type: "text", text: "Delegating the retained task." },
          {
            type: "tool-call",
            id: "delegate-1",
            options: { openai: { itemId: "fc_provider_item" } },
          },
          { type: "tool-call", id: "current-sibling" },
        ],
      });
      expect(yield* Ref.get(lookups)).toBe(2);
      const final = yield* readLog(receipt.threadId);

      expect(
        final.filter(
          ({ record }) =>
            record.payload._tag === "CompactionCreated" && record.payload.kind === "rollover",
        ),
      ).toHaveLength(1);
      expect(
        final.filter(
          ({ record }) =>
            record.payload._tag === "ToolCallSettled" &&
            ["prior-failure", "prior-success"].includes(record.payload.toolCallId),
        ),
      ).toHaveLength(2);
    }),
  );
  it.effect(
    "finishes the original requested child admission when a stale admission races its recovery lookup",
    () =>
      Effect.gen(function* () {
        yield* clearFailpoint;
        const ledger = yield* SubmissionLedger;
        const original = yield* makeHarness();

        const parent = yield* original.submitParent(
          "removed-requested-delegation-race",
          "original",
        );

        yield* armFailpoint("subagent:after-request-append");
        expect(failureTag(yield* Effect.exit(drive(original)(parent.threadId)))).toBe(
          "DurableRuntimeFailpointError",
        );
        yield* clearFailpoint;
        const retained = yield* readLog(parent.threadId);
        const request = payloadsOf(retained, "SubagentRequested")[0]?.record.payload;

        if (request?._tag !== "SubagentRequested")
          return yield* Effect.die("Expected the original child launch request");

        const admission = AdmissionRequest.make({
          threadId: request.childThreadId,
          principal: Schema.decodeSync(Principal)(request.childPrincipal),
          idempotencyKey: decodeIdempotencyKey(request.childIdempotencyKey),
          agentId: request.targetAgentId,
          agentDigests: request.targetDigests,
          deploymentId: Schema.decodeSync(DeploymentId)("deployment-durable-subagents"),
          inputPayload: request.childInput,
          inputDigest: request.childInputDigest,
          parentLinkage: ParentLinkage.make({
            parentSubmissionId: parent.submissionId,
            parentToolCallId: request.toolCallId,
          }),
        });

        const staleReceipt = yield* Deferred.make<AdmissionResult>();
        const sawNotAdmitted = yield* Ref.make(false);
        const raced = yield* Ref.make(false);
        const admissions: Array<AdmissionRequest> = [];

        const racingLedger = SubmissionLedger.of({
          ...ledger,
          admit: (value) =>
            Effect.gen(function* () {
              if (
                value.threadId === request.childThreadId &&
                !(yield* Ref.getAndSet(raced, true))
              ) {
                expect(yield* Ref.get(sawNotAdmitted)).toBe(true);
                expect(
                  (yield* ledger.loadRecoverySnapshot(
                    RecoverySnapshotRequest.make({ submissionId: parent.submissionId }),
                  )).childReservations.map((reservation) => reservation.status),
                ).toEqual(["reserved"]);
                // The stale owner's request lands after recovery observed authoritative absence,
                // immediately before recovery completes that same idempotent admission.
                admissions.push(admission);
                yield* Deferred.succeed(staleReceipt, yield* ledger.admit(admission));
              }
              admissions.push(value);

              return yield* ledger.admit(value);
            }),
          resolveAdmission: (lookup) =>
            Effect.gen(function* () {
              const observed = yield* ledger.resolveAdmission(lookup);

              if (lookup.threadId === request.childThreadId && observed._tag === "NotAdmitted") {
                yield* Ref.set(sawNotAdmitted, true);
              }

              return observed;
            }),
        });

        const currentModel = yield* makeScriptedModel(() =>
          finalParts('{"report":"original requested child joined"}'),
        );

        const currentDefinition = Agent.make(coordinatorDefinition.id, {
          input: coordinatorDefinition.input,
          output: coordinatorDefinition.output,
          instructions: "Finish from the existing child request.",
          toolkit: Toolkit.empty,
          policy: coordinatorDefinition.policy,
        });

        const currentBinding = yield* DurableWorkerBinding.make(
          Agent.withModel(currentDefinition, currentModel.model),
          WRONG_CHILD_DIGESTS,
        );

        const runtime = yield* DurableAgentRuntime.pipe(
          Effect.provide(
            DurableAgentRuntime.layerWithBindings([
              currentBinding,
              ...original.bindings.filter((binding) => binding.agentId === childDefinition.id),
            ]).pipe(
              Layer.provide([
                RunToolAuthorization.allowAll,
                Layer.succeed(SubmissionLedger)(racingLedger),
              ]),
            ),
          ),
        );

        expect(yield* runtime.processThreadResolved(parent.threadId)).toEqual([]);
        expect(yield* Ref.get(raced)).toBe(true);
        const racedAdmission = yield* Deferred.await(staleReceipt);

        expect(admissions).toHaveLength(2);
        expect(
          admissions.every((value) => Schema.toEquivalence(AdmissionRequest)(value, admission)),
        ).toBe(true);
        expect(
          (yield* parentReservations(parent.submissionId)).map((reservation) => ({
            status: reservation.status,
            childSubmissionId: reservation.childSubmissionId,
          })),
        ).toEqual([{ status: "reserved", childSubmissionId: racedAdmission.submissionId }]);
        expect(yield* currentModel.calls).toBe(0);
        const child = yield* runtime.processThreadResolved(request.childThreadId);

        expect(child).toMatchObject([
          { submissionId: racedAdmission.submissionId, outcome: "completed" },
        ]);
        expect(yield* runtime.processThreadResolved(parent.threadId)).toMatchObject([
          { submissionId: parent.submissionId, outcome: "completed" },
        ]);
        expect(yield* original.childInvocations).toBe(1);
        expect(original.parentPrompts).toHaveLength(1);
        expect(yield* currentModel.calls).toBe(1);
        const after = yield* readLog(parent.threadId);

        expect(after.slice(0, retained.length)).toEqual(retained);
        expect(payloadsOf(after, "SubagentRequested")).toHaveLength(1);
        expect(
          payloadsOf(after, "SubagentStarted").map(({ record }) => record.payload),
        ).toMatchObject([
          {
            childSubmissionId: racedAdmission.submissionId,
            childThreadId: request.childThreadId,
            childReceiptId: racedAdmission.receiptId,
            toolCallId: request.toolCallId,
          },
        ]);
        expect(payloadsOf(after, "SubagentJoined")).toHaveLength(1);
        expect(
          (yield* parentReservations(parent.submissionId)).map((reservation) => reservation.status),
        ).toEqual(["released"]);
        expect(payloadsOf(yield* readLog(request.childThreadId), "ThreadCreated")).toHaveLength(1);
      }),
  );

  it.effect(
    "answers later input under host authorization while an original mutation is unknown, then resumes its original identity",
    () =>
      Effect.gen(function* () {
        yield* clearFailpoint;
        const laterEntered = yield* Deferred.make<void>();
        const releaseLater = yield* Deferred.make<void>();
        const modelCalls = yield* Ref.make(0);
        const handlerCalls = yield* Ref.make(0);
        const prompts: Array<Prompt.Prompt> = [];

        const model = Model.make(
          "scripted",
          "unknown-followup",
          Layer.effect(
            LanguageModel.LanguageModel,
            LanguageModel.make({
              generateText: () => Effect.succeed([]),
              streamText: (request) =>
                Stream.unwrap(
                  Effect.gen(function* () {
                    const call = yield* Ref.getAndUpdate(modelCalls, (count) => count + 1);

                    prompts.push(request.prompt);
                    if (call === 3) {
                      yield* Deferred.succeed(laterEntered, undefined);
                      yield* Deferred.await(releaseLater);
                    }

                    return Stream.fromIterable(
                      call === 0
                        ? toolTurn(toolCall("original-mutation", "lookup", { key: "original" }))
                        : call === 1
                          ? toolTurn(toolCall("fresh-mutation", "lookup", { key: "original" }))
                          : call === 2
                            ? toolTurn(
                                toolCall("fresh-delegation", "delegate_research", {
                                  topic: "redo the uncertain action",
                                }),
                              )
                            : finalParts(
                                call === 3
                                  ? '{"report":"later question answered"}'
                                  : '{"report":"original result confirmed"}',
                              ),
                    );
                  }),
                ),
            }),
          ),
        );

        const agent = Agent.withModel(mixedCoordinatorDefinition, model);
        const { childScripted, childBinding } = yield* makeChildFixture;

        const delegation = Subagent.layer(researchDelegation, childBinding, {
          mapChildFailure,
          durable: { targetDigests: CHILD_DIGEST_STRINGS },
        }).pipe(Layer.provide(delegationSupport));

        const handlers = Toolkit.make(Lookup).toLayer({
          lookup: () =>
            Ref.update(handlerCalls, (count) => count + 1).pipe(
              Effect.as({ value: "must not execute" }),
            ),
        });

        const parentBinding = yield* DurableWorkerBinding.make(agent, PARENT_DIGESTS).pipe(
          Effect.provide(Layer.merge(delegation, handlers)),
        );

        const childResolved = yield* DurableWorkerBinding.make(childBinding, CHILD_DIGESTS);
        const denied: Array<string> = [];
        let originalRunId: RunId | undefined;

        const runtime = yield* DurableAgentRuntime.pipe(
          Effect.provide(
            DurableAgentRuntime.layerWithBindings([parentBinding, childResolved]).pipe(
              Layer.provide(
                Layer.succeed(RunToolAuthorization)({
                  authorize: ({ runId, call }) => {
                    if (runId === originalRunId && call.toolCallId === "original-mutation")
                      return Effect.succeed({ _tag: "allowed" });
                    denied.push(call.toolCallId);

                    return Effect.succeed({
                      _tag: "denied",
                      reason:
                        "An unresolved supplier action does not authorize replacement mutations or delegation",
                    });
                  },
                }),
              ),
            ),
          ),
        );

        const original = yield* runtime.submit(
          agent,
          { mission: "perform the original action" },
          submitOptions("thread-unknown-later-input", "original"),
        );

        originalRunId = runIdForSubmission(original.submissionId);
        yield* armFailpoint("tools:after-prepared-append");
        expect(failureTag(yield* Effect.exit(runtime.processThreadHead(original.threadId)))).toBe(
          "DurableRuntimeFailpointError",
        );
        yield* clearFailpoint;
        yield* runtime.runRecovery();
        expect((yield* parentState(original.submissionId)).state).toBe("unknown");
        const retained = yield* readLog(original.threadId);

        for (const request of ["unsafe mutation", "unsafe delegation"]) {
          const deniedReceipt = yield* runtime.submit(
            agent,
            { mission: request },
            submitOptions(original.threadId, request),
          );

          const refused = yield* runtime.processThreadHead(original.threadId);

          expect(Option.isSome(refused) && refused.value).toMatchObject({
            submissionId: deniedReceipt.submissionId,
            outcome: "failed",
            failure: { errorTag: "AgentToolAuthorizationDenied" },
          });
          expect((yield* parentState(original.submissionId)).state).toBe("unknown");
        }

        const later = yield* runtime.submit(
          agent,
          { mission: "answer this later question without duplicating the action" },
          submitOptions(original.threadId, "later"),
        );

        const laterWorker = yield* Effect.forkChild(runtime.processThreadHead(original.threadId));

        yield* Deferred.await(laterEntered);
        expect((yield* parentState(original.submissionId)).state).toBe("unknown");

        const laterSnapshot = yield* SubmissionLedger.use((ledger) =>
          ledger.loadRecoverySnapshot(
            RecoverySnapshotRequest.make({ submissionId: later.submissionId }),
          ),
        );

        expect(laterSnapshot.ownership).toBeDefined();
        yield* runtime.resolveUnknown(
          UnknownResolutionCommand.make({
            submissionId: original.submissionId,
            toolCallId: decodeToolCallId("original-mutation"),
            author: "supplier-operator",
            reason: "The supplier confirms the exact original operation",
            resolution: ResolutionCompletedWithResult.make({
              result: { value: "original supplier receipt" },
              isFailure: false,
            }),
          }),
        );
        // Waking the original Run must not displace the later Run's live ownership.
        expect(Option.isNone(yield* runtime.processThreadHead(original.threadId))).toBe(true);
        yield* Deferred.succeed(releaseLater, undefined);
        const laterSettlement = yield* Fiber.join(laterWorker);

        expect(Option.isSome(laterSettlement) && laterSettlement.value).toMatchObject({
          submissionId: later.submissionId,
          outcome: "completed",
        });
        const originalSettlement = yield* runtime.processThreadHead(original.threadId);

        expect(Option.isSome(originalSettlement) && originalSettlement.value).toMatchObject({
          submissionId: original.submissionId,
          outcome: "completed",
        });
        expect(denied).toEqual(["fresh-mutation", "fresh-delegation"]);
        expect(yield* Ref.get(handlerCalls)).toBe(0);
        expect(yield* childScripted.calls).toBe(0);
        expect(yield* Ref.get(modelCalls)).toBe(5);
        expect(JSON.stringify(prompts[3])).toContain("answer this later question");
        expect(JSON.stringify(prompts[4])).toContain("original supplier receipt");
        const after = yield* readLog(original.threadId);

        expect(after.slice(0, retained.length)).toEqual(retained);
        expect(
          payloadsOf(after, "ToolCallUnknown").map(({ record }) => record.payload),
        ).toMatchObject([{ toolCallId: "original-mutation" }]);
        expect(payloadsOf(after, "SubagentRequested")).toEqual([]);
        expect(
          payloadsOf(after, "ToolCallSettled").map(({ record }) => record.payload),
        ).toMatchObject([
          {
            runId: runIdForSubmission(original.submissionId),
            toolCallId: "original-mutation",
            result: { value: "original supplier receipt" },
          },
        ]);
        expect(
          payloadsOf(after, "SubmissionSettled")
            .map(({ record }) => record.payload)
            .filter(
              (payload) => payload._tag === "SubmissionSettled" && payload.outcome === "completed",
            ),
        ).toMatchObject([
          { submissionId: later.submissionId, result: { report: "later question answered" } },
          { submissionId: original.submissionId, result: { report: "original result confirmed" } },
        ]);
      }),
  );
});

layer(faultTestLayer)("S2 durable Subagents under indeterminate admission (SUB-031)", (it) => {
  it.effect("an indeterminate admission resolution never admits a second child", () =>
    Effect.gen(function* () {
      yield* clearFailpoint;
      admissionFault = undefined;
      const harness = yield* makeHarness();
      const run = drive(harness);
      const thread = "thread-s2-indeterminate";
      const parent = yield* harness.submitParent(thread, "indeterminate-1");
      const childThreadId = childThreadIdFor(parent.submissionId, DELEGATE_CALL);
      const runtime = yield* DurableAgentRuntime;

      admissionFault = "the authoritative child owner is unreachable";
      // The Attempt aborts typed: an indeterminate answer never permits an admission attempt.
      const exit = yield* Effect.exit(run(parent.threadId));

      expect(failureTag(exit)).toBe("LedgerError");
      // Recovery classifies the wait honestly and defers — no second admission either.
      const reports = (yield* runtime.runRecovery()).reports;
      const parentReport = reports.find((report) => report.submissionId === parent.submissionId);

      expect(parentReport?.decision._tag).toBe("AwaitChildAdmissionResolution");
      expect(parentReport?.disposition).toBe("deferred");

      admissionFault = undefined;
      // The authoritative owner answers: exactly one child is admitted and joined.
      yield* run(parent.threadId);
      const childSettlements = yield* run(childThreadId);

      expect(childSettlements.map((settlement) => settlement.outcome)).toEqual(["completed"]);
      const settlements = yield* run(parent.threadId);

      expect(settlements.map((settlement) => settlement.outcome)).toEqual(["completed"]);
      expect(yield* harness.childInvocations).toBe(1);
      const childLog = yield* readLog(childThreadId);

      expect(payloadsOf(childLog, "ThreadCreated")).toHaveLength(1);
    }),
  );
});
