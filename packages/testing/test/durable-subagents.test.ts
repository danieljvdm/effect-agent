import {
  Subagent,
  SubagentPolicy,
  SubagentReservationsMemoryLive,
  SubagentRuntime,
} from "@effect-agent/capabilities";
import {
  Agent,
  AgentPolicy,
  ConversationId,
  IdGenerator,
  RunId,
  ToolCallId,
  TurnId,
  type SubmissionId,
} from "@effect-agent/core";
import { ToolExecutionClass } from "@effect-agent/engine";
import {
  AbortCommand,
  AgentBindingResolver,
  ClaimRequest,
  ConversationRead,
  ConversationStore,
  DefinitionDigests,
  DeploymentId,
  Digest,
  DurableAgentRuntime,
  DurableRuntimeConfig,
  DurableRuntimeFailpoint,
  DurableRuntimeFailpointError,
  DurableRuntimeFailpointTestControl,
  DurableWorkerBinding,
  IdempotencyKey,
  Principal,
  ProducerId,
  RecoverySnapshotRequest,
  RecordEnvelope,
  SettlementFinalization,
  SubmissionLedger,
  SubmissionLookupById,
  SubmissionLookupByKey,
  ToolReconciler,
  ToolCallPrepared,
  UnknownResolutionCommand,
  WakeScheduler,
  childConversationIdFor,
  runIdForSubmission,
  submissionSettlementId,
  type DurableRuntimeFailpointLocation,
  type DurableSubmitOptions,
  type ResolvedBinding,
  type CanonicalRecordEnvelope,
} from "@effect-agent/session";
import {
  MemoryConversationStoreLive,
  MemorySubmissionLedgerLive,
  memorySubmissionLedgerLayer,
} from "@effect-agent/storage-memory";
import { NodeCrypto } from "@effect/platform-node";
import { expect, layer } from "@effect/vitest";
import { Cause, Duration, Effect, Exit, Fiber, Layer, Option, Ref, Schema, Stream } from "effect";
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
const decodeConversationId = Schema.decodeSync(ConversationId);
const decodeIdempotencyKey = Schema.decodeSync(IdempotencyKey);
const decodeToolCallId = Schema.decodeSync(ToolCallId);

const submitOptions = (conversationId: string, idempotencyKey: string): DurableSubmitOptions => ({
  conversationId: decodeConversationId(conversationId),
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

const childDefinition = Agent.define("research-child", {
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

const coordinatorDefinition = Agent.define("travel-coordinator", {
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

const mixedCoordinatorDefinition = Agent.define("travel-coordinator-mixed", {
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

const containedCoordinatorDefinition = Agent.define("travel-coordinator-contained", {
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
        MemoryConversationStoreLive,
        WakeScheduler.layerNoop,
        DurableRuntimeFailpoint.layerTest,
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
      nextConversationId: next(decodeConversationId, "fixture-conversation"),
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
  (conversation: string, key: string) =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const receipt = yield* runtime.submit(
        // The structural submit slice only needs identity + input schema.
        { definition: { id: definition.id, input: coordinatorDefinition.input } },
        { mission: "plan" },
        submitOptions(conversation, key),
      );
      return { submissionId: receipt.submissionId, conversationId: receipt.conversationId };
    });

/**
 * One durable parent/child fixture: the parent coordinator delegates
 * `delegate_research` to the scripted child; both bindings register with the
 * resolver under their exact digests (the child optionally under WRONG
 * digests to force the compatibility path).
 */
const makeHarness = (options?: { readonly childRegistrationDigests?: DefinitionDigests }) =>
  Effect.gen(function* () {
    const { childScripted, childBinding } = yield* makeChildFixture;
    const parentScripted = yield* makeScriptedModel((call) =>
      call === 0
        ? toolTurn(toolCall("delegate-1", "delegate_research", { topic: "paris" }))
        : finalParts('{"report":"done"}'),
    );
    const parentBinding = Agent.withModel(coordinatorDefinition, parentScripted.model);
    const delegationLayer = SubagentRuntime.layer(researchDelegation, childBinding, {
      mapChildFailure,
      durable: { targetDigests: CHILD_DIGEST_STRINGS },
    }).pipe(Layer.provide(delegationSupport));
    const parentResolved: ResolvedBinding = yield* DurableWorkerBinding.make(
      parentBinding,
      PARENT_DIGESTS,
    ).pipe(Effect.provide(delegationLayer));
    const childResolved: ResolvedBinding = yield* DurableWorkerBinding.make(
      childBinding,
      options?.childRegistrationDigests ?? CHILD_DIGESTS,
    );
    const resolver = AgentBindingResolver.fromBindings([parentResolved, childResolved]);
    return {
      resolver,
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
    const definition = Agent.define(mixedCoordinatorDefinition.id, {
      input: mixedCoordinatorDefinition.input,
      output: mixedCoordinatorDefinition.output,
      instructions: mixedCoordinatorDefinition.instructions,
      toolkit: Toolkit.make(researchDelegation.tool, lookupTool),
      policy: mixedCoordinatorDefinition.policy,
    });
    const parentBinding = Agent.withModel(definition, parentScripted.model);
    const lookupInvocations = yield* Ref.make(0);
    const lookupFinalizers = yield* Ref.make(0);
    const delegationLayer = SubagentRuntime.layer(researchDelegation, childBinding, {
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
    const resolver = AgentBindingResolver.fromBindings([parentResolved, childResolved]);
    return {
      resolver,
      childInvocations: childScripted.calls,
      parentPrompts: parentScripted.prompts,
      submitParent: submitParentWith(mixedCoordinatorDefinition),
      lookupInvocations: Ref.get(lookupInvocations),
      lookupFinalizers: Ref.get(lookupFinalizers),
    };
  });

const makeSiblingHarness = makeSiblingHarnessWith();

const DELEGATE_CALL = decodeToolCallId("delegate-1");

const drive =
  (harness: { readonly resolver: (typeof AgentBindingResolver)["Service"] }) =>
  (conversationId: ConversationId) =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      return yield* runtime
        .processConversationResolved(conversationId)
        .pipe(Effect.provideService(AgentBindingResolver, harness.resolver));
    });

const readLog = (conversationId: ConversationId) =>
  Effect.gen(function* () {
    const store = yield* ConversationStore;
    return yield* Stream.runCollect(
      store.read(ConversationRead.make({ conversationId, limit: 1_024 })),
    );
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

const recordIds = (records: ReadonlyArray<CanonicalRecordEnvelope>): ReadonlyArray<string> =>
  records.map((envelope) => envelope.record.recordId as string);

const payloadsOf = <Tag extends string>(
  records: ReadonlyArray<CanonicalRecordEnvelope>,
  tag: Tag,
): ReadonlyArray<CanonicalRecordEnvelope> =>
  records.filter((envelope) => envelope.record.payload._tag === tag);

layer(testLayer)("S2 durable attached Subagents (WP4 coordinator)", (it) => {
  it.effect(
    "establishes the child, suspends waitingForChild without a worker permit, and joins the settled child",
    () =>
      Effect.gen(function* () {
        yield* clearFailpoint;
        const harness = yield* makeHarness();
        const run = drive(harness);
        const conversation = "conversation-s2-happy";
        const parent = yield* harness.submitParent(conversation, "happy-1");
        const parentRunId = runIdForSubmission(parent.submissionId);
        const childConversationId = childConversationIdFor(parent.submissionId, DELEGATE_CALL);

        // Phase 1: establishment + waitingForChild suspension.
        const first = yield* run(parent.conversationId);
        expect(first).toHaveLength(0);
        expect((yield* parentState(parent.submissionId)).state).toBe("suspended");
        // The waiting lane holds no worker permit and is not claimable (SUB-030).
        const ledger = yield* SubmissionLedger;
        const claimed = yield* ledger.claim(
          ClaimRequest.make({
            conversationId: parent.conversationId,
            producerId: Schema.decodeSync(ProducerId)("producer-durable-subagents"),
          }),
        );
        expect(Option.isNone(claimed)).toBe(true);
        // The child never ran in-process while the parent waited.
        expect(yield* harness.childInvocations).toBe(0);
        const afterEstablish = yield* readLog(parent.conversationId);
        expect(recordIds(afterEstablish)).toContain(`subagent-requested:${parentRunId}:delegate-1`);
        expect(recordIds(afterEstablish)).toContain(`subagent-started:${parentRunId}:delegate-1`);
        const reservations = yield* parentReservations(parent.submissionId);
        expect(reservations).toHaveLength(1);
        expect(reservations[0]?.status).toBe("reserved");

        // Phase 2: the child lane runs to Settlement and wakes the parent durably.
        const childSettlements = yield* run(childConversationId);
        expect(childSettlements).toHaveLength(1);
        expect(childSettlements[0]?.outcome).toBe("completed");
        expect(yield* harness.childInvocations).toBe(1);
        expect((yield* parentState(parent.submissionId)).state).toBe("input-applied");
        const childLog = yield* readLog(childConversationId);
        expect(recordIds(childLog)).toContain(`subagent-lineage:${childConversationId}`);

        // Phase 3: the woken parent joins the verified child Settlement atomically.
        const settlements = yield* run(parent.conversationId);
        expect(settlements).toHaveLength(1);
        expect(settlements[0]?.outcome).toBe("completed");
        expect(yield* harness.childInvocations).toBe(1);
        const log = yield* readLog(parent.conversationId);
        const joined = payloadsOf(log, "SubagentJoined");
        expect(joined).toHaveLength(1);
        expect(joined[0]?.batchId).toBe(`subagent-join:${parentRunId}:delegate-1`);
        const joinSettle = log.find(
          (envelope) => envelope.record.recordId === `tool-settled:${parentRunId}:1:delegate-1`,
        );
        expect(joinSettle?.batchId).toBe(`subagent-join:${parentRunId}:delegate-1`);
        expect(
          joinSettle?.record.payload._tag === "ToolCallSettled"
            ? joinSettle.record.payload.result
            : undefined,
        ).toEqual({ summary: "finding:child-answer" });
        const released = yield* parentReservations(parent.submissionId);
        expect(released[0]?.status).toBe("released");
        // Task #12: the resumed Attempt's next model request still carries the
        // Turn-1 leading messages (instructions + input) before the assistant
        // tool-call message.
        const finalPrompt = harness.parentPrompts.at(-1);
        expect(finalPrompt).toBeDefined();
        const roles = (finalPrompt?.content ?? []).map((message) => message.role);
        expect(roles.indexOf("user")).toBeGreaterThanOrEqual(0);
        expect(roles.indexOf("user")).toBeLessThan(roles.indexOf("assistant"));
      }),
  );

  it.effect("RUN-030: counts waitingForChild wall clock against parent maxDuration", () =>
    Effect.gen(function* () {
      yield* clearFailpoint;
      const harness = yield* makeHarness();
      const run = drive(harness);
      const conversation = "conversation-s2-parent-duration";
      const parent = yield* harness.submitParent(conversation, "parent-duration-1");
      const childConversationId = childConversationIdFor(parent.submissionId, DELEGATE_CALL);

      const first = yield* run(parent.conversationId);
      expect(first).toHaveLength(0);
      expect((yield* parentState(parent.submissionId)).state).toBe("suspended");

      // `maxDuration` is wall clock for the logical Run, not a fresh allowance per Attempt.
      // Advancing deterministic time while no parent worker is held must therefore exhaust the
      // same parent Run before its replacement Attempt can continue after the child join.
      yield* TestClock.adjust(Duration.seconds(31));

      const childSettlements = yield* run(childConversationId);
      expect(childSettlements.map((settlement) => settlement.outcome)).toEqual(["completed"]);
      expect((yield* parentState(parent.submissionId)).state).toBe("input-applied");

      const parentSettlements = yield* run(parent.conversationId);
      expect(parentSettlements.map((settlement) => settlement.outcome)).toEqual(["failed"]);
      expect(yield* harness.childInvocations).toBe(1);
      expect(harness.parentPrompts).toHaveLength(1);

      const log = yield* readLog(parent.conversationId);
      expect(payloadsOf(log, "SubagentJoined")).toHaveLength(1);
      expect((yield* parentReservations(parent.submissionId)).map((row) => row.status)).toEqual([
        "released",
      ]);
      const settled = payloadsOf(log, "SubmissionSettled")[0]?.record.payload;
      if (settled?._tag !== "SubmissionSettled") throw new Error("Expected SubmissionSettled");
      expect(settled.policyLimit).toBe("duration");
      expect(settled.result).toMatchObject({ errorTag: "AgentPolicyError" });
    }),
  );

  it.effect(
    "RUN-030: expired mixed batches join settled children without retrying ordinary Tools",
    () =>
      Effect.gen(function* () {
        const locations: ReadonlyArray<DurableRuntimeFailpointLocation | undefined> = [
          undefined,
          "subagent:after-join-append",
          "subagent:after-release-pending",
          "subagent:after-release",
        ];
        for (const location of locations) {
          yield* clearFailpoint;
          const harness = yield* makeSiblingHarnessWith(true);
          const run = drive(harness);
          const parent = yield* harness.submitParent(
            `expired-mixed-${location ?? "complete"}`,
            "parent",
          );
          const childConversationId = childConversationIdFor(parent.submissionId, DELEGATE_CALL);
          const firstAttempt = yield* Effect.forkChild(run(parent.conversationId));
          yield* TestClock.adjust(Duration.seconds(31));
          expect(yield* Fiber.join(firstAttempt)).toEqual([]);
          expect(yield* harness.lookupInvocations).toBe(1);
          expect(yield* harness.lookupFinalizers).toBe(1);
          expect(payloadsOf(yield* readLog(parent.conversationId), "ToolCallSettled")).toHaveLength(
            0,
          );
          expect((yield* run(childConversationId)).map((entry) => entry.outcome)).toEqual([
            "completed",
          ]);
          if (location !== undefined) {
            yield* armFailpoint(location);
            expect(failureTag(yield* Effect.exit(run(parent.conversationId)))).toBe(
              "DurableRuntimeFailpointError",
            );
            yield* clearFailpoint;
          }
          expect((yield* run(parent.conversationId)).map((entry) => entry.outcome)).toEqual([
            "failed",
          ]);
          expect(yield* run(parent.conversationId)).toEqual([]);
          expect(yield* harness.lookupInvocations).toBe(1);
          expect(yield* harness.lookupFinalizers).toBe(1);
          expect(yield* harness.childInvocations).toBe(1);
          expect(harness.parentPrompts).toHaveLength(1);
          const records = yield* readLog(parent.conversationId);
          expect(payloadsOf(records, "SubagentJoined")).toHaveLength(1);
          expect(
            payloadsOf(records, "ToolCallSettled").map(({ record }) => record.payload),
          ).toMatchObject([
            {
              toolCallId: DELEGATE_CALL,
              isFailure: true,
              result: { errorTag: "SubagentParentDurationExceeded" },
            },
          ]);
          expect((yield* parentReservations(parent.submissionId)).map((row) => row.status)).toEqual(
            ["released"],
          );
          expect(records.at(-1)?.record.payload).toMatchObject({
            _tag: "SubmissionSettled",
            outcome: "failed",
            policyLimit: "duration",
          });
        }
      }),
  );

  it.effect(
    "RUN-030: expired child cleanup preserves an uncertain ordinary call until operator resolution",
    () =>
      Effect.gen(function* () {
        yield* clearFailpoint;
        const runtime = yield* DurableAgentRuntime;
        const harness = yield* makeSiblingHarnessWith(true, false);
        const run = drive(harness);
        const parent = yield* harness.submitParent("expired-uncertain-mixed", "parent");
        const firstAttempt = yield* Effect.forkChild(run(parent.conversationId));
        yield* TestClock.adjust(Duration.seconds(31));
        expect(yield* Fiber.join(firstAttempt)).toEqual([]);
        expect(yield* run(parent.conversationId)).toEqual([]);
        yield* runtime.runRecovery;
        expect((yield* parentState(parent.submissionId)).state).not.toBe("unknown");
        const childConversationId = childConversationIdFor(parent.submissionId, DELEGATE_CALL);
        expect((yield* run(childConversationId)).map((entry) => entry.outcome)).toEqual([
          "completed",
        ]);
        yield* runtime.runRecovery;
        expect((yield* parentState(parent.submissionId)).state).not.toBe("unknown");
        expect(yield* run(parent.conversationId)).toEqual([]);
        expect((yield* parentState(parent.submissionId)).state).toBe("unknown");
        const records = yield* readLog(parent.conversationId);
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
        expect((yield* run(parent.conversationId)).map((entry) => entry.outcome)).toEqual([
          "failed",
        ]);
        expect(yield* harness.lookupInvocations).toBe(1);
        expect(yield* harness.lookupFinalizers).toBe(1);
        expect(harness.parentPrompts).toHaveLength(1);
      }),
  );

  it.effect(
    "RUN-030: expired cleanup releases unstarted reservations before marking an ordinary call Unknown",
    () =>
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const ledger = yield* SubmissionLedger;
        for (const scenario of [
          { location: "subagent:after-reserve" as const, requested: false },
          { location: "subagent:after-request-append" as const, requested: true },
        ]) {
          for (const order of ["worker-first", "recovery-first"] as const) {
            yield* clearFailpoint;
            const harness = yield* makeSiblingHarnessWith(true, false);
            const run = drive(harness);
            const parent = yield* harness.submitParent(
              `expired-unstarted-${scenario.location.replaceAll(":", "-")}-${order}`,
              "parent",
            );
            const childConversationId = childConversationIdFor(parent.submissionId, DELEGATE_CALL);
            const childLookup = SubmissionLookupByKey.make({
              conversationId: childConversationId,
              principal: PRINCIPAL,
              idempotencyKey: decodeIdempotencyKey(
                `subagent:${runIdForSubmission(parent.submissionId)}:delegate-1`,
              ),
            });

            yield* armFailpoint(scenario.location);
            expect(failureTag(yield* Effect.exit(run(parent.conversationId)))).toBe(
              "DurableRuntimeFailpointError",
            );
            yield* clearFailpoint;
            const lookupStarts = yield* harness.lookupInvocations;
            const lookupFinalizers = yield* harness.lookupFinalizers;
            expect(
              (yield* parentReservations(parent.submissionId)).map((row) => row.status),
            ).toEqual(["reserved"]);
            expect(
              payloadsOf(yield* readLog(parent.conversationId), "SubagentRequested"),
            ).toHaveLength(scenario.requested ? 1 : 0);
            expect(yield* ledger.resolveAdmission(childLookup)).toMatchObject({
              _tag: "NotAdmitted",
            });

            yield* TestClock.adjust(Duration.seconds(31));
            if (order === "worker-first") {
              expect(yield* run(parent.conversationId)).toEqual([]);
              yield* runtime.runRecovery;
            } else {
              yield* runtime.runRecovery;
              expect(yield* ledger.resolveAdmission(childLookup)).toMatchObject({
                _tag: "NotAdmitted",
              });
              expect(yield* harness.childInvocations).toBe(0);
              expect(yield* harness.lookupInvocations).toBe(lookupStarts);
              expect(yield* harness.lookupFinalizers).toBe(lookupFinalizers);
              expect(harness.parentPrompts).toHaveLength(1);
              expect(
                payloadsOf(yield* readLog(parent.conversationId), "SubagentStarted"),
              ).toHaveLength(0);
              expect(yield* run(parent.conversationId)).toEqual([]);
            }
            for (let pass = 0; pass < 4; pass += 1) {
              const state = yield* parentState(parent.submissionId);
              const reservations = yield* parentReservations(parent.submissionId);
              if (
                state.state === "unknown" &&
                reservations.every((row) => row.status === "released")
              ) {
                break;
              }
              expect(yield* run(parent.conversationId)).toEqual([]);
              yield* runtime.runRecovery;
            }

            expect(
              (yield* parentReservations(parent.submissionId)).map((row) => row.status),
            ).toEqual(["released"]);
            expect((yield* parentState(parent.submissionId)).state).toBe("unknown");
            const beforeResolution = yield* readLog(parent.conversationId);
            expect(payloadsOf(beforeResolution, "SubagentStarted")).toHaveLength(0);
            expect(payloadsOf(beforeResolution, "SubagentJoined")).toHaveLength(0);
            expect(
              payloadsOf(beforeResolution, "ToolCallUnknown").map(({ record }) => record.payload),
            ).toMatchObject([{ toolCallId: "lookup-1" }]);
            expect(payloadsOf(beforeResolution, "SubmissionSettled")).toHaveLength(0);

            yield* runtime.resolveUnknown(
              UnknownResolutionCommand.make({
                submissionId: parent.submissionId,
                toolCallId: decodeToolCallId("lookup-1"),
                author: "test-operator",
                reason: "The fixture has no external side effect.",
                resolution: { _tag: "NeverHappened" },
              }),
            );
            expect((yield* run(parent.conversationId)).map((entry) => entry.outcome)).toEqual([
              "failed",
            ]);
            const settled = payloadsOf(
              yield* readLog(parent.conversationId),
              "SubmissionSettled",
            )[0]?.record.payload;
            expect(settled).toMatchObject({ outcome: "failed", policyLimit: "duration" });
            expect(yield* harness.childInvocations).toBe(0);
            expect(yield* harness.lookupInvocations).toBe(lookupStarts);
            expect(yield* harness.lookupFinalizers).toBe(lookupFinalizers);
            expect(harness.parentPrompts).toHaveLength(1);
            expect(yield* ledger.resolveAdmission(childLookup)).toMatchObject({
              _tag: "NotAdmitted",
            });
          }
        }
      }),
  );

  it.effect(
    "RUN-030: expired cleanup repairs an admitted child before marking an ordinary call Unknown",
    () =>
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const ledger = yield* SubmissionLedger;
        for (const order of ["worker-first", "recovery-first"] as const) {
          yield* clearFailpoint;
          const harness = yield* makeSiblingHarnessWith(true, false);
          const run = drive(harness);
          const parent = yield* harness.submitParent(
            `expired-admitted-before-start-${order}`,
            "parent",
          );

          yield* armFailpoint("subagent:after-admit");
          expect(failureTag(yield* Effect.exit(run(parent.conversationId)))).toBe(
            "DurableRuntimeFailpointError",
          );
          yield* clearFailpoint;
          const lookupStarts = yield* harness.lookupInvocations;
          const lookupFinalizers = yield* harness.lookupFinalizers;
          const requested = payloadsOf(
            yield* readLog(parent.conversationId),
            "SubagentRequested",
          )[0]?.record.payload;
          if (requested?._tag !== "SubagentRequested") {
            throw new Error("Expected SubagentRequested");
          }
          const lookup = SubmissionLookupByKey.make({
            conversationId: requested.childConversationId,
            principal: PRINCIPAL,
            idempotencyKey: decodeIdempotencyKey(requested.childIdempotencyKey),
          });
          const admittedBefore = yield* ledger.resolveAdmission(lookup);
          expect(admittedBefore._tag).toBe("Admitted");
          if (admittedBefore._tag !== "Admitted") throw new Error("Expected admitted child");
          expect(payloadsOf(yield* readLog(parent.conversationId), "SubagentStarted")).toHaveLength(
            0,
          );

          yield* TestClock.adjust(Duration.seconds(31));
          if (order === "worker-first") {
            expect(yield* run(parent.conversationId)).toEqual([]);
            yield* runtime.runRecovery;
          } else {
            yield* runtime.runRecovery;
            const admittedAfterRecovery = yield* ledger.resolveAdmission(lookup);
            expect(admittedAfterRecovery._tag).toBe("Admitted");
            if (admittedAfterRecovery._tag !== "Admitted") {
              throw new Error("Expected admitted child");
            }
            expect(admittedAfterRecovery.submission.submissionId).toBe(
              admittedBefore.submission.submissionId,
            );
            expect(admittedAfterRecovery.submission.receiptId).toBe(
              admittedBefore.submission.receiptId,
            );
            expect(yield* harness.childInvocations).toBe(0);
            expect(yield* harness.lookupInvocations).toBe(lookupStarts);
            expect(yield* harness.lookupFinalizers).toBe(lookupFinalizers);
            expect(harness.parentPrompts).toHaveLength(1);
            expect(yield* run(parent.conversationId)).toEqual([]);
          }
          for (let pass = 0; pass < 4; pass += 1) {
            if (payloadsOf(yield* readLog(parent.conversationId), "SubagentStarted").length === 1) {
              break;
            }
            expect(yield* run(parent.conversationId)).toEqual([]);
            yield* runtime.runRecovery;
          }
          const repairedLog = yield* readLog(parent.conversationId);
          const started = payloadsOf(repairedLog, "SubagentStarted")[0]?.record.payload;
          if (started?._tag !== "SubagentStarted") throw new Error("Expected SubagentStarted");
          expect(started.childSubmissionId).toBe(admittedBefore.submission.submissionId);
          expect(started.childReceiptId).toBe(admittedBefore.submission.receiptId);
          expect((yield* parentState(parent.submissionId)).state).not.toBe("unknown");

          expect((yield* run(requested.childConversationId)).map((entry) => entry.outcome)).toEqual(
            ["completed"],
          );
          yield* runtime.runRecovery;
          for (let pass = 0; pass < 4; pass += 1) {
            const state = yield* parentState(parent.submissionId);
            const reservations = yield* parentReservations(parent.submissionId);
            if (
              state.state === "unknown" &&
              reservations.every((row) => row.status === "released")
            ) {
              break;
            }
            expect(yield* run(parent.conversationId)).toEqual([]);
            yield* runtime.runRecovery;
          }

          expect((yield* parentReservations(parent.submissionId)).map((row) => row.status)).toEqual(
            ["released"],
          );
          expect((yield* parentState(parent.submissionId)).state).toBe("unknown");
          const joinedLog = yield* readLog(parent.conversationId);
          expect(payloadsOf(joinedLog, "SubagentStarted")).toHaveLength(1);
          expect(payloadsOf(joinedLog, "SubagentJoined")).toHaveLength(1);
          expect(
            payloadsOf(joinedLog, "ToolCallUnknown").map(({ record }) => record.payload),
          ).toMatchObject([{ toolCallId: "lookup-1" }]);
          const admittedAfter = yield* ledger.resolveAdmission(lookup);
          expect(admittedAfter._tag).toBe("Admitted");
          if (admittedAfter._tag !== "Admitted") throw new Error("Expected admitted child");
          expect(admittedAfter.submission.submissionId).toBe(
            admittedBefore.submission.submissionId,
          );
          expect(admittedAfter.submission.receiptId).toBe(admittedBefore.submission.receiptId);

          yield* runtime.resolveUnknown(
            UnknownResolutionCommand.make({
              submissionId: parent.submissionId,
              toolCallId: decodeToolCallId("lookup-1"),
              author: "test-operator",
              reason: "The fixture has no external side effect.",
              resolution: { _tag: "NeverHappened" },
            }),
          );
          expect((yield* run(parent.conversationId)).map((entry) => entry.outcome)).toEqual([
            "failed",
          ]);
          const settled = payloadsOf(yield* readLog(parent.conversationId), "SubmissionSettled")[0]
            ?.record.payload;
          expect(settled).toMatchObject({ outcome: "failed", policyLimit: "duration" });
          expect(yield* harness.childInvocations).toBe(1);
          expect(yield* harness.lookupInvocations).toBe(lookupStarts);
          expect(yield* harness.lookupFinalizers).toBe(lookupFinalizers);
          expect(harness.parentPrompts).toHaveLength(1);
        }
      }),
  );

  it.effect("RUN-030: expired cleanup rejects missing or mismatched canonical Tool identity", () =>
    Effect.gen(function* () {
      const store = yield* ConversationStore;
      for (const corrupt of ["missing", "mismatched"] as const) {
        yield* clearFailpoint;
        const harness = yield* makeHarness();
        const run = drive(harness);
        const parent = yield* harness.submitParent(`expired-identity-${corrupt}`, "parent");
        yield* run(parent.conversationId);
        yield* run(childConversationIdFor(parent.submissionId, DELEGATE_CALL));
        yield* TestClock.adjust(Duration.seconds(31));
        const faulty = ConversationStore.of({
          ...store,
          read: (request) =>
            store.read(request).pipe(
              Stream.filter(
                ({ record }) =>
                  !(corrupt === "missing" && record.payload._tag === "ToolCallPrepared"),
              ),
              Stream.map((envelope) =>
                envelope.record.payload._tag === "ToolCallPrepared"
                  ? {
                      ...envelope,
                      record: RecordEnvelope.make({
                        ...envelope.record,
                        payload: ToolCallPrepared.make({
                          ...envelope.record.payload,
                          toolName: "ordinary",
                        }),
                      }),
                    }
                  : envelope,
              ),
            ),
        });
        const rejected = yield* Effect.exit(
          run(parent.conversationId).pipe(
            Effect.provide(Layer.fresh(DurableAgentRuntime.layer)),
            Effect.provideService(ConversationStore, faulty),
          ),
        );
        expect(failureTag(rejected)).toBe("LedgerError");
        expect(payloadsOf(yield* readLog(parent.conversationId), "SubagentJoined")).toHaveLength(0);
        expect((yield* parentReservations(parent.submissionId)).map((row) => row.status)).toEqual([
          "reserved",
        ]);
      }
    }),
  );

  it.effect(
    "RUN-030: canonical child attachment survives loss before the ledger attachment marker",
    () =>
      Effect.gen(function* () {
        yield* clearFailpoint;
        const harness = yield* makeHarness();
        const run = drive(harness);
        const parent = yield* harness.submitParent("expired-before-attachment-marker", "parent");
        yield* armFailpoint("subagent:after-start-append");
        expect(failureTag(yield* Effect.exit(run(parent.conversationId)))).toBe(
          "DurableRuntimeFailpointError",
        );
        yield* clearFailpoint;
        expect(
          (yield* parentReservations(parent.submissionId))[0]?.childSubmissionId,
        ).toBeUndefined();
        yield* run(childConversationIdFor(parent.submissionId, DELEGATE_CALL));
        yield* TestClock.adjust(Duration.seconds(31));
        expect((yield* run(parent.conversationId)).map((entry) => entry.outcome)).toEqual([
          "failed",
        ]);
        expect(payloadsOf(yield* readLog(parent.conversationId), "SubagentJoined")).toHaveLength(1);
        expect((yield* parentReservations(parent.submissionId)).map((row) => row.status)).toEqual([
          "released",
        ]);
      }),
  );

  it.effect("every establishment failpoint converges on one child Receipt and Conversation", () =>
    Effect.gen(function* () {
      const locations: ReadonlyArray<DurableRuntimeFailpointLocation> = [
        "subagent:after-reserve",
        "subagent:after-request-append",
        "subagent:after-admit",
        "subagent:after-child-ready",
        "subagent:after-start-append",
        "subagent:after-suspend",
      ];
      for (const location of locations) {
        yield* clearFailpoint;
        const harness = yield* makeHarness();
        const run = drive(harness);
        const conversation = `conversation-s2-${location.replaceAll(":", "-")}`;
        const parent = yield* harness.submitParent(conversation, `kill-${location}`);
        const childConversationId = childConversationIdFor(parent.submissionId, DELEGATE_CALL);

        yield* armFailpoint(location);
        const exit = yield* Effect.exit(run(parent.conversationId));
        expect(failureTag(exit)).toBe("DurableRuntimeFailpointError");
        yield* clearFailpoint;

        // Idempotent re-entry converges: one child, one Receipt, one start link.
        yield* run(parent.conversationId);
        const childSettlements = yield* run(childConversationId);
        expect(childSettlements.map((settlement) => settlement.outcome)).toEqual(["completed"]);
        const settlements = yield* run(parent.conversationId);
        expect(settlements.map((settlement) => settlement.outcome)).toEqual(["completed"]);
        expect(yield* harness.childInvocations).toBe(1);
        const log = yield* readLog(parent.conversationId);
        expect(payloadsOf(log, "SubagentRequested")).toHaveLength(1);
        expect(payloadsOf(log, "SubagentStarted")).toHaveLength(1);
        expect(payloadsOf(log, "SubagentJoined")).toHaveLength(1);
        const childLog = yield* readLog(childConversationId);
        expect(payloadsOf(childLog, "ConversationCreated")).toHaveLength(1);
        expect(payloadsOf(childLog, "SubagentLineageRecorded")).toHaveLength(1);
        // SUB-016/SUB-017: the one recorded Receipt matches the one admitted child.
        const started = payloadsOf(log, "SubagentStarted")[0]?.record.payload;
        if (started?._tag !== "SubagentStarted") throw new Error("Expected SubagentStarted");
        const child = yield* parentState(started.childSubmissionId);
        expect(child.receiptId).toBe(started.childReceiptId);
        expect(child.state).toBe("settled");
      }
    }),
  );

  it.effect(
    "a kill at subagent:after-join-append replays the accounting and never re-executes the child",
    () =>
      Effect.gen(function* () {
        for (const location of [
          "subagent:after-join-append",
          "subagent:after-release-pending",
          "subagent:after-release",
        ] satisfies ReadonlyArray<DurableRuntimeFailpointLocation>) {
          yield* clearFailpoint;
          const harness = yield* makeHarness();
          const run = drive(harness);
          const conversation = `conversation-s2-${location.replaceAll(":", "-")}`;
          const parent = yield* harness.submitParent(conversation, `join-${location}`);
          const childConversationId = childConversationIdFor(parent.submissionId, DELEGATE_CALL);

          yield* run(parent.conversationId);
          yield* run(childConversationId);
          expect(yield* harness.childInvocations).toBe(1);

          yield* armFailpoint(location);
          const exit = yield* Effect.exit(run(parent.conversationId));
          expect(failureTag(exit)).toBe("DurableRuntimeFailpointError");
          yield* clearFailpoint;

          const settlements = yield* run(parent.conversationId);
          expect(settlements.map((settlement) => settlement.outcome)).toEqual(["completed"]);
          // The completed child was never re-executed on the lost join acknowledgment.
          expect(yield* harness.childInvocations).toBe(1);
          const log = yield* readLog(parent.conversationId);
          expect(payloadsOf(log, "SubagentJoined")).toHaveLength(1);
          const reservations = yield* parentReservations(parent.submissionId);
          expect(reservations.map((row) => row.status)).toEqual(["released"]);
        }
      }),
  );

  it.effect(
    "SUB-033 a kill around the contained join yields ONE non-failure settlement carrying the bounded child failure",
    () =>
      Effect.gen(function* () {
        for (const location of [
          "subagent:after-join-append",
          "subagent:after-release-pending",
          "subagent:after-release",
        ] satisfies ReadonlyArray<DurableRuntimeFailpointLocation>) {
          yield* clearFailpoint;
          // A child whose model emits invalid output: its lane settles FAILED.
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
          const delegationLayer = SubagentRuntime.layer(containedResearchDelegation, childBinding, {
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
            resolver: AgentBindingResolver.fromBindings([parentResolved, childResolved]),
            childInvocations: childScripted.calls,
            parentPrompts: parentScripted.prompts,
            submitParent: submitParentWith(containedCoordinatorDefinition),
            lookupInvocations: Effect.succeed(0),
          };
          const run = drive(harness);
          const conversation = `conversation-s2-contained-${location.replaceAll(":", "-")}`;
          const parent = yield* harness.submitParent(conversation, `contained-${location}`);
          const parentRunId = runIdForSubmission(parent.submissionId);
          const childConversationId = childConversationIdFor(parent.submissionId, DELEGATE_CALL);

          yield* run(parent.conversationId);
          const childSettlements = yield* run(childConversationId);
          expect(childSettlements.map((settlement) => settlement.outcome)).toEqual(["failed"]);
          expect(yield* harness.childInvocations).toBe(1);

          yield* armFailpoint(location);
          const exit = yield* Effect.exit(run(parent.conversationId));
          expect(failureTag(exit)).toBe("DurableRuntimeFailpointError");
          yield* clearFailpoint;

          // Recovery: the parent COMPLETES — the failed child is contained
          // result data, exactly one non-failure Tool settlement exists, and
          // the child was never re-executed.
          const settlements = yield* run(parent.conversationId);
          expect(settlements.map((settlement) => settlement.outcome)).toEqual(["completed"]);
          expect(yield* harness.childInvocations).toBe(1);
          const log = yield* readLog(parent.conversationId);
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
          // The rebuilt model context carries the contained failure as an
          // ordinary (non-error) tool result: the parent's final prompt saw it.
          const finalPrompt = JSON.stringify(harness.parentPrompts.at(-1));
          expect(finalPrompt).toContain("SubagentExecutionFailure");
        }
      }),
  );

  it.effect("commits settled sibling results before the waitingForChild suspension", () =>
    Effect.gen(function* () {
      for (const armed of [false, true]) {
        yield* clearFailpoint;
        const harness = yield* makeSiblingHarness;
        const run = drive(harness);
        const conversation = `conversation-s2-sibling-${armed ? "killed" : "clean"}`;
        const parent = yield* harness.submitParent(conversation, `sibling-${armed}`);
        const parentRunId = runIdForSubmission(parent.submissionId);
        const childConversationId = childConversationIdFor(parent.submissionId, DELEGATE_CALL);

        if (armed) {
          yield* armFailpoint("subagent:after-sibling-settle");
          const exit = yield* Effect.exit(run(parent.conversationId));
          expect(failureTag(exit)).toBe("DurableRuntimeFailpointError");
          yield* clearFailpoint;
        }
        yield* run(parent.conversationId);
        // The sibling's terminal result is canonical as a per-call late-settle batch even
        // though the batch suspended before its results commit.
        const log = yield* readLog(parent.conversationId);
        const siblingSettle = log.find(
          (envelope) => envelope.record.recordId === `tool-settled:${parentRunId}:1:lookup-1`,
        );
        expect(siblingSettle?.batchId).toBe(`turn-results:${parentRunId}:1:lookup-1`);
        expect(yield* harness.lookupInvocations).toBe(1);
        expect((yield* parentState(parent.submissionId)).state).toBe("suspended");

        yield* run(childConversationId);
        const settlements = yield* run(parent.conversationId);
        expect(settlements.map((settlement) => settlement.outcome)).toEqual(["completed"]);
        // The settled sibling was injected on resume, never re-executed.
        expect(yield* harness.lookupInvocations).toBe(1);
        expect(yield* harness.childInvocations).toBe(1);
      }
    }),
  );

  it.effect("request-abort-and-join settles the parent aborted only after every join", () =>
    Effect.gen(function* () {
      yield* clearFailpoint;
      const harness = yield* makeHarness();
      const run = drive(harness);
      const conversation = "conversation-s2-abort";
      const parent = yield* harness.submitParent(conversation, "abort-1");
      const parentRunId = runIdForSubmission(parent.submissionId);
      const childConversationId = childConversationIdFor(parent.submissionId, DELEGATE_CALL);
      const runtime = yield* DurableAgentRuntime;

      yield* run(parent.conversationId);
      expect((yield* parentState(parent.submissionId)).state).toBe("suspended");
      const started = payloadsOf(yield* readLog(parent.conversationId), "SubagentStarted")[0]
        ?.record.payload;
      if (started?._tag !== "SubagentStarted") throw new Error("Expected SubagentStarted");
      yield* runtime.abort(
        AbortCommand.make({
          submissionId: parent.submissionId,
          author: "operator",
          reason: "test abort",
        }),
      );
      // PropagateChildAbort: the one idempotent durable child abort command; the parent stays
      // suspended waiting for the join (spec §13.1). The same recovery pass then settles the
      // never-started child aborted on its own lane — no delegation code runs anywhere.
      const reports = yield* runtime.runRecovery;
      const parentReport = reports.find((report) => report.submissionId === parent.submissionId);
      expect(parentReport?.decision._tag).toBe("PropagateChildAbort");
      expect(parentReport?.disposition).toBe("repaired");
      const childReport = reports.find(
        (report) => report.submissionId === started.childSubmissionId,
      );
      expect(childReport?.decision._tag).toBe("SettleAborted");
      const child = yield* parentState(started.childSubmissionId);
      expect(child.state).toBe("settled");
      expect(child.settledOutcome).toBe("aborted");
      expect(yield* harness.childInvocations).toBe(0);
      // Replaying the propagation is a no-op repair: the recorded child abort intent IS the
      // marker (DUR-012), and the settled child now classifies as a pending join.
      const secondReports = yield* runtime.runRecovery;
      const secondParentReport = secondReports.find(
        (report) => report.submissionId === parent.submissionId,
      );
      expect(secondParentReport?.decision._tag).toBe("ResumeWaitingParent");
      void childConversationId;

      const settlements = yield* run(parent.conversationId);
      expect(settlements.map((settlement) => settlement.outcome)).toEqual(["aborted"]);
      const log = yield* readLog(parent.conversationId);
      const joined = payloadsOf(log, "SubagentJoined");
      expect(joined).toHaveLength(1);
      const joinedPayload = joined[0]?.record.payload;
      if (joinedPayload?._tag !== "SubagentJoined") throw new Error("Expected SubagentJoined");
      expect(joinedPayload.childOutcome).toBe("aborted");
      // The join committed BEFORE the aborted parent settlement (spec §13.1).
      const settledEnvelope = log.find(
        (envelope) => envelope.record.recordId === `settlement:${parent.submissionId}`,
      );
      const joinedEnvelope = joined[0];
      expect(joinedEnvelope !== undefined && settledEnvelope !== undefined).toBe(true);
      if (joinedEnvelope !== undefined && settledEnvelope !== undefined) {
        expect(Number(joinedEnvelope.sequence)).toBeLessThan(Number(settledEnvelope.sequence));
      }
      expect((yield* parentReservations(parent.submissionId)).map((row) => row.status)).toEqual([
        "released",
      ]);
      // The join batch carries the parent-aborted projection under the delegation call id.
      expect(recordIds(log)).toContain(`tool-settled:${parentRunId}:1:delegate-1`);
    }),
  );

  it.effect("recovery completes a crashed establishment binding-free and one child exists", () =>
    Effect.gen(function* () {
      yield* clearFailpoint;
      const harness = yield* makeHarness();
      const run = drive(harness);
      const conversation = "conversation-s2-recovery-admission";
      const parent = yield* harness.submitParent(conversation, "recovery-1");
      const childConversationId = childConversationIdFor(parent.submissionId, DELEGATE_CALL);
      const runtime = yield* DurableAgentRuntime;

      yield* armFailpoint("subagent:after-request-append");
      const exit = yield* Effect.exit(run(parent.conversationId));
      expect(failureTag(exit)).toBe("DurableRuntimeFailpointError");
      yield* clearFailpoint;

      // Pass 1: the canonical request alone admits the one intended child (D3, SUB-016).
      const first = yield* runtime.runRecovery;
      const admissionReport = first.find((report) => report.submissionId === parent.submissionId);
      expect(admissionReport?.decision._tag).toBe("CompleteChildAdmission");
      expect(admissionReport?.disposition).toBe("repaired");
      const childLog = yield* readLog(childConversationId);
      expect(payloadsOf(childLog, "ConversationCreated")).toHaveLength(1);
      expect(payloadsOf(childLog, "SubagentLineageRecorded")).toHaveLength(1);

      // Pass 2: the exact deterministic start link is appended for the same Receipt.
      const second = yield* runtime.runRecovery;
      const startReport = second.find((report) => report.submissionId === parent.submissionId);
      expect(startReport?.decision._tag).toBe("RepairSubagentStartLink");
      expect(startReport?.disposition).toBe("repaired");

      // Pass 3: the waitingForChild checkpoint is restored; the lane holds no permit.
      const third = yield* runtime.runRecovery;
      const waitingReport = third.find((report) => report.submissionId === parent.submissionId);
      expect(waitingReport?.decision._tag).toBe("EnsureWaitingForChild");
      expect(waitingReport?.disposition).toBe("repaired");
      expect((yield* parentState(parent.submissionId)).state).toBe("suspended");

      const childSettlements = yield* run(childConversationId);
      expect(childSettlements.map((settlement) => settlement.outcome)).toEqual(["completed"]);
      const settlements = yield* run(parent.conversationId);
      expect(settlements.map((settlement) => settlement.outcome)).toEqual(["completed"]);
      expect(yield* harness.childInvocations).toBe(1);
      const log = yield* readLog(parent.conversationId);
      expect(payloadsOf(log, "SubagentStarted")).toHaveLength(1);
    }),
  );

  it.effect("a dropped child-settlement wake is replayed by ResumeWaitingParent", () =>
    Effect.gen(function* () {
      yield* clearFailpoint;
      const harness = yield* makeHarness();
      const run = drive(harness);
      const conversation = "conversation-s2-dropped-wake";
      const parent = yield* harness.submitParent(conversation, "wake-1");
      const childConversationId = childConversationIdFor(parent.submissionId, DELEGATE_CALL);
      const runtime = yield* DurableAgentRuntime;
      const ledger = yield* SubmissionLedger;

      yield* run(parent.conversationId);
      const started = payloadsOf(yield* readLog(parent.conversationId), "SubagentStarted")[0]
        ?.record.payload;
      if (started?._tag !== "SubagentStarted") throw new Error("Expected SubagentStarted");

      // The child's settlement record commits but the finalize/notify never runs (a crash
      // between the canonical append and the cross-lane wake).
      yield* armFailpoint("terminalize:after-canonical-append");
      const exit = yield* Effect.exit(run(childConversationId));
      expect(failureTag(exit)).toBe("DurableRuntimeFailpointError");
      yield* clearFailpoint;
      // Finalize the child directly WITHOUT the coordinator's drive-forward notification.
      yield* ledger.finalizeSettlement(
        SettlementFinalization.make({
          submissionId: started.childSubmissionId,
          settlementId: submissionSettlementId(started.childSubmissionId),
        }),
      );
      expect((yield* parentState(parent.submissionId)).state).toBe("suspended");

      const reports = yield* runtime.runRecovery;
      const parentReport = reports.find((report) => report.submissionId === parent.submissionId);
      expect(parentReport?.decision._tag).toBe("ResumeWaitingParent");
      expect(parentReport?.disposition).toBe("repaired");
      expect((yield* parentState(parent.submissionId)).state).toBe("input-applied");

      const settlements = yield* run(parent.conversationId);
      expect(settlements.map((settlement) => settlement.outcome)).toEqual(["completed"]);
      expect(yield* harness.childInvocations).toBe(1);
    }),
  );

  it.effect("a reservation without a request under abort releases exactly once", () =>
    Effect.gen(function* () {
      yield* clearFailpoint;
      const harness = yield* makeHarness();
      const run = drive(harness);
      const conversation = "conversation-s2-orphan";
      const parent = yield* harness.submitParent(conversation, "orphan-1");
      const runtime = yield* DurableAgentRuntime;

      yield* armFailpoint("subagent:after-reserve");
      const exit = yield* Effect.exit(run(parent.conversationId));
      expect(failureTag(exit)).toBe("DurableRuntimeFailpointError");
      yield* clearFailpoint;
      expect((yield* parentReservations(parent.submissionId)).map((row) => row.status)).toEqual([
        "reserved",
      ]);

      yield* runtime.abort(
        AbortCommand.make({
          submissionId: parent.submissionId,
          author: "operator",
          reason: "abandon before request",
        }),
      );
      const first = yield* runtime.runRecovery;
      const orphanReport = first.find((report) => report.submissionId === parent.submissionId);
      expect(orphanReport?.decision._tag).toBe("ReleaseOrphanChildReservation");
      expect(orphanReport?.disposition).toBe("repaired");
      expect((yield* parentReservations(parent.submissionId)).map((row) => row.status)).toEqual([
        "released",
      ]);

      const second = yield* runtime.runRecovery;
      const settleReport = second.find((report) => report.submissionId === parent.submissionId);
      expect(settleReport?.decision._tag).toBe("SettleAborted");
      expect(settleReport?.disposition).toBe("repaired");
      expect((yield* parentState(parent.submissionId)).state).toBe("settled");
      // No child was ever admitted for the orphaned reservation.
      const childConversationId = childConversationIdFor(parent.submissionId, DELEGATE_CALL);
      const ledger = yield* SubmissionLedger;
      const resolution = yield* ledger.resolveAdmission(
        SubmissionLookupByKey.make({
          conversationId: childConversationId,
          principal: PRINCIPAL,
          idempotencyKey: decodeIdempotencyKey(
            `subagent:${runIdForSubmission(parent.submissionId)}:delegate-1`,
          ),
        }),
      );
      expect(resolution._tag).toBe("NotAdmitted");
      // The delegation call was never marked Unknown (spec §13 vs. DUR-009).
      const log = yield* readLog(parent.conversationId);
      expect(payloadsOf(log, "ToolCallUnknown")).toHaveLength(0);
    }),
  );

  it.effect(
    "a resolver digest mismatch writes ChildCompatibilityFailure without running child code",
    () =>
      Effect.gen(function* () {
        yield* clearFailpoint;
        const harness = yield* makeHarness({ childRegistrationDigests: WRONG_CHILD_DIGESTS });
        const run = drive(harness);
        const conversation = "conversation-s2-compat";
        const parent = yield* harness.submitParent(conversation, "compat-1");
        const childConversationId = childConversationIdFor(parent.submissionId, DELEGATE_CALL);

        yield* run(parent.conversationId);
        expect((yield* parentState(parent.submissionId)).state).toBe("suspended");

        // The child lane's claimed head cannot resolve its exact stored Binding: framework
        // code writes the Schema-stable ChildCompatibilityFailure Settlement (SUB-023/SUB-032).
        const childSettlements = yield* run(childConversationId);
        expect(childSettlements.map((settlement) => settlement.outcome)).toEqual(["failed"]);
        expect(yield* harness.childInvocations).toBe(0);
        const childLog = yield* readLog(childConversationId);
        const settled = payloadsOf(childLog, "SubmissionSettled")[0]?.record.payload;
        if (settled?._tag !== "SubmissionSettled") throw new Error("Expected SubmissionSettled");
        expect(
          typeof settled.result === "object" &&
            settled.result !== null &&
            "errorTag" in settled.result
            ? settled.result.errorTag
            : undefined,
        ).toBe("ChildCompatibilityFailure");

        // The woken parent joins the bounded compatibility failure and settles failed.
        const settlements = yield* run(parent.conversationId);
        expect(settlements.map((settlement) => settlement.outcome)).toEqual(["failed"]);
        expect(yield* harness.childInvocations).toBe(0);
        const log = yield* readLog(parent.conversationId);
        const joined = payloadsOf(log, "SubagentJoined")[0]?.record.payload;
        if (joined?._tag !== "SubagentJoined") throw new Error("Expected SubagentJoined");
        expect(joined.childOutcome).toBe("failed");
        expect((yield* parentReservations(parent.submissionId)).map((row) => row.status)).toEqual([
          "released",
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
      const conversation = "conversation-s2-indeterminate";
      const parent = yield* harness.submitParent(conversation, "indeterminate-1");
      const childConversationId = childConversationIdFor(parent.submissionId, DELEGATE_CALL);
      const runtime = yield* DurableAgentRuntime;

      admissionFault = "the authoritative child owner is unreachable";
      // The Attempt aborts typed: an indeterminate answer never permits an admission attempt.
      const exit = yield* Effect.exit(run(parent.conversationId));
      expect(failureTag(exit)).toBe("LedgerError");
      // Recovery classifies the wait honestly and defers — no second admission either.
      const reports = yield* runtime.runRecovery;
      const parentReport = reports.find((report) => report.submissionId === parent.submissionId);
      expect(parentReport?.decision._tag).toBe("AwaitChildAdmissionResolution");
      expect(parentReport?.disposition).toBe("deferred");

      admissionFault = undefined;
      // The authoritative owner answers: exactly one child is admitted and joined.
      yield* run(parent.conversationId);
      const childSettlements = yield* run(childConversationId);
      expect(childSettlements.map((settlement) => settlement.outcome)).toEqual(["completed"]);
      const settlements = yield* run(parent.conversationId);
      expect(settlements.map((settlement) => settlement.outcome)).toEqual(["completed"]);
      expect(yield* harness.childInvocations).toBe(1);
      const childLog = yield* readLog(childConversationId);
      expect(payloadsOf(childLog, "ConversationCreated")).toHaveLength(1);
    }),
  );
});
