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
  AbortCommand,
  AdmissionRequest,
  ParentLinkage,
  type AdmissionResult,
  ClaimRequest,
  IdempotencyKey,
  Principal,
  RecoverySnapshotRequest,
  ReleaseOwnershipRequest,
  ResolutionCompletedWithResult,
  SettlementFinalization,
  SubmissionLedger,
  SubmissionLookupById,
  SubmissionLookupByKey,
  UnknownResolutionCommand,
  submissionSettlementId,
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

const versionedRootDefinition = Agent.make("versioned-root", {
  input: Schema.Struct({ mission: Schema.String }),
  output: Schema.Struct({ report: Schema.String }),
  instructions: "Look up the mission, then answer as JSON.",
  toolkit: Toolkit.make(Lookup),
  policy: AgentPolicy.make({
    maxTurns: 2,
    maxToolCalls: 1,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
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

const makeSiblingHarness = makeSiblingHarnessWith();

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

const recordIds = (records: ReadonlyArray<CanonicalRecordEnvelope>): ReadonlyArray<string> =>
  records.map((envelope) => envelope.record.recordId as string);

const payloadsOf = <Tag extends string>(
  records: ReadonlyArray<CanonicalRecordEnvelope>,
  tag: Tag,
): ReadonlyArray<CanonicalRecordEnvelope> =>
  records.filter((envelope) => envelope.record.payload._tag === tag);

layer(testLayer)("S2 durable attached Subagents (WP4 coordinator)", (it) => {
  it.effect(
    "refuses missing, ambiguous or different target registrations before reserving work",
    () =>
      Effect.gen(function* () {
        for (const registration of ["missing", "ambiguous", "different-definition"] as const) {
          const harness = yield* makeHarness({ registration });
          const parent = yield* harness.submitParent(`registration-${registration}`, "parent");

          expect((yield* drive(harness)(parent.threadId)).map((entry) => entry.outcome)).toEqual([
            "failed",
          ]);
          expect(yield* harness.childInvocations).toBe(0);
          expect(yield* parentReservations(parent.submissionId)).toEqual([]);
          expect(payloadsOf(yield* readLog(parent.threadId), "SubagentRequested")).toHaveLength(0);
        }
      }),
  );

  it.effect("rejects an explicit digest override that differs from the registered target", () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness({ declaredDigests: WRONG_CHILD_DIGESTS });
      const parent = yield* harness.submitParent("registration-wrong-override", "parent");

      expect((yield* drive(harness)(parent.threadId)).map((entry) => entry.outcome)).toEqual([
        "failed",
      ]);
      expect(yield* harness.childInvocations).toBe(0);
      expect(yield* parentReservations(parent.submissionId)).toEqual([]);
    }),
  );

  it.effect("uses the current root binding and releases claims when no binding is registered", () =>
    Effect.gen(function* () {
      yield* clearFailpoint;
      const runtime = yield* DurableAgentRuntime;
      const ledger = yield* SubmissionLedger;

      const exactScripted = yield* makeScriptedModel((call) =>
        call === 0
          ? toolTurn(toolCall("lookup-version", "lookup", { key: "version-a" }))
          : finalParts('{"report":"version-a"}'),
      );

      const currentScripted = yield* makeScriptedModel(() => finalParts('{"report":"current"}'));
      const toolInvocations = yield* Ref.make(0);

      const lookupLayer = Toolkit.make(Lookup).toLayer({
        lookup: ({ key }) =>
          Ref.update(toolInvocations, (count) => count + 1).pipe(
            Effect.as({ value: `found-${key}` }),
          ),
      });

      const exactAgent = Agent.withModel(versionedRootDefinition, exactScripted.model);
      const currentAgent = Agent.withModel(versionedRootDefinition, currentScripted.model);

      const currentBinding = yield* DurableWorkerBinding.make(
        currentAgent,
        WRONG_CHILD_DIGESTS,
      ).pipe(Effect.provide(lookupLayer));

      const receipt = yield* runtime.submit(
        exactAgent,
        { mission: "select version A" },
        submitOptions("thread-versioned-root", "versioned-root-1"),
      );

      const assertClaimReleased = Effect.gen(function* () {
        expect((yield* parentState(receipt.submissionId)).state).toBe("running");

        const reclaimed = yield* ledger.claim(
          ClaimRequest.make({
            threadId: receipt.threadId,
            producerId: Schema.decodeSync(ProducerId)("producer-versioned-root-proof"),
          }),
        );

        expect(Option.isSome(reclaimed)).toBe(true);
        if (Option.isNone(reclaimed)) throw new Error("Expected the refused root claim to release");
        yield* ledger.releaseOwnership(
          ReleaseOwnershipRequest.make({
            submissionId: reclaimed.value.submissionId,
            ownershipToken: reclaimed.value.ownershipToken,
          }),
        );
      });

      expect(failureTag(yield* Effect.exit(runtime.processThreadResolved(receipt.threadId)))).toBe(
        "BindingUnavailable",
      );
      expect(yield* exactScripted.calls).toBe(0);
      expect(yield* currentScripted.calls).toBe(0);
      expect(yield* Ref.get(toolInvocations)).toBe(0);
      yield* assertClaimReleased;

      const registeredRuntime = yield* DurableAgentRuntime.pipe(
        Effect.provide(
          DurableAgentRuntime.layerWithBindings([currentBinding]).pipe(
            Layer.provide(RunToolAuthorization.allowAll),
          ),
        ),
      );

      const settlements = yield* registeredRuntime.processThreadResolved(receipt.threadId);

      expect(settlements.map((settlement) => settlement.outcome)).toEqual(["completed"]);
      expect((yield* parentState(receipt.submissionId)).state).toBe("settled");
      expect(yield* exactScripted.calls).toBe(0);
      expect(yield* currentScripted.calls).toBe(1);
      expect(yield* Ref.get(toolInvocations)).toBe(0);
      expect(settlements[0]?.submissionId).toBe(receipt.submissionId);
      expect(
        payloadsOf(yield* readLog(receipt.threadId), "RunCompleted").map(
          ({ record }) => record.payload,
        ),
      ).toMatchObject([
        { runId: runIdForSubmission(receipt.submissionId), output: { report: "current" } },
      ]);
    }),
  );

  it.effect(
    "establishes the child, suspends waitingForChild without a worker permit, and joins the settled child",
    () =>
      Effect.gen(function* () {
        yield* clearFailpoint;
        const harness = yield* makeHarness();
        const run = drive(harness);
        const thread = "thread-s2-happy";
        const parent = yield* harness.submitParent(thread, "happy-1");
        const parentRunId = runIdForSubmission(parent.submissionId);
        const childThreadId = childThreadIdFor(parent.submissionId, DELEGATE_CALL);

        // Phase 1: establishment + waitingForChild suspension.
        const first = yield* run(parent.threadId);

        expect(first).toHaveLength(0);
        expect((yield* parentState(parent.submissionId)).state).toBe("suspended");
        // The waiting lane holds no worker permit and is not claimable (SUB-030).
        const ledger = yield* SubmissionLedger;

        const claimed = yield* ledger.claim(
          ClaimRequest.make({
            threadId: parent.threadId,
            producerId: Schema.decodeSync(ProducerId)("producer-durable-subagents"),
          }),
        );

        expect(Option.isNone(claimed)).toBe(true);
        // The child never ran in-process while the parent waited.
        expect(yield* harness.childInvocations).toBe(0);
        const afterEstablish = yield* readLog(parent.threadId);

        expect(recordIds(afterEstablish)).toContain(`subagent-requested:${parentRunId}:delegate-1`);
        expect(recordIds(afterEstablish)).toContain(`subagent-started:${parentRunId}:delegate-1`);
        const reservations = yield* parentReservations(parent.submissionId);

        expect(reservations).toHaveLength(1);
        expect(reservations[0]?.status).toBe("reserved");

        // Phase 2: the child lane runs to Settlement and wakes the parent durably.
        const childSettlements = yield* run(childThreadId);

        expect(childSettlements).toHaveLength(1);
        expect(childSettlements[0]?.outcome).toBe("completed");
        expect(yield* harness.childInvocations).toBe(1);
        expect((yield* parentState(parent.submissionId)).state).toBe("input-applied");
        const childLog = yield* readLog(childThreadId);

        expect(recordIds(childLog)).toContain(`subagent-lineage:${childThreadId}`);

        // Phase 3: the woken parent joins the verified child Settlement atomically.
        const settlements = yield* run(parent.threadId);

        expect(settlements).toHaveLength(1);
        expect(settlements[0]?.outcome).toBe("completed");
        expect(yield* harness.childInvocations).toBe(1);
        const log = yield* readLog(parent.threadId);
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

  it.effect("RUN-030: retains a waiting child across deployment downtime", () =>
    Effect.gen(function* () {
      yield* clearFailpoint;
      const harness = yield* makeHarness();
      const run = drive(harness);
      const thread = "thread-s2-parent-duration";
      const parent = yield* harness.submitParent(thread, "parent-duration-1");
      const childThreadId = childThreadIdFor(parent.submissionId, DELEGATE_CALL);

      const first = yield* run(parent.threadId);

      expect(first).toHaveLength(0);
      expect((yield* parentState(parent.submissionId)).state).toBe("suspended");

      // No active execution: the parent retains its request and child obligation.
      yield* TestClock.adjust("2 days");

      const childSettlements = yield* run(childThreadId);

      expect(childSettlements.map((settlement) => settlement.outcome)).toEqual(["completed"]);
      expect((yield* parentState(parent.submissionId)).state).toBe("input-applied");

      const parentSettlements = yield* run(parent.threadId);

      expect(parentSettlements.map((settlement) => settlement.outcome)).toEqual(["completed"]);
      expect(yield* harness.childInvocations).toBe(1);
      expect(harness.parentPrompts).toHaveLength(2);

      const log = yield* readLog(parent.threadId);

      expect(payloadsOf(log, "SubagentJoined")).toHaveLength(1);
      expect((yield* parentReservations(parent.submissionId)).map((row) => row.status)).toEqual([
        "released",
      ]);
      const settled = payloadsOf(log, "SubmissionSettled")[0]?.record.payload;

      if (settled?._tag !== "SubmissionSettled") throw new Error("Expected SubmissionSettled");
      expect(settled.policyLimit).toBeUndefined();
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

          const childThreadId = childThreadIdFor(parent.submissionId, DELEGATE_CALL);
          const firstAttempt = yield* Effect.forkChild(run(parent.threadId));

          yield* TestClock.adjust(Duration.seconds(31));
          expect(yield* Fiber.join(firstAttempt)).toEqual([]);
          expect(yield* harness.lookupInvocations).toBe(1);
          expect(yield* harness.lookupFinalizers).toBe(1);
          expect(payloadsOf(yield* readLog(parent.threadId), "ToolCallSettled")).toHaveLength(0);
          expect((yield* run(childThreadId)).map((entry) => entry.outcome)).toEqual(["completed"]);
          if (location !== undefined) {
            yield* armFailpoint(location);
            expect(failureTag(yield* Effect.exit(run(parent.threadId)))).toBe(
              "DurableRuntimeFailpointError",
            );
            yield* clearFailpoint;
          }
          expect((yield* run(parent.threadId)).map((entry) => entry.outcome)).toEqual(["failed"]);
          expect(yield* run(parent.threadId)).toEqual([]);
          expect(yield* harness.lookupInvocations).toBe(1);
          expect(yield* harness.lookupFinalizers).toBe(1);
          expect(yield* harness.childInvocations).toBe(1);
          expect(harness.parentPrompts).toHaveLength(1);
          const records = yield* readLog(parent.threadId);

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

  it.effect("RUN-030: recovery rejects conflicting canonical Run starts before child cleanup", () =>
    Effect.gen(function* () {
      yield* clearFailpoint;
      const runtime = yield* DurableAgentRuntime;
      const ledger = yield* SubmissionLedger;
      const store = yield* ThreadStore;
      const harness = yield* makeSiblingHarnessWith(true, false);
      const run = drive(harness);
      const parent = yield* harness.submitParent("expired-conflicting-run-start", "parent");
      const childThreadId = childThreadIdFor(parent.submissionId, DELEGATE_CALL);

      const firstAttempt = yield* Effect.forkChild(run(parent.threadId));

      yield* TestClock.adjust(Duration.seconds(31));
      expect(yield* Fiber.join(firstAttempt)).toEqual([]);
      expect(yield* run(parent.threadId)).toEqual([]);
      expect((yield* run(childThreadId)).map((entry) => entry.outcome)).toEqual(["completed"]);

      const beforeRecords = yield* readLog(parent.threadId);

      const canonicalStart = beforeRecords.find(
        ({ record }) => record.payload._tag === "RunStarted",
      )?.record.payload;

      if (canonicalStart?._tag !== "RunStarted") throw new Error("Expected RunStarted");
      expect(payloadsOf(beforeRecords, "SubagentJoined")).toHaveLength(0);
      expect((yield* parentReservations(parent.submissionId)).map((row) => row.status)).toEqual([
        "reserved",
      ]);

      const beforeSnapshot = yield* ledger.loadRecoverySnapshot(
        RecoverySnapshotRequest.make({ submissionId: parent.submissionId }),
      );

      const conflicting = ThreadStore.of({
        ...store,
        read: (request) =>
          store.read(request).pipe(
            Stream.map((envelope) =>
              request.threadId === parent.threadId &&
              envelope.record.payload._tag === "ThreadCreated"
                ? {
                    ...envelope,
                    record: RecordEnvelope.make({
                      ...envelope.record,
                      payload: canonicalStart,
                    }),
                  }
                : envelope,
            ),
          ),
      });

      const rejected = yield* DurableAgentRuntime.pipe(
        Effect.flatMap((hostileRuntime) => hostileRuntime.runRecovery()),
        Effect.provide(Layer.fresh(DurableAgentRuntime.layer)),
        Effect.provideService(ThreadStore, conflicting),
      );

      expect(rejected.blocked.find((fault) => fault.threadId === parent.threadId)).toMatchObject({
        failure: { errorTag: "RunJournalError" },
      });

      const afterSnapshot = yield* ledger.loadRecoverySnapshot(
        RecoverySnapshotRequest.make({ submissionId: parent.submissionId }),
      );

      expect(afterSnapshot).toEqual(beforeSnapshot);
      const afterRejected = yield* readLog(parent.threadId);

      expect(afterRejected).toEqual(beforeRecords);
      expect(payloadsOf(afterRejected, "SubagentJoined")).toHaveLength(0);
      expect(payloadsOf(afterRejected, "ToolCallUnknown")).toHaveLength(0);
      expect((yield* parentReservations(parent.submissionId)).map((row) => row.status)).toEqual([
        "reserved",
      ]);

      yield* runtime.runRecovery();
      expect(payloadsOf(yield* readLog(parent.threadId), "SubagentJoined")).toHaveLength(1);
      expect((yield* parentReservations(parent.submissionId)).map((row) => row.status)).toEqual([
        "released",
      ]);
    }),
  );

  it.effect("RUN-030: recovery rejects missing or mismatched canonical Tool identity", () =>
    Effect.gen(function* () {
      const store = yield* ThreadStore;

      for (const corrupt of ["missing", "mismatched"] as const) {
        yield* clearFailpoint;
        const harness = yield* makeHarness();
        const run = drive(harness);
        const parent = yield* harness.submitParent(`expired-identity-${corrupt}`, "parent");

        yield* run(parent.threadId);
        yield* run(childThreadIdFor(parent.submissionId, DELEGATE_CALL));
        yield* TestClock.adjust(Duration.seconds(31));

        const faulty = ThreadStore.of({
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

        const hostileRuntime = yield* DurableAgentRuntime.pipe(
          Effect.provide(
            DurableAgentRuntime.layerWithBindings(harness.bindings).pipe(
              Layer.provide(RunToolAuthorization.allowAll),
            ),
          ),
          Effect.provideService(ThreadStore, faulty),
        );

        const rejected = yield* Effect.exit(hostileRuntime.processThreadResolved(parent.threadId));

        expect(failureTag(rejected)).toBe(
          corrupt === "missing" ? "ThreadStoreError" : "RunJournalError",
        );
        expect(payloadsOf(yield* readLog(parent.threadId), "SubagentJoined")).toHaveLength(0);
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
        expect(failureTag(yield* Effect.exit(run(parent.threadId)))).toBe(
          "DurableRuntimeFailpointError",
        );
        yield* clearFailpoint;
        expect(
          (yield* parentReservations(parent.submissionId))[0]?.childSubmissionId,
        ).toBeUndefined();
        yield* run(childThreadIdFor(parent.submissionId, DELEGATE_CALL));
        yield* TestClock.adjust(Duration.seconds(31));
        expect((yield* run(parent.threadId)).map((entry) => entry.outcome)).toEqual(["completed"]);
        expect(payloadsOf(yield* readLog(parent.threadId), "SubagentJoined")).toHaveLength(1);
        expect((yield* parentReservations(parent.submissionId)).map((row) => row.status)).toEqual([
          "released",
        ]);
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

  it.effect("every establishment failpoint converges on one child Receipt and Thread", () =>
    Effect.gen(function* () {
      const locations: ReadonlyArray<DurableRuntimeFailpointLocation> = [
        "tools:before-prepared-append",
        "tools:after-prepared-append",
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
        const thread = `thread-s2-${location.replaceAll(":", "-")}`;
        const parent = yield* harness.submitParent(thread, `kill-${location}`);
        const childThreadId = childThreadIdFor(parent.submissionId, DELEGATE_CALL);

        yield* armFailpoint(location);
        const exit = yield* Effect.exit(run(parent.threadId));

        expect(failureTag(exit)).toBe("DurableRuntimeFailpointError");
        yield* clearFailpoint;

        // Idempotent re-entry converges: one child, one Receipt, one start link.
        if (location === "tools:after-prepared-append") {
          const prepared = payloadsOf(yield* readLog(parent.threadId), "ToolCallPrepared");

          expect(prepared[0]?.record.payload).toMatchObject({ executionKind: "delegation" });
          expect(payloadsOf(yield* readLog(parent.threadId), "SubagentRequested")).toHaveLength(0);
        }
        yield* run(parent.threadId);
        const childSettlements = yield* run(childThreadId);

        expect(childSettlements.map((settlement) => settlement.outcome)).toEqual(["completed"]);
        const settlements = yield* run(parent.threadId);

        expect(settlements.map((settlement) => settlement.outcome)).toEqual(["completed"]);
        expect(yield* harness.childInvocations).toBe(1);
        const log = yield* readLog(parent.threadId);

        expect(payloadsOf(log, "SubagentRequested")).toHaveLength(1);
        expect(payloadsOf(log, "SubagentStarted")).toHaveLength(1);
        expect(payloadsOf(log, "SubagentJoined")).toHaveLength(1);
        const childLog = yield* readLog(childThreadId);

        expect(payloadsOf(childLog, "ThreadCreated")).toHaveLength(1);
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
          const thread = `thread-s2-${location.replaceAll(":", "-")}`;
          const parent = yield* harness.submitParent(thread, `join-${location}`);
          const childThreadId = childThreadIdFor(parent.submissionId, DELEGATE_CALL);

          yield* run(parent.threadId);
          yield* run(childThreadId);
          expect(yield* harness.childInvocations).toBe(1);

          yield* armFailpoint(location);
          const exit = yield* Effect.exit(run(parent.threadId));

          expect(failureTag(exit)).toBe("DurableRuntimeFailpointError");
          yield* clearFailpoint;

          const settlements = yield* run(parent.threadId);

          expect(settlements.map((settlement) => settlement.outcome)).toEqual(["completed"]);
          // The completed child was never re-executed on the lost join acknowledgment.
          expect(yield* harness.childInvocations).toBe(1);
          const log = yield* readLog(parent.threadId);

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

          // Recovery: the parent COMPLETES — the failed child is contained
          // result data, exactly one non-failure Tool settlement exists, and
          // the child was never re-executed.
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
        const thread = `thread-s2-sibling-${armed ? "killed" : "clean"}`;
        const parent = yield* harness.submitParent(thread, `sibling-${armed}`);
        const parentRunId = runIdForSubmission(parent.submissionId);
        const childThreadId = childThreadIdFor(parent.submissionId, DELEGATE_CALL);

        if (armed) {
          yield* armFailpoint("subagent:after-sibling-settle");
          const exit = yield* Effect.exit(run(parent.threadId));

          expect(failureTag(exit)).toBe("DurableRuntimeFailpointError");
          yield* clearFailpoint;
        }
        yield* run(parent.threadId);
        // The sibling's terminal result is canonical as a per-call late-settle batch even
        // though the batch suspended before its results commit.
        const log = yield* readLog(parent.threadId);

        const siblingSettle = log.find(
          (envelope) => envelope.record.recordId === `tool-settled:${parentRunId}:1:lookup-1`,
        );

        expect(siblingSettle?.batchId).toBe(`turn-results:${parentRunId}:1:lookup-1`);
        expect(yield* harness.lookupInvocations).toBe(1);
        expect((yield* parentState(parent.submissionId)).state).toBe("suspended");

        yield* run(childThreadId);
        const settlements = yield* run(parent.threadId);

        expect(settlements.map((settlement) => settlement.outcome)).toEqual(["completed"]);
        // The settled sibling was injected on resume, never re-executed.
        expect(yield* harness.lookupInvocations).toBe(1);
        expect(yield* harness.childInvocations).toBe(1);
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

  it.effect("request-abort-and-join settles the parent aborted only after every join", () =>
    Effect.gen(function* () {
      yield* clearFailpoint;
      const harness = yield* makeHarness();
      const run = drive(harness);
      const thread = "thread-s2-abort";
      const parent = yield* harness.submitParent(thread, "abort-1");
      const parentRunId = runIdForSubmission(parent.submissionId);
      const childThreadId = childThreadIdFor(parent.submissionId, DELEGATE_CALL);
      const runtime = yield* DurableAgentRuntime;

      yield* run(parent.threadId);
      expect((yield* parentState(parent.submissionId)).state).toBe("suspended");

      const started = payloadsOf(yield* readLog(parent.threadId), "SubagentStarted")[0]?.record
        .payload;

      if (started?._tag !== "SubagentStarted") throw new Error("Expected SubagentStarted");
      yield* runtime.abort(
        AbortCommand.make({
          submissionId: parent.submissionId,
          author: "operator",
          reason: "test abort",
        }),
      );

      // PropagateChildAbort: the one idempotent durable child abort command; the parent stays
      // suspended waiting for the join (spec §13.1). Two passes cover either lexical lane
      // order: the child settles after the parent propagates its abort, without running code.
      const reports = [
        ...(yield* runtime.runRecovery()).reports,
        ...(yield* runtime.runRecovery()).reports,
      ];

      const parentReport = reports.find((report) => report.submissionId === parent.submissionId);

      expect(parentReport?.decision._tag).toBe("PropagateChildAbort");
      expect(parentReport?.disposition).toBe("repaired");

      const childReport = reports.find(
        (report) =>
          report.submissionId === started.childSubmissionId &&
          report.decision._tag === "SettleAborted",
      );

      expect(childReport?.decision._tag).toBe("SettleAborted");
      const child = yield* parentState(started.childSubmissionId);

      expect(child.state).toBe("settled");
      expect(child.settledOutcome).toBe("aborted");
      expect(yield* harness.childInvocations).toBe(0);
      // Replaying the propagation is a no-op repair: the recorded child abort intent IS the
      // marker (DUR-012), and the settled child now classifies as a pending join.
      const secondReports = (yield* runtime.runRecovery()).reports;

      const secondParentReport = secondReports.find(
        (report) => report.submissionId === parent.submissionId,
      );

      expect(secondParentReport?.decision._tag).toBe("ResumeWaitingParent");
      void childThreadId;

      const settlements = yield* run(parent.threadId);

      expect(settlements.map((settlement) => settlement.outcome)).toEqual(["aborted"]);
      const log = yield* readLog(parent.threadId);
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
      const thread = "thread-s2-recovery-admission";
      const parent = yield* harness.submitParent(thread, "recovery-1");
      const childThreadId = childThreadIdFor(parent.submissionId, DELEGATE_CALL);
      const runtime = yield* DurableAgentRuntime;

      yield* armFailpoint("subagent:after-request-append");
      const exit = yield* Effect.exit(run(parent.threadId));

      expect(failureTag(exit)).toBe("DurableRuntimeFailpointError");
      yield* clearFailpoint;

      // Pass 1: the canonical request alone admits the one intended child (D3, SUB-016).
      const first = (yield* runtime.runRecovery()).reports;
      const admissionReport = first.find((report) => report.submissionId === parent.submissionId);

      expect(admissionReport?.decision._tag).toBe("CompleteChildAdmission");
      expect(admissionReport?.disposition).toBe("repaired");
      const childLog = yield* readLog(childThreadId);

      expect(payloadsOf(childLog, "ThreadCreated")).toHaveLength(1);
      expect(payloadsOf(childLog, "SubagentLineageRecorded")).toHaveLength(1);

      // Pass 2: the exact deterministic start link is appended for the same Receipt.
      const second = (yield* runtime.runRecovery()).reports;
      const startReport = second.find((report) => report.submissionId === parent.submissionId);

      expect(startReport?.decision._tag).toBe("RepairSubagentStartLink");
      expect(startReport?.disposition).toBe("repaired");

      // Pass 3: the waitingForChild checkpoint is restored; the lane holds no permit.
      const third = (yield* runtime.runRecovery()).reports;
      const waitingReport = third.find((report) => report.submissionId === parent.submissionId);

      expect(waitingReport?.decision._tag).toBe("EnsureWaitingForChild");
      expect(waitingReport?.disposition).toBe("repaired");
      expect((yield* parentState(parent.submissionId)).state).toBe("suspended");

      const childSettlements = yield* run(childThreadId);

      expect(childSettlements.map((settlement) => settlement.outcome)).toEqual(["completed"]);
      const settlements = yield* run(parent.threadId);

      expect(settlements.map((settlement) => settlement.outcome)).toEqual(["completed"]);
      expect(yield* harness.childInvocations).toBe(1);
      const log = yield* readLog(parent.threadId);

      expect(payloadsOf(log, "SubagentStarted")).toHaveLength(1);
    }),
  );

  it.effect("a dropped child-settlement wake is replayed by ResumeWaitingParent", () =>
    Effect.gen(function* () {
      yield* clearFailpoint;
      const harness = yield* makeHarness();
      const run = drive(harness);
      const thread = "thread-s2-dropped-wake";
      const parent = yield* harness.submitParent(thread, "wake-1");
      const childThreadId = childThreadIdFor(parent.submissionId, DELEGATE_CALL);
      const runtime = yield* DurableAgentRuntime;
      const ledger = yield* SubmissionLedger;

      yield* run(parent.threadId);

      const started = payloadsOf(yield* readLog(parent.threadId), "SubagentStarted")[0]?.record
        .payload;

      if (started?._tag !== "SubagentStarted") throw new Error("Expected SubagentStarted");

      // The child's settlement record commits but the finalize/notify never runs (a crash
      // between the canonical append and the cross-lane wake).
      yield* armFailpoint("terminalize:after-canonical-append");
      const exit = yield* Effect.exit(run(childThreadId));

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

      const reports = (yield* runtime.runRecovery()).reports;
      const parentReport = reports.find((report) => report.submissionId === parent.submissionId);

      expect(parentReport?.decision._tag).toBe("ResumeWaitingParent");
      expect(parentReport?.disposition).toBe("repaired");
      expect((yield* parentState(parent.submissionId)).state).toBe("input-applied");

      const settlements = yield* run(parent.threadId);

      expect(settlements.map((settlement) => settlement.outcome)).toEqual(["completed"]);
      expect(yield* harness.childInvocations).toBe(1);
    }),
  );

  it.effect("a reservation without a request under abort releases exactly once", () =>
    Effect.gen(function* () {
      yield* clearFailpoint;
      const harness = yield* makeHarness();
      const run = drive(harness);
      const thread = "thread-s2-orphan";
      const parent = yield* harness.submitParent(thread, "orphan-1");
      const runtime = yield* DurableAgentRuntime;

      yield* armFailpoint("subagent:after-reserve");
      const exit = yield* Effect.exit(run(parent.threadId));

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
      const first = (yield* runtime.runRecovery()).reports;
      const orphanReport = first.find((report) => report.submissionId === parent.submissionId);

      expect(orphanReport?.decision._tag).toBe("ReleaseOrphanChildReservation");
      expect(orphanReport?.disposition).toBe("repaired");
      expect((yield* parentReservations(parent.submissionId)).map((row) => row.status)).toEqual([
        "released",
      ]);

      const second = (yield* runtime.runRecovery()).reports;
      const settleReport = second.find((report) => report.submissionId === parent.submissionId);

      expect(settleReport?.decision._tag).toBe("SettleAborted");
      expect(settleReport?.disposition).toBe("repaired");
      expect((yield* parentState(parent.submissionId)).state).toBe("settled");
      // No child was ever admitted for the orphaned reservation.
      const childThreadId = childThreadIdFor(parent.submissionId, DELEGATE_CALL);
      const ledger = yield* SubmissionLedger;

      const resolution = yield* ledger.resolveAdmission(
        SubmissionLookupByKey.make({
          threadId: childThreadId,
          principal: PRINCIPAL,
          idempotencyKey: decodeIdempotencyKey(
            `subagent:${runIdForSubmission(parent.submissionId)}:delegate-1`,
          ),
        }),
      );

      expect(resolution._tag).toBe("NotAdmitted");
      // The delegation call was never marked Unknown (spec §13 vs. DUR-009).
      const log = yield* readLog(parent.threadId);

      expect(payloadsOf(log, "ToolCallUnknown")).toHaveLength(0);
    }),
  );

  // Incident: https://reve-r6.sentry.io/issues/KOMMUNIKASIE-API-68
  it.effect(
    "runs an admitted child with the current binding while preserving its original join and reservation",
    () =>
      Effect.gen(function* () {
        yield* clearFailpoint;
        const harness = yield* makeHarness();
        const thread = "thread-s2-compat";
        const parent = yield* harness.submitParent(thread, "compat-1");
        const childThreadId = childThreadIdFor(parent.submissionId, DELEGATE_CALL);

        yield* drive(harness)(parent.threadId);
        expect((yield* parentState(parent.submissionId)).state).toBe("suspended");

        // A deployment changes binding evidence without replacing the admitted child identity.
        const runtime = yield* DurableAgentRuntime.pipe(
          Effect.provide(
            DurableAgentRuntime.layerWithBindings(
              harness.bindings.map((binding) =>
                binding.agentId === childDefinition.id
                  ? { ...binding, digests: WRONG_CHILD_DIGESTS }
                  : binding,
              ),
            ).pipe(Layer.provide(RunToolAuthorization.allowAll)),
          ),
        );

        const run = drive({ ...harness, runtime });

        expect((yield* run(childThreadId)).map((settlement) => settlement.outcome)).toEqual([
          "completed",
        ]);
        expect((yield* run(parent.threadId)).map((settlement) => settlement.outcome)).toEqual([
          "completed",
        ]);
        expect(payloadsOf(yield* readLog(childThreadId), "SubmissionSettled")).toHaveLength(1);
        expect(yield* harness.childInvocations).toBe(1);
        const joined = payloadsOf(yield* readLog(parent.threadId), "SubagentJoined");

        expect(joined).toHaveLength(1);
        expect((yield* parentReservations(parent.submissionId)).map((row) => row.status)).toEqual([
          "released",
        ]);
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
    "joins an already-admitted child after its delegation is removed without restoring the old handler",
    () =>
      Effect.gen(function* () {
        yield* clearFailpoint;
        const original = yield* makeHarness();
        const parent = yield* original.submitParent("removed-admitted-delegation", "original");
        const childThread = childThreadIdFor(parent.submissionId, DELEGATE_CALL);

        expect(yield* drive(original)(parent.threadId)).toEqual([]);
        const retained = yield* readLog(parent.threadId);
        const reservations = yield* parentReservations(parent.submissionId);

        expect(reservations).toHaveLength(1);
        expect(reservations[0]?.status).toBe("reserved");

        const currentModel = yield* makeScriptedModel(() =>
          finalParts('{"report":"original child settled; delegation retired"}'),
        );

        const currentDefinition = Agent.make(coordinatorDefinition.id, {
          input: coordinatorDefinition.input,
          output: coordinatorDefinition.output,
          instructions: "Use the original child outcome to finish this request.",
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
            ]).pipe(Layer.provide(RunToolAuthorization.allowAll)),
          ),
        );

        expect(yield* runtime.processThreadResolved(parent.threadId)).toEqual([]);
        expect(yield* currentModel.calls).toBe(0);
        const childSettlements = yield* runtime.processThreadResolved(childThread);

        expect(childSettlements.map((settlement) => settlement.outcome)).toEqual(["completed"]);
        yield* armFailpoint("subagent:after-join-append");
        expect(failureTag(yield* Effect.exit(runtime.processThreadResolved(parent.threadId)))).toBe(
          "DurableRuntimeFailpointError",
        );
        yield* clearFailpoint;
        const completed = yield* runtime.processThreadResolved(parent.threadId);

        expect(completed).toMatchObject([
          { submissionId: parent.submissionId, outcome: "completed" },
        ]);
        expect(yield* currentModel.calls).toBe(1);
        expect(yield* original.childInvocations).toBe(1);
        expect(original.parentPrompts).toHaveLength(1);
        const after = yield* readLog(parent.threadId);

        expect(after.slice(0, retained.length)).toEqual(retained);
        expect(payloadsOf(after, "SubagentRequested")).toHaveLength(1);
        expect(
          payloadsOf(after, "SubagentJoined").map(({ record }) => record.payload),
        ).toMatchObject([
          {
            runId: runIdForSubmission(parent.submissionId),
            toolCallId: DELEGATE_CALL,
            childSubmissionId: childSettlements[0]?.submissionId,
            childSettlementId: childSettlements[0]?.settlementId,
            childOutcome: "completed",
          },
        ]);
        expect(payloadsOf(after, "ToolCallSettled")).toHaveLength(1);
        expect(payloadsOf(after, "SubmissionSettled")).toHaveLength(1);
        expect(
          (yield* parentReservations(parent.submissionId)).map((reservation) => reservation.status),
        ).toEqual(["released"]);
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
