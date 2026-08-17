import {
  ApprovalApproved,
  ApprovalAuditMemoryLive,
  ApprovalResolver,
  ApprovalResolverError,
  StructuralRedactorLive,
  toDurableRunApprovalHook,
} from "@effect-agent/capabilities";
import {
  Agent,
  AgentPolicy,
  ConversationId,
  RunId,
  SubmissionId,
  ToolCallId,
  TurnId,
} from "@effect-agent/core";
import type {
  RunApprovalDecision,
  RunApprovalHook,
  RunApprovalRequest,
} from "@effect-agent/engine";
import {
  AbortCommand,
  ApprovalDecisionCommand,
  CanonicalRecordEnvelope,
  ConversationRead,
  ConversationStore,
  DefinitionDigests,
  DeploymentId,
  Digest,
  DurableAgentRuntime,
  DurableApprovalResolver,
  DurableRuntimeConfig,
  DurableRuntimeFailpoint,
  DurableRuntimeFailpointError,
  DurableRuntimeFailpointTestControl,
  IdempotencyKey,
  OperationCaller,
  Principal,
  ProducerId,
  SubmissionLedger,
  SubmissionLookupById,
  ClaimRequest,
  ToolReconciler,
  WakeScheduler,
  runIdForSubmission,
  type DurableRuntimeFailpointLocation,
  type DurableSubmitOptions,
} from "@effect-agent/session";
import {
  MemoryConversationStoreLive,
  MemorySubmissionLedgerLive,
} from "@effect-agent/storage-memory";
import { NodeCrypto } from "@effect/platform-node";
import { describe, expect, it, layer } from "@effect/vitest";
import { Cause, Context, Duration, Effect, Exit, Layer, Option, Ref, Schema, Stream } from "effect";
import { LanguageModel, Model, Prompt, Response, Tool, Toolkit } from "effect/unstable/ai";

import { TrustedLocalDurableAuthorizationLayer } from "../src/durable-test-authorization.ts";

const SHA_A = Schema.decodeSync(Digest)("a".repeat(64));
const PRINCIPAL = Schema.decodeSync(Principal)("principal-durable-approval");
const CALLER = OperationCaller.make({ principal: PRINCIPAL });
const DIGESTS = DefinitionDigests.make({ agent: SHA_A, model: SHA_A, tools: SHA_A });
const decodeConversationId = Schema.decodeSync(ConversationId);
const decodeIdempotencyKey = Schema.decodeSync(IdempotencyKey);
const decodeToolCallId = Schema.decodeSync(ToolCallId);

const submitOptions = (conversationId: string, idempotencyKey: string): DurableSubmitOptions => ({
  conversationId: decodeConversationId(conversationId),
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
const makeScriptedModel = (script: (call: number) => ReadonlyArray<Response.StreamPartEncoded>) =>
  Effect.gen(function* () {
    const calls = yield* Ref.make(0);
    const prompts: Array<Prompt.Prompt> = [];
    const model = Model.make(
      "scripted",
      "durable-approval-test",
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
const approvalDefinition = Agent.define("durable-approval-book", {
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
  MemoryConversationStoreLive,
  WakeScheduler.layerNoop,
  DurableRuntimeFailpoint.layerTest,
  ToolReconciler.uncertain,
  configLayer,
  TrustedLocalDurableAuthorizationLayer,
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

const readLog = (conversationId: string) =>
  Effect.gen(function* () {
    const store = yield* ConversationStore;
    return yield* Stream.runCollect(
      store.read(
        ConversationRead.make({
          conversationId: decodeConversationId(conversationId),
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
      const conversation = "conversation-approval-suspend";

      const receipt = yield* runtime.submit(
        agent,
        { question: "book it" },
        submitOptions(conversation, "suspend-1"),
      );
      const settlements = yield* runtime
        .processConversation(agent, decodeConversationId(conversation), DIGESTS)
        .pipe(Effect.provide(desk.toolLayer));

      // No settlement: the accepted-work obligation stays owed while the lane waits durably.
      expect(settlements).toHaveLength(0);
      expect(yield* lookupState(receipt.submissionId)).toBe("suspended");
      expect(yield* desk.count("r-suspend")).toBe(0);

      // Ownership ended with the suspension: the suspended head is never worker-claimable, so
      // the lane consumes no worker permit (durability §16).
      const claimed = yield* ledger.claim(
        ClaimRequest.make({
          conversationId: decodeConversationId(conversation),
          producerId: PRODUCER_ID,
        }),
      );
      expect(Option.isNone(claimed)).toBe(true);

      const runId = runIdForSubmission(receipt.submissionId);
      const records = yield* readLog(conversation);
      // The response committed BEFORE approval preflight (the durable boundary), the request is
      // canonical (durability §8), and nothing was prepared or executed.
      expect(logTags(records)).toEqual([
        "ConversationCreated",
        "UserInputRecorded",
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
        const conversation = "conversation-approval-approve";

        const receipt = yield* runtime.submit(
          agent,
          { question: "book it" },
          submitOptions(conversation, "approve-1"),
        );
        const first = yield* runtime
          .processConversation(agent, decodeConversationId(conversation), DIGESTS)
          .pipe(Effect.provide(desk.toolLayer));
        expect(first).toHaveLength(0);
        expect(yield* lookupState(receipt.submissionId)).toBe("suspended");

        const intent = yield* runtime.resolveApproval(
          approveCommand(receipt.submissionId, "approved", "reviewed and approved"),
          CALLER,
        );
        expect(intent.decision).toBe("approved");
        // The covering decision wakes the lane: suspended → input-applied (plan §2.6).
        expect(yield* lookupState(receipt.submissionId)).toBe("input-applied");

        const settlements = yield* runtime
          .processConversation(agent, decodeConversationId(conversation), DIGESTS)
          .pipe(Effect.provide(desk.toolLayer));
        expect(settlements).toHaveLength(1);
        expect(settlements[0]?.outcome).toBe("completed");
        expect(yield* desk.count("r-approve")).toBe(1);

        // Exactly two model requests ever: the declaring Turn and the continuation — the resumed
        // batch replayed the canonical declaration instead of re-invoking the model.
        expect(scripted.prompts).toHaveLength(2);

        const runId = runIdForSubmission(receipt.submissionId);
        const records = yield* readLog(conversation);
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
      const conversation = "conversation-approval-deny";

      const receipt = yield* runtime.submit(
        agent,
        { question: "book it" },
        submitOptions(conversation, "deny-1"),
      );
      const first = yield* runtime
        .processConversation(agent, decodeConversationId(conversation), DIGESTS)
        .pipe(Effect.provide(desk.toolLayer));
      expect(first).toHaveLength(0);

      yield* runtime.resolveApproval(
        approveCommand(receipt.submissionId, "denied", "policy forbids this booking"),
        CALLER,
      );
      const settlements = yield* runtime
        .processConversation(agent, decodeConversationId(conversation), DIGESTS)
        .pipe(Effect.provide(desk.toolLayer));
      expect(settlements).toHaveLength(1);
      // Denial-terminal (P2 policy default): the Run fails through `AgentApprovalDenied` with
      // the denial already canonical; the handler never started.
      expect(settlements[0]?.outcome).toBe("failed");
      expect(yield* desk.count("r-deny")).toBe(0);

      const runId = runIdForSubmission(receipt.submissionId);
      const records = yield* readLog(conversation);
      const byId = recordsById(records);
      const decision = byId.get(`approval-decision:${runId}:1:book-1`);
      if (decision?.record.payload._tag === "ToolApprovalDecided") {
        expect(decision.record.payload.decision).toBe("denied");
      } else {
        throw new Error("Expected a canonical ToolApprovalDecided record");
      }
      expect(byId.has(`tool-prepared:${runId}:1:book-1`)).toBe(false);
      const settled = yield* runtime.awaitSettlement(receipt, CALLER);
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
      const conversation = "conversation-approval-idempotent";

      const receipt = yield* runtime.submit(
        agent,
        { question: "book it" },
        submitOptions(conversation, "idem-1"),
      );
      yield* runtime
        .processConversation(agent, decodeConversationId(conversation), DIGESTS)
        .pipe(Effect.provide(desk.toolLayer));

      const original = yield* runtime.resolveApproval(
        approveCommand(receipt.submissionId, "approved", "first decision"),
        CALLER,
      );
      // The replay returns the stored intent unchanged (original resolver/reason/decidedAt).
      const replayed = yield* runtime.resolveApproval(
        approveCommand(receipt.submissionId, "approved", "first decision"),
        CALLER,
      );
      expect(replayed.reason).toBe(original.reason);
      expect(replayed.decidedAt).toStrictEqual(original.decidedAt);

      const divergent = yield* Effect.exit(
        runtime.resolveApproval(
          approveCommand(receipt.submissionId, "denied", "changed my mind"),
          CALLER,
        ),
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
      const conversation = "conversation-approval-abort";

      const receipt = yield* runtime.submit(
        agent,
        { question: "book it" },
        submitOptions(conversation, "abort-1"),
      );
      yield* runtime
        .processConversation(agent, decodeConversationId(conversation), DIGESTS)
        .pipe(Effect.provide(desk.toolLayer));
      expect(yield* lookupState(receipt.submissionId)).toBe("suspended");

      yield* runtime.abort(
        AbortCommand.make({
          submissionId: receipt.submissionId,
          author: "operator",
          reason: "stop the suspended booking",
        }),
        CALLER,
      );
      // A suspended head is never worker-claimable, so the durable abort settles through the
      // recovery pass (durability §13: inactive accepted work settles aborted).
      const reports = yield* runtime.runRecovery;
      const report = reports.find((entry) => entry.submissionId === receipt.submissionId);
      expect(report?.decision._tag).toBe("SettleAborted");
      expect(report?.disposition).toBe("repaired");

      const settled = yield* runtime.awaitSettlement(receipt, CALLER);
      expect(settled.outcome).toBe("aborted");
      expect(yield* lookupState(receipt.submissionId)).toBe("settled");
      expect(yield* desk.count("r-abort")).toBe(0);

      const records = yield* readLog(conversation);
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
      const conversation = "conversation-approval-fp-request";

      const receipt = yield* runtime.submit(
        agent,
        { question: "book it" },
        submitOptions(conversation, "fp-request-1"),
      );
      yield* armFailpoint("approval:after-request-append");
      const killed = yield* Effect.exit(
        runtime
          .processConversation(agent, decodeConversationId(conversation), DIGESTS)
          .pipe(Effect.provide(desk.toolLayer)),
      );
      expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
      yield* clearFailpoint;

      // The request is canonical but the ledger never suspended: recovery repairs the
      // suspension from history — no execution, no settlement (plan §4.3).
      expect(yield* lookupState(receipt.submissionId)).toBe("input-applied");
      const reports = yield* runtime.runRecovery;
      const report = reports.find((entry) => entry.submissionId === receipt.submissionId);
      expect(report?.decision._tag).toBe("AwaitApprovalDecision");
      expect(report?.disposition).toBe("repaired");
      expect(yield* lookupState(receipt.submissionId)).toBe("suspended");
      expect(yield* desk.count("r-fp-request")).toBe(0);

      // The durable decision converges the lane to one settlement, executing exactly once.
      yield* runtime.resolveApproval(
        approveCommand(receipt.submissionId, "approved", "approved after repair"),
        CALLER,
      );
      const settlements = yield* runtime
        .processConversation(agent, decodeConversationId(conversation), DIGESTS)
        .pipe(Effect.provide(desk.toolLayer));
      expect(settlements[0]?.outcome).toBe("completed");
      expect(yield* desk.count("r-fp-request")).toBe(1);

      const records = yield* readLog(conversation);
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
      const conversation = "conversation-approval-multi";

      const receipt = yield* runtime.submit(
        agent,
        { question: "book both" },
        submitOptions(conversation, "multi-1"),
      );
      // The engine resolves approvals in declaration order and fails the batch on the FIRST
      // unresolved call, so each undecided approval suspends the lane one at a time.
      const first = yield* runtime
        .processConversation(agent, decodeConversationId(conversation), DIGESTS)
        .pipe(Effect.provide(desk.toolLayer));
      expect(first).toHaveLength(0);
      expect(yield* lookupState(receipt.submissionId)).toBe("suspended");

      yield* runtime.resolveApproval(
        approveCommand(receipt.submissionId, "approved", "first ok"),
        CALLER,
      );
      const second = yield* runtime
        .processConversation(agent, decodeConversationId(conversation), DIGESTS)
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
        CALLER,
      );
      const settlements = yield* runtime
        .processConversation(agent, decodeConversationId(conversation), DIGESTS)
        .pipe(Effect.provide(desk.toolLayer));
      expect(settlements).toHaveLength(1);
      expect(settlements[0]?.outcome).toBe("completed");
      expect(yield* desk.count("r-multi-1")).toBe(1);
      expect(yield* desk.count("r-multi-2")).toBe(1);
      // Three model-free resumptions of the same declaration: exactly two model requests ever.
      expect(scripted.prompts).toHaveLength(2);

      const runId = runIdForSubmission(receipt.submissionId);
      const records = yield* readLog(conversation);
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
        const conversation = "conversation-approval-fp-suspend";

        const receipt = yield* runtime.submit(
          agent,
          { question: "book it" },
          submitOptions(conversation, "fp-suspend-1"),
        );
        yield* armFailpoint("approval:after-suspend");
        const killed = yield* Effect.exit(
          runtime
            .processConversation(agent, decodeConversationId(conversation), DIGESTS)
            .pipe(Effect.provide(desk.toolLayer)),
        );
        expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
        yield* clearFailpoint;

        // The suspend transaction committed before the kill: the lane is durably suspended and
        // recovery has nothing to repair — it waits for the authorized decision path.
        expect(yield* lookupState(receipt.submissionId)).toBe("suspended");
        const reports = yield* runtime.runRecovery;
        const report = reports.find((entry) => entry.submissionId === receipt.submissionId);
        expect(report?.decision._tag).toBe("AwaitApprovalDecision");
        expect(report?.disposition).toBe("deferred");
        expect(yield* lookupState(receipt.submissionId)).toBe("suspended");

        yield* runtime.resolveApproval(
          approveCommand(receipt.submissionId, "approved", "approved after crash"),
          CALLER,
        );
        const settlements = yield* runtime
          .processConversation(agent, decodeConversationId(conversation), DIGESTS)
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
      const conversation = "conversation-approval-auto";

      const receipt = yield* runtime.submit(
        agent,
        { question: "book it" },
        submitOptions(conversation, "auto-1"),
      );
      const settlements = yield* runtime
        .processConversation(agent, decodeConversationId(conversation), DIGESTS)
        .pipe(Effect.provide(desk.toolLayer));
      expect(settlements).toHaveLength(1);
      expect(settlements[0]?.outcome).toBe("completed");
      expect(yield* desk.count("r-auto")).toBe(1);

      const runId = runIdForSubmission(receipt.submissionId);
      const records = yield* readLog(conversation);
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
      const conversation = "conversation-approval-auto-deny";

      const receipt = yield* runtime.submit(
        agent,
        { question: "book it" },
        submitOptions(conversation, "auto-deny-1"),
      );
      const settlements = yield* runtime
        .processConversation(agent, decodeConversationId(conversation), DIGESTS)
        .pipe(Effect.provide(desk.toolLayer));
      expect(settlements).toHaveLength(1);
      expect(settlements[0]?.outcome).toBe("failed");
      expect(yield* desk.count("r-auto-deny")).toBe(0);

      const runId = runIdForSubmission(receipt.submissionId);
      const records = yield* readLog(conversation);
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
      const conversation = "conversation-approval-race";

      const receipt = yield* runtime.submit(
        agent,
        { question: "book it" },
        submitOptions(conversation, "race-1"),
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
        .processConversation(agent, decodeConversationId(conversation), DIGESTS)
        .pipe(Effect.provide(desk.toolLayer));
      expect(settlements).toHaveLength(1);
      expect(settlements[0]?.outcome).toBe("completed");
      expect(yield* desk.count("r-race")).toBe(1);
      // The declaring Turn was never re-invoked: the resumed batch replayed the declaration.
      expect(scripted.prompts).toHaveLength(2);

      const runId = runIdForSubmission(receipt.submissionId);
      const records = yield* readLog(conversation);
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
    conversationId: decodeConversationId("conversation-adapter"),
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
