import { Agent, AgentPolicy, ConversationId, ToolCallId } from "@effect-agent/core";
import {
  AbortCommand,
  ApprovalDecisionCommand,
  CanonicalSequence,
  ConversationStore,
  DefinitionDigests,
  DeploymentId,
  Digest,
  DurableAgentRuntime,
  DurableRuntimeConfig,
  DurableRuntimeFailpoint,
  IdempotencyKey,
  ObligationThresholds,
  OperationAuthorizer,
  OperationDenied,
  OperationCaller,
  OperationMutationPreparer,
  Principal,
  ProducerId,
  ResolutionNeverHappened,
  RetryCommand,
  SubmissionLedger,
  ToolReconciler,
  UnknownResolutionCommand,
  WakeScheduler,
  possessionChildAdmissionAuthorizerLayer,
  type AuthorizedOperation,
  type DurableSubmitOptions,
  type OperationAuthorizationRequest,
  type OperationAuthorizerService,
} from "@effect-agent/session";
import {
  MemoryConversationStoreLive,
  MemorySubmissionLedgerLive,
} from "@effect-agent/storage-memory";
import { NodeCrypto } from "@effect/platform-node";
import { expect, layer } from "@effect/vitest";
import { Cause, Context, Duration, Effect, Exit, Layer, Option, Ref, Schema, Stream } from "effect";
import { LanguageModel, Model, Toolkit, type Response } from "effect/unstable/ai";

// ---------------------------------------------------------------------------
// Red-team suite: IDOR SWEEP OVER THE ADMIN SURFACE (P7 WP5, plan §4;
// security-operations §2/§3, testing.md §10 "tenant isolation and IDOR
// attempts"; SEC-002/SEC-003, DUR-017).
//
// The threat: a caller holding a legitimate identity for conversation A tries
// to read or mutate conversation B (or a foreign Submission) through the
// administrative surface — awaitSettlement, awaitProgress, observe, abort, explain,
// explainConversation, verify, retry, wake, scanObligations, resolveUnknown, resolveApproval.
// Identifier
// knowledge is never a capability (D10): a non-default `OperationAuthorizer`
// that binds each request to the CALLER's own conversation must deny every
// cross-tenant request fail-closed (typed `OperationDenied` before any read or
// write), and every admin operation must consult it.
//
// This complements `admin-operations.test.ts` (which denies EVERY operation to
// prove the seam is consulted): here the authorizer denies SELECTIVELY by
// target, which is the actual IDOR decision a tenant-scoped host enforces.
// ---------------------------------------------------------------------------

const decodeConversationId = Schema.decodeSync(ConversationId);
const decodeIdempotencyKey = Schema.decodeSync(IdempotencyKey);
const decodeToolCallId = Schema.decodeSync(ToolCallId);
const SHA_A = Schema.decodeSync(Digest)("a".repeat(64));
const PRINCIPAL = Schema.decodeSync(Principal)("principal-idor-sweep");
const FOREIGN_PRINCIPAL = Schema.decodeSync(Principal)("principal-idor-foreign");
const CALLER = OperationCaller.make({ principal: PRINCIPAL });
const DENIAL_REASON = "cross-tenant access denied by the tenant-scoped authorization policy";
const PRODUCER_ID = Schema.decodeSync(ProducerId)("producer-idor-sweep");
const DIGESTS = DefinitionDigests.make({ agent: SHA_A, model: SHA_A, tools: SHA_A });
const ZERO_SEQUENCE = Schema.decodeSync(CanonicalSequence)(0);

/** The tenant boundary the authorizer enforces: only this Conversation is the caller's own. */
const OWNED_CONVERSATION = "idor-owned-conversation";
/** A foreign Conversation the caller must never reach through any admin operation. */
const FOREIGN_CONVERSATION = "idor-foreign-conversation";

const submitOptions = (
  conversation: string,
  key: string,
  principal: Principal = PRINCIPAL,
): DurableSubmitOptions => ({
  conversationId: decodeConversationId(conversation),
  principal,
  idempotencyKey: decodeIdempotencyKey(key),
  definitions: DIGESTS,
});

const usage = { inputTokens: {}, outputTokens: {} };

const finalParts = (text: string): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: text },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage },
];

const makeScriptedModel = (script: (call: number) => ReadonlyArray<Response.StreamPartEncoded>) =>
  Effect.gen(function* () {
    const calls = yield* Ref.make(0);
    const model = Model.make(
      "scripted",
      "idor-sweep",
      Layer.effect(
        LanguageModel.LanguageModel,
        LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: () =>
            Stream.unwrap(
              Ref.getAndUpdate(calls, (call) => call + 1).pipe(
                Effect.map((call) => Stream.fromIterable(script(call))),
              ),
            ),
        }),
      ),
    );
    return { model };
  });

const policy = AgentPolicy.make({
  maxTurns: 2,
  maxToolCalls: 1,
  maxDuration: "30 seconds",
  toolConcurrency: 1,
});

const plainDefinition = Agent.define("idor-plain", {
  input: Schema.Struct({ question: Schema.String }),
  output: Schema.Struct({ answer: Schema.String }),
  instructions: "Answer.",
  toolkit: Toolkit.empty,
  policy,
});

const configLayer = DurableRuntimeConfig.layer({
  deploymentId: Schema.decodeSync(DeploymentId)("deployment-idor-sweep"),
  producerId: PRODUCER_ID,
  settlementPollInterval: Duration.millis(100),
  leaseRenewalInterval: Duration.seconds(5),
  abortPollInterval: Duration.millis(100),
});

/**
 * A tenant-scoped authorizer modelling the IDOR decision a real host enforces. Ownership is
 * explicitly keyed by the caller principal, and it denies:
 *
 *  - any request naming a Conversation other than the caller's own (explain/explainConversation/
 *    verify/wake/observe carry `conversationId`); and
 *  - any request naming a Submission the host has resolved to a foreign tenant.
 *
 * The second clause matters because `abort`, `retry`, `resolveUnknown`, and `resolveApproval`
 * authorize by `submissionId` WITHOUT a `conversationId` (durable-runtime.ts): the framework
 * gives the authorizer no conversation context for those, so a tenant-scoped host must resolve the
 * Submission→tenant mapping itself. The test models that host-side resolution with an explicit
 * Submission-owner map (see FINDINGS SEC-P7-002).
 */
class AuthorizerControl extends Context.Service<
  AuthorizerControl,
  {
    readonly recordSubmissionOwner: (
      submissionId: string,
      principal: Principal,
    ) => Effect.Effect<void>;
    readonly requests: Effect.Effect<ReadonlyArray<OperationAuthorizationRequest>>;
  }
>()("@effect-agent/testing/IdorAuthorizerControl") {}

const tenantScopedAuthorizerLayer = Layer.effectContext(
  Effect.gen(function* () {
    const owned = decodeConversationId(OWNED_CONVERSATION);
    const foreign = decodeConversationId(FOREIGN_CONVERSATION);
    const conversationOwners = new Map<ConversationId, Principal>([
      [owned, PRINCIPAL],
      [foreign, FOREIGN_PRINCIPAL],
    ]);
    const seen = yield* Ref.make<ReadonlyArray<OperationAuthorizationRequest>>([]);
    const submissionOwners = yield* Ref.make<ReadonlyMap<string, Principal>>(new Map());
    const service: OperationAuthorizerService = {
      authorize: (request) =>
        Effect.gen(function* () {
          yield* Ref.update(seen, (all) => [...all, request]);
          const owners = yield* Ref.get(submissionOwners);
          const foreignConversation =
            request.conversationId !== undefined &&
            conversationOwners.get(request.conversationId) !== request.principal;
          const foreignSubmission =
            request.submissionId !== undefined &&
            owners.get(request.submissionId) !== request.principal;
          if (foreignConversation || foreignSubmission) {
            return yield* OperationDenied.make({
              operation: request.operation,
              principal: request.principal,
              reason: DENIAL_REASON,
              ...(request.conversationId === undefined
                ? {}
                : { conversationId: request.conversationId }),
              ...(request.submissionId === undefined ? {} : { submissionId: request.submissionId }),
            });
          }
        }),
    };
    return Context.make(OperationAuthorizer, service).pipe(
      Context.add(
        AuthorizerControl,
        AuthorizerControl.of({
          recordSubmissionOwner: (submissionId, principal) =>
            Ref.update(submissionOwners, (all) => new Map(all).set(submissionId, principal)),
          requests: Ref.get(seen),
        }),
      ),
    );
  }),
);

/** Records every protected storage or wait-boundary entry; armed foreign sweeps also fail it. */
class ProtectedBoundaryControl extends Context.Service<
  ProtectedBoundaryControl,
  {
    readonly enable: Effect.Effect<void>;
    readonly disable: Effect.Effect<void>;
    readonly record: (operation: string) => Effect.Effect<void>;
    readonly takeAccesses: Effect.Effect<ReadonlyArray<string>>;
  }
>()("@effect-agent/testing/IdorProtectedBoundaryControl") {}

const protectedBoundaryControlLayer = Layer.effect(
  ProtectedBoundaryControl,
  Effect.gen(function* () {
    const enabled = yield* Ref.make(false);
    const accesses = yield* Ref.make<ReadonlyArray<string>>([]);
    return ProtectedBoundaryControl.of({
      enable: Ref.set(accesses, []).pipe(Effect.andThen(Ref.set(enabled, true))),
      disable: Ref.set(enabled, false),
      record: (operation) =>
        Ref.update(accesses, (all) => [...all, operation]).pipe(
          Effect.andThen(Ref.get(enabled)),
          Effect.flatMap((isEnabled) =>
            isEnabled
              ? Effect.die(
                  new Error(
                    `Authorization reached protected boundary ${operation} before denying the foreign target`,
                  ),
                )
              : Effect.void,
          ),
        ),
      takeAccesses: Ref.getAndSet(accesses, []),
    });
  }),
);

const guardedSubmissionLedgerLayer = Layer.effect(
  SubmissionLedger,
  Effect.gen(function* () {
    const inner = yield* SubmissionLedger;
    const control = yield* ProtectedBoundaryControl;
    const guard = <A, E, R>(operation: string, effect: Effect.Effect<A, E, R>) =>
      control.record(`ledger.${operation}`).pipe(Effect.andThen(effect));
    return SubmissionLedger.of({
      capabilities: guard("capabilities", inner.capabilities),
      admit: (request) => guard("admit", inner.admit(request)),
      markReady: (request) => guard("markReady", inner.markReady(request)),
      lookup: (request) => guard("lookup", inner.lookup(request)),
      resolveAdmission: (request) => guard("resolveAdmission", inner.resolveAdmission(request)),
      claim: (request) => guard("claim", inner.claim(request)),
      renewOwnership: (request) => guard("renewOwnership", inner.renewOwnership(request)),
      releaseOwnership: (request) => guard("releaseOwnership", inner.releaseOwnership(request)),
      markInputApplied: (request) => guard("markInputApplied", inner.markInputApplied(request)),
      reserveSettlement: (request) => guard("reserveSettlement", inner.reserveSettlement(request)),
      finalizeSettlement: (request) =>
        guard("finalizeSettlement", inner.finalizeSettlement(request)),
      repairSettlementFromCanonical: (request) =>
        guard("repairSettlementFromCanonical", inner.repairSettlementFromCanonical(request)),
      requestAbort: (request) => guard("requestAbort", inner.requestAbort(request)),
      claimJoining: (request) => guard("claimJoining", inner.claimJoining(request)),
      markJoined: (request) => guard("markJoined", inner.markJoined(request)),
      revertJoining: (request) => guard("revertJoining", inner.revertJoining(request)),
      suspend: (request) => guard("suspend", inner.suspend(request)),
      resumeSuspension: (request) => guard("resumeSuspension", inner.resumeSuspension(request)),
      recordApprovalDecision: (request) =>
        guard("recordApprovalDecision", inner.recordApprovalDecision(request)),
      markUnknown: (request) => guard("markUnknown", inner.markUnknown(request)),
      recordUnknownResolution: (request) =>
        guard("recordUnknownResolution", inner.recordUnknownResolution(request)),
      recordChildSettled: (request) =>
        guard("recordChildSettled", inner.recordChildSettled(request)),
      reserveChildBudget: (request) =>
        guard("reserveChildBudget", inner.reserveChildBudget(request)),
      attachChildToReservation: (request) =>
        guard("attachChildToReservation", inner.attachChildToReservation(request)),
      beginChildBudgetRelease: (request) =>
        guard("beginChildBudgetRelease", inner.beginChildBudgetRelease(request)),
      releaseChildBudget: (request) =>
        guard("releaseChildBudget", inner.releaseChildBudget(request)),
      scanNonterminal: Stream.unwrap(
        control.record("ledger.scanNonterminal").pipe(Effect.as(inner.scanNonterminal)),
      ),
      scanConversationNonterminal: (request) =>
        Stream.unwrap(
          control
            .record("ledger.scanConversationNonterminal")
            .pipe(Effect.as(inner.scanConversationNonterminal(request))),
        ),
      loadRecoverySnapshot: (request) =>
        guard("loadRecoverySnapshot", inner.loadRecoverySnapshot(request)),
    });
  }),
).pipe(Layer.provide(MemorySubmissionLedgerLive));

const guardedConversationStoreLayer = Layer.effect(
  ConversationStore,
  Effect.gen(function* () {
    const inner = yield* ConversationStore;
    const control = yield* ProtectedBoundaryControl;
    const guard = <A, E, R>(operation: string, effect: Effect.Effect<A, E, R>) =>
      control.record(`store.${operation}`).pipe(Effect.andThen(effect));
    const guardStream = <A, E, R>(operation: string, stream: Stream.Stream<A, E, R>) =>
      Stream.unwrap(control.record(`store.${operation}`).pipe(Effect.as(stream)));
    return ConversationStore.of({
      materialize: (request) => guard("materialize", inner.materialize(request)),
      append: (request) => guard("append", inner.append(request)),
      read: (request) => guardStream("read", inner.read(request)),
      observe: (request) => guardStream("observe", inner.observe(request)),
      export: (request) => guard("export", inner.export(request)),
      inspectTail: (request) => guard("inspectTail", inner.inspectTail(request)),
      saveCheckpoint: (request) => guard("saveCheckpoint", inner.saveCheckpoint(request)),
      loadCheckpoint: (request) => guard("loadCheckpoint", inner.loadCheckpoint(request)),
    });
  }),
).pipe(Layer.provide(MemoryConversationStoreLive));

const guardedWakeSchedulerLayer = Layer.effect(
  WakeScheduler,
  Effect.gen(function* () {
    const inner = yield* WakeScheduler;
    const control = yield* ProtectedBoundaryControl;
    const guardStream = <A, E, R>(operation: string, stream: Stream.Stream<A, E, R>) =>
      Stream.unwrap(control.record(`wake.${operation}`).pipe(Effect.as(stream)));
    return WakeScheduler.of({
      notify: (conversationId) =>
        control.record("wake.notify").pipe(Effect.andThen(inner.notify(conversationId))),
      subscribe: (conversationId) =>
        control.record("wake.subscribe").pipe(Effect.andThen(inner.subscribe(conversationId))),
      wakes: guardStream("wakes", inner.wakes),
    });
  }),
).pipe(Layer.provide(WakeScheduler.layerNoop));

const guardedOperationMutationPreparerLayer = Layer.effect(
  OperationMutationPreparer,
  Effect.gen(function* () {
    const control = yield* ProtectedBoundaryControl;
    return OperationMutationPreparer.of({
      prepare: (request) => control.record(`mutation.prepare.${request.operation}`),
    });
  }),
);

const protectedBoundaryLayer = Layer.mergeAll(
  guardedSubmissionLedgerLayer,
  guardedConversationStoreLayer,
  guardedWakeSchedulerLayer,
  guardedOperationMutationPreparerLayer,
).pipe(Layer.provideMerge(protectedBoundaryControlLayer));

const baseLayer = Layer.mergeAll(
  protectedBoundaryLayer,
  DurableRuntimeFailpoint.layerTest,
  ToolReconciler.uncertain,
  configLayer,
  tenantScopedAuthorizerLayer,
  possessionChildAdmissionAuthorizerLayer,
).pipe(Layer.provideMerge(NodeCrypto.layer));

const testLayer = DurableAgentRuntime.layer.pipe(Layer.provideMerge(baseLayer));

const failure = <A, E>(exit: Exit.Exit<A, E>): unknown => {
  expect(Exit.isFailure(exit)).toBe(true);
  if (Exit.isSuccess(exit)) throw new Error("Expected the Effect to fail");
  const found = Cause.findErrorOption(exit.cause);
  if (Option.isNone(found)) throw new Error("Expected a typed failure");
  return found.value;
};

const failureTag = <A, E>(exit: Exit.Exit<A, E>): string => {
  const error = failure(exit);
  return typeof error === "object" && error !== null && "_tag" in error
    ? String(error._tag)
    : "unknown";
};

/** Run one plain lane on the given Conversation to a completed settlement, return its Receipt. */
const runSettledLane = (conversation: string, key: string, principal: Principal = PRINCIPAL) =>
  Effect.gen(function* () {
    const runtime = yield* DurableAgentRuntime;
    const scripted = yield* makeScriptedModel(() => finalParts('{"answer":"done"}'));
    const agent = Agent.withModel(plainDefinition, scripted.model);
    const receipt = yield* runtime.submit(
      agent,
      { question: "answer" },
      submitOptions(conversation, key, principal),
    );
    const settlements = yield* runtime.processConversation(
      agent,
      decodeConversationId(conversation),
      DIGESTS,
    );
    expect(settlements[0]?.outcome).toBe("completed");
    return receipt;
  });

layer(testLayer)("SEC-002/D10 admin surface IDOR sweep under a tenant-scoped authorizer", (it) => {
  it.effect(
    "every targeted admin operation denies a foreign Conversation or Submission fail-closed, and permits the caller's own",
    () =>
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const control = yield* AuthorizerControl;

        // Two lanes exist: the caller's own and a foreign tenant's.
        const ownReceipt = yield* runSettledLane(OWNED_CONVERSATION, "idor-own-1");
        const foreignReceipt = yield* runSettledLane(
          FOREIGN_CONVERSATION,
          "idor-foreign-1",
          FOREIGN_PRINCIPAL,
        );
        const foreignConversationId = decodeConversationId(FOREIGN_CONVERSATION);
        const ownConversationId = decodeConversationId(OWNED_CONVERSATION);

        // The host resolves each Submission to its owning principal — the mapping the framework
        // does not supply to the submission-only resolution operations.
        yield* control.recordSubmissionOwner(ownReceipt.submissionId, PRINCIPAL);
        yield* control.recordSubmissionOwner(foreignReceipt.submissionId, FOREIGN_PRINCIPAL);
        const protectedBoundaries = yield* ProtectedBoundaryControl;
        yield* protectedBoundaries.enable;

        // --- Foreign target: every targeted operation is denied BEFORE any read/write. ---
        const explainForeign = yield* Effect.exit(
          runtime.explain(foreignReceipt.submissionId, CALLER),
        );
        expect(failureTag(explainForeign)).toBe("OperationDenied");

        const explainConvForeign = yield* Effect.exit(
          runtime.explainConversation(foreignConversationId, CALLER),
        );
        expect(failureTag(explainConvForeign)).toBe("OperationDenied");

        const verifyForeign = yield* Effect.exit(runtime.verify(foreignConversationId, CALLER));
        expect(failureTag(verifyForeign)).toBe("OperationDenied");

        const retryForeign = yield* Effect.exit(
          runtime.retry(
            RetryCommand.make({
              submissionId: foreignReceipt.submissionId,
              author: "attacker",
              reason: "cross-tenant re-drive",
            }),
            CALLER,
          ),
        );
        expect(failureTag(retryForeign)).toBe("OperationDenied");

        const wakeForeign = yield* Effect.exit(runtime.wake(foreignConversationId, CALLER));
        expect(failureTag(wakeForeign)).toBe("OperationDenied");

        const observeForeign = yield* Effect.exit(
          Stream.runCollect(runtime.observe(foreignReceipt, CALLER)),
        );
        expect(failureTag(observeForeign)).toBe("OperationDenied");

        const abortForeign = yield* Effect.exit(
          runtime.abort(
            AbortCommand.make({
              submissionId: foreignReceipt.submissionId,
              author: "attacker",
              reason: "cross-tenant abort",
            }),
            CALLER,
          ),
        );
        expect(failure(abortForeign)).toEqual(
          OperationDenied.make({
            operation: "abort",
            principal: PRINCIPAL,
            reason: DENIAL_REASON,
            submissionId: foreignReceipt.submissionId,
          }),
        );

        const awaitSettlementForeign = yield* Effect.exit(
          runtime.awaitSettlement(foreignReceipt, CALLER),
        );
        expect(failure(awaitSettlementForeign)).toEqual(
          OperationDenied.make({
            operation: "awaitSettlement",
            principal: PRINCIPAL,
            reason: DENIAL_REASON,
            conversationId: foreignConversationId,
            submissionId: foreignReceipt.submissionId,
          }),
        );

        const awaitProgressForeign = yield* Effect.exit(
          runtime.awaitProgress(foreignConversationId, ZERO_SEQUENCE, CALLER),
        );
        expect(failure(awaitProgressForeign)).toEqual(
          OperationDenied.make({
            operation: "observe",
            principal: PRINCIPAL,
            reason: DENIAL_REASON,
            conversationId: foreignConversationId,
          }),
        );

        const resolveUnknownForeign = yield* Effect.exit(
          runtime.resolveUnknown(
            UnknownResolutionCommand.make({
              submissionId: foreignReceipt.submissionId,
              toolCallId: decodeToolCallId("cross-tenant-call"),
              author: "attacker",
              reason: "cross-tenant resolution",
              resolution: ResolutionNeverHappened.make(),
            }),
            CALLER,
          ),
        );
        expect(failureTag(resolveUnknownForeign)).toBe("OperationDenied");

        const resolveApprovalForeign = yield* Effect.exit(
          runtime.resolveApproval(
            ApprovalDecisionCommand.make({
              submissionId: foreignReceipt.submissionId,
              toolCallId: decodeToolCallId("cross-tenant-call"),
              decision: "approved",
              resolver: "attacker",
              reason: "cross-tenant approval",
            }),
            CALLER,
          ),
        );
        expect(failureTag(resolveApprovalForeign)).toBe("OperationDenied");

        expect(yield* protectedBoundaries.takeAccesses).toEqual([]);
        yield* protectedBoundaries.disable;

        // --- Own target: the caller's own Conversation is permitted (default-behavior allow). ---
        const explainOwn = yield* runtime.explain(ownReceipt.submissionId, CALLER);
        expect(explainOwn.submission.submissionId).toBe(ownReceipt.submissionId);

        // Isolate the two Conversation-targeted admin reads. Both must use the scoped ledger
        // port; a global scan would cross tenant lanes even though its returned rows are filtered.
        yield* protectedBoundaries.takeAccesses;
        const explainConvOwn = yield* runtime.explainConversation(ownConversationId, CALLER);
        expect(explainConvOwn).toEqual([]); // settled lane: no nonterminal explanations

        const verifyOwn = yield* runtime.verify(ownConversationId, CALLER);
        expect(verifyOwn.ok).toBe(true);
        const ownAdminAccesses = yield* protectedBoundaries.takeAccesses;
        expect(ownAdminAccesses.filter((operation) => operation.startsWith("ledger.scan"))).toEqual(
          ["ledger.scanConversationNonterminal", "ledger.scanConversationNonterminal"],
        );
        expect(ownAdminAccesses).not.toContain("ledger.scanNonterminal");

        yield* runtime.wake(ownConversationId, CALLER);

        const observeOwn = yield* Stream.runCollect(
          runtime.observe(ownReceipt, CALLER).pipe(Stream.take(1)),
        );
        expect(observeOwn.length).toBeGreaterThan(0);

        // scanObligations carries no Conversation target, so a tenant-scoped host allows it and
        // scopes its own rows; here it is permitted and returns rows for both lanes' (settled →
        // none). This documents that untargeted scans are the host's responsibility to scope.
        const obligations = yield* runtime.scanObligations(
          ObligationThresholds.make({ agingSeconds: 60, overdueSeconds: 600 }),
          CALLER,
        );
        expect(obligations.entries).toEqual([]);

        // Every targeted operation reached the authorization seam (proof the sweep is not bypassed).
        const requests = yield* control.requests;
        expect(requests.every((request) => request.principal === CALLER.principal)).toBe(true);
        expect(requests).toContainEqual({
          operation: "awaitSettlement",
          principal: CALLER.principal,
          conversationId: foreignConversationId,
          submissionId: foreignReceipt.submissionId,
        });
        // `awaitProgress` deliberately shares the read-only `observe` authorization operation.
        expect(requests).toContainEqual({
          operation: "observe",
          principal: CALLER.principal,
          conversationId: foreignConversationId,
        });
        const operations = new Set(requests.map((request) => request.operation));
        const expectedOperations: ReadonlyArray<AuthorizedOperation> = [
          "explain",
          "verify",
          "retry",
          "wake",
          "observe",
          "awaitSettlement",
          "abort",
          "resolveUnknown",
          "resolveApproval",
          "scanObligations",
        ];
        for (const operation of expectedOperations) {
          expect(operations.has(operation)).toBe(true);
        }
      }),
  );
});
