import { Agent, AgentPolicy, ThreadId, ToolCallId } from "@effect-agent/core";
import { MemoryThreadStoreLive, MemorySubmissionLedgerLive } from "@effect-agent/storage-memory";
import {
  ApprovalDecisionCommand,
  DefinitionDigests,
  DeploymentId,
  Digest,
  DurableAgentRuntime,
  DurableRuntimeConfig,
  IdempotencyKey,
  ObligationThresholds,
  OperationAuthorizer,
  OperationDenied,
  Principal,
  ProducerId,
  ResolutionNeverHappened,
  RetryCommand,
  ToolReconciler,
  UnknownResolutionCommand,
  WakeScheduler,
  type AuthorizedOperation,
  type DurableSubmitOptions,
  type OperationAuthorizationRequest,
  type OperationAuthorizerService,
} from "@effect-agent/thread";
import { DurableRuntimeFailpointTestControl } from "@effect-agent/thread/testing";
import { NodeCrypto } from "@effect/platform-node";
import { expect, layer } from "@effect/vitest";
import { Cause, Context, Duration, Effect, Exit, Layer, Option, Ref, Schema, Stream } from "effect";
import { LanguageModel, Model, Toolkit, type Response } from "effect/unstable/ai";

// ---------------------------------------------------------------------------
// Red-team suite: IDOR SWEEP OVER THE ADMIN SURFACE (P7 WP5, plan §4;
// security-operations §2/§3, testing.md §10 "tenant isolation and IDOR
// attempts"; SEC-002/SEC-003, DUR-017).
//
// The threat: a caller holding a legitimate identity for thread A tries
// to read or mutate thread B (or a foreign Submission) through the
// administrative surface — observe, explain, explainThread, verify,
// retry, wake, scanObligations, resolveUnknown, resolveApproval. Identifier
// knowledge is never a capability (D10): a non-default `OperationAuthorizer`
// that binds each request to the CALLER's own thread must deny every
// cross-tenant request fail-closed (typed `OperationDenied` before any read or
// write), and every admin operation must consult it.
//
// This complements `admin-operations.test.ts` (which denies EVERY operation to
// prove the seam is consulted): here the authorizer denies SELECTIVELY by
// target, which is the actual IDOR decision a tenant-scoped host enforces.
// ---------------------------------------------------------------------------

const decodeThreadId = Schema.decodeSync(ThreadId);
const decodeIdempotencyKey = Schema.decodeSync(IdempotencyKey);
const decodeToolCallId = Schema.decodeSync(ToolCallId);
const SHA_A = Schema.decodeSync(Digest)("a".repeat(64));
const PRINCIPAL = Schema.decodeSync(Principal)("principal-idor-sweep");
const PRODUCER_ID = Schema.decodeSync(ProducerId)("producer-idor-sweep");
const DIGESTS = DefinitionDigests.make({ agent: SHA_A, model: SHA_A, tools: SHA_A });

/** The tenant boundary the authorizer enforces: only this Thread is the caller's own. */
const OWNED_THREAD = "idor-owned-thread";
/** A foreign Thread the caller must never reach through any admin operation. */
const FOREIGN_THREAD = "idor-foreign-thread";

const submitOptions = (thread: string, key: string): DurableSubmitOptions => ({
  threadId: decodeThreadId(thread),
  principal: PRINCIPAL,
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

const plainDefinition = Agent.make("idor-plain", {
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
 * A tenant-scoped authorizer modelling the IDOR decision a real host enforces. It denies:
 *
 *  - any request naming a Thread other than the caller's own (explain/explainThread/
 *    verify/wake/observe carry `threadId`); and
 *  - any request naming a Submission the host has resolved to a foreign tenant.
 *
 * The second clause matters because `retry`, `resolveUnknown`, and `resolveApproval` authorize
 * by `submissionId` WITHOUT a `threadId` (durable-runtime.ts): the framework gives the
 * authorizer no thread context for those, so a tenant-scoped host must resolve the
 * Submission→tenant mapping itself. The test models that host-side resolution with an explicit
 * foreign-Submission set (see FINDINGS SEC-P7-002).
 */
class AuthorizerControl extends Context.Service<
  AuthorizerControl,
  {
    readonly markForeignSubmission: (submissionId: string) => Effect.Effect<void>;
    readonly requests: Effect.Effect<ReadonlyArray<OperationAuthorizationRequest>>;
  }
>()("@effect-agent/testing/IdorAuthorizerControl") {}

const tenantScopedAuthorizerLayer = Layer.effectContext(
  Effect.gen(function* () {
    const owned = decodeThreadId(OWNED_THREAD);
    const seen = yield* Ref.make<ReadonlyArray<OperationAuthorizationRequest>>([]);
    const foreignSubmissions = yield* Ref.make<ReadonlySet<string>>(new Set());

    const service: OperationAuthorizerService = {
      authorize: (request) =>
        Effect.gen(function* () {
          yield* Ref.update(seen, (all) => [...all, request]);
          const deniedSubmissions = yield* Ref.get(foreignSubmissions);
          const foreignThread = request.threadId !== undefined && request.threadId !== owned;

          const foreignSubmission =
            request.submissionId !== undefined && deniedSubmissions.has(request.submissionId);

          if (foreignThread || foreignSubmission) {
            return yield* OperationDenied.make({
              operation: request.operation,
              reason: "cross-tenant access denied by the tenant-scoped authorization policy",
              ...(request.threadId === undefined ? {} : { threadId: request.threadId }),
              ...(request.submissionId === undefined ? {} : { submissionId: request.submissionId }),
            });
          }
        }),
    };

    return Context.make(OperationAuthorizer, service).pipe(
      Context.add(
        AuthorizerControl,
        AuthorizerControl.of({
          markForeignSubmission: (submissionId) =>
            Ref.update(foreignSubmissions, (all) => new Set(all).add(submissionId)),
          requests: Ref.get(seen),
        }),
      ),
    );
  }),
);

const baseLayer = Layer.mergeAll(
  MemorySubmissionLedgerLive,
  MemoryThreadStoreLive,
  WakeScheduler.layerNoop,
  DurableRuntimeFailpointTestControl.layer,
  ToolReconciler.uncertain,
  configLayer,
  tenantScopedAuthorizerLayer,
).pipe(Layer.provideMerge(NodeCrypto.layer));

const testLayer = DurableAgentRuntime.layer.pipe(Layer.provideMerge(baseLayer));

const failureTag = <A, E>(exit: Exit.Exit<A, E>): string => {
  expect(Exit.isFailure(exit)).toBe(true);
  if (Exit.isSuccess(exit)) throw new Error("Expected the Effect to fail");
  const failure = Cause.findErrorOption(exit.cause);

  if (Option.isNone(failure)) throw new Error("Expected a typed failure");
  const error: unknown = failure.value;

  return typeof error === "object" && error !== null && "_tag" in error
    ? String(error._tag)
    : "unknown";
};

/** Run one plain lane on the given Thread to a completed settlement, return its Receipt. */
const runSettledLane = (thread: string, key: string) =>
  Effect.gen(function* () {
    const runtime = yield* DurableAgentRuntime;
    const scripted = yield* makeScriptedModel(() => finalParts('{"answer":"done"}'));
    const agent = Agent.withModel(plainDefinition, scripted.model);

    const receipt = yield* runtime.submit(
      agent,
      { question: "answer" },
      submitOptions(thread, key),
    );

    const settlements = yield* runtime.processThread(agent, decodeThreadId(thread));

    expect(settlements[0]?.outcome).toBe("completed");

    return receipt;
  });

layer(testLayer)("SEC-002/D10 admin surface IDOR sweep under a tenant-scoped authorizer", (it) => {
  it.effect(
    "every targeted admin operation denies a foreign Thread or Submission fail-closed, and permits the caller's own",
    () =>
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const control = yield* AuthorizerControl;

        // Two lanes exist: the caller's own and a foreign tenant's.
        const ownReceipt = yield* runSettledLane(OWNED_THREAD, "idor-own-1");
        const foreignReceipt = yield* runSettledLane(FOREIGN_THREAD, "idor-foreign-1");
        const foreignThreadId = decodeThreadId(FOREIGN_THREAD);
        const ownThreadId = decodeThreadId(OWNED_THREAD);

        // The host resolves the foreign Submission to its (foreign) tenant — the mapping the
        // framework does not supply to the submission-only resolution operations.
        yield* control.markForeignSubmission(foreignReceipt.submissionId);

        // --- Foreign target: every targeted operation is denied BEFORE any read/write. ---
        const explainForeign = yield* Effect.exit(runtime.explain(foreignReceipt.submissionId));

        expect(failureTag(explainForeign)).toBe("OperationDenied");

        const explainConvForeign = yield* Effect.exit(runtime.explainThread(foreignThreadId));

        expect(failureTag(explainConvForeign)).toBe("OperationDenied");

        const verifyForeign = yield* Effect.exit(runtime.verify(foreignThreadId));

        expect(failureTag(verifyForeign)).toBe("OperationDenied");

        const retryForeign = yield* Effect.exit(
          runtime.retry(
            RetryCommand.make({
              submissionId: foreignReceipt.submissionId,
              author: "attacker",
              reason: "cross-tenant re-drive",
            }),
          ),
        );

        expect(failureTag(retryForeign)).toBe("OperationDenied");

        const wakeForeign = yield* Effect.exit(runtime.wake(foreignThreadId));

        expect(failureTag(wakeForeign)).toBe("OperationDenied");

        const observeForeign = yield* Effect.exit(
          Stream.runCollect(runtime.observe(foreignReceipt)),
        );

        expect(failureTag(observeForeign)).toBe("OperationDenied");

        const resolveUnknownForeign = yield* Effect.exit(
          runtime.resolveUnknown(
            UnknownResolutionCommand.make({
              submissionId: foreignReceipt.submissionId,
              toolCallId: decodeToolCallId("cross-tenant-call"),
              author: "attacker",
              reason: "cross-tenant resolution",
              resolution: ResolutionNeverHappened.make(),
            }),
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
          ),
        );

        expect(failureTag(resolveApprovalForeign)).toBe("OperationDenied");

        // --- Own target: the caller's own Thread is permitted (default-behavior allow). ---
        const explainOwn = yield* runtime.explain(ownReceipt.submissionId);

        expect(explainOwn.submission.submissionId).toBe(ownReceipt.submissionId);

        const explainConvOwn = yield* runtime.explainThread(ownThreadId);

        expect(explainConvOwn).toEqual([]); // settled lane: no nonterminal explanations

        const verifyOwn = yield* runtime.verify(ownThreadId);

        expect(verifyOwn.ok).toBe(true);

        yield* runtime.wake(ownThreadId);

        const observeOwn = yield* Stream.runCollect(
          runtime.observe(ownReceipt).pipe(Stream.take(1)),
        );

        expect(observeOwn.length).toBeGreaterThan(0);

        // scanObligations carries no Thread target, so a tenant-scoped host allows it and
        // scopes its own rows; here it is permitted and returns rows for both lanes' (settled →
        // none). This documents that untargeted scans are the host's responsibility to scope.
        const obligations = yield* runtime.scanObligations(
          ObligationThresholds.make({ agingSeconds: 60, overdueSeconds: 600 }),
        );

        expect(obligations.entries).toEqual([]);

        // Every targeted operation reached the authorization seam (proof the sweep is not bypassed).
        const operations = new Set((yield* control.requests).map((request) => request.operation));

        const expectedOperations: ReadonlyArray<AuthorizedOperation> = [
          "explain",
          "verify",
          "retry",
          "wake",
          "observe",
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
