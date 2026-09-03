import * as Agent from "@effect-agent/core/Agent";
import { AgentPolicy } from "@effect-agent/core/AgentPolicy";
import { ThreadId } from "@effect-agent/core/Identifiers";
import { RunToolAuthorization } from "@effect-agent/engine/RunOptions";
import { MemorySubmissionLedgerLive } from "@effect-agent/storage-memory/MemorySubmissionLedger";
import { MemoryThreadStoreLive } from "@effect-agent/storage-memory/MemoryThreadStore";
import { DurableWorkerBinding, type ResolvedBinding } from "@effect-agent/thread/AgentRegistration";
import {
  DurableAgentRuntime,
  DurableRuntimeConfig,
  Receipt,
} from "@effect-agent/thread/DurableAgentRuntime";
import { DurableRuntimeFailpointError } from "@effect-agent/thread/DurableFailpoint";
import { OperationAuthorizer, OperationDenied } from "@effect-agent/thread/OperationAuthorizer";
import { DefinitionDigests, DeploymentId, Digest, ProducerId } from "@effect-agent/thread/Records";
import {
  AbortCommand,
  IdempotencyKey,
  LedgerError,
  OwnershipRenewal,
  OwnershipToken,
  Principal,
  RecoverySnapshotRequest,
  ReleaseOwnershipRequest,
  SubmissionLedger,
} from "@effect-agent/thread/SubmissionLedger";
import { DurableRuntimeFailpointTestControl } from "@effect-agent/thread/testing/DurableFailpointTestControl";
import { ToolReconciler } from "@effect-agent/thread/ToolReconciler";
import { WakeScheduler } from "@effect-agent/thread/WakeScheduler";
import { NodeCrypto } from "@effect/platform-node";
import { expect, layer } from "@effect/vitest";
import {
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
import { LanguageModel, Model, Tool, Toolkit, type Response } from "effect/unstable/ai";

const digest = Schema.decodeSync(Digest)("a".repeat(64));
const digests = DefinitionDigests.make({ agent: digest, model: digest, tools: digest });

const policy = AgentPolicy.make({
  maxTurns: 2,
  maxToolCalls: 2,
  maxDuration: "30 seconds",
  toolConcurrency: 1,
});

const definition = Agent.make("bounded-head", {
  input: Schema.String,
  output: Schema.String,
  instructions: "Answer as JSON.",
  toolkit: Toolkit.empty,
  policy,
});

const options = (thread: string, key: string) => ({
  threadId: Schema.decodeSync(ThreadId)(thread),
  principal: Schema.decodeSync(Principal)("head-test"),
  idempotencyKey: Schema.decodeSync(IdempotencyKey)(key),
  definitions: digests,
});

const finalParts: ReadonlyArray<Response.StreamPartEncoded> = [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: '"done"' },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage: { inputTokens: {}, outputTokens: {} } },
];

const makeModel = (
  response: Stream.Stream<Response.StreamPartEncoded>,
  close: Effect.Effect<void> = Effect.void,
) =>
  Model.make(
    "scripted",
    "bounded-head",
    Layer.effect(
      LanguageModel.LanguageModel,
      Effect.gen(function* () {
        yield* Effect.addFinalizer(() => close);

        return yield* LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: () => response,
        });
      }),
    ),
  );

const baseLayer = Layer.mergeAll(
  MemorySubmissionLedgerLive,
  MemoryThreadStoreLive,
  WakeScheduler.layerNoop,
  DurableRuntimeFailpointTestControl.layer,
  ToolReconciler.uncertain,
  DurableRuntimeConfig.layer({
    deploymentId: Schema.decodeSync(DeploymentId)("head-test"),
    producerId: Schema.decodeSync(ProducerId)("head-test"),
    leaseRenewalInterval: Duration.seconds(5),
    settlementPollInterval: Duration.millis(100),
  }),
).pipe(Layer.provideMerge(NodeCrypto.layer));

const makeRuntime = (bindings: ReadonlyArray<ResolvedBinding> = []) =>
  DurableAgentRuntime.pipe(
    Effect.provide(
      DurableAgentRuntime.layerWithBindings(bindings).pipe(
        Layer.provide(RunToolAuthorization.allowAll),
      ),
    ),
  );

const snapshot = Effect.fn(function* (receipt: Receipt) {
  const ledger = yield* SubmissionLedger;

  return yield* ledger.loadRecoverySnapshot(
    RecoverySnapshotRequest.make({ submissionId: receipt.submissionId }),
  );
});

layer(baseLayer)("bounded durable Thread processing", (it) => {
  it.effect("settles only the FIFO head and closes its provider before returning", () =>
    Effect.gen(function* () {
      const closed = yield* Ref.make(0);

      const model = makeModel(
        Stream.fromIterable(finalParts),
        Ref.update(closed, (n) => n + 1),
      );

      const agent = Agent.withModel(definition, model);
      const binding = yield* DurableWorkerBinding.make(agent, digests);
      const bindings = [binding];
      const runtime = yield* makeRuntime(bindings);

      bindings.length = 0;
      const first = yield* runtime.submit(agent, "first", options("bounded", "first"));
      const reserved = yield* Deferred.make<void>();
      const finish = yield* Deferred.make<void>();
      const control = yield* DurableRuntimeFailpointTestControl;

      yield* control.setHandler((location) =>
        location === "terminalize:after-reserve"
          ? Deferred.succeed(reserved, undefined).pipe(Effect.andThen(Deferred.await(finish)))
          : Effect.void,
      );

      expect((yield* runtime.submissionStatus(first))._tag).toBe("pending");

      const worker = yield* runtime.processThreadHead(first.threadId).pipe(Effect.forkChild);

      yield* Deferred.await(reserved);
      // Admission after the final Turn cannot join that Run and must remain FIFO work.
      const second = yield* runtime.submit(agent, "second", options("bounded", "second"));

      yield* control.clear;
      yield* Deferred.succeed(finish, undefined);
      const result = yield* Fiber.join(worker);

      expect(Option.isSome(result)).toBe(true);
      if (Option.isSome(result)) expect(result.value.submissionId).toBe(first.submissionId);
      expect((yield* runtime.submissionStatus(first))._tag).toBe("settled");
      expect((yield* runtime.submissionStatus(second))._tag).toBe("pending");
      expect((yield* snapshot(second)).ownership).toBeUndefined();
      expect((yield* snapshot(second)).submission.state).toBe("ready");
      expect(yield* Ref.get(closed)).toBe(1);

      const remainder = yield* runtime.processThreadResolved(first.threadId);

      expect(remainder.map((settlement) => settlement.submissionId)).toEqual([second.submissionId]);
      expect(yield* Ref.get(closed)).toBe(2);
    }),
  );

  it.effect.each(["failpoint", "lookup", "defect"] as const)(
    "releases a claim after an early %s failure",
    (failure) =>
      Effect.gen(function* () {
        const ledger = yield* SubmissionLedger;
        const armed = yield* Ref.make(false);

        const runtime = yield* makeRuntime().pipe(
          Effect.provideService(SubmissionLedger, {
            ...ledger,
            lookup: (request) =>
              Ref.get(armed).pipe(
                Effect.flatMap((enabled) => {
                  if (!enabled || failure === "failpoint") return ledger.lookup(request);

                  return failure === "lookup"
                    ? Effect.fail(LedgerError.make({ operation: "lookup", message: "unavailable" }))
                    : Effect.die("lookup defect");
                }),
              ),
          }),
        );

        const receipt = yield* runtime.submit(
          { definition },
          "first",
          options(`early-${failure}`, "first"),
        );

        const control = yield* DurableRuntimeFailpointTestControl;

        yield* Ref.set(armed, true);
        if (failure === "failpoint") {
          yield* control.setHandler((location) =>
            location === "claim:after-claim"
              ? Effect.fail(DurableRuntimeFailpointError.make({ location }))
              : Effect.void,
          );
        }
        const result = yield* Effect.exit(runtime.processThreadHead(receipt.threadId));

        expect(Exit.isFailure(result)).toBe(true);
        expect((yield* snapshot(receipt)).ownership).toBeUndefined();
        yield* control.clear;
        yield* Ref.set(armed, false);
        expect((yield* runtime.submissionStatus(receipt))._tag).toBe("pending");
      }),
  );

  it.effect("registers claim cleanup even when interrupted during the acquisition handoff", () =>
    Effect.gen(function* () {
      const ledger = yield* SubmissionLedger;
      const acquired = yield* Deferred.make<void>();
      const handoff = yield* Deferred.make<void>();

      const runtime = yield* makeRuntime().pipe(
        Effect.provideService(SubmissionLedger, {
          ...ledger,
          claim: (request) =>
            ledger.claim(request).pipe(
              Effect.tap(() => Deferred.succeed(acquired, undefined)),
              Effect.tap(() => Deferred.await(handoff)),
            ),
        }),
      );

      const receipt = yield* runtime.submit({ definition }, "first", options("handoff", "first"));

      const worker = yield* runtime.processThreadHead(receipt.threadId).pipe(Effect.forkChild);

      yield* Deferred.await(acquired);
      const interrupt = yield* Fiber.interrupt(worker).pipe(Effect.forkChild);

      yield* Effect.yieldNow;
      yield* Deferred.succeed(handoff, undefined);
      yield* Fiber.join(interrupt);
      expect((yield* snapshot(receipt)).ownership).toBeUndefined();
      expect((yield* runtime.submissionStatus(receipt))._tag).toBe("pending");
    }),
  );

  it.effect(
    "detaches a waiter and releases the latest renewed token when its Attempt is interrupted",
    () =>
      Effect.gen(function* () {
        const ledger = yield* SubmissionLedger;
        const started = yield* Deferred.make<void>();
        const renewalReturned = yield* Deferred.make<void>();
        const renewalHandoff = yield* Deferred.make<void>();
        const renewedToken = Schema.decodeSync(OwnershipToken)("renewed-token");
        const originalToken = yield* Ref.make<Option.Option<OwnershipToken>>(Option.none());
        const releasedTokens = yield* Ref.make<ReadonlyArray<OwnershipToken>>([]);
        const closed = yield* Ref.make(false);

        const agent = Agent.withModel(
          definition,
          makeModel(
            Stream.fromEffect(Deferred.succeed(started, undefined)).pipe(
              Stream.drain,
              Stream.concat(Stream.never),
            ),
            Ref.set(closed, true),
          ),
        );

        const binding = yield* DurableWorkerBinding.make(agent, digests);

        const runtime = yield* makeRuntime([binding]).pipe(
          Effect.provideService(SubmissionLedger, {
            ...ledger,
            renewOwnership: (request) =>
              ledger.renewOwnership(request).pipe(
                Effect.tap(() => Deferred.await(started)),
                Effect.tap((renewed) =>
                  Ref.set(originalToken, Option.some(renewed.ownershipToken)),
                ),
                Effect.map((renewed) =>
                  OwnershipRenewal.make({ ...renewed, ownershipToken: renewedToken }),
                ),
                Effect.tap(() => Deferred.succeed(renewalReturned, undefined)),
                Effect.tap(() => Deferred.await(renewalHandoff)),
              ),
            releaseOwnership: (request) =>
              Effect.gen(function* () {
                yield* Ref.update(releasedTokens, (tokens) => [...tokens, request.ownershipToken]);
                const original = yield* Ref.get(originalToken);

                return yield* ledger.releaseOwnership(
                  ReleaseOwnershipRequest.make({
                    ...request,
                    ownershipToken:
                      request.ownershipToken === renewedToken && Option.isSome(original)
                        ? original.value
                        : request.ownershipToken,
                  }),
                );
              }),
          }),
        );

        const receipt = yield* runtime.submit(agent, "first", options("renewed", "first"));

        const worker = yield* runtime.processThreadHead(receipt.threadId).pipe(Effect.forkChild);

        yield* Deferred.await(started);
        yield* Deferred.await(renewalReturned);
        const waiter = yield* runtime.awaitSettlement(receipt).pipe(Effect.forkChild);

        yield* Fiber.interrupt(waiter);
        expect((yield* snapshot(receipt)).ownership).toBeDefined();
        expect(yield* Ref.get(closed)).toBe(false);
        const interrupt = yield* Fiber.interrupt(worker).pipe(Effect.forkChild);

        yield* Effect.yieldNow;
        yield* Deferred.succeed(renewalHandoff, undefined);
        yield* Fiber.join(interrupt);
        expect(yield* Ref.get(releasedTokens)).toEqual([renewedToken]);
        expect((yield* snapshot(receipt)).ownership).toBeUndefined();
        expect(yield* Ref.get(closed)).toBe(true);
        expect((yield* runtime.submissionStatus(receipt))._tag).toBe("pending");
      }),
  );

  it.effect("keeps approval suspension pending without touching a queued follower", () =>
    Effect.gen(function* () {
      const approvalTools = Toolkit.make(
        Tool.make("approve", {
          parameters: Schema.Struct({}),
          success: Schema.String,
          needsApproval: true,
        }),
      );

      const agent = Agent.withModel(
        Agent.make("approval-head", {
          input: Schema.String,
          output: Schema.String,
          instructions: "Call approve.",
          toolkit: approvalTools,
          policy,
        }),
        makeModel(
          Stream.fromIterable<Response.StreamPartEncoded>([
            {
              type: "tool-call",
              id: "approval-1",
              name: "approve",
              params: {},
              providerExecuted: false,
            },
            { type: "finish", reason: "tool-calls", usage: { inputTokens: {}, outputTokens: {} } },
          ]),
        ),
      );

      const binding = yield* DurableWorkerBinding.make(agent, digests).pipe(
        Effect.provide(approvalTools.toLayer({ approve: () => Effect.die("unapproved handler") })),
      );

      const runtime = yield* makeRuntime([binding]);

      const first = yield* runtime.submit(agent, "first", options("suspended", "first"));
      const second = yield* runtime.submit(agent, "second", options("suspended", "second"));

      expect(Option.isNone(yield* runtime.processThreadHead(first.threadId))).toBe(true);
      expect((yield* snapshot(first)).submission.state).toBe("suspended");
      expect((yield* runtime.submissionStatus(first))._tag).toBe("pending");
      expect(Option.isNone(yield* runtime.processThreadHead(first.threadId))).toBe(true);
      expect((yield* snapshot(second)).ownership).toBeUndefined();
      expect((yield* runtime.submissionStatus(second))._tag).toBe("pending");
    }),
  );

  it.effect("recovers a queued abort without claiming or releasing the head", () =>
    Effect.gen(function* () {
      const ledger = yield* SubmissionLedger;
      const claims = yield* Ref.make(0);

      const runtime = yield* makeRuntime().pipe(
        Effect.provideService(SubmissionLedger, {
          ...ledger,
          claim: (request) =>
            Ref.update(claims, (n) => n + 1).pipe(Effect.andThen(ledger.claim(request))),
        }),
      );

      const first = yield* runtime.submit(
        { definition },
        "first",
        options("queued-abort", "first"),
      );

      const second = yield* runtime.submit(
        { definition },
        "second",
        options("queued-abort", "second"),
      );

      yield* runtime.abort(
        AbortCommand.make({
          submissionId: second.submissionId,
          author: "test",
          reason: "cancel queued work",
        }),
      );
      yield* runtime.recoverSubmission(second.submissionId);
      expect(yield* Ref.get(claims)).toBe(0);
      expect((yield* snapshot(first)).ownership).toBeUndefined();
      const status = yield* runtime.submissionStatus(second);

      expect(status._tag).toBe("settled");
      if (status._tag === "settled") expect(status.settlement.outcome).toBe("aborted");
    }),
  );

  it.effect(
    "releases recovery ownership when settlement reservation fails before its canonical append",
    () =>
      Effect.gen(function* () {
        const runtime = yield* makeRuntime();
        const control = yield* DurableRuntimeFailpointTestControl;

        const receipt = yield* runtime.submit(
          { definition },
          "first",
          options("recovery-failure", "first"),
        );

        yield* runtime.abort(
          AbortCommand.make({
            submissionId: receipt.submissionId,
            author: "test",
            reason: "cancel",
          }),
        );
        yield* control.setHandler((location) =>
          location === "terminalize:after-reserve"
            ? Effect.fail(DurableRuntimeFailpointError.make({ location }))
            : Effect.void,
        );

        expect(
          (yield* runtime.recoverSubmission(receipt.submissionId).pipe(Effect.flip))._tag,
        ).toBe("DurableRuntimeFailpointError");
        expect((yield* snapshot(receipt)).ownership).toBeUndefined();
        expect((yield* runtime.submissionStatus(receipt))._tag).toBe("pending");
        yield* control.clear;
        yield* runtime.recoverSubmission(receipt.submissionId);
        expect((yield* runtime.submissionStatus(receipt))._tag).toBe("settled");
      }),
  );

  it.effect("authorizes status before ledger reads and rejects a mismatched receipt Thread", () =>
    Effect.gen(function* () {
      const runtime = yield* makeRuntime();

      const receipt = yield* runtime.submit(
        { definition },
        "first",
        options("authorized", "first"),
      );

      const ledger = yield* SubmissionLedger;
      const reads = yield* Ref.make(0);

      const agent = Agent.withModel(definition, makeModel(Stream.fromIterable(finalParts)));
      const binding = yield* DurableWorkerBinding.make(agent, digests);

      const denied = yield* makeRuntime([binding]).pipe(
        Effect.provideService(OperationAuthorizer, {
          authorize: (request) =>
            Effect.fail(OperationDenied.make({ operation: request.operation, reason: "denied" })),
        }),
        Effect.provideService(SubmissionLedger, {
          ...ledger,
          lookup: (request) =>
            Ref.update(reads, (n) => n + 1).pipe(Effect.andThen(ledger.lookup(request))),
        }),
      );

      expect((yield* denied.submissionStatus(receipt).pipe(Effect.flip))._tag).toBe(
        "OperationDenied",
      );
      expect(yield* Ref.get(reads)).toBe(0);
      expect((yield* denied.inspectSubmissionStatus(receipt))._tag).toBe("pending");

      expect(Option.isSome(yield* denied.processThreadHead(receipt.threadId))).toBe(true);
      expect((yield* denied.inspectSubmissionStatus(receipt))._tag).toBe("settled");

      const mismatched = Receipt.make({
        ...receipt,
        threadId: Schema.decodeSync(ThreadId)("other"),
      });

      expect((yield* runtime.submissionStatus(mismatched).pipe(Effect.flip))._tag).toBe(
        "OperationDenied",
      );
    }),
  );
});
