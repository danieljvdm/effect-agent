import { Agent, ConversationId, SubmissionId, ToolCallId } from "@effect-agent/core";
import type {
  RunApprovalDecision,
  RunApprovalHook,
  RunApprovalRequest,
} from "@effect-agent/engine";
import { NodeDurableRuntime, type NodeDurableRuntimeOptions } from "@effect-agent/platform-node";
import {
  ApprovalDecisionCommand,
  CanonicalRecordEnvelope,
  ConversationRead,
  ConversationStore,
  DurableAgentRuntime,
  DurableApprovalResolver,
  DurableRuntimeConfig,
  DurableRuntimeFailpoint,
  DurableRuntimeFailpointError,
  DurableRuntimeFailpointTestControl,
  IdempotencyKey,
  OperationCaller,
  PersistedJson,
  ResolutionCompletedWithResult,
  SubmissionLedger,
  SubmissionLookupById,
  ToolReconciler,
  UnknownResolutionCommand,
  WakeScheduler,
  promptFromCanonicalRecords,
  runIdForSubmission,
  toolCallPreparedRecordId,
  toolStepSettledRecordId,
  type DurableRuntimeFailpointLocation,
} from "@effect-agent/session";
import {
  MemoryConversationStoreLive,
  MemorySubmissionLedgerLive,
} from "@effect-agent/storage-memory";
import { NodeCrypto, NodeFileSystem } from "@effect/platform-node";
import { describe, expect, it, layer } from "@effect/vitest";
import {
  Cause,
  Context,
  Duration,
  Effect,
  Exit,
  Fiber,
  FileSystem,
  Layer,
  Option,
  PlatformError,
  Ref,
  Schema,
  Stream,
} from "effect";
import { LanguageModel, Model, Prompt, type Response } from "effect/unstable/ai";

import { TrustedLocalDurableAuthorizationLayer } from "../src/durable-test-authorization.ts";
import {
  assertSettledBookingsExistAtSupplier,
  bookFlightIdempotencyKey,
  cancelBookingIdempotencyKey,
  itineraryStepIdempotencyKey,
  phase1Trip,
  phase5TravelPlannerDefinitionDigests,
  phase5TravelPlannerDeploymentId,
  phase5TravelPlannerPrincipal,
  phase5TravelPlannerProducerId,
  phase5TravelPlannerProfile,
  phase5TravelPlannerSubmitOptions,
  phase5TravelPlannerWorkerLayer,
  SupplierBookingConfirmation,
  SupplierBookingDesk,
  supplierBookingRefFor,
  TravelPlannerBookingProfile,
  TravelPlannerPhase5,
  TravelSupplierReconcilerLayer,
  TripRequest,
} from "../src/index.ts";

const decodeConversationId = Schema.decodeSync(ConversationId);
const decodeIdempotencyKey = Schema.decodeSync(IdempotencyKey);
const decodeToolCallId = Schema.decodeSync(ToolCallId);
const toPersistedJson = Schema.decodeUnknownSync(PersistedJson);
const encodeConfirmation = Schema.encodeSync(SupplierBookingConfirmation);
const CALLER = OperationCaller.make({ principal: phase5TravelPlannerPrincipal });

const submitOptions = (conversationId: string, idempotencyKey: string) =>
  phase5TravelPlannerSubmitOptions(
    decodeConversationId(conversationId),
    decodeIdempotencyKey(idempotencyKey),
  );

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

const report = (bookingRefs: ReadonlyArray<string>): ReadonlyArray<Response.StreamPartEncoded> =>
  finalParts(JSON.stringify({ summary: "trip booked", bookingRefs }));

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
      "travel-planner-phase-5",
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

const flightParams = (travelerRef: string) => ({
  quoteId: "quote-sfo-lhr-001",
  travelerRef,
  departOn: "2026-09-14",
});

const itineraryParams = (travelerRef: string) => ({
  quoteId: "quote-sfo-lhr-001",
  destination: "LHR",
  nights: 4,
  travelerRef,
});

const followUpTrip = Schema.decodeSync(TripRequest)({
  request: "Add a museum day to the London plan.",
  origin: "SFO",
  destination: "LHR",
  departOn: "2026-09-14",
  nights: 4,
  travelers: 2,
  budgetCents: 350_000,
  currency: "USD",
});

const configLayer = DurableRuntimeConfig.layer({
  deploymentId: phase5TravelPlannerDeploymentId,
  producerId: phase5TravelPlannerProducerId,
  settlementPollInterval: Duration.millis(100),
  leaseRenewalInterval: Duration.seconds(5),
  abortPollInterval: Duration.millis(100),
});

const infraLayer = Layer.mergeAll(
  MemorySubmissionLedgerLive,
  MemoryConversationStoreLive,
  WakeScheduler.layerNoop,
  DurableRuntimeFailpoint.layerTest,
  configLayer,
  TrustedLocalDurableAuthorizationLayer,
).pipe(Layer.provideMerge(NodeCrypto.layer));

/** Test control replacing the policy-auto approval delegate per test (default: auto-approve). */
class ApprovalDelegateTestControl extends Context.Service<
  ApprovalDelegateTestControl,
  {
    readonly set: (
      handler: (request: RunApprovalRequest) => Effect.Effect<RunApprovalDecision>,
    ) => Effect.Effect<void>;
    readonly reset: Effect.Effect<void>;
  }
>()("@effect-agent/testing/TravelApprovalDelegateTestControl") {}

const autoApprove = (): Effect.Effect<RunApprovalDecision> =>
  Effect.succeed({ _tag: "approved", reason: "travel policy auto-approves scripted bookings" });

const approvalDelegateLayer = Layer.effectContext(
  Effect.gen(function* () {
    const handler =
      yield* Ref.make<(request: RunApprovalRequest) => Effect.Effect<RunApprovalDecision>>(
        autoApprove,
      );
    const hook: RunApprovalHook<never, never> = {
      request: (request) => Ref.get(handler).pipe(Effect.flatMap((current) => current(request))),
    };
    return Context.make(DurableApprovalResolver, hook).pipe(
      Context.add(
        ApprovalDelegateTestControl,
        ApprovalDelegateTestControl.of({
          set: (next) => Ref.set(handler, next),
          reset: Ref.set(handler, autoApprove),
        }),
      ),
    );
  }),
);

/** One supplier desk per layer block; tests isolate through unique idempotency keys. */
const reconciledDeskLayer = TravelSupplierReconcilerLayer.pipe(
  Layer.provideMerge(SupplierBookingDesk.layer),
);

/** The fixture's real reconciliation policy queries this desk by idempotency key. */
const reconciledTestLayer = DurableAgentRuntime.layer.pipe(
  Layer.provideMerge(Layer.mergeAll(infraLayer, approvalDelegateLayer, reconciledDeskLayer)),
);

/** The fail-closed default: no proof is ever asserted, open booking calls stay Unknown. */
const uncertainTestLayer = DurableAgentRuntime.layer.pipe(
  Layer.provideMerge(
    Layer.mergeAll(
      infraLayer,
      approvalDelegateLayer,
      SupplierBookingDesk.layer,
      ToolReconciler.uncertain,
    ),
  ),
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

const promptOccurrences = (prompt: Prompt.Prompt, needle: string): number =>
  JSON.stringify(prompt).split(needle).length - 1;

/** Fork one Attempt, wait for the armed supplier crash window, and kill the Attempt mid-handler. */
const interruptAtSupplierWrite = <A, E, R>(
  attempt: Effect.Effect<A, E, R>,
  held: Effect.Effect<void>,
) =>
  Effect.gen(function* () {
    const fiber = yield* Effect.forkChild(attempt);
    yield* held;
    yield* Fiber.interrupt(fiber);
  });

describe("TEST-014 P5 Travel Planner booking profile", () => {
  it("pins the P5 booking claim: uncertainty protocol and recorded Steps, never exactly-once external effects", () => {
    const decoded = Schema.decodeUnknownSync(TravelPlannerBookingProfile)(
      Schema.encodeSync(TravelPlannerBookingProfile)(phase5TravelPlannerProfile),
    );
    expect(decoded).toEqual(phase5TravelPlannerProfile);
    expect(phase5TravelPlannerProfile).toEqual({
      deploymentClass: "DN",
      durableAcceptedWork: true,
      canonicalSchemaVersion: 1,
      supplierBookingUncertaintyProtocol: true,
      durableStepsRecorded: true,
      exactlyOnceExternalEffects: false,
    });
  });
});

layer(reconciledTestLayer)(
  "P5 Travel Planner bookings under the supplier reconciler (plan §7)",
  (it) => {
    it.effect(
      "a recovered Attempt replays the settled booking result without a second supplier call",
      () =>
        Effect.gen(function* () {
          const runtime = yield* DurableAgentRuntime;
          const desk = yield* SupplierBookingDesk;
          const scripted = yield* makeScriptedModel((call) =>
            call === 0
              ? toolTurn(toolCall("bf-replay-1", "book_flight", flightParams("traveler-replay")))
              : report([supplierBookingRefFor(bookFlightIdempotencyKey("bf-replay-1"))]),
          );
          const agent = Agent.withModel(TravelPlannerPhase5, scripted.model);
          const conversation = "travel-p5-settled-replay";
          const key = bookFlightIdempotencyKey("bf-replay-1");

          const receipt = yield* runtime.submit(
            agent,
            phase1Trip,
            submitOptions(conversation, "p5-replay-1"),
          );
          yield* armFailpoint("turn:after-results-append");
          const killed = yield* Effect.exit(
            runtime
              .processConversation(
                agent,
                decodeConversationId(conversation),
                phase5TravelPlannerDefinitionDigests,
              )
              .pipe(Effect.provide(phase5TravelPlannerWorkerLayer)),
          );
          expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
          yield* clearFailpoint;
          // The booking executed once and its result is already canonical.
          expect(yield* desk.callCount(key)).toBe(1);

          const settlements = yield* runtime
            .processConversation(
              agent,
              decodeConversationId(conversation),
              phase5TravelPlannerDefinitionDigests,
            )
            .pipe(Effect.provide(phase5TravelPlannerWorkerLayer));
          expect(settlements[0]?.outcome).toBe("completed");
          // Exit gate: the recorded Tool outcome did not rerun.
          expect(yield* desk.callCount(key)).toBe(1);

          const runId = runIdForSubmission(receipt.submissionId);
          const records = yield* readLog(conversation);
          expect(
            records.filter(
              (envelope) => envelope.record.recordId === `tool-settled:${runId}:1:bf-replay-1`,
            ),
          ).toHaveLength(1);
          yield* assertSettledBookingsExistAtSupplier(records);
        }),
    );

    it.effect(
      "re-entering book_itinerary replays reserve-flight from its ToolStepSettled record (supplier count 1)",
      () =>
        Effect.gen(function* () {
          const runtime = yield* DurableAgentRuntime;
          const desk = yield* SupplierBookingDesk;
          const scripted = yield* makeScriptedModel((call) =>
            call === 0
              ? toolTurn(
                  toolCall("bi-steps-1", "book_itinerary", itineraryParams("traveler-steps")),
                )
              : report([
                  supplierBookingRefFor(
                    itineraryStepIdempotencyKey("bi-steps-1", "reserve-flight"),
                  ),
                  supplierBookingRefFor(
                    itineraryStepIdempotencyKey("bi-steps-1", "reserve-lodging"),
                  ),
                ]),
          );
          const agent = Agent.withModel(TravelPlannerPhase5, scripted.model);
          const conversation = "travel-p5-step-replay";
          const flightKey = itineraryStepIdempotencyKey("bi-steps-1", "reserve-flight");
          const lodgingKey = itineraryStepIdempotencyKey("bi-steps-1", "reserve-lodging");
          const confirmKey = itineraryStepIdempotencyKey("bi-steps-1", "issue-confirmation");

          const receipt = yield* runtime.submit(
            agent,
            phase1Trip,
            submitOptions(conversation, "p5-steps-1"),
          );
          // Kill right after the FIRST Step commit: reserve-flight is exactly-once-recorded,
          // reserve-lodging never ran.
          yield* armFailpoint("step:after-step-append");
          const killed = yield* Effect.exit(
            runtime
              .processConversation(
                agent,
                decodeConversationId(conversation),
                phase5TravelPlannerDefinitionDigests,
              )
              .pipe(Effect.provide(phase5TravelPlannerWorkerLayer)),
          );
          expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
          yield* clearFailpoint;
          expect(yield* desk.callCount(flightKey)).toBe(1);
          expect(yield* desk.callCount(lodgingKey)).toBe(0);

          const runId = runIdForSubmission(receipt.submissionId);
          const callId = decodeToolCallId("bi-steps-1");
          expect(
            (yield* readLog(conversation)).map((envelope) => envelope.record.recordId),
          ).toContain(toolStepSettledRecordId(runId, callId, "reserve-flight"));

          // The fixture reconciler proves book_itinerary safe to re-enter BY CONSTRUCTION:
          // every mutation inside is a Step whose supplier key derives from (toolCallId, stepName).
          const settlements = yield* runtime
            .processConversation(
              agent,
              decodeConversationId(conversation),
              phase5TravelPlannerDefinitionDigests,
            )
            .pipe(Effect.provide(phase5TravelPlannerWorkerLayer));
          expect(settlements[0]?.outcome).toBe("completed");
          // Exit gate: the committed Step replayed WITHOUT executing; later Steps ran once.
          expect(yield* desk.callCount(flightKey)).toBe(1);
          expect(yield* desk.callCount(lodgingKey)).toBe(1);
          expect(yield* desk.callCount(confirmKey)).toBe(1);

          const records = yield* readLog(conversation);
          for (const stepName of ["reserve-flight", "reserve-lodging", "issue-confirmation"]) {
            expect(
              records.filter(
                (envelope) =>
                  envelope.record.recordId === toolStepSettledRecordId(runId, callId, stepName),
              ),
            ).toHaveLength(1);
          }
          yield* assertSettledBookingsExistAtSupplier(records);
        }),
    );

    it.effect(
      "a kill inside reserve-lodging re-executes the step and the supplier count of 2 is observable",
      () =>
        Effect.gen(function* () {
          const runtime = yield* DurableAgentRuntime;
          const desk = yield* SupplierBookingDesk;
          const scripted = yield* makeScriptedModel((call) =>
            call === 0
              ? toolTurn(
                  toolCall("bi-lodging-1", "book_itinerary", itineraryParams("traveler-lodging")),
                )
              : report([
                  supplierBookingRefFor(
                    itineraryStepIdempotencyKey("bi-lodging-1", "reserve-flight"),
                  ),
                ]),
          );
          const agent = Agent.withModel(TravelPlannerPhase5, scripted.model);
          const conversation = "travel-p5-step-atleastonce";
          const flightKey = itineraryStepIdempotencyKey("bi-lodging-1", "reserve-flight");
          const lodgingKey = itineraryStepIdempotencyKey("bi-lodging-1", "reserve-lodging");

          const receipt = yield* runtime.submit(
            agent,
            phase1Trip,
            submitOptions(conversation, "p5-lodging-1"),
          );
          // Crash window INSIDE the second Step: the supplier write lands, the Step commit never
          // does — the durable state says "reserve-lodging may have happened".
          const hold = yield* desk.holdAfterWrite(lodgingKey);
          yield* interruptAtSupplierWrite(
            runtime
              .processConversation(
                agent,
                decodeConversationId(conversation),
                phase5TravelPlannerDefinitionDigests,
              )
              .pipe(Effect.provide(phase5TravelPlannerWorkerLayer)),
            hold.held,
          );
          expect(yield* desk.callCount(flightKey)).toBe(1);
          expect(yield* desk.callCount(lodgingKey)).toBe(1);
          const runId = runIdForSubmission(receipt.submissionId);
          const callId = decodeToolCallId("bi-lodging-1");
          const committed = yield* readLog(conversation);
          const committedIds = committed.map((envelope) => envelope.record.recordId);
          expect(committedIds).toContain(toolStepSettledRecordId(runId, callId, "reserve-flight"));
          expect(committedIds).not.toContain(
            toolStepSettledRecordId(runId, callId, "reserve-lodging"),
          );

          // Recovery defers to the worker (SafeToRetry), and the re-entered handler re-executes
          // the uncommitted Step.
          const reports = yield* runtime.runRecovery;
          const recoveryReport = reports.find(
            (entry) => entry.submissionId === receipt.submissionId,
          );
          expect(recoveryReport?.decision._tag).toBe("MarkUnknown");
          expect(recoveryReport?.disposition).toBe("deferred");

          const settlements = yield* runtime
            .processConversation(
              agent,
              decodeConversationId(conversation),
              phase5TravelPlannerDefinitionDigests,
            )
            .pipe(Effect.provide(phase5TravelPlannerWorkerLayer));
          expect(settlements[0]?.outcome).toBe("completed");
          // Exit gate: the external Step side effect stayed honestly at-least-once — TWO
          // supplier calls are observable, deduped to ONE booking by the supplier's own key.
          expect(yield* desk.callCount(flightKey)).toBe(1);
          expect(yield* desk.callCount(lodgingKey)).toBe(2);
          const lodgingBookings = (yield* desk.bookings).filter(
            (booking) => booking.idempotencyKey === lodgingKey,
          );
          expect(lodgingBookings).toHaveLength(1);
          yield* assertSettledBookingsExistAtSupplier(yield* readLog(conversation));
        }),
    );

    it.effect("the reconciler recovers the confirmed supplier booking canonically", () =>
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const desk = yield* SupplierBookingDesk;
        const scripted = yield* makeScriptedModel((call) =>
          call === 0
            ? toolTurn(toolCall("bf-rec-1", "book_flight", flightParams("traveler-reconciled")))
            : report([supplierBookingRefFor(bookFlightIdempotencyKey("bf-rec-1"))]),
        );
        const agent = Agent.withModel(TravelPlannerPhase5, scripted.model);
        const conversation = "travel-p5-reconciled";
        const key = bookFlightIdempotencyKey("bf-rec-1");

        const receipt = yield* runtime.submit(
          agent,
          phase1Trip,
          submitOptions(conversation, "p5-reconciled-1"),
        );
        // The supplier confirms the booking; the Attempt dies before any outcome is recorded.
        const hold = yield* desk.holdAfterWrite(key);
        yield* interruptAtSupplierWrite(
          runtime
            .processConversation(
              agent,
              decodeConversationId(conversation),
              phase5TravelPlannerDefinitionDigests,
            )
            .pipe(Effect.provide(phase5TravelPlannerWorkerLayer)),
          hold.held,
        );
        expect(yield* desk.callCount(key)).toBe(1);
        expect(Option.isSome(yield* desk.lookup(key))).toBe(true);

        // Recovery consults TravelSupplierReconciler, which finds the confirmed booking under
        // the derived idempotency key: the recovered supplier truth becomes canonical WITHOUT
        // executing anything.
        const reports = yield* runtime.runRecovery;
        const recoveryReport = reports.find((entry) => entry.submissionId === receipt.submissionId);
        expect(recoveryReport?.decision._tag).toBe("MarkUnknown");
        expect(recoveryReport?.disposition).toBe("repaired");
        expect(yield* desk.callCount(key)).toBe(1);

        const runId = runIdForSubmission(receipt.submissionId);
        const afterRecovery = yield* readLog(conversation);
        const byId = recordsById(afterRecovery);
        const settled = byId.get(`tool-settled:${runId}:1:bf-rec-1`)?.record.payload;
        expect(settled?._tag).toBe("ToolCallSettled");
        if (settled?._tag === "ToolCallSettled") {
          expect(settled.result).toEqual(
            toPersistedJson(
              encodeConfirmation(
                SupplierBookingConfirmation.make({
                  bookingRef: supplierBookingRefFor(key),
                  status: "confirmed",
                  detail: `flight quote-sfo-lhr-001 for traveler-reconciled on 2026-09-14`,
                }),
              ),
            ),
          );
        }
        const resolved = byId.get(`tool-resolved:${runId}:1:bf-rec-1`)?.record.payload;
        expect(resolved?._tag).toBe("ToolCallResolved");
        if (resolved?._tag === "ToolCallResolved") {
          expect(resolved.resolution).toBe("completed-with-result");
          expect(resolved.author).toBe("reconciler");
        }

        const settlements = yield* runtime
          .processConversation(
            agent,
            decodeConversationId(conversation),
            phase5TravelPlannerDefinitionDigests,
          )
          .pipe(Effect.provide(phase5TravelPlannerWorkerLayer));
        expect(settlements[0]?.outcome).toBe("completed");
        expect(yield* desk.callCount(key)).toBe(1);
        yield* assertSettledBookingsExistAtSupplier(yield* readLog(conversation));
      }),
    );

    it.effect("an idempotent-annotated cancel repeats safely under its bookingRef contract", () =>
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const desk = yield* SupplierBookingDesk;
        const bookedRef = supplierBookingRefFor(bookFlightIdempotencyKey("bf-cancel-1"));
        const scripted = yield* makeScriptedModel((call) => {
          switch (call) {
            case 0:
              return toolTurn(
                toolCall("bf-cancel-1", "book_flight", flightParams("traveler-cancel")),
              );
            case 1:
              return report([bookedRef]);
            case 2:
              return toolTurn(
                toolCall("cb-cancel-1", "cancel_booking", {
                  bookingRef: bookedRef,
                  travelerRef: "traveler-cancel",
                }),
              );
            default:
              return report([]);
          }
        });
        const agent = Agent.withModel(TravelPlannerPhase5, scripted.model);
        const conversation = "travel-p5-idempotent-cancel";
        const cancelKey = cancelBookingIdempotencyKey(bookedRef);

        // Submission 1 books and settles.
        yield* runtime.submit(agent, phase1Trip, submitOptions(conversation, "p5-cancel-book"));
        const booked = yield* runtime
          .processConversation(
            agent,
            decodeConversationId(conversation),
            phase5TravelPlannerDefinitionDigests,
          )
          .pipe(Effect.provide(phase5TravelPlannerWorkerLayer));
        expect(booked[0]?.outcome).toBe("completed");

        // Submission 2 cancels; the Attempt dies AFTER the supplier cancellation lands but
        // before any outcome is recorded.
        const receipt = yield* runtime.submit(
          agent,
          followUpTrip,
          submitOptions(conversation, "p5-cancel-cancel"),
        );
        const hold = yield* desk.holdAfterWrite(cancelKey);
        yield* interruptAtSupplierWrite(
          runtime
            .processConversation(
              agent,
              decodeConversationId(conversation),
              phase5TravelPlannerDefinitionDigests,
            )
            .pipe(Effect.provide(phase5TravelPlannerWorkerLayer)),
          hold.held,
        );
        expect(yield* desk.callCount(cancelKey)).toBe(1);

        // The declared idempotency contract lets recovery defer to plain re-execution: no
        // reconciliation proof, no Unknown Outcome.
        const reports = yield* runtime.runRecovery;
        const recoveryReport = reports.find((entry) => entry.submissionId === receipt.submissionId);
        expect(recoveryReport?.decision._tag).toBe("MarkUnknown");
        expect(recoveryReport?.disposition).toBe("deferred");

        const settlements = yield* runtime
          .processConversation(
            agent,
            decodeConversationId(conversation),
            phase5TravelPlannerDefinitionDigests,
          )
          .pipe(Effect.provide(phase5TravelPlannerWorkerLayer));
        expect(settlements[0]?.outcome).toBe("completed");
        // Honest at-least-once: TWO supplier calls, ONE cancelled booking, one settled record.
        expect(yield* desk.callCount(cancelKey)).toBe(2);
        const cancelled = (yield* desk.bookings).filter(
          (booking) => booking.bookingRef === bookedRef,
        );
        expect(cancelled).toHaveLength(1);
        expect(cancelled[0]?.status).toBe("cancelled");

        const runId = runIdForSubmission(receipt.submissionId);
        const records = yield* readLog(conversation);
        expect(
          records.filter(
            (envelope) => envelope.record.recordId === `tool-settled:${runId}:1:cb-cancel-1`,
          ),
        ).toHaveLength(1);
        expect(logTags(records)).not.toContain("ToolCallUnknown");
        yield* assertSettledBookingsExistAtSupplier(records);
      }),
    );

    it.effect(
      "an unapproved booking suspends durably without a settlement and resumes on resolveApproval",
      () =>
        Effect.gen(function* () {
          const control = yield* ApprovalDelegateTestControl;
          yield* control.set(() => Effect.succeed({ _tag: "unresolved" }));
          const runtime = yield* DurableAgentRuntime;
          const desk = yield* SupplierBookingDesk;
          const scripted = yield* makeScriptedModel((call) =>
            call === 0
              ? toolTurn(toolCall("bf-appr-1", "book_flight", flightParams("traveler-approval")))
              : report([supplierBookingRefFor(bookFlightIdempotencyKey("bf-appr-1"))]),
          );
          const agent = Agent.withModel(TravelPlannerPhase5, scripted.model);
          const conversation = "travel-p5-approval-suspend";
          const key = bookFlightIdempotencyKey("bf-appr-1");

          const receipt = yield* runtime.submit(
            agent,
            phase1Trip,
            submitOptions(conversation, "p5-approval-1"),
          );
          const first = yield* runtime
            .processConversation(
              agent,
              decodeConversationId(conversation),
              phase5TravelPlannerDefinitionDigests,
            )
            .pipe(Effect.provide(phase5TravelPlannerWorkerLayer));
          // No settlement: the accepted-work obligation stays owed while the lane waits durably,
          // with the canonical ToolApprovalRequested as the safe boundary (durability §8).
          expect(first).toHaveLength(0);
          expect(yield* lookupState(receipt.submissionId)).toBe("suspended");
          expect(yield* desk.callCount(key)).toBe(0);

          const runId = runIdForSubmission(receipt.submissionId);
          const suspendedLog = yield* readLog(conversation);
          expect(recordsById(suspendedLog).has(`approval-request:${runId}:1:bf-appr-1`)).toBe(true);
          expect(logTags(suspendedLog)).not.toContain("ToolCallPrepared");

          yield* runtime.resolveApproval(
            ApprovalDecisionCommand.make({
              submissionId: receipt.submissionId,
              toolCallId: decodeToolCallId("bf-appr-1"),
              decision: "approved",
              resolver: "operator",
              reason: "the traveler confirmed the charge",
            }),
            CALLER,
          );
          const settlements = yield* runtime
            .processConversation(
              agent,
              decodeConversationId(conversation),
              phase5TravelPlannerDefinitionDigests,
            )
            .pipe(Effect.provide(phase5TravelPlannerWorkerLayer));
          expect(settlements[0]?.outcome).toBe("completed");
          expect(yield* desk.callCount(key)).toBe(1);
          // The resumed Attempt replayed the declared batch: exactly two model requests ever.
          expect(scripted.prompts).toHaveLength(2);

          const records = yield* readLog(conversation);
          const byId = recordsById(records);
          const decision = byId.get(`approval-decision:${runId}:1:bf-appr-1`)?.record.payload;
          expect(decision?._tag).toBe("ToolApprovalDecided");
          if (decision?._tag === "ToolApprovalDecided") {
            expect(decision.decision).toBe("approved");
            expect(decision.resolver).toBe("operator");
          }
          yield* assertSettledBookingsExistAtSupplier(records);
          yield* control.reset;
        }),
    );

    it.effect("a denied booking settles failed with canonical request and decision records", () =>
      Effect.gen(function* () {
        const control = yield* ApprovalDelegateTestControl;
        yield* control.set(() => Effect.succeed({ _tag: "unresolved" }));
        const runtime = yield* DurableAgentRuntime;
        const desk = yield* SupplierBookingDesk;
        const scripted = yield* makeScriptedModel((call) =>
          call === 0
            ? toolTurn(toolCall("bf-deny-1", "book_flight", flightParams("traveler-denied")))
            : report([]),
        );
        const agent = Agent.withModel(TravelPlannerPhase5, scripted.model);
        const conversation = "travel-p5-approval-deny";
        const key = bookFlightIdempotencyKey("bf-deny-1");

        const receipt = yield* runtime.submit(
          agent,
          phase1Trip,
          submitOptions(conversation, "p5-deny-1"),
        );
        const first = yield* runtime
          .processConversation(
            agent,
            decodeConversationId(conversation),
            phase5TravelPlannerDefinitionDigests,
          )
          .pipe(Effect.provide(phase5TravelPlannerWorkerLayer));
        expect(first).toHaveLength(0);
        expect(yield* lookupState(receipt.submissionId)).toBe("suspended");

        yield* runtime.resolveApproval(
          ApprovalDecisionCommand.make({
            submissionId: receipt.submissionId,
            toolCallId: decodeToolCallId("bf-deny-1"),
            decision: "denied",
            resolver: "operator",
            reason: "the traveler declined the charge",
          }),
          CALLER,
        );
        const settlements = yield* runtime
          .processConversation(
            agent,
            decodeConversationId(conversation),
            phase5TravelPlannerDefinitionDigests,
          )
          .pipe(Effect.provide(phase5TravelPlannerWorkerLayer));
        // Denial-terminal (P2 default): the Run fails with the denial canonical; the supplier
        // was never called.
        expect(settlements[0]?.outcome).toBe("failed");
        expect(yield* desk.callCount(key)).toBe(0);

        const runId = runIdForSubmission(receipt.submissionId);
        const byId = recordsById(yield* readLog(conversation));
        expect(byId.has(`approval-request:${runId}:1:bf-deny-1`)).toBe(true);
        const decision = byId.get(`approval-decision:${runId}:1:bf-deny-1`)?.record.payload;
        expect(decision?._tag).toBe("ToolApprovalDecided");
        if (decision?._tag === "ToolApprovalDecided") {
          expect(decision.decision).toBe("denied");
        }
        expect(byId.has(`tool-prepared:${runId}:1:bf-deny-1`)).toBe(false);
        yield* control.reset;
      }),
    );

    it.effect(
      "a traveler follow-up killed at join:after-claim reverts to ready and joins exactly once on resume",
      () =>
        Effect.gen(function* () {
          const runtime = yield* DurableAgentRuntime;
          const scripted = yield* makeScriptedModel((call) =>
            call === 0
              ? toolTurn(toolCall("bf-joinA-1", "book_flight", flightParams("traveler-join-a")))
              : report([supplierBookingRefFor(bookFlightIdempotencyKey("bf-joinA-1"))]),
          );
          const agent = Agent.withModel(TravelPlannerPhase5, scripted.model);
          const conversation = "travel-p5-join-claim";

          const host = yield* runtime.submit(
            agent,
            phase1Trip,
            submitOptions(conversation, "p5-join-a-host"),
          );
          const followUp = yield* runtime.submit(
            agent,
            followUpTrip,
            submitOptions(conversation, "p5-join-a-followup"),
          );
          yield* armFailpoint("join:after-claim");
          const killed = yield* Effect.exit(
            runtime
              .processConversation(
                agent,
                decodeConversationId(conversation),
                phase5TravelPlannerDefinitionDigests,
              )
              .pipe(Effect.provide(phase5TravelPlannerWorkerLayer)),
          );
          expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
          yield* clearFailpoint;

          // Pre-append joining reverts to ready (DUR-016): no canonical input exists yet.
          expect(yield* lookupState(followUp.submissionId)).toBe("joining");
          expect(
            recordsById(yield* readLog(conversation)).has(`input:${followUp.submissionId}`),
          ).toBe(false);
          const reports = yield* runtime.runRecovery;
          const joinReport = reports.find((entry) => entry.submissionId === followUp.submissionId);
          expect(joinReport?.decision._tag).toBe("RevertJoining");
          expect(joinReport?.disposition).toBe("repaired");
          expect(yield* lookupState(followUp.submissionId)).toBe("ready");

          const settlements = yield* runtime
            .processConversation(
              agent,
              decodeConversationId(conversation),
              phase5TravelPlannerDefinitionDigests,
            )
            .pipe(Effect.provide(phase5TravelPlannerWorkerLayer));
          expect(settlements.map((settlement) => settlement.submissionId)).toEqual([
            host.submissionId,
          ]);
          const joined = yield* runtime.awaitSettlement(followUp, CALLER);
          expect(joined.outcome).toBe("completed");

          // Exactly one canonical input record ever, delivered into exactly one model request.
          const records = yield* readLog(conversation);
          expect(
            records.filter(
              (envelope) => envelope.record.recordId === `input:${followUp.submissionId}`,
            ),
          ).toHaveLength(1);
          const firstPrompt = scripted.prompts[0];
          expect(
            firstPrompt === undefined
              ? 0
              : promptOccurrences(firstPrompt, "Add a museum day to the London plan."),
          ).toBe(1);
        }),
    );

    it.effect(
      "a traveler follow-up killed at join:after-canonical-append reattaches without duplicate delivery",
      () =>
        Effect.gen(function* () {
          const runtime = yield* DurableAgentRuntime;
          const scripted = yield* makeScriptedModel((call) =>
            call === 0
              ? toolTurn(toolCall("bf-joinB-1", "book_flight", flightParams("traveler-join-b")))
              : report([supplierBookingRefFor(bookFlightIdempotencyKey("bf-joinB-1"))]),
          );
          const agent = Agent.withModel(TravelPlannerPhase5, scripted.model);
          const conversation = "travel-p5-join-append";

          const host = yield* runtime.submit(
            agent,
            phase1Trip,
            submitOptions(conversation, "p5-join-b-host"),
          );
          const followUp = yield* runtime.submit(
            agent,
            followUpTrip,
            submitOptions(conversation, "p5-join-b-followup"),
          );
          yield* armFailpoint("join:after-canonical-append");
          const killed = yield* Effect.exit(
            runtime
              .processConversation(
                agent,
                decodeConversationId(conversation),
                phase5TravelPlannerDefinitionDigests,
              )
              .pipe(Effect.provide(phase5TravelPlannerWorkerLayer)),
          );
          expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
          yield* clearFailpoint;

          // The canonical input committed; only the joined marker was lost.
          expect(yield* lookupState(followUp.submissionId)).toBe("joining");
          expect(
            recordsById(yield* readLog(conversation)).has(`input:${followUp.submissionId}`),
          ).toBe(true);
          const reports = yield* runtime.runRecovery;
          const joinReport = reports.find((entry) => entry.submissionId === followUp.submissionId);
          expect(joinReport?.decision._tag).toBe("RepairJoinMarker");
          expect(joinReport?.disposition).toBe("repaired");
          expect(yield* lookupState(followUp.submissionId)).toBe("joined");

          const settlements = yield* runtime
            .processConversation(
              agent,
              decodeConversationId(conversation),
              phase5TravelPlannerDefinitionDigests,
            )
            .pipe(Effect.provide(phase5TravelPlannerWorkerLayer));
          expect(settlements.map((settlement) => settlement.submissionId)).toEqual([
            host.submissionId,
          ]);
          const joined = yield* runtime.awaitSettlement(followUp, CALLER);
          expect(joined.outcome).toBe("completed");

          // Reattachment, never duplication: one canonical record, one prompt delivery.
          const records = yield* readLog(conversation);
          expect(
            records.filter(
              (envelope) => envelope.record.recordId === `input:${followUp.submissionId}`,
            ),
          ).toHaveLength(1);
          const firstPrompt = scripted.prompts[0];
          expect(
            firstPrompt === undefined
              ? 0
              : promptOccurrences(firstPrompt, "Add a museum day to the London plan."),
          ).toBe(1);
        }),
    );
  },
);

layer(uncertainTestLayer)(
  "P5 Travel Planner bookings under the fail-closed default reconciler (plan §7)",
  (it) => {
    it.effect(
      "a kill during book_flight stops at UnknownToolOutcome under the default reconciler and the lane blocks",
      () =>
        Effect.gen(function* () {
          const runtime = yield* DurableAgentRuntime;
          const desk = yield* SupplierBookingDesk;
          const scripted = yield* makeScriptedModel((call) =>
            call === 0
              ? toolTurn(toolCall("bf-unknown-1", "book_flight", flightParams("traveler-unknown")))
              : report([supplierBookingRefFor(bookFlightIdempotencyKey("bf-unknown-1"))]),
          );
          const agent = Agent.withModel(TravelPlannerPhase5, scripted.model);
          const conversation = "travel-p5-unknown-block";
          const key = bookFlightIdempotencyKey("bf-unknown-1");

          const receipt = yield* runtime.submit(
            agent,
            phase1Trip,
            submitOptions(conversation, "p5-unknown-1"),
          );
          const hold = yield* desk.holdAfterWrite(key);
          yield* interruptAtSupplierWrite(
            runtime
              .processConversation(
                agent,
                decodeConversationId(conversation),
                phase5TravelPlannerDefinitionDigests,
              )
              .pipe(Effect.provide(phase5TravelPlannerWorkerLayer)),
            hold.held,
          );
          expect(yield* desk.callCount(key)).toBe(1);

          // No proof either way: the outcome is Unknown and the lane blocks durably (DUR-009).
          const reports = yield* runtime.runRecovery;
          const recoveryReport = reports.find(
            (entry) => entry.submissionId === receipt.submissionId,
          );
          expect(recoveryReport?.decision._tag).toBe("MarkUnknown");
          expect(recoveryReport?.disposition).toBe("unknown");
          expect(yield* lookupState(receipt.submissionId)).toBe("unknown");

          const runId = runIdForSubmission(receipt.submissionId);
          const records = yield* readLog(conversation);
          expect(records.map((envelope) => envelope.record.recordId)).toContain(
            `tool-unknown:${runId}:1:bf-unknown-1`,
          );
          expect(records.map((envelope) => envelope.record.recordId)).not.toContain(
            `tool-settled:${runId}:1:bf-unknown-1`,
          );

          // Exit gate: the uncertain ordinary effect does NOT replay automatically — the blocked
          // lane grants no worker claim and the supplier call count never moves.
          const settlements = yield* runtime
            .processConversation(
              agent,
              decodeConversationId(conversation),
              phase5TravelPlannerDefinitionDigests,
            )
            .pipe(Effect.provide(phase5TravelPlannerWorkerLayer));
          expect(settlements).toEqual([]);
          expect(yield* desk.callCount(key)).toBe(1);
          expect(yield* lookupState(receipt.submissionId)).toBe("unknown");
        }),
    );

    it.effect(
      "an unprovable booking stops at UnknownToolOutcome and resolveUnknown with supplier truth converges to one settlement",
      () =>
        Effect.gen(function* () {
          const runtime = yield* DurableAgentRuntime;
          const desk = yield* SupplierBookingDesk;
          const scripted = yield* makeScriptedModel((call) =>
            call === 0
              ? toolTurn(toolCall("bf-truth-1", "book_flight", flightParams("traveler-truth")))
              : report([supplierBookingRefFor(bookFlightIdempotencyKey("bf-truth-1"))]),
          );
          const agent = Agent.withModel(TravelPlannerPhase5, scripted.model);
          const conversation = "travel-p5-unknown-resolve";
          const key = bookFlightIdempotencyKey("bf-truth-1");

          const receipt = yield* runtime.submit(
            agent,
            phase1Trip,
            submitOptions(conversation, "p5-truth-1"),
          );
          const hold = yield* desk.holdAfterWrite(key);
          yield* interruptAtSupplierWrite(
            runtime
              .processConversation(
                agent,
                decodeConversationId(conversation),
                phase5TravelPlannerDefinitionDigests,
              )
              .pipe(Effect.provide(phase5TravelPlannerWorkerLayer)),
            hold.held,
          );
          yield* runtime.runRecovery;
          expect(yield* lookupState(receipt.submissionId)).toBe("unknown");

          // The operator recovers supplier truth out of band (here: the desk itself) and
          // resolves the Unknown Outcome with it — the DUR-017 authorized resolution path.
          const booking = yield* desk.lookup(key);
          expect(Option.isSome(booking)).toBe(true);
          if (Option.isNone(booking)) throw new Error("Expected the supplier booking");
          const supplierTruth = toPersistedJson(
            encodeConfirmation(
              SupplierBookingConfirmation.make({
                bookingRef: booking.value.bookingRef,
                status: "confirmed",
                detail: booking.value.detail,
              }),
            ),
          );
          yield* runtime.resolveUnknown(
            UnknownResolutionCommand.make({
              submissionId: receipt.submissionId,
              toolCallId: decodeToolCallId("bf-truth-1"),
              author: "operator",
              reason: "the supplier desk shows the confirmed booking",
              resolution: ResolutionCompletedWithResult.make({
                result: supplierTruth,
                isFailure: false,
              }),
            }),
            CALLER,
          );

          const settlements = yield* runtime
            .processConversation(
              agent,
              decodeConversationId(conversation),
              phase5TravelPlannerDefinitionDigests,
            )
            .pipe(Effect.provide(phase5TravelPlannerWorkerLayer));
          expect(settlements[0]?.outcome).toBe("completed");
          // The resolved call never re-executed; the recovered result is the settled truth.
          expect(yield* desk.callCount(key)).toBe(1);

          const runId = runIdForSubmission(receipt.submissionId);
          const records = yield* readLog(conversation);
          const settledRecords = records.filter(
            (envelope) => envelope.record.recordId === `tool-settled:${runId}:1:bf-truth-1`,
          );
          expect(settledRecords).toHaveLength(1);
          const settled = settledRecords[0]?.record.payload;
          if (settled?._tag === "ToolCallSettled") {
            expect(settled.result).toEqual(supplierTruth);
          }
          expect(
            records.filter((envelope) => envelope.record.payload._tag === "SubmissionSettled"),
          ).toHaveLength(1);
          const resolved = recordsById(records).get(`tool-resolved:${runId}:1:bf-truth-1`)?.record
            .payload;
          if (resolved?._tag === "ToolCallResolved") {
            expect(resolved.resolution).toBe("completed-with-result");
            expect(resolved.author).toBe("operator");
          }
          // Never fabricate: the settled bookingRef exists at the supplier.
          yield* assertSettledBookingsExistAtSupplier(records);
        }),
    );
  },
);

const withTemporaryDirectory = <A, E>(
  use: (directory: string) => Effect.Effect<A, E>,
): Effect.Effect<A, E | PlatformError.PlatformError> =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "effect-agent-travel-planner-p5-",
      });
      return yield* use(directory);
    }),
  ).pipe(Effect.provide(NodeFileSystem.layer));

const runtimeOptions = (filename: string): NodeDurableRuntimeOptions => ({
  filename,
  deploymentId: phase5TravelPlannerDeploymentId,
  producerId: phase5TravelPlannerProducerId,
  observationPollInterval: 1,
});

describe("TEST-014 P5 Travel Planner on the DN SQLite assembly", () => {
  it.effect(
    "books an itinerary on SQLite: class-shaped Tool parameters, prepared records, and canonical Step records persist end-to-end",
    () =>
      withTemporaryDirectory((directory) =>
        Effect.gen(function* () {
          const runtime = yield* DurableAgentRuntime;
          const desk = yield* SupplierBookingDesk;
          const conversationId = decodeConversationId("travel-p5-sqlite");
          const params = itineraryParams("traveler-sqlite");
          const scripted = yield* makeScriptedModel((call) =>
            call === 0
              ? toolTurn(toolCall("bi-sqlite-1", "book_itinerary", params))
              : report([
                  supplierBookingRefFor(
                    itineraryStepIdempotencyKey("bi-sqlite-1", "reserve-flight"),
                  ),
                  supplierBookingRefFor(
                    itineraryStepIdempotencyKey("bi-sqlite-1", "reserve-lodging"),
                  ),
                ]),
          );
          const agent = Agent.withModel(TravelPlannerPhase5, scripted.model);

          const receipt = yield* runtime.submit(
            agent,
            phase1Trip,
            phase5TravelPlannerSubmitOptions(conversationId, decodeIdempotencyKey("p5-sqlite-1")),
          );
          const settlements = yield* runtime.processConversation(
            agent,
            conversationId,
            phase5TravelPlannerDefinitionDigests,
          );
          expect(settlements[0]?.outcome).toBe("completed");
          const settlement = yield* runtime.awaitSettlement(receipt, CALLER);
          expect(settlement.outcome).toBe("completed");

          const store = yield* ConversationStore;
          const records = yield* Stream.runCollect(
            store.read(ConversationRead.make({ conversationId, limit: 1_024 })),
          );
          expect(logTags(records)).toEqual([
            "ConversationCreated",
            "UserInputRecorded",
            "ModelResponseRecorded",
            "ToolCallPrepared",
            "ToolStepSettled",
            "ToolStepSettled",
            "ToolStepSettled",
            "ToolCallSettled",
            "ModelResponseRecorded",
            "SubmissionSettled",
          ]);

          const runId = runIdForSubmission(receipt.submissionId);
          const callId = decodeToolCallId("bi-sqlite-1");
          const byId = recordsById(records);
          // Since the P5 engine fix, official history carries Schema-ENCODED Tool parameters:
          // the class-shaped params persist as the exact plain-JSON wire form on SQLite.
          const prepared = byId.get(toolCallPreparedRecordId(runId, 1, callId))?.record.payload;
          expect(prepared?._tag).toBe("ToolCallPrepared");
          if (prepared?._tag === "ToolCallPrepared") {
            expect(prepared.parameters).toEqual(params);
            expect(prepared.toolName).toBe("book_itinerary");
          }
          for (const stepName of ["reserve-flight", "reserve-lodging", "issue-confirmation"]) {
            expect(byId.has(toolStepSettledRecordId(runId, callId, stepName))).toBe(true);
            expect(
              yield* desk.callCount(itineraryStepIdempotencyKey("bi-sqlite-1", stepName)),
            ).toBe(1);
          }

          // The canonical journal alone rebuilds the model-visible prompt (encoded params are
          // Prompt-valid, the P4 Struct workaround is unnecessary).
          const prompt = yield* promptFromCanonicalRecords(records);
          expect(prompt.content.map((message) => message.role)).toEqual([
            "system",
            "user",
            "assistant",
            "tool",
            "assistant",
          ]);
          yield* assertSettledBookingsExistAtSupplier(records);
        }).pipe(
          Effect.provide(
            Layer.mergeAll(
              phase5TravelPlannerWorkerLayer,
              SupplierBookingDesk.layer,
              NodeDurableRuntime.layer(runtimeOptions(`${directory}/p5.sqlite`)).pipe(
                Layer.provide(TrustedLocalDurableAuthorizationLayer),
              ),
            ),
          ),
        ),
      ),
  );
});
