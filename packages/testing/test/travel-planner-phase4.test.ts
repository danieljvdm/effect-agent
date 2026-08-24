import { ConversationId, type SubmissionId } from "@effect-agent/core";
import { NodeDurableRuntime, type NodeDurableRuntimeOptions } from "@effect-agent/platform-node";
import {
  AbortCommand,
  ClaimRequest,
  ConversationRead,
  ConversationStore,
  DurableAgentRuntime,
  DurableRuntimeConfig,
  DurableRuntimeFailpoint,
  DurableRuntimeFailpointError,
  IdempotencyKey,
  ProducerId,
  RecoverySnapshotRequest,
  ReleaseOwnershipRequest,
  SubmissionLedger,
  SubmissionLookupById,
  ToolReconciler,
  WakeScheduler,
  type CanonicalRecordEnvelope,
} from "@effect-agent/session";
import {
  MemoryConversationStoreLive,
  MemorySubmissionLedgerLive,
} from "@effect-agent/storage-memory";
import { NodeCrypto, NodeFileSystem } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import type { PlatformError } from "effect";
import { Cause, Effect, Exit, FileSystem, Layer, Option, Schema, Stream } from "effect";

import {
  expectedTravelPlan,
  makePhase4TravelPlannerAgent,
  normalizeDurableTravelPlannerEvidence,
  phase1Trip,
  phase4TravelPlannerDeploymentId,
  phase4TravelPlannerProducerId,
  phase4TravelPlannerProfile,
  phase4TravelPlannerSubmitOptions,
  phase4TravelPlannerWorkerLayer,
  travelPlanFromDurableSettlement,
  TravelPlannerDurabilityProfile,
} from "../src/index.ts";

const decodeConversationId = Schema.decodeSync(ConversationId);
const decodeIdempotencyKey = Schema.decodeSync(IdempotencyKey);
const decodeProducerId = Schema.decodeSync(ProducerId);

const submitOptions = (conversationId: ConversationId, idempotencyKey: string) =>
  phase4TravelPlannerSubmitOptions(conversationId, decodeIdempotencyKey(idempotencyKey));

const runtimeOptions = (
  filename: string,
  overrides?: Partial<NodeDurableRuntimeOptions>,
): NodeDurableRuntimeOptions => ({
  filename,
  deploymentId: phase4TravelPlannerDeploymentId,
  producerId: phase4TravelPlannerProducerId,
  observationPollInterval: 1,
  ...overrides,
});

const withTemporaryDirectory = <A, E>(
  use: (directory: string) => Effect.Effect<A, E>,
): Effect.Effect<A, E | PlatformError.PlatformError> =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "effect-agent-travel-planner-p4-",
      });
      return yield* use(directory);
    }),
  ).pipe(Effect.provide(NodeFileSystem.layer));

/** In-memory reference pair for scenarios that need no process-durability claim. */
const memoryRuntimeLayer = DurableAgentRuntime.layer.pipe(
  Layer.provideMerge(
    Layer.mergeAll(
      MemorySubmissionLedgerLive,
      MemoryConversationStoreLive,
      WakeScheduler.layerNoop,
      DurableRuntimeFailpoint.layer,
      ToolReconciler.uncertain,
      DurableRuntimeConfig.layer({
        deploymentId: phase4TravelPlannerDeploymentId,
        producerId: phase4TravelPlannerProducerId,
      }),
    ).pipe(Layer.provideMerge(NodeCrypto.layer)),
  ),
);

const readLog = (conversationId: ConversationId) =>
  Effect.gen(function* () {
    const store = yield* ConversationStore;
    return yield* Stream.runCollect(
      store.read(ConversationRead.make({ conversationId, limit: 1_024 })),
    );
  });

const logTags = (records: ReadonlyArray<CanonicalRecordEnvelope>): ReadonlyArray<string> =>
  records.map((envelope) => envelope.record.payload._tag);

const settledSubmissionIds = (
  records: ReadonlyArray<CanonicalRecordEnvelope>,
): ReadonlyArray<string> =>
  records.flatMap((envelope) =>
    envelope.record.payload._tag === "SubmissionSettled"
      ? [envelope.record.payload.submissionId]
      : [],
  );

const lookupState = (submissionId: SubmissionId) =>
  Effect.gen(function* () {
    const ledger = yield* SubmissionLedger;
    const snapshot = yield* ledger.lookup(SubmissionLookupById.make({ submissionId }));
    expect(Option.isSome(snapshot)).toBe(true);
    if (Option.isNone(snapshot)) throw new Error("Expected the Submission to exist");
    return snapshot.value.state;
  });

const failureOf = <A, E>(exit: Exit.Exit<A, E>): unknown => {
  expect(Exit.isFailure(exit)).toBe(true);
  if (Exit.isSuccess(exit)) throw new Error("Expected the Effect to fail");
  return Cause.squash(exit.cause);
};

/** One DN "host process": the full Node/SQLite runtime stack plus the deterministic services. */
const dnLayer = (options: NodeDurableRuntimeOptions) =>
  Layer.mergeAll(phase4TravelPlannerWorkerLayer, NodeDurableRuntime.layer(options));

/** One expected happy-path Run: Turn 1 declares the three searches, Turn 2 emits the plan. */
const RUN_TAGS = [
  "ModelResponseRecorded",
  "ToolCallSettled",
  "ToolCallSettled",
  "ToolCallSettled",
  "ModelResponseRecorded",
  "RunCompleted",
] as const;

describe("TEST-014 P4 durable Travel Planner profile (DN) — supplier booking is NOT claimed safely replayable (P5 scope)", () => {
  it("pins the DN durability claim and explicitly does not claim replay-safe supplier booking", () => {
    const decoded = Schema.decodeUnknownSync(TravelPlannerDurabilityProfile)(
      Schema.encodeSync(TravelPlannerDurabilityProfile)(phase4TravelPlannerProfile),
    );
    expect(decoded).toEqual(phase4TravelPlannerProfile);
    expect(phase4TravelPlannerProfile).toEqual({
      deploymentClass: "DN",
      durableAcceptedWork: true,
      canonicalSchemaVersion: 1,
      supplierBookingReplaySafe: false,
    });
  });

  it.effect(
    "submit returns a durable Receipt and a same-key resubmit returns the same Receipt (memory reference adapters)",
    () =>
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const conversationId = decodeConversationId("travel-planner-p4-receipt");
        const agent = makePhase4TravelPlannerAgent();

        const receipt = yield* runtime.submit(
          agent,
          phase1Trip,
          submitOptions(conversationId, "p4-receipt-1"),
        );
        expect(receipt.conversationId).toBe(conversationId);
        expect(receipt.queueSequence).toBe(1);
        expect(yield* lookupState(receipt.submissionId)).toBe("ready");

        const replayed = yield* runtime.submit(
          agent,
          phase1Trip,
          submitOptions(conversationId, "p4-receipt-1"),
        );
        expect(replayed).toEqual(receipt);
      }).pipe(Effect.provide(memoryRuntimeLayer)),
  );

  it.effect(
    "runs one durable planning Submission to Settlement with canonical input, per-Turn, and settlement records on SQLite (booking stays read-only: supplier booking is NOT claimed safely replayable, P5 scope)",
    () =>
      withTemporaryDirectory((directory) =>
        Effect.gen(function* () {
          const runtime = yield* DurableAgentRuntime;
          const conversationId = decodeConversationId("travel-planner-p4-settlement");
          const agent = makePhase4TravelPlannerAgent();

          const receipt = yield* runtime.submit(
            agent,
            phase1Trip,
            submitOptions(conversationId, "p4-settlement-1"),
          );
          const settlements = yield* runtime.processConversation(agent, conversationId);
          expect(settlements).toHaveLength(1);
          expect(settlements[0]?.submissionId).toBe(receipt.submissionId);

          const settlement = yield* runtime.awaitSettlement(receipt);
          expect(settlement.outcome).toBe("completed");
          expect(settlement.receiptId).toBe(receipt.receiptId);
          expect(yield* lookupState(receipt.submissionId)).toBe("settled");

          const records = yield* readLog(conversationId);
          expect(logTags(records)).toEqual([
            "ConversationCreated",
            "UserInputRecorded",
            ...RUN_TAGS,
            "SubmissionSettled",
          ]);
          const plan = yield* travelPlanFromDurableSettlement(records);
          expect(plan).toEqual(expectedTravelPlan);
        }).pipe(Effect.provide(dnLayer(runtimeOptions(`${directory}/dn.sqlite`)))),
      ),
  );

  it.effect(
    "keeps one trip lane FIFO: the second Submission is never claimable and joins the active Run",
    () =>
      withTemporaryDirectory((directory) =>
        Effect.gen(function* () {
          const runtime = yield* DurableAgentRuntime;
          const ledger = yield* SubmissionLedger;
          const conversationId = decodeConversationId("travel-planner-p4-fifo");
          const agent = makePhase4TravelPlannerAgent();

          const first = yield* runtime.submit(
            agent,
            phase1Trip,
            submitOptions(conversationId, "p4-fifo-1"),
          );
          const second = yield* runtime.submit(
            agent,
            phase1Trip,
            submitOptions(conversationId, "p4-fifo-2"),
          );
          expect(first.queueSequence).toBe(1);
          expect(second.queueSequence).toBe(2);

          // The lane head is the ONLY claimable Submission (DUR-004/DUR-005): while the first is
          // owned and unsettled, a competing claim gets nothing — never the second Submission.
          const head = yield* ledger.claim(
            ClaimRequest.make({
              conversationId,
              producerId: decodeProducerId("travel-planner-p4-probe"),
            }),
          );
          expect(Option.isSome(head)).toBe(true);
          if (Option.isNone(head)) throw new Error("Expected the lane head claim");
          expect(head.value.submissionId).toBe(first.submissionId);
          const blocked = yield* ledger.claim(
            ClaimRequest.make({
              conversationId,
              producerId: decodeProducerId("travel-planner-p4-probe-2"),
            }),
          );
          expect(Option.isNone(blocked)).toBe(true);
          yield* ledger.releaseOwnership(
            ReleaseOwnershipRequest.make({
              submissionId: head.value.submissionId,
              ownershipToken: head.value.ownershipToken,
            }),
          );

          const settlements = yield* runtime.processConversation(agent, conversationId);
          // P5 (plan §2.5): the active host Run claims the contiguous ready prefix, so the
          // second Submission JOINS the first Run — one head settlement, and the joined
          // Submission settles with the host (DUR-002) in admitted FIFO order (DUR-004).
          expect(settlements.map((settlement) => settlement.submissionId)).toEqual([
            first.submissionId,
          ]);
          expect(settlements.map((settlement) => settlement.outcome)).toEqual(["completed"]);
          const joinedSettlement = yield* runtime.awaitSettlement(second);
          expect(joinedSettlement.outcome).toBe("completed");

          // Canonical order keeps the admitted FIFO order: both inputs are canonical before
          // the host Run's Turns (the joined input joins BEFORE the next model request), and
          // the settlement records commit host-first.
          const records = yield* readLog(conversationId);
          expect(logTags(records)).toEqual([
            "ConversationCreated",
            "UserInputRecorded",
            "UserInputRecorded",
            ...RUN_TAGS,
            "SubmissionSettled",
            "SubmissionSettled",
          ]);
          expect(settledSubmissionIds(records)).toEqual([first.submissionId, second.submissionId]);
        }).pipe(Effect.provide(dnLayer(runtimeOptions(`${directory}/fifo.sqlite`)))),
      ),
  );

  it.effect(
    "restart-equivalence: a worker killed at terminalize:after-reserve recovers on the reopened SQLite file to the exact reserved settlement and an uninterrupted run's projection",
    () =>
      withTemporaryDirectory((directory) =>
        Effect.gen(function* () {
          const conversationId = decodeConversationId("travel-planner-p4-restart");
          const restartFile = `${directory}/restart.sqlite`;
          const agent = makePhase4TravelPlannerAgent();

          // Host process 1: the Attempt dies cleanly (typed failpoint) AFTER reserving the exact
          // settlement record but BEFORE appending it canonically (durability §12 step 1→2 gap).
          const crashed = yield* Effect.gen(function* () {
            const runtime = yield* DurableAgentRuntime;
            const ledger = yield* SubmissionLedger;
            const receipt = yield* runtime.submit(
              agent,
              phase1Trip,
              submitOptions(conversationId, "p4-restart-1"),
            );
            const exit = yield* Effect.exit(runtime.processConversation(agent, conversationId));
            const failure = failureOf(exit);
            expect(failure).toHaveProperty("_tag", "DurableRuntimeFailpointError");
            expect(failure).toHaveProperty("location", "terminalize:after-reserve");
            expect(yield* lookupState(receipt.submissionId)).toBe("terminalizing");

            const snapshot = yield* ledger.loadRecoverySnapshot(
              RecoverySnapshotRequest.make({ submissionId: receipt.submissionId }),
            );
            expect(snapshot.reservation?.finalized).toBe(false);
            expect(logTags(yield* readLog(conversationId))).not.toContain("SubmissionSettled");
            return { receipt, reservation: snapshot.reservation };
          }).pipe(
            Effect.provide(
              dnLayer(
                runtimeOptions(restartFile, {
                  runtimeFailpoint: (location) =>
                    location === "terminalize:after-reserve"
                      ? Effect.fail(DurableRuntimeFailpointError.make({ location }))
                      : Effect.void,
                }),
              ),
            ),
          );

          // Host process 2: reopen the SAME file in-process; recovery appends the EXACT reserved
          // record, finalizes the ledger, and the accepted work settles once.
          const recovered = yield* Effect.gen(function* () {
            const runtime = yield* DurableAgentRuntime;
            const reports = yield* runtime.runRecovery;
            const report = reports.find(
              (candidate) => candidate.submissionId === crashed.receipt.submissionId,
            );
            expect(report?.decision._tag).toBe("AppendReservedSettlement");
            expect(report?.disposition).toBe("repaired");

            const settlement = yield* runtime.awaitSettlement(crashed.receipt);
            expect(settlement.outcome).toBe("completed");
            expect(yield* lookupState(crashed.receipt.submissionId)).toBe("settled");
            return yield* readLog(conversationId);
          }).pipe(Effect.provide(NodeDurableRuntime.layer(runtimeOptions(restartFile))));

          const settledEnvelope = recovered.find(
            (envelope) => envelope.record.payload._tag === "SubmissionSettled",
          );
          expect(settledEnvelope?.record).toEqual(crashed.reservation?.record);
          const audit = recovered.at(-1);
          expect(audit?.record.payload._tag).toBe("RepairAnnotated");
          if (audit?.record.payload._tag === "RepairAnnotated") {
            expect(audit.record.payload.reason).toBe("recovery:AppendReservedSettlement");
          }

          // Control: the same Submission runs uninterrupted on a separate database. Modulo the
          // ledger-minted Submission identity and the trailing recovery audit record, the
          // recovered Conversation projects to the same canonical evidence.
          const control = yield* Effect.gen(function* () {
            const runtime = yield* DurableAgentRuntime;
            const receipt = yield* runtime.submit(
              agent,
              phase1Trip,
              submitOptions(conversationId, "p4-restart-1"),
            );
            const settlements = yield* runtime.processConversation(agent, conversationId);
            expect(settlements).toHaveLength(1);
            return { receipt, records: yield* readLog(conversationId) };
          }).pipe(Effect.provide(dnLayer(runtimeOptions(`${directory}/control.sqlite`))));

          const normalizedRecovered = yield* normalizeDurableTravelPlannerEvidence(
            recovered.slice(0, -1),
            crashed.receipt,
          );
          const normalizedControl = yield* normalizeDurableTravelPlannerEvidence(
            control.records,
            control.receipt,
          );
          expect(normalizedRecovered).toEqual(normalizedControl);
          expect(yield* travelPlanFromDurableSettlement(recovered)).toEqual(
            yield* travelPlanFromDurableSettlement(control.records),
          );
        }),
      ),
  );

  it.effect(
    "durable abort of a ready Submission settles aborted through recovery without running an Attempt",
    () =>
      withTemporaryDirectory((directory) =>
        Effect.gen(function* () {
          const runtime = yield* DurableAgentRuntime;
          const conversationId = decodeConversationId("travel-planner-p4-abort");
          // An empty script: any model invocation would fail the Run instead of settling aborted.
          const agent = makePhase4TravelPlannerAgent([]);

          const receipt = yield* runtime.submit(
            agent,
            phase1Trip,
            submitOptions(conversationId, "p4-abort-1"),
          );
          expect(yield* lookupState(receipt.submissionId)).toBe("ready");

          const intent = yield* runtime.abort(
            AbortCommand.make({
              submissionId: receipt.submissionId,
              author: "traveler",
              reason: "The trip was cancelled before planning started.",
            }),
          );
          expect(intent.submissionId).toBe(receipt.submissionId);

          const reports = yield* runtime.runRecovery;
          const report = reports.find(
            (candidate) => candidate.submissionId === receipt.submissionId,
          );
          expect(report?.decision._tag).toBe("SettleAborted");
          expect(report?.disposition).toBe("repaired");

          const settlement = yield* runtime.awaitSettlement(receipt);
          expect(settlement.outcome).toBe("aborted");

          // No Attempt ran: the abort command and settlement are canonical, but no input was
          // applied and no model Turn was committed.
          const records = yield* readLog(conversationId);
          expect(logTags(records)).toEqual([
            "ConversationCreated",
            "AbortRequested",
            "SubmissionSettled",
            "RepairAnnotated",
          ]);
        }).pipe(Effect.provide(dnLayer(runtimeOptions(`${directory}/abort.sqlite`)))),
      ),
  );
});
