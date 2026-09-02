import { ToolCallId } from "@effect-agent/core";
import {
  DestinationShortlist,
  TravelPlannerPhase4,
  TravelPlannerPhase5,
  TravelCoordinator,
  assertSettledBookingsExistAtSupplier,
  bookFlightIdempotencyKey,
  durableResearchCallId,
  durableResearchShortlist,
  expectedTravelPlan,
  normalizeCrossPlatformTravelPlannerEvidence,
  phase1Trip,
  phase4TravelPlannerPrincipal,
  phase4TravelPlannerSubmitOptions,
  phase5TravelPlannerSubmitOptions,
  phase6BookingToolCallId,
  phase6BookingTrip,
  phase6GatedPlannerDefinitionDigests,
  phase6GatedTrip,
  phase6GuideInvocationCount,
  phase6ResearchDestination,
  phase6ResearchMission,
  phase6SupplierDesk,
  phase6SupplierDeskLayer,
  phase6TravelPlannerDeploymentId,
  phase6TravelPlannerGoldenEvidence,
  phase6TravelPlannerProducerPrefix,
  releasePhase6PlannerGate,
  releasePhase6ResearcherGate,
  resetPhase6PlannerGate,
  resetPhase6ResearcherGate,
  s2TravelPlannerSubmitOptions,
  travelPlanFromDurableSettlement,
  TripRequest,
} from "@effect-agent/testing/fixtures/travel-planner";
import {
  ApprovalDecisionCommand,
  CanonicalRecordEnvelope,
  ResolutionNeverHappened,
  UnknownResolutionCommand,
  type DurableSubmitAgent,
  type DurableSubmitOptions,
  type Receipt,
} from "@effect-agent/thread";
import { SqliteClient } from "@effect/sql-sqlite-do";
import { runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { Effect, Schema } from "effect";
import * as SqlClientService from "effect/unstable/sql/SqlClient";
import { describe, expect, it } from "vite-plus/test";

import { CloudflareThreadClient } from "../src/index.ts";
import {
  armRuntimeEviction,
  armedEvictionsRemaining,
  decodeThreadId,
  decodeIdempotencyKey,
} from "./fixtures.ts";
import {
  allSettled,
  anyInState,
  assertConvergence,
  drainAlarmsUntil,
  laneRows,
  readCanonical,
  runClient,
  stubFor,
  type TestNamespace,
} from "./harness.ts";

/**
 * The Travel Planner DC slice (plan §6): the SAME cumulative Travel Planner fixtures the DN
 * suites run — assembled with `ThreadObject.layer` inside real SQLite-backed
 * Durable Objects — while eviction (`ctx.abort()`) and alarm redelivery exercise the
 * Cloudflare-specific recovery path. Together with `travel-planner-phase6.test.ts` (the DN
 * half in `@effect-agent/testing`), the golden-evidence rows here close the P6 exit gate
 * "Travel Planner produces equivalent canonical outcomes under DN and DC".
 */

let laneCounter = 0;
const lane = (label: string): string => `cf-tp-${label}-${laneCounter++}`;

const decodeToolCallId = Schema.decodeSync(ToolCallId);
const encodeEnvelope = Schema.encodeSync(CanonicalRecordEnvelope);

const submitAgent = <InputSchema extends Schema.Top & { readonly EncodingServices: never }>(
  agent: DurableSubmitAgent<InputSchema>,
  input: InputSchema["Type"],
  options: DurableSubmitOptions,
  namespace: TestNamespace = "THREADS",
): Promise<Receipt> =>
  runClient(
    Effect.gen(function* () {
      const client = yield* CloudflareThreadClient;
      return yield* client.submit(agent, input, options);
    }),
    namespace,
  );

const submitPlanner = (thread: string, trip: TripRequest = phase1Trip): Promise<Receipt> =>
  submitAgent(
    { definition: TravelPlannerPhase4 },
    trip,
    phase4TravelPlannerSubmitOptions(decodeThreadId(thread), decodeIdempotencyKey(`${thread}-key`)),
  );

const submitBooking = (thread: string): Promise<Receipt> =>
  submitAgent(
    { definition: TravelPlannerPhase5 },
    phase6BookingTrip(thread),
    phase5TravelPlannerSubmitOptions(decodeThreadId(thread), decodeIdempotencyKey(`${thread}-key`)),
  );

const submitCoordinator = (thread: string): Promise<Receipt> =>
  submitAgent(
    { definition: TravelCoordinator },
    phase6ResearchMission,
    s2TravelPlannerSubmitOptions(decodeThreadId(thread), decodeIdempotencyKey(`${thread}-key`)),
  );

const logTags = (records: ReadonlyArray<CanonicalRecordEnvelope>): ReadonlyArray<string> =>
  records.map((envelope) => envelope.record.payload._tag);

/** Scenario-semantic tags: the canonical history minus DUR-013 repair audit records. */
const canonicalTags = (records: ReadonlyArray<CanonicalRecordEnvelope>): ReadonlyArray<string> =>
  logTags(records).filter((tag) => tag !== "RepairAnnotated");

const modelResponseCount = (records: ReadonlyArray<CanonicalRecordEnvelope>): number =>
  records.filter((envelope) => envelope.record.payload._tag === "ModelResponseRecorded").length;

const settledPayloadOf = (records: ReadonlyArray<CanonicalRecordEnvelope>) => {
  const payload = records.find((envelope) => envelope.record.payload._tag === "SubmissionSettled")
    ?.record.payload;
  if (payload?._tag !== "SubmissionSettled") throw new Error("Expected a SubmissionSettled record");
  return payload;
};

/** The DC run's cross-platform normalized canonical evidence (plan §1.8, D-P6-6). */
const normalizedDcEvidence = (
  records: ReadonlyArray<CanonicalRecordEnvelope>,
  receipt: Receipt,
  thread: string,
): Promise<Schema.Json> =>
  Effect.runPromise(
    normalizeCrossPlatformTravelPlannerEvidence(records, receipt, {
      threadId: thread,
      deploymentId: phase6TravelPlannerDeploymentId,
      producerId: `${phase6TravelPlannerProducerPrefix}:${thread}`,
    }),
  );

const supplierCallCount = (idempotencyKey: string): Promise<number> =>
  Effect.runPromise(phase6SupplierDesk.callCount(idempotencyKey));

interface ReservationRow {
  readonly record_json: string;
  readonly finalized_at: string | null;
}

/** Raw settlement reservations straight from the Object's storage (never an entry point). */
const reservationRows = (thread: string): Promise<ReadonlyArray<ReservationRow>> =>
  runInDurableObject(stubFor(thread), (_instance, state) =>
    Effect.runPromise(
      Effect.gen(function* () {
        const sql = yield* SqlClientService.SqlClient;
        return yield* sql<ReservationRow>`
          SELECT record_json, finalized_at
          FROM effect_agent_settlement_reservations
        `;
      }).pipe(Effect.provide(SqliteClient.layer({ storage: state.storage }))),
    ),
  );

const sleep = (millis: number) => new Promise((resolve) => setTimeout(resolve, millis));

/** Abort the CURRENT incarnation between host operations (chaos lever; in-memory state dies). */
const abortIncarnation = (thread: string): Promise<void> =>
  runInDurableObject(stubFor(thread), (_instance, state) => {
    state.abort("travel planner chaos abort");
  }).then(
    () => undefined,
    () => undefined,
  );

/**
 * Alarm-only convergence across the parent AND child lanes of one delegation: fires each
 * lane's persisted alarm in turn (both auto-fire in production; this only accelerates), never
 * a client entry point. The cadence is deliberately GENTLER than the single-lane drain:
 * production serializes one alarm handler per Object, and hammering two Objects with forced
 * deliveries every few milliseconds can starve a live Attempt's lease-renewal cadence —
 * manufacturing an ownership loss no real eviction caused.
 */
const drainLanesUntil = async (
  lanes: ReadonlyArray<string>,
  predicate: () => Promise<boolean>,
  rounds = 600,
): Promise<void> => {
  for (let round = 0; round < rounds; round++) {
    if (await predicate()) return;
    for (const thread of lanes) {
      try {
        await runDurableObjectAlarm(stubFor(thread));
      } catch {
        // An aborted incarnation may reject the delivery; the pre-armed alarm stays committed.
      }
    }
    await sleep(50);
  }
  const states = await Promise.all(
    lanes.map(async (thread) => {
      const rows = await laneRows(thread).catch(() => "unreadable");
      const tags = await readCanonical(thread)
        .then(logTags)
        .catch(() => "unreadable");
      return `${thread}: rows=${JSON.stringify(rows)} tags=${JSON.stringify(tags)}`;
    }),
  );
  throw new Error(`Alarm drain did not converge:\n${states.join("\n")}`);
};

// ---------------------------------------------------------------------------
// Baseline DC claim (DEPLOY-001 label `DC`)
// ---------------------------------------------------------------------------

describe("DC Travel Planner — baseline settlement", () => {
  it("runs one durable DC planning Submission to Settlement with canonical input, per-Turn, and settlement records in the Thread's Durable Object", async () => {
    const thread = lane("settlement");
    const receipt = await submitPlanner(thread);
    await drainAlarmsUntil(thread, allSettled(thread));
    await assertConvergence(thread);

    const records = await readCanonical(thread);
    // Repair audit records (DUR-013) are host evidence, not scenario semantics: on DC even a
    // clean run carries `recovery:ApplyInput`, because every pass reconciles before it claims
    // (plan §1.4) and the ready lane's input is applied through the recovery path.
    expect(canonicalTags(records)).toEqual([
      "ThreadCreated",
      "UserInputRecorded",
      "RunStarted",
      "ModelResponseRecorded",
      "ToolCallSettled",
      "ToolCallSettled",
      "ToolCallSettled",
      "ModelResponseRecorded",
      "RunCompleted",
      "SubmissionSettled",
    ]);
    const settled = settledPayloadOf(records);
    expect(settled.outcome).toBe("completed");
    expect(settled.receiptId).toBe(receipt.receiptId);
    expect(await Effect.runPromise(travelPlanFromDurableSettlement(records))).toEqual(
      expectedTravelPlan,
    );
  }, 30_000);

  it("DN/DC equivalence: the DC run's cross-platform normalized canonical evidence equals the committed golden that the DN suite asserts", async () => {
    const thread = lane("equivalence");
    const receipt = await submitPlanner(thread);
    await drainAlarmsUntil(thread, allSettled(thread));
    await assertConvergence(thread);

    const records = await readCanonical(thread);
    expect(await normalizedDcEvidence(records, receipt, thread)).toEqual(
      phase6TravelPlannerGoldenEvidence,
    );
  }, 30_000);
});

// ---------------------------------------------------------------------------
// Eviction-equivalence (exit gates 1–2 on the Travel Planner itself)
// ---------------------------------------------------------------------------

describe("DC Travel Planner — eviction equivalence", () => {
  it("eviction-equivalence: a DO aborted at terminalize:after-reserve converges by alarm alone to the exact reserved settlement, with no further client request", async () => {
    const thread = lane("terminalize-after-reserve");
    armRuntimeEviction(thread, "terminalize:after-reserve");
    const receipt = await submitPlanner(thread);

    // Alarms alone drive recovery. The ledger settles before recoverSubmission appends
    // its repair audit, so read-only probes must wait for both before taking the snapshot.
    await drainAlarmsUntil(thread, async () => {
      if (!(await allSettled(thread)())) return false;
      const records = await readCanonical(thread);
      return records.some(
        ({ record: { payload } }) =>
          payload._tag === "RepairAnnotated" &&
          payload.reason === "recovery:AppendReservedSettlement",
      );
    });
    expect(armedEvictionsRemaining(thread)).toBe(0);
    await assertConvergence(thread);

    const records = await readCanonical(thread);
    const settled = settledPayloadOf(records);
    expect(settled.outcome).toBe("completed");

    // Recovery appended the EXACT reserved record (durability §12 step 1→2 gap).
    const reservations = await reservationRows(thread);
    expect(reservations).toHaveLength(1);
    expect(reservations[0]?.finalized_at).not.toBeNull();
    const settledEnvelope = records.find(
      (envelope) => envelope.record.payload._tag === "SubmissionSettled",
    );
    if (settledEnvelope === undefined) throw new Error("Expected the settlement envelope");
    expect(JSON.parse(reservations[0]?.record_json ?? "")).toEqual(
      encodeEnvelope(settledEnvelope).record,
    );

    // The recovery left its DUR-013 audit trail …
    const repairs = records.flatMap((envelope) =>
      envelope.record.payload._tag === "RepairAnnotated" ? [envelope.record.payload] : [],
    );
    expect(repairs.map((repair) => repair.reason)).toContain("recovery:AppendReservedSettlement");
    // … and modulo that audit, the evicted run's normalized evidence equals the SAME golden
    // an uninterrupted DN run produces.
    expect(await normalizedDcEvidence(records, receipt, thread)).toEqual(
      phase6TravelPlannerGoldenEvidence,
    );
  }, 30_000);

  it("chaos-abort between every host operation preserves the normalized evidence", async () => {
    const thread = lane("chaos");
    const receipt = await submitPlanner(thread);
    for (let round = 0; round < 200; round++) {
      await abortIncarnation(thread);
      const rows = await laneRows(thread);
      if (rows.length > 0 && rows.every((row) => row.state === "settled")) break;
      try {
        await runDurableObjectAlarm(stubFor(thread));
      } catch {
        // The aborted incarnation may reject the delivery; the alarm stays committed.
      }
      await sleep(10);
    }
    await drainAlarmsUntil(thread, allSettled(thread));
    await assertConvergence(thread);

    // Nothing that mattered ever lived in a Durable Object memory field: the chaosed run's
    // cross-platform normalized evidence equals the committed golden of an uninterrupted run.
    const records = await readCanonical(thread);
    expect(await normalizedDcEvidence(records, receipt, thread)).toEqual(
      phase6TravelPlannerGoldenEvidence,
    );
    expect(await Effect.runPromise(travelPlanFromDurableSettlement(records))).toEqual(
      expectedTravelPlan,
    );
  }, 120_000);
});

// ---------------------------------------------------------------------------
// P5 booking semantics under DC recovery paths
// ---------------------------------------------------------------------------

describe("DC Travel Planner — approval and uncertainty under eviction", () => {
  it("approval-gated booking suspends durably across eviction and resumes from the recorded decision without re-invoking the model", async () => {
    const thread = lane("approval");
    const bookKey = bookFlightIdempotencyKey(phase6BookingToolCallId(thread));
    armRuntimeEviction(thread, "approval:after-suspend");
    const receipt = await submitBooking(thread);

    // The suspension survives the eviction durably; nothing executed at the supplier.
    await drainAlarmsUntil(thread, anyInState(thread, "suspended"));
    expect(armedEvictionsRemaining(thread)).toBe(0);
    expect(await supplierCallCount(bookKey)).toBe(0);

    await runClient(
      Effect.gen(function* () {
        const client = yield* CloudflareThreadClient;
        return yield* client.resolveApproval(
          decodeThreadId(thread),
          ApprovalDecisionCommand.make({
            submissionId: receipt.submissionId,
            toolCallId: decodeToolCallId(phase6BookingToolCallId(thread)),
            decision: "approved",
            resolver: "travel-desk",
            reason: "phase-6 approval row",
          }),
        );
      }),
    );
    await drainAlarmsUntil(thread, allSettled(thread));
    await assertConvergence(thread);

    const records = await readCanonical(thread);
    // Resumed from the canonical declaration and the recorded decision: exactly two model
    // responses ever became canonical (the declaring Turn and the post-batch report Turn).
    expect(modelResponseCount(records)).toBe(2);
    expect(logTags(records)).toContain("ToolApprovalRequested");
    expect(logTags(records)).toContain("ToolApprovalDecided");
    expect(settledPayloadOf(records).outcome).toBe("completed");
    // The approved booking executed exactly once at the supplier, and every settled booking
    // result references supplier truth (never fabricated).
    expect(await supplierCallCount(bookKey)).toBe(1);
    await Effect.runPromise(
      assertSettledBookingsExistAtSupplier(records).pipe(Effect.provide(phase6SupplierDeskLayer)),
    );
  }, 30_000);

  it("unknown supplier outcome under eviction never fabricates a booking result and wakes only through resolveUnknown", async () => {
    const thread = lane("unknown");
    const bookKey = bookFlightIdempotencyKey(phase6BookingToolCallId(thread));
    armRuntimeEviction(thread, "tools:after-prepared-append");
    const receipt = await submitBooking(thread);

    // Approve the booking; the armed eviction then strands the PREPARED call before any
    // outcome commits, and the fail-closed supplier reconciler (no booking at the desk)
    // answers Uncertain — a durable Unknown Outcome.
    await drainAlarmsUntil(thread, anyInState(thread, "suspended"));
    await runClient(
      Effect.gen(function* () {
        const client = yield* CloudflareThreadClient;
        return yield* client.resolveApproval(
          decodeThreadId(thread),
          ApprovalDecisionCommand.make({
            submissionId: receipt.submissionId,
            toolCallId: decodeToolCallId(phase6BookingToolCallId(thread)),
            decision: "approved",
            resolver: "travel-desk",
            reason: "phase-6 unknown row",
          }),
        );
      }),
    );
    await drainAlarmsUntil(thread, anyInState(thread, "unknown"));
    expect(armedEvictionsRemaining(thread)).toBe(0);

    // The unresolved ordinary call is never auto-replayed and never fabricated: further alarm
    // passes grant nothing — no supplier call, no settled booking result, state still unknown.
    for (let round = 0; round < 10; round++) {
      try {
        await runDurableObjectAlarm(stubFor(thread));
      } catch {
        // Ignore rejected deliveries; the alarm stays committed.
      }
      await sleep(10);
    }
    expect(await anyInState(thread, "unknown")()).toBe(true);
    expect(await supplierCallCount(bookKey)).toBe(0);
    const blocked = await readCanonical(thread);
    expect(logTags(blocked)).toContain("ToolCallUnknown");
    expect(logTags(blocked)).not.toContain("SubmissionSettled");

    // Only the authorized resolution path releases the lane: never-happened is supplier
    // truth here (the desk holds no booking), so the call may honestly execute once.
    await runClient(
      Effect.gen(function* () {
        const client = yield* CloudflareThreadClient;
        return yield* client.resolveUnknown(
          decodeThreadId(thread),
          UnknownResolutionCommand.make({
            submissionId: receipt.submissionId,
            toolCallId: decodeToolCallId(phase6BookingToolCallId(thread)),
            author: "travel-desk-operator",
            reason: "phase-6 unknown row: the desk shows no booking under this key",
            resolution: ResolutionNeverHappened.make(),
          }),
        );
      }),
    );
    await drainAlarmsUntil(thread, allSettled(thread));
    await assertConvergence(thread);

    const records = await readCanonical(thread);
    expect(logTags(records)).toContain("ToolCallResolved");
    expect(settledPayloadOf(records).outcome).toBe("completed");
    expect(await supplierCallCount(bookKey)).toBe(1);
    await Effect.runPromise(
      assertSettledBookingsExistAtSupplier(records).pipe(Effect.provide(phase6SupplierDeskLayer)),
    );
  }, 60_000);
});

// ---------------------------------------------------------------------------
// S2 delegation across two Durable Objects
// ---------------------------------------------------------------------------

describe("DC Travel Planner — cross-Object delegation", () => {
  it("coordinator→researcher delegation joins across two Durable Objects after parent and child evictions; the completed child never re-executes", async () => {
    const thread = lane("delegation");
    resetPhase6ResearcherGate();
    armRuntimeEviction(thread, "subagent:after-reserve");
    const receipt = await submitCoordinator(thread);
    // The deterministic child identity (SUB-016): parent Submission and Tool Call pair.
    const childThread = `subagent:${receipt.submissionId}:${durableResearchCallId}`;
    armRuntimeEviction(childThread, "terminalize:after-reserve");

    // Establishment first: the PARENT's Object is evicted mid-reservation and its persisted
    // alarm alone re-establishes the one child (request, admission, lineage, readiness,
    // start link). The researcher gate holds the child's first model response meanwhile —
    // real model latency, made deterministic.
    await drainAlarmsUntil(thread, async () => {
      // A probe can land on the incarnation the armed eviction just aborted and reject with
      // the abort reason (production behavior: the next call reaches a fresh instance);
      // treat it as "not yet" and let the next round retry, as any real caller would.
      const records = await readCanonical(thread).catch(
        () => [] as ReadonlyArray<CanonicalRecordEnvelope>,
      );
      return logTags(records).includes("SubagentStarted");
    });
    expect(armedEvictionsRemaining(thread)).toBe(0);
    releasePhase6ResearcherGate();

    await drainLanesUntil([thread, childThread], allSettled(thread));
    expect(armedEvictionsRemaining(childThread)).toBe(0);
    await assertConvergence(thread);
    await assertConvergence(childThread);

    // The child's external side effect happened exactly once, and the completed child was
    // never re-executed: two model Turns and one Settlement in the child's own Object.
    expect(phase6GuideInvocationCount()).toBe(1);
    const childRecords = await readCanonical(childThread);
    expect(modelResponseCount(childRecords)).toBe(2);
    expect(
      childRecords.filter((envelope) => envelope.record.payload._tag === "SubmissionSettled"),
    ).toHaveLength(1);

    // The parent joined the projected finding and settled on the fixture shortlist.
    const parentRecords = await readCanonical(thread);
    expect(logTags(parentRecords)).toContain("SubagentRequested");
    expect(logTags(parentRecords)).toContain("SubagentStarted");
    expect(logTags(parentRecords)).toContain("SubagentJoined");
    const settled = settledPayloadOf(parentRecords);
    expect(settled.outcome).toBe("completed");
    expect(Schema.decodeUnknownSync(DestinationShortlist)(settled.result)).toEqual(
      durableResearchShortlist(phase6ResearchDestination),
    );
  }, 60_000);
});

// ---------------------------------------------------------------------------
// Resource limits before admission (exit gate; DEPLOY-007)
// ---------------------------------------------------------------------------

describe("DC Travel Planner — admission limits", () => {
  const submitFlipped = (
    input: TripRequest,
    options: DurableSubmitOptions,
    definitions = options.definitions,
  ) =>
    runClient(
      Effect.gen(function* () {
        const client = yield* CloudflareThreadClient;
        return yield* client.submit({ definition: TravelPlannerPhase4 }, input, {
          ...options,
          definitions,
        });
      }).pipe(Effect.flip),
      "LIMITED",
    );

  it("admission refuses over-limit input before any ledger row exists", async () => {
    const thread = lane("limit-input");
    const oversizedTrip = Schema.decodeUnknownSync(TripRequest)({
      request: `An itinerary brief far beyond the quota: ${"x".repeat(2_048)}`,
      origin: "SFO",
      destination: "LHR",
      departOn: "2026-09-14",
      nights: 4,
      travelers: 2,
      budgetCents: 350_000,
      currency: "USD",
    });
    const refusal = await submitFlipped(
      oversizedTrip,
      phase4TravelPlannerSubmitOptions(
        decodeThreadId(thread),
        decodeIdempotencyKey(`${thread}-key`),
      ),
    );
    expect(refusal._tag).toBe("AdmissionLimitExceeded");
    if (refusal._tag === "AdmissionLimitExceeded") {
      expect(refusal.limit).toBe("input-bytes");
    }
    // Refused BEFORE admission: no ledger row exists on the lane.
    expect(await laneRows(thread, "LIMITED")).toHaveLength(0);
  }, 30_000);

  it("admission refuses over-limit queue depth before any third ledger row exists", async () => {
    const thread = lane("limit-queue");
    resetPhase6PlannerGate(thread);
    const gatedOptions = (key: string): DurableSubmitOptions => ({
      threadId: decodeThreadId(thread),
      principal: phase4TravelPlannerPrincipal,
      idempotencyKey: decodeIdempotencyKey(key),
      definitions: phase6GatedPlannerDefinitionDigests,
    });
    // The gated planner holds the lane durably busy, so both admissions stay nonterminal.
    const first = await submitAgent(
      { definition: TravelPlannerPhase4 },
      phase6GatedTrip(thread),
      gatedOptions(`${thread}-k1`),
      "LIMITED",
    );
    const second = await submitAgent(
      { definition: TravelPlannerPhase4 },
      phase6GatedTrip(thread),
      gatedOptions(`${thread}-k2`),
      "LIMITED",
    );
    expect(first.queueSequence).toBe(1);
    expect(second.queueSequence).toBe(2);

    const refusal = await submitFlipped(phase6GatedTrip(thread), {
      ...gatedOptions(`${thread}-k3`),
    });
    expect(refusal._tag).toBe("AdmissionLimitExceeded");
    if (refusal._tag === "AdmissionLimitExceeded") {
      expect(refusal.limit).toBe("queue-depth");
      expect(refusal.maximum).toBe(2);
    }
    // The refused Submission left no third row; the accepted work then settles normally.
    expect(await laneRows(thread, "LIMITED")).toHaveLength(2);
    releasePhase6PlannerGate(thread);
    await drainAlarmsUntil(thread, allSettled(thread, "LIMITED"), {
      namespace: "LIMITED",
    });
    await assertConvergence(thread, { namespace: "LIMITED" });
  }, 60_000);
});
