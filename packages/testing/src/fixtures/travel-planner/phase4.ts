import { Agent, AgentPolicy, type ThreadId } from "@effect-agent/core";
import { ToolExecutionClass } from "@effect-agent/engine";
import {
  CanonicalRecordEnvelope,
  DefinitionDigests,
  DeploymentId,
  Digest,
  Principal,
  ProducerId,
  type DurableSubmitOptions,
  type IdempotencyKey,
  type Receipt,
} from "@effect-agent/thread";
import { Effect, Layer, Schema } from "effect";
import { Model, Tool, Toolkit } from "effect/unstable/ai";

import { ScriptedModel, type ScriptedTurnInput } from "../../scripted-model.ts";
import {
  ActivityCatalog,
  ActivityQuery,
  ActivitySearchResult,
  ActivityUnavailable,
  FlightCatalog,
  FlightOption,
  FlightQuery,
  FlightUnavailable,
  LodgingCatalog,
  LodgingOption,
  LodgingQuery,
  LodgingUnavailable,
  TravelGuidance,
  TravelPlan,
  TripRequest,
} from "./definition.ts";
import {
  ActivityCatalogLayer,
  CatalogLifecycle,
  FlightCatalogLayer,
  LodgingCatalogLayer,
  TravelGuidanceLayer,
} from "./deterministic-layers.ts";
import { phase1HappyPathTurns } from "./scenarios.ts";

/**
 * The Phase 4 profile claims durable accepted work on the Node/SQLite runtime (deployment class
 * DN): once `submit` returns a Receipt, the Submission settles exactly once even across process
 * loss. The claim is limited to safe-to-repeat toolkits (D6): supplier booking is explicitly NOT
 * claimed safely replayable — replay-safe external mutation is P5 (Durable Tools) scope.
 */
export class TravelPlannerDurabilityProfile extends Schema.Class<TravelPlannerDurabilityProfile>(
  "@effect-agent/testing/travel-planner/TravelPlannerDurabilityProfile",
)({
  deploymentClass: Schema.Literal("DN"),
  durableAcceptedWork: Schema.Literal(true),
  canonicalSchemaVersion: Schema.Literal(1),
  /** Supplier booking replay safety is P5 (Durable Tools) scope; DN does not claim it. */
  supplierBookingReplaySafe: Schema.Literal(false),
}) {}

export const phase4TravelPlannerProfile = TravelPlannerDurabilityProfile.make({
  deploymentClass: "DN",
  durableAcceptedWork: true,
  canonicalSchemaVersion: 1,
  supplierBookingReplaySafe: false,
});

export const phase4TravelPlannerDeploymentId = Schema.decodeSync(DeploymentId)(
  "travel-planner-p4-deployment",
);
export const phase4TravelPlannerProducerId = Schema.decodeSync(ProducerId)(
  "travel-planner-p4-producer",
);
export const phase4TravelPlannerPrincipal = Schema.decodeSync(Principal)(
  "travel-planner-p4-principal",
);

const digest = (character: string) => Schema.decodeSync(Digest)(character.repeat(64));

/** Redacted, deterministic definition identities for the current fixture version. */
export const phase4TravelPlannerDefinitionDigests = DefinitionDigests.make({
  agent: digest("d"),
  model: digest("e"),
  tools: digest("f"),
});

/** Durable admission options for one Travel Planner Submission on one trip lane. */
export const phase4TravelPlannerSubmitOptions = (
  threadId: ThreadId,
  idempotencyKey: IdempotencyKey,
): DurableSubmitOptions => ({
  threadId,
  principal: phase4TravelPlannerPrincipal,
  idempotencyKey,
  definitions: phase4TravelPlannerDefinitionDigests,
});

/**
 * The P4 read-only search Tools, calling the same deterministic catalogs as the P1 toolkit.
 *
 * The plain-Struct parameter shape is a historical remnant of the P4 carry-in workaround: since
 * the P5 engine fix, official history carries Schema-ENCODED Tool-call parameters, so class-typed
 * parameter codecs persist canonically too — the Structs simply need no change here. The
 * `ToolExecutionClass` `readonly` annotation is the deliberate P5 migration (plan §4.3): these
 * Tools perform no external mutation, so a crash between start and settlement is a free re-run
 * and they never enter the prepared/settled uncertainty protocol — keeping the P4 canonical
 * history byte-stable (an unannotated Tool fails closed to `uncertain`).
 */
export const DurableSearchFlights = Tool.make("search_flights", {
  parameters: Schema.Struct(FlightQuery.fields),
  success: FlightOption,
  failure: FlightUnavailable,
  failureMode: "error",
  dependencies: [FlightCatalog],
}).annotate(ToolExecutionClass, "readonly");
export const DurableSearchLodging = Tool.make("search_lodging", {
  parameters: Schema.Struct(LodgingQuery.fields),
  success: LodgingOption,
  failure: LodgingUnavailable,
  failureMode: "error",
  dependencies: [LodgingCatalog],
}).annotate(ToolExecutionClass, "readonly");
export const DurableSearchActivities = Tool.make("search_activities", {
  parameters: Schema.Struct(ActivityQuery.fields),
  success: ActivitySearchResult,
  failure: ActivityUnavailable,
  failureMode: "error",
  dependencies: [ActivityCatalog],
}).annotate(ToolExecutionClass, "readonly");

export const TravelPlannerPhase4Toolkit = Toolkit.make(
  DurableSearchFlights,
  DurableSearchLodging,
  DurableSearchActivities,
);

export const TravelPlannerPhase4ToolkitLayer = TravelPlannerPhase4Toolkit.toLayer({
  search_flights: (query) =>
    Effect.flatMap(FlightCatalog, (catalog) => catalog.search(FlightQuery.make(query))),
  search_lodging: (query) =>
    Effect.flatMap(LodgingCatalog, (catalog) => catalog.search(LodgingQuery.make(query))),
  search_activities: (query) =>
    Effect.flatMap(ActivityCatalog, (catalog) => catalog.search(ActivityQuery.make(query))),
});

/**
 * The cumulative Travel Planner, Phase 4: the P1 planning behavior on the durable Node/SQLite
 * runtime. The searches are read-only and safe to repeat across Attempts (D6); supplier booking
 * is deliberately absent because DN does NOT claim replay-safe external mutation (P5 scope).
 */
export const TravelPlannerPhase4 = Agent.make("travel-planner-phase-4", {
  input: TripRequest,
  output: TravelPlan,
  instructions: (input) =>
    Effect.flatMap(TravelGuidance, (guidance) => guidance.instructions(input)),
  toolkit: TravelPlannerPhase4Toolkit,
  policy: AgentPolicy.make({
    maxTurns: 2,
    maxToolCalls: 3,
    maxDuration: "30 seconds",
    toolConcurrency: 3,
  }),
  description:
    "Durably plan one review-only itinerary from safe-to-repeat deterministic searches; supplier booking is not claimed safely replayable.",
  metadata: { deploymentClass: "DN", phase: "P4" },
});

/**
 * The P4 Agent Binding: the durable Travel Planner definition bound to a finite scripted model.
 * The scripted Layer is rebuilt per Run, so every Run of one Binding replays the same
 * deterministic script.
 */
export const makePhase4TravelPlannerAgent = (
  turns: ReadonlyArray<ScriptedTurnInput> = phase1HappyPathTurns,
) =>
  Agent.withModel(
    TravelPlannerPhase4,
    Model.make("scripted", "travel-planner-phase-4", ScriptedModel.layer(turns)),
  );

/**
 * Everything a durable worker needs beyond the runtime stack, reusing the deterministic P1
 * travel-service Layers. The durable coordinator supplies its own deterministic `IdGenerator`,
 * so this Layer deliberately provides none.
 */
export const phase4TravelPlannerWorkerLayer = Layer.mergeAll(
  TravelPlannerPhase4ToolkitLayer,
  FlightCatalogLayer,
  LodgingCatalogLayer,
  ActivityCatalogLayer,
  TravelGuidanceLayer,
).pipe(Layer.provide(CatalogLifecycle.layerNoDeps));

export class TravelPlannerDurableEvidenceError extends Schema.TaggedError<TravelPlannerDurableEvidenceError>()(
  "TravelPlannerDurableEvidenceError",
  { message: Schema.String },
) {}

/**
 * Decode the completed itinerary from the canonical `SubmissionSettled` record. Canonical history
 * is the outcome authority (DUR-015): the settled result — not any ledger cache — must decode
 * through the trip output schema.
 */
export const travelPlanFromDurableSettlement = Effect.fn(
  "TravelPlannerPhase4.travelPlanFromDurableSettlement",
)(function* (
  records: ReadonlyArray<CanonicalRecordEnvelope>,
): Effect.fn.Return<TravelPlan, TravelPlannerDurableEvidenceError> {
  const settlements = records.flatMap((envelope) =>
    envelope.record.payload._tag === "SubmissionSettled" ? [envelope.record.payload] : [],
  );
  const settled = settlements.at(0);
  if (settled === undefined) {
    return yield* TravelPlannerDurableEvidenceError.make({
      message: "The canonical Thread Log has no SubmissionSettled record.",
    });
  }
  if (settled.outcome !== "completed" || settled.result === undefined) {
    return yield* TravelPlannerDurableEvidenceError.make({
      message: `The Submission settled ${settled.outcome} without a completed itinerary result.`,
    });
  }
  return yield* Schema.decodeUnknownEffect(TravelPlan)(settled.result).pipe(
    Effect.mapError((error) =>
      TravelPlannerDurableEvidenceError.make({
        message: `The settled result does not decode through the TravelPlan schema: ${error.message}`,
      }),
    ),
  );
});

const encodeEvidence = Schema.encodeEffect(Schema.Array(CanonicalRecordEnvelope));
const decodeComparableJson = Schema.decodeUnknownEffect(Schema.Json);

/**
 * Project canonical evidence into a Submission-identity-independent comparable form: batch
 * identity, canonical sequence, and the full encoded record, with the ledger-minted
 * `submissionId`/`receiptId` (and every identity derived from them: run, turn, batch, record,
 * and settlement ids) replaced by stable placeholders. Two Threads whose normalized
 * evidence is equal took byte-equivalent canonical histories, so restart-equivalence can compare
 * a recovered run against an uninterrupted control run on a separate database.
 */
export const normalizeDurableTravelPlannerEvidence = Effect.fn(
  "TravelPlannerPhase4.normalizeDurableTravelPlannerEvidence",
)(function* (
  records: ReadonlyArray<CanonicalRecordEnvelope>,
  receipt: Receipt,
): Effect.fn.Return<Schema.Json, TravelPlannerDurableEvidenceError> {
  const encoded = yield* encodeEvidence(records).pipe(
    Effect.mapError((error) =>
      TravelPlannerDurableEvidenceError.make({
        message: `Canonical evidence failed to encode: ${error.message}`,
      }),
    ),
  );
  const comparable = encoded.map((envelope) => ({
    batchId: envelope.batchId,
    sequence: envelope.sequence,
    record: envelope.record,
  }));
  const substituted: unknown = JSON.parse(
    JSON.stringify(comparable)
      .replaceAll(receipt.submissionId, "{submissionId}")
      .replaceAll(receipt.receiptId, "{receiptId}"),
  );
  return yield* decodeComparableJson(substituted).pipe(
    Effect.mapError((error) =>
      TravelPlannerDurableEvidenceError.make({
        message: `Normalized evidence is not comparable JSON: ${error.message}`,
      }),
    ),
  );
});
