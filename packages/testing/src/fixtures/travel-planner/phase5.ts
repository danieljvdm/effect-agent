import { Agent, AgentPolicy, type ConversationId } from "@effect-agent/core";
import { DurableStep, DurableStepError, ToolExecutionClass } from "@effect-agent/engine";
import {
  DefinitionDigests,
  DeploymentId,
  Digest,
  PersistedJson,
  Principal,
  ProducerId,
  ReconciliationCompleted,
  ReconciliationSafeToRetry,
  ReconciliationUncertain,
  ToolReconciler,
  ToolReconcilerError,
  type CanonicalRecordEnvelope,
  type DurableSubmitOptions,
  type IdempotencyKey,
} from "@effect-agent/session";
import { Effect, Layer, Option, Schema } from "effect";
import { Tool, Toolkit } from "effect/unstable/ai";

import {
  ActivityCatalog,
  ActivityQuery,
  AirportCode,
  FlightCatalog,
  FlightQuery,
  LodgingCatalog,
  LodgingQuery,
  QuoteId,
  TripRequest,
} from "./definition.ts";
import {
  ActivityCatalogLayer,
  BookingRef,
  CatalogLifecycle,
  FlightCatalogLayer,
  LodgingCatalogLayer,
  SupplierBookingDesk,
  SupplierBookingRecord,
  SupplierUnavailable,
} from "./deterministic-layers.ts";
import { DurableSearchActivities, DurableSearchFlights, DurableSearchLodging } from "./phase4.ts";

/**
 * The Phase 5 profile extends the P4 `DN` claim to consequential supplier mutation: booking
 * Tools enter the prepared/settled uncertainty protocol, unresolved external effects stop at
 * Unknown Outcomes instead of replaying, Durable Steps replay recorded results, and queued
 * traveler input joins the active Run. Exactly-once EXTERNAL execution is still — deliberately —
 * not claimed (DUR-003): the supplier's own idempotency keys are what dedupe repeats.
 */
export class TravelPlannerBookingProfile extends Schema.Class<TravelPlannerBookingProfile>(
  "@effect-agent/testing/travel-planner/TravelPlannerBookingProfile",
)({
  deploymentClass: Schema.Literal("DN"),
  durableAcceptedWork: Schema.Literal(true),
  canonicalSchemaVersion: Schema.Literal(1),
  /** P5: supplier mutations get prepared/settled records, Unknown Outcomes, and reconciliation. */
  supplierBookingUncertaintyProtocol: Schema.Literal(true),
  /** P5: Durable Steps are exactly-once-RECORDED; their side effects stay at-least-once. */
  durableStepsRecorded: Schema.Literal(true),
  /** Never claimed at any phase (DUR-003). */
  exactlyOnceExternalEffects: Schema.Literal(false),
}) {}

export const phase5TravelPlannerProfile = TravelPlannerBookingProfile.make({
  deploymentClass: "DN",
  durableAcceptedWork: true,
  canonicalSchemaVersion: 1,
  supplierBookingUncertaintyProtocol: true,
  durableStepsRecorded: true,
  exactlyOnceExternalEffects: false,
});

export const phase5TravelPlannerDeploymentId = Schema.decodeSync(DeploymentId)(
  "travel-planner-p5-deployment",
);
export const phase5TravelPlannerProducerId = Schema.decodeSync(ProducerId)(
  "travel-planner-p5-producer",
);
export const phase5TravelPlannerPrincipal = Schema.decodeSync(Principal)(
  "travel-planner-p5-principal",
);

const digest = (character: string) => Schema.decodeSync(Digest)(character.repeat(64));

/** Redacted, deterministic definition identities for the current fixture version. */
export const phase5TravelPlannerDefinitionDigests = DefinitionDigests.make({
  agent: digest("1"),
  model: digest("2"),
  tools: digest("3"),
});

/** Durable admission options for one Travel Planner Submission on one trip lane. */
export const phase5TravelPlannerSubmitOptions = (
  conversationId: ConversationId,
  idempotencyKey: IdempotencyKey,
): DurableSubmitOptions => ({
  conversationId,
  principal: phase5TravelPlannerPrincipal,
  idempotencyKey,
  definitions: phase5TravelPlannerDefinitionDigests,
});

export const TravelerRef = Schema.NonEmptyString.pipe(
  Schema.brand("@effect-agent/testing/travel-planner/TravelerRef"),
);
export type TravelerRef = typeof TravelerRef.Type;

/**
 * Class-shaped booking parameters with branded fields: since the P5 engine fix, official history
 * carries Schema-ENCODED Tool-call parameters, so class-typed parameter codecs persist
 * canonically end-to-end (the P4 Struct workaround is gone for new Tools).
 */
export class FlightBookingRequest extends Schema.Class<FlightBookingRequest>(
  "FlightBookingRequest",
)({
  quoteId: QuoteId,
  travelerRef: TravelerRef,
  departOn: Schema.String,
}) {}

export class SupplierBookingConfirmation extends Schema.Class<SupplierBookingConfirmation>(
  "SupplierBookingConfirmation",
)({
  bookingRef: BookingRef,
  status: Schema.Literal("confirmed"),
  detail: Schema.String,
}) {}

export class CancelBookingRequest extends Schema.Class<CancelBookingRequest>(
  "CancelBookingRequest",
)({
  bookingRef: BookingRef,
  travelerRef: TravelerRef,
}) {}

export class CancellationConfirmation extends Schema.Class<CancellationConfirmation>(
  "CancellationConfirmation",
)({
  bookingRef: BookingRef,
  status: Schema.Literal("cancelled"),
}) {}

export class ItineraryBookingRequest extends Schema.Class<ItineraryBookingRequest>(
  "ItineraryBookingRequest",
)({
  quoteId: QuoteId,
  destination: AirportCode,
  nights: Schema.Int.check(Schema.isGreaterThan(0)),
  travelerRef: TravelerRef,
}) {}

export class ItineraryConfirmation extends Schema.Class<ItineraryConfirmation>(
  "ItineraryConfirmation",
)({
  flightBookingRef: BookingRef,
  lodgingBookingRef: BookingRef,
  confirmationCode: Schema.String,
}) {}

/** The P5 Run output: a booked (or explicitly not-booked) trip report. */
export class TravelBookingReport extends Schema.Class<TravelBookingReport>("TravelBookingReport")({
  summary: Schema.String,
  bookingRefs: Schema.Array(BookingRef),
}) {}

/**
 * Supplier idempotency-key derivations. The handler owns key derivation (the `idempotent` and
 * `uncertain` execution classes carry no key), and the reconciler MUST use the same derivations
 * to query external truth — both sides are exported so they cannot drift.
 */
export const bookFlightIdempotencyKey = (toolCallId: string): string => `book-flight:${toolCallId}`;
export const itineraryStepIdempotencyKey = (toolCallId: string, stepName: string): string =>
  `${toolCallId}:${stepName}`;

/**
 * Approval-gated supplier booking, explicitly `uncertain`: a crash after the call may have
 * mutated supplier state without a recorded outcome, so recovery must reconcile or stop at an
 * Unknown Outcome — never replay automatically (DUR-009, ADR-0004).
 */
export const BookFlight = Tool.make("book_flight", {
  parameters: FlightBookingRequest,
  success: SupplierBookingConfirmation,
  failure: SupplierUnavailable,
  failureMode: "error",
  needsApproval: true,
  dependencies: [SupplierBookingDesk],
}).annotate(ToolExecutionClass, "uncertain");

/**
 * Approval-gated cancellation, annotated `idempotent`: the DECLARED external contract is that
 * cancellation is idempotent by `bookingRef` (the supplier desk enforces it), so recovery may
 * re-execute a prepared-but-unsettled cancel without reconciliation proof. Repeats stay
 * observable in the supplier call counters — the annotation never claims exactly-once execution.
 */
export const CancelBooking = Tool.make("cancel_booking", {
  parameters: CancelBookingRequest,
  success: CancellationConfirmation,
  failure: SupplierUnavailable,
  failureMode: "error",
  needsApproval: true,
  dependencies: [SupplierBookingDesk],
}).annotate(ToolExecutionClass, "idempotent");

/**
 * The Durable Tool: declaring `DurableStep` in `dependencies` is what makes it durable
 * (CONTEXT.md). Its handler divides supplier mutation into three named Steps — `reserve-flight`,
 * `reserve-lodging`, `issue-confirmation` — each deriving its supplier idempotency key from
 * `(toolCallId, stepName)`, so re-entry after interruption replays recorded Step results and the
 * supplier dedupes any honestly-repeated call. The Tool itself carries no execution-class
 * annotation: it stays fail-closed `uncertain`, and `TravelSupplierReconcilerLayer` proves
 * re-entry safe from the keyed-Step construction instead.
 */
export const BookItinerary = Tool.make("book_itinerary", {
  parameters: ItineraryBookingRequest,
  success: ItineraryConfirmation,
  failure: Schema.Union([SupplierUnavailable, DurableStepError]),
  failureMode: "error",
  dependencies: [DurableStep, SupplierBookingDesk],
});

export const TravelPlannerPhase5Toolkit = Toolkit.make(
  DurableSearchFlights,
  DurableSearchLodging,
  DurableSearchActivities,
  BookFlight,
  CancelBooking,
  BookItinerary,
);

const requireToolCallId = (
  toolName: string,
  toolCallId: string | undefined,
): Effect.Effect<string, SupplierUnavailable> =>
  toolCallId === undefined
    ? Effect.fail(
        SupplierUnavailable.make({
          message: `${toolName} needs its stable Tool Call ID to derive the supplier idempotency key.`,
        }),
      )
    : Effect.succeed(toolCallId);

export const TravelPlannerPhase5ToolkitLayer = TravelPlannerPhase5Toolkit.toLayer({
  search_flights: (query) =>
    Effect.flatMap(FlightCatalog, (catalog) => catalog.search(FlightQuery.make(query))),
  search_lodging: (query) =>
    Effect.flatMap(LodgingCatalog, (catalog) => catalog.search(LodgingQuery.make(query))),
  search_activities: (query) =>
    Effect.flatMap(ActivityCatalog, (catalog) => catalog.search(ActivityQuery.make(query))),
  book_flight: (request, context) =>
    Effect.gen(function* () {
      const desk = yield* SupplierBookingDesk;
      const toolCallId = yield* requireToolCallId("book_flight", context.toolCallId);
      const record = yield* desk.book({
        operation: "book-flight",
        idempotencyKey: bookFlightIdempotencyKey(toolCallId),
        detail: `flight ${request.quoteId} for ${request.travelerRef} on ${request.departOn}`,
      });
      return SupplierBookingConfirmation.make({
        bookingRef: record.bookingRef,
        status: "confirmed",
        detail: record.detail,
      });
    }),
  cancel_booking: (request) =>
    Effect.gen(function* () {
      const desk = yield* SupplierBookingDesk;
      const record = yield* desk.cancel(request.bookingRef);
      return CancellationConfirmation.make({
        bookingRef: record.bookingRef,
        status: "cancelled",
      });
    }),
  book_itinerary: (request, context) =>
    Effect.gen(function* () {
      const desk = yield* SupplierBookingDesk;
      const step = yield* DurableStep;
      const toolCallId = yield* requireToolCallId("book_itinerary", context.toolCallId);
      const bookStep = (
        stepName: "reserve-flight" | "reserve-lodging" | "issue-confirmation",
        detail: string,
      ) =>
        step.do(
          stepName,
          SupplierBookingRecord,
          desk.book({
            operation: stepName,
            idempotencyKey: itineraryStepIdempotencyKey(toolCallId, stepName),
            detail,
          }),
        );
      const flight = yield* bookStep(
        "reserve-flight",
        `flight ${request.quoteId} for ${request.travelerRef}`,
      );
      const lodging = yield* bookStep(
        "reserve-lodging",
        `lodging ${request.destination} for ${request.nights} nights (${request.travelerRef})`,
      );
      const confirmation = yield* bookStep(
        "issue-confirmation",
        `itinerary confirmation for ${request.travelerRef}`,
      );
      return ItineraryConfirmation.make({
        flightBookingRef: flight.bookingRef,
        lodgingBookingRef: lodging.bookingRef,
        confirmationCode: confirmation.bookingRef,
      });
    }),
});

const decodePersistedJson = Schema.decodeUnknownEffect(PersistedJson);
const encodeConfirmation = Schema.encodeEffect(SupplierBookingConfirmation);

/**
 * The application reconciliation policy (durability §10): a claim about EXTERNAL truth, queried
 * from the supplier desk by the same idempotency-key derivations the handlers use.
 *
 * - `book_flight`: a confirmed booking under `book-flight:{toolCallId}` is recovered supplier
 *   truth — `CompletedWithResult` settles it canonically without executing anything. Absence is
 *   NOT proof the call never started (a real supplier write could be in flight), so the desk
 *   answer stays fail-closed `Uncertain`.
 * - `book_itinerary`: every external mutation inside is a named Step whose supplier idempotency
 *   key derives from `(toolCallId, stepName)`, so re-entry is provably safe by construction —
 *   `SafeToRetry`. Committed Steps replay from their records; repeated calls dedupe at the desk.
 * - `cancel_booking`: declared `idempotent`, so the coordinator re-executes without consulting
 *   this policy; if ever asked, the bookingRef contract makes `SafeToRetry` honest.
 * - anything else: fail-closed `Uncertain` (AGENTS rule 11).
 */
export const TravelSupplierReconcilerLayer: Layer.Layer<
  ToolReconciler,
  never,
  SupplierBookingDesk
> = Layer.effect(
  ToolReconciler,
  Effect.gen(function* () {
    const desk = yield* SupplierBookingDesk;
    return ToolReconciler.of({
      reconcile: (evidence) =>
        Effect.gen(function* () {
          switch (evidence.toolName) {
            case "book_flight": {
              const key = bookFlightIdempotencyKey(evidence.toolCallId);
              const booking = yield* desk.lookup(key);
              if (Option.isSome(booking) && booking.value.status === "confirmed") {
                const confirmation = yield* encodeConfirmation(
                  SupplierBookingConfirmation.make({
                    bookingRef: booking.value.bookingRef,
                    status: "confirmed",
                    detail: booking.value.detail,
                  }),
                ).pipe(Effect.flatMap(decodePersistedJson));
                return ReconciliationCompleted.make({ result: confirmation, isFailure: false });
              }
              return ReconciliationUncertain.make({
                reason: `The supplier desk shows no confirmed booking under ${key}; a write may still be in flight.`,
              });
            }
            case "book_itinerary":
              return ReconciliationSafeToRetry.make();
            case "cancel_booking":
              return ReconciliationSafeToRetry.make();
            default:
              return ReconciliationUncertain.make({
                reason: `No supplier reconciliation exists for ${evidence.toolName}.`,
              });
          }
        }).pipe(
          Effect.mapError((error) =>
            ToolReconcilerError.make({
              toolCallId: evidence.toolCallId,
              message: `Supplier reconciliation failed: ${error.message}`,
            }),
          ),
        ),
    });
  }),
);

/**
 * The cumulative Travel Planner, Phase 5: the durable planner now performs consequential
 * supplier mutation under the full uncertainty protocol — approval-gated uncertain booking,
 * idempotent-by-contract cancellation, and one Durable Tool whose Steps carry supplier
 * idempotency keys.
 */
export const TravelPlannerPhase5 = Agent.define("travel-planner-phase-5", {
  input: TripRequest,
  output: TravelBookingReport,
  instructions: [
    "You are the Effect Agent Travel Planner P5 booking fixture.",
    "Search with the read-only tools, then book with book_flight, book_itinerary, or",
    "cancel_booking exactly as scripted. Every consequential mutation is approval-gated",
    "or Step-structured. Return only a JSON object with summary and bookingRefs.",
  ].join(" "),
  toolkit: TravelPlannerPhase5Toolkit,
  policy: AgentPolicy.make({
    maxTurns: 4,
    maxToolCalls: 6,
    maxDuration: "30 seconds",
    toolConcurrency: 2,
  }),
  description:
    "Durably book one itinerary with prepared/settled supplier records, Unknown Outcomes, named Steps, and joined traveler input.",
  metadata: { deploymentClass: "DN", phase: "P5" },
});

export class TravelPlannerBookingEvidenceError extends Schema.TaggedErrorClass<TravelPlannerBookingEvidenceError>()(
  "TravelPlannerBookingEvidenceError",
  { message: Schema.String },
) {}

const bookingResultRefs = (result: unknown): ReadonlyArray<string> => {
  if (typeof result !== "object" || result === null) return [];
  const refs: Array<string> = [];
  for (const [field, value] of Object.entries(result)) {
    if (
      typeof value === "string" &&
      (field === "bookingRef" ||
        field === "flightBookingRef" ||
        field === "lodgingBookingRef" ||
        field === "confirmationCode")
    ) {
      refs.push(value);
    }
  }
  return refs;
};

const bookingToolNames = new Set(["book_flight", "cancel_booking", "book_itinerary"]);

/**
 * Never-fabricate assertion (ROADMAP P5 exit gate): every successfully settled booking result in
 * canonical history must reference a booking that actually exists in the supplier store. A
 * `ToolCallSettled` whose bookingRef the supplier cannot produce would be a fabricated result —
 * the exact lie the uncertainty protocol exists to prevent.
 */
export const assertSettledBookingsExistAtSupplier = Effect.fn(
  "TravelPlannerPhase5.assertSettledBookingsExistAtSupplier",
)(function* (
  records: ReadonlyArray<CanonicalRecordEnvelope>,
): Effect.fn.Return<void, TravelPlannerBookingEvidenceError, SupplierBookingDesk> {
  const desk = yield* SupplierBookingDesk;
  const bookings = yield* desk.bookings;
  const knownRefs = new Set<string>(bookings.map((booking) => booking.bookingRef));
  for (const envelope of records) {
    const payload = envelope.record.payload;
    if (
      payload._tag !== "ToolCallSettled" ||
      payload.isFailure ||
      !bookingToolNames.has(payload.toolName)
    ) {
      continue;
    }
    for (const ref of bookingResultRefs(payload.result)) {
      if (!knownRefs.has(ref)) {
        return yield* TravelPlannerBookingEvidenceError.make({
          message: `Canonical record ${envelope.record.recordId} settled bookingRef ${ref}, which the supplier store cannot produce — a fabricated result.`,
        });
      }
    }
  }
});

/**
 * Everything a durable P5 worker needs beyond the runtime stack and the supplier desk: the
 * booking toolkit plus the deterministic P1 travel-service Layers. `SupplierBookingDesk` is
 * deliberately NOT provided here — tests own the desk's lifetime so its counters, bookings, and
 * crash windows survive Tool-Layer rebuilds across Attempts.
 */
export const phase5TravelPlannerWorkerLayer = Layer.mergeAll(
  TravelPlannerPhase5ToolkitLayer,
  FlightCatalogLayer,
  LodgingCatalogLayer,
  ActivityCatalogLayer,
).pipe(Layer.provide(CatalogLifecycle.layerNoDeps));
