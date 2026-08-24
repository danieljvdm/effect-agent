import { SubagentReservationsMemoryLive } from "@effect-agent/capabilities";
import { Agent } from "@effect-agent/core";
import {
  DefinitionDigests,
  DeploymentId,
  Digest,
  DurableWorkerBinding,
  ProducerId,
  type CanonicalRecordEnvelope,
  type Receipt,
  type ResolvedBinding,
  type ToolReconciler,
} from "@effect-agent/session";
import { Duration, Effect, Layer, Schema, Stream } from "effect";
import { LanguageModel, Model, type Response } from "effect/unstable/ai";

import { TravelPlan, TripRequest } from "./definition.ts";
import {
  DeterministicIdGeneratorLayer,
  SupplierBookingDesk,
  supplierBookingRefFor,
} from "./deterministic-layers.ts";
import {
  TravelPlannerDurableEvidenceError,
  TravelPlannerPhase4,
  normalizeDurableTravelPlannerEvidence,
  phase4TravelPlannerDefinitionDigests,
  phase4TravelPlannerWorkerLayer,
} from "./phase4.ts";
import {
  TravelPlannerPhase5,
  TravelSupplierReconcilerLayer,
  bookFlightIdempotencyKey,
  phase5TravelPlannerDefinitionDigests,
  phase5TravelPlannerWorkerLayer,
} from "./phase5.ts";
import { expectedTravelPlan } from "./scenarios.ts";
import {
  durableDestinationResearchHandlersLayer,
  durableResearchCallId,
  durableResearchShortlist,
  s2CoordinatorDigests,
  s2ResearcherDigests,
} from "./subagents-durable.ts";
import {
  DestinationGuide,
  DestinationResearcher,
  DestinationResearcherToolkitLayer,
  DestinationShortlist,
  ResearchDispatchGate,
  ResearchMission,
  TravelCoordinator,
  destinationLookup,
  encodedDestinationReport,
} from "./subagents.ts";

// ---------------------------------------------------------------------------
// Phase 6 (P6): the SAME cumulative Travel Planner on the Cloudflare Durable
// Object runtime, deployment class DC. This module is deliberately
// platform-neutral (it imports no Cloudflare types): the worker Bindings it
// builds are plain `ResolvedBinding` values a Conversation Object registers,
// and the cross-platform evidence normal form is shared by the DN and DC
// halves of the equivalence suite (plan §1.8, D-P6-6).
// ---------------------------------------------------------------------------

/**
 * The Phase 6 profile: the P4/P5/S2 Travel Planner claims re-earned on the Cloudflare Durable
 * Object runtime (deployment class `DC`), where eviction and alarm redelivery replace process
 * kill and restart as the exercised recovery path. `cloudflareEquivalence` is the claim the S2
 * fixture explicitly deferred to P6 (`TravelPlannerSubagentDurabilityProfile` pins it `false`
 * for `DN`): it flips to `true` here ONLY because the phase-6 suites assert byte-equal
 * cross-platform normalized canonical evidence against one committed golden. Exactly-once
 * EXTERNAL effects remain — deliberately — unclaimed on every platform (DUR-003).
 */
export class TravelPlannerCloudflareProfile extends Schema.Class<TravelPlannerCloudflareProfile>(
  "@effect-agent/testing/travel-planner/TravelPlannerCloudflareProfile",
)({
  deploymentClass: Schema.Literal("DC"),
  durableAcceptedWork: Schema.Literal(true),
  canonicalSchemaVersion: Schema.Literal(1),
  /** P5 semantics under DC recovery: prepared/settled records, Unknown Outcomes, approvals. */
  supplierBookingUncertaintyProtocol: Schema.Literal(true),
  /** S2 semantics under DC recovery: cross-Object establishment/join, completed child never re-runs. */
  durableAttachedSubagents: Schema.Literal(true),
  /** DN and DC produce byte-equal cross-platform normalized canonical evidence (one golden). */
  cloudflareEquivalence: Schema.Literal(true),
  /** Never claimed at any phase on any platform (DUR-003). */
  exactlyOnceExternalEffects: Schema.Literal(false),
}) {}

export const phase6TravelPlannerProfile = TravelPlannerCloudflareProfile.make({
  deploymentClass: "DC",
  durableAcceptedWork: true,
  canonicalSchemaVersion: 1,
  supplierBookingUncertaintyProtocol: true,
  durableAttachedSubagents: true,
  cloudflareEquivalence: true,
  exactlyOnceExternalEffects: false,
});

export const phase6TravelPlannerDeploymentId = Schema.decodeSync(DeploymentId)(
  "travel-planner-p6-deployment",
);
/** Producer prefix of the DC host; each Object mints `{prefix}:{conversationId}`. */
export const phase6TravelPlannerProducerPrefix = "travel-planner-p6-producer";
/** The full producer identity one DC Conversation Object mints for itself. */
export const phase6TravelPlannerProducerId = (conversationId: string): ProducerId =>
  Schema.decodeSync(ProducerId)(`${phase6TravelPlannerProducerPrefix}:${conversationId}`);

const digestOf = (character: string) => Schema.decodeSync(Digest)(character.repeat(64));

/**
 * Registration digests of the GATED planner Binding: the same `TravelPlannerPhase4` definition
 * bound to a model whose first response waits on a test gate, addressable separately so the
 * admission-limits rows can hold a lane busy deterministically without touching the ordinary
 * planner registration.
 */
export const phase6GatedPlannerDefinitionDigests = DefinitionDigests.make({
  agent: digestOf("9"),
  model: digestOf("8"),
  tools: digestOf("7"),
});

// ---------------------------------------------------------------------------
// Cross-platform evidence normal form (plan §1.8, D-P6-6)
// ---------------------------------------------------------------------------

/** The run-specific identities the cross-platform normal form scrubs. */
export interface CrossPlatformEvidenceIdentity {
  /** The Conversation lane the evidence came from. */
  readonly conversationId: string;
  /** The host's deployment identity (`DeploymentId` on every record envelope). */
  readonly deploymentId: string;
  /** The full producer identity of the run (DN: configured; DC: `{prefix}:{conversationId}`). */
  readonly producerId: string;
}

const decodeComparableJson = Schema.decodeUnknownEffect(Schema.Json);

/** The base normal form's element shape, re-decoded so sequences can be renumbered. */
const ComparableEnvelope = Schema.Struct({
  batchId: Schema.String,
  sequence: Schema.Number,
  record: Schema.Json,
});
const decodeComparableEnvelopes = Schema.decodeUnknownEffect(Schema.Array(ComparableEnvelope));

/**
 * The CROSS-PLATFORM extension of `normalizeDurableTravelPlannerEvidence` (D-P6-6): after the
 * base normalization replaces the two ledger-minted identities (which also normalizes the
 * DC-format routable `{uuidv7}:{conversationId}` Submission identities and everything derived
 * from them), this form additionally scrubs everything that legitimately differs between a DN
 * process and a DC Durable Object over the same scenario:
 *
 * - `RepairAnnotated` audit records are dropped BEFORE normalization and the canonical
 *   sequence is renumbered to the surviving order: repairs are DUR-013 evidence of recovery
 *   itself, legally present in a recovered run and legally absent from an uninterrupted
 *   control (on DC even a CLEAN run carries one, because every pass reconciles before it
 *   claims, so the ready lane's input is applied through the recovery path). Canonical ORDER
 *   is the durability §5 claim; sequence contiguity is a platform artifact of who appended;
 * - the Conversation identity (DC lanes mint unique names per test run);
 * - the deployment and producer identities (host configuration, not canonical semantics);
 * - `createdAt` commit timestamps (wall clock);
 * - 64-hex digests (they hash RAW content that legally embeds run-specific identity, so they
 *   can never be byte-equal across runs; chain integrity is asserted separately by the
 *   adapters and the convergence helpers).
 *
 * Two runs whose cross-platform normalized evidence is byte-equal took canonically equivalent
 * histories — the exact sense in which durability §5 permits storage differences while
 * requiring the same observable ordering. Both the DN and DC suites assert equality against
 * the one committed `phase6TravelPlannerGoldenEvidence`, so DN ≡ DC transitively.
 */
export const normalizeCrossPlatformTravelPlannerEvidence = Effect.fn(
  "TravelPlannerPhase6.normalizeCrossPlatformTravelPlannerEvidence",
)(function* (
  records: ReadonlyArray<CanonicalRecordEnvelope>,
  receipt: Receipt,
  identity: CrossPlatformEvidenceIdentity,
): Effect.fn.Return<Schema.Json, TravelPlannerDurableEvidenceError> {
  const canonical = records.filter(
    (envelope) => envelope.record.payload._tag !== "RepairAnnotated",
  );
  const base = yield* normalizeDurableTravelPlannerEvidence(canonical, receipt);
  const scrubbed: unknown = JSON.parse(
    JSON.stringify(base)
      .replaceAll(identity.producerId, "{producerId}")
      .replaceAll(identity.deploymentId, "{deploymentId}")
      .replaceAll(identity.conversationId, "{conversationId}")
      .replaceAll(/\d{4}-\d{2}-\d{2}T[0-9:.]+Z/g, "{timestamp}")
      .replaceAll(/"[0-9a-f]{64}"/g, '"{digest}"'),
  );
  const comparable = yield* decodeComparableEnvelopes(scrubbed).pipe(
    Effect.mapError((error) =>
      TravelPlannerDurableEvidenceError.make({
        message: `Cross-platform normalized evidence lost the comparable shape: ${error.message}`,
      }),
    ),
  );
  const renumbered = comparable.map((entry, index) => ({
    batchId: entry.batchId,
    sequence: index + 1,
    record: entry.record,
  }));
  return yield* decodeComparableJson(renumbered).pipe(
    Effect.mapError((error) =>
      TravelPlannerDurableEvidenceError.make({
        message: `Cross-platform normalized evidence is not comparable JSON: ${error.message}`,
      }),
    ),
  );
});

// ---------------------------------------------------------------------------
// Prompt-aware scripted models. A DC Attempt may resume on a FRESH Object
// incarnation whose Layers (and any in-memory turn counter) were rebuilt, so
// every phase-6 model derives its response purely from the committed history
// in its prompt — the way a real model would — instead of from call order.
// ---------------------------------------------------------------------------

const scriptedUsage = { inputTokens: { total: 128 }, outputTokens: { total: 96 } };

const promptAwareModel = (
  name: string,
  decide: (promptJson: string) => Stream.Stream<Response.StreamPartEncoded>,
) =>
  Model.make(
    "scripted",
    name,
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: (options) =>
          Stream.unwrap(Effect.sync(() => decide(JSON.stringify(options.prompt)))),
      }),
    ),
  );

/** The P1/P4 happy-path Tool Call identities (scenarios.ts, byte-stable since P1). */
export const phase6FlightCallId = "flight-call-1";
export const phase6LodgingCallId = "lodging-call-1";
export const phase6ActivityCallId = "activity-call-1";

/** Turn 1 of the planner: the SAME three search declarations as `phase1HappyPathTurns`. */
const plannerSearchTurnParts: ReadonlyArray<Response.StreamPartEncoded> = [
  {
    type: "tool-call",
    id: phase6FlightCallId,
    name: "search_flights",
    params: { origin: "SFO", destination: "LHR", departOn: "2026-09-14", travelers: 2 },
  },
  {
    type: "tool-call",
    id: phase6LodgingCallId,
    name: "search_lodging",
    params: { destination: "LHR", departOn: "2026-09-14", nights: 4, travelers: 2 },
  },
  {
    type: "tool-call",
    id: phase6ActivityCallId,
    name: "search_activities",
    params: { destination: "LHR", departOn: "2026-09-14", nights: 4, travelers: 2 },
  },
  { type: "finish", reason: "tool-calls", usage: scriptedUsage },
];

/** Turn 2 of the planner: the SAME itinerary text as `phase1HappyPathTurns`. */
const plannerPlanTurnParts: ReadonlyArray<Response.StreamPartEncoded> = [
  { type: "text-start", id: "itinerary-json" },
  {
    type: "text-delta",
    id: "itinerary-json",
    delta: JSON.stringify(Schema.encodeSync(TravelPlan)(expectedTravelPlan)),
  },
  { type: "text-end", id: "itinerary-json" },
  { type: "finish", reason: "stop", usage: scriptedUsage },
];

const plannerDecide = (promptJson: string): Stream.Stream<Response.StreamPartEncoded> =>
  promptJson.includes(phase6FlightCallId)
    ? Stream.fromIterable(plannerPlanTurnParts)
    : Stream.fromIterable(plannerSearchTurnParts);

/**
 * The P4 planner script (`phase1HappyPathTurns`) as a prompt-aware model: once the search
 * batch is committed history, every later request gets the plan — identical parts, so the DC
 * canonical evidence is byte-equivalent to the DN ScriptedModel run after normalization.
 */
export const phase6PlannerModel = promptAwareModel("travel-planner-phase-4", plannerDecide);

// ---------------------------------------------------------------------------
// Deterministic test gate for the admission-limits rows. Module state is
// intentionally NOT durable: it plays the external world's role (a slow
// upstream model), never Conversation state.
// ---------------------------------------------------------------------------

const releasedPlannerGates = new Set<string>();

/** Release the gated planner model for one `[gate:...]` marker. */
export const releasePhase6PlannerGate = (marker: string): void => {
  releasedPlannerGates.add(marker);
};

/** Re-close one gate marker (fresh suites reuse markers safely). */
export const resetPhase6PlannerGate = (marker: string): void => {
  releasedPlannerGates.delete(marker);
};

const awaitPlannerGate = (marker: string): Effect.Effect<void> =>
  Effect.gen(function* () {
    while (!releasedPlannerGates.has(marker)) {
      yield* Effect.sleep(Duration.millis(10));
    }
  });

const gateMarkerFromPrompt = (promptJson: string): string =>
  /\[gate:([^\]]+)\]/.exec(promptJson)?.[1] ?? "unknown-gate";

/** A trip whose request text carries the gate marker the gated model waits on. */
export const phase6GatedTrip = (marker: string): TripRequest =>
  Schema.decodeUnknownSync(TripRequest)({
    request: `Plan a review-only London trip, but wait for the concierge. [gate:${marker}]`,
    origin: "SFO",
    destination: "LHR",
    departOn: "2026-09-14",
    nights: 4,
    travelers: 2,
    budgetCents: 350_000,
    currency: "USD",
  });

/**
 * The SAME planner behavior with a hanging first response: the model waits on the released
 * gate before answering, keeping its lane durably busy so queue-depth admission limits can be
 * exercised deterministically.
 */
export const phase6GatedPlannerModel = Model.make(
  "scripted",
  "travel-planner-phase-4-gated",
  Layer.effect(
    LanguageModel.LanguageModel,
    LanguageModel.make({
      generateText: () => Effect.succeed([]),
      streamText: (options) =>
        Stream.unwrap(
          Effect.sync(() => {
            const promptJson = JSON.stringify(options.prompt);
            return promptJson.includes(phase6FlightCallId)
              ? plannerDecide(promptJson)
              : Stream.fromEffectDrain(awaitPlannerGate(gateMarkerFromPrompt(promptJson))).pipe(
                  Stream.concat(plannerDecide(promptJson)),
                );
          }),
        ),
    }),
  ),
);

// ---------------------------------------------------------------------------
// P5 booking slice: the SAME phase-5 booking agent and supplier desk. The desk
// is a module-level singleton because it IS the external supplier: like a real
// supplier's ledger, its bookings and call counters survive `ctx.abort()` and
// incarnation loss, which is exactly what the never-fabricate and
// executed-once assertions measure.
// ---------------------------------------------------------------------------

const sharedSupplierDesk = Effect.runSync(
  Effect.flatMap(SupplierBookingDesk, Effect.succeed).pipe(
    Effect.provide(SupplierBookingDesk.layer),
  ),
);

/** The shared external supplier desk instance (module-level external truth). */
export const phase6SupplierDesk = sharedSupplierDesk;

/** Layer handing the shared desk to Bindings, reconcilers, and assertions. */
export const phase6SupplierDeskLayer: Layer.Layer<SupplierBookingDesk> = Layer.succeed(
  SupplierBookingDesk,
  sharedSupplierDesk,
);

/**
 * The REAL P5 supplier reconciliation policy over the shared desk, closed to no requirements
 * so a Conversation Object can install it directly: `book_flight` recovers only from supplier
 * truth (absence stays fail-closed `Uncertain` → durable Unknown Outcome), keyed Steps are
 * provably re-enterable.
 */
export const phase6SupplierReconcilerLayer: Layer.Layer<ToolReconciler> =
  TravelSupplierReconcilerLayer.pipe(Layer.provide(phase6SupplierDeskLayer));

const bookingMarkerFromPrompt = (promptJson: string): string =>
  /\[case:([^\]]+)\]/.exec(promptJson)?.[1] ?? "unknown-case";

/** The deterministic booking Tool Call identity for one `[case:...]` marker. */
export const phase6BookingToolCallId = (marker: string): string => `book-${marker}`;

/** The bookingRef the supplier desk mints for one marker's approved booking. */
export const phase6BookingRef = (marker: string): string =>
  supplierBookingRefFor(bookFlightIdempotencyKey(phase6BookingToolCallId(marker)));

/** A trip whose request text carries the per-lane booking case marker. */
export const phase6BookingTrip = (marker: string): TripRequest =>
  Schema.decodeUnknownSync(TripRequest)({
    request: `Book the approved London flight for the traveler. [case:${marker}]`,
    origin: "SFO",
    destination: "LHR",
    departOn: "2026-09-14",
    nights: 4,
    travelers: 2,
    budgetCents: 350_000,
    currency: "USD",
  });

const bookingCallParts = (marker: string): ReadonlyArray<Response.StreamPartEncoded> => [
  {
    type: "tool-call",
    id: phase6BookingToolCallId(marker),
    name: "book_flight",
    params: {
      quoteId: "quote-sfo-lhr-001",
      travelerRef: `traveler-${marker}`,
      departOn: "2026-09-14",
    },
    providerExecuted: false,
  },
  { type: "finish", reason: "tool-calls", usage: scriptedUsage },
];

const bookingReportParts = (marker: string): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "booking-report" },
  {
    type: "text-delta",
    id: "booking-report",
    delta: JSON.stringify({
      summary: "trip booked",
      bookingRefs: [phase6BookingRef(marker)],
    }),
  },
  { type: "text-end", id: "booking-report" },
  { type: "finish", reason: "stop", usage: scriptedUsage },
];

/**
 * The P5 booking script as a prompt-aware model: request 1 declares the approval-gated
 * `book_flight` call (identity derived from the lane's `[case:...]` marker so supplier
 * idempotency keys never collide across lanes); once that call is committed history, the model
 * writes the booking report.
 */
export const phase6BookingModel = promptAwareModel("travel-planner-phase-5", (promptJson) => {
  const marker = bookingMarkerFromPrompt(promptJson);
  return promptJson.includes(phase6BookingToolCallId(marker))
    ? Stream.fromIterable(bookingReportParts(marker))
    : Stream.fromIterable(bookingCallParts(marker));
});

// ---------------------------------------------------------------------------
// S2 delegation slice: the SAME coordinator → destination-researcher pair.
// The guide invocation counter is module state for the same reason as the
// desk: it is the child's external side-effect record, and "the completed
// child never re-executes" is asserted against it across evictions.
// ---------------------------------------------------------------------------

let guideInvocations = 0;

/** Deterministic guide-lookup handler executions across every incarnation. */
export const phase6GuideInvocationCount = (): number => guideInvocations;

const countingGuideLayer = Layer.succeed(
  DestinationGuide,
  DestinationGuide.of({
    lookup: (query) =>
      Effect.suspend(() => {
        guideInvocations += 1;
        return destinationLookup(query);
      }),
  }),
);

/** The one-candidate research mission of the DC delegation slice. */
export const phase6ResearchMission = Schema.decodeUnknownSync(ResearchMission)({
  request: "Shortlist one September culture city for the DC delegation slice.",
  candidates: ["LHR"],
});

export const phase6ResearchDestination = "LHR";

/** The child's scripted guide-lookup Tool Call identity. */
export const phase6ChildLookupCallId = `lookup-${phase6ResearchDestination}`;

const coordinatorDelegationParts: ReadonlyArray<Response.StreamPartEncoded> = [
  {
    type: "tool-call",
    id: durableResearchCallId,
    name: "delegate_destination_research",
    params: { destination: phase6ResearchDestination, focus: "museums" },
    providerExecuted: false,
  },
  { type: "finish", reason: "tool-calls", usage: scriptedUsage },
];

const coordinatorShortlistParts: ReadonlyArray<Response.StreamPartEncoded> = [
  { type: "text-start", id: "shortlist" },
  {
    type: "text-delta",
    id: "shortlist",
    delta: JSON.stringify(
      Schema.encodeSync(DestinationShortlist)(durableResearchShortlist(phase6ResearchDestination)),
    ),
  },
  { type: "text-end", id: "shortlist" },
  { type: "finish", reason: "stop", usage: scriptedUsage },
];

const researcherLookupParts: ReadonlyArray<Response.StreamPartEncoded> = [
  {
    type: "tool-call",
    id: phase6ChildLookupCallId,
    name: "lookup_destination",
    params: { destination: phase6ResearchDestination },
    providerExecuted: false,
  },
  { type: "finish", reason: "tool-calls", usage: scriptedUsage },
];

const researcherReportParts: ReadonlyArray<Response.StreamPartEncoded> = [
  { type: "text-start", id: "destination-report" },
  {
    type: "text-delta",
    id: "destination-report",
    delta: encodedDestinationReport(phase6ResearchDestination),
  },
  { type: "text-end", id: "destination-report" },
  { type: "finish", reason: "stop", usage: scriptedUsage },
];

/** Prompt-aware S2 coordinator: delegation call first, shortlist once it is history. */
export const phase6CoordinatorModel = promptAwareModel("travel-coordinator-p6", (promptJson) =>
  promptJson.includes(durableResearchCallId)
    ? Stream.fromIterable(coordinatorShortlistParts)
    : Stream.fromIterable(coordinatorDelegationParts),
);

let researcherGateReleased = false;

/** Allow the researcher's FIRST model response to proceed (sticky across incarnations). */
export const releasePhase6ResearcherGate = (): void => {
  researcherGateReleased = true;
};

/** Re-close the researcher gate (each delegation scenario starts gated). */
export const resetPhase6ResearcherGate = (): void => {
  researcherGateReleased = false;
};

const awaitResearcherGate: Effect.Effect<void> = Effect.gen(function* () {
  while (!researcherGateReleased) {
    yield* Effect.sleep(Duration.millis(10));
  }
});

/**
 * Prompt-aware S2 researcher: guide lookup first, report once it is history. The FIRST
 * response waits on the researcher gate — a stand-in for real model latency. The child's own
 * Object may legally start its Attempt the moment its routed admission commits, while the
 * parent is still appending the lineage record into the child's log; a child whose first
 * batch commits during that window races the parent's append on one tail. Real models answer
 * in seconds, so establishment always wins that race in production; the gate reproduces that
 * timing deterministically instead of relying on scheduler luck.
 */
export const phase6ResearcherModel = promptAwareModel("destination-researcher-p6", (promptJson) =>
  promptJson.includes(phase6ChildLookupCallId)
    ? Stream.fromIterable(researcherReportParts)
    : Stream.fromEffectDrain(awaitResearcherGate).pipe(
        Stream.concat(Stream.fromIterable(researcherLookupParts)),
      ),
);

// ---------------------------------------------------------------------------
// Worker Binding registrations for the DC Conversation Object
// ---------------------------------------------------------------------------

/**
 * Every phase-6 Travel Planner worker Binding, captured with its requirement Contexts
 * (spec/subagents.md §11): the P4 planner and its gated twin, the P5 booking agent over the
 * shared supplier desk, and the S2 coordinator/researcher pair wired through the durable
 * delegation Layer. A Conversation Object registers these via its `bindings` option; the
 * capture runs once per incarnation, and everything stateful the assertions rely on (desk,
 * guide counter, gates) lives at module level so it survives incarnation loss.
 */
export const makePhase6TravelPlannerBindings: Effect.Effect<ReadonlyArray<ResolvedBinding>> =
  Effect.gen(function* () {
    const planner: ResolvedBinding = yield* DurableWorkerBinding.make(
      Agent.withModel(TravelPlannerPhase4, phase6PlannerModel),
      phase4TravelPlannerDefinitionDigests,
    ).pipe(Effect.provide(phase4TravelPlannerWorkerLayer));

    const gatedPlanner: ResolvedBinding = yield* DurableWorkerBinding.make(
      Agent.withModel(TravelPlannerPhase4, phase6GatedPlannerModel),
      phase6GatedPlannerDefinitionDigests,
    ).pipe(Effect.provide(phase4TravelPlannerWorkerLayer));

    const booking: ResolvedBinding = yield* DurableWorkerBinding.make(
      Agent.withModel(TravelPlannerPhase5, phase6BookingModel),
      phase5TravelPlannerDefinitionDigests,
    ).pipe(
      Effect.provide(
        phase5TravelPlannerWorkerLayer.pipe(Layer.provideMerge(phase6SupplierDeskLayer)),
      ),
    );

    const researcherBinding = Agent.withModel(DestinationResearcher, phase6ResearcherModel);
    const childToolkitLayer = DestinationResearcherToolkitLayer.pipe(
      Layer.provideMerge(countingGuideLayer),
    );

    const coordinator: ResolvedBinding = yield* DurableWorkerBinding.make(
      Agent.withModel(TravelCoordinator, phase6CoordinatorModel),
      s2CoordinatorDigests,
    ).pipe(
      Effect.provide(
        durableDestinationResearchHandlersLayer(researcherBinding).pipe(
          Layer.provide(
            Layer.mergeAll(
              childToolkitLayer,
              SubagentReservationsMemoryLive,
              DeterministicIdGeneratorLayer,
              ResearchDispatchGate.layerOpen,
            ),
          ),
        ),
      ),
    );

    const researcher: ResolvedBinding = yield* DurableWorkerBinding.make(
      researcherBinding,
      s2ResearcherDigests,
    ).pipe(Effect.provide(childToolkitLayer));

    return [planner, gatedPlanner, booking, coordinator, researcher];
  });

// ---------------------------------------------------------------------------
// The committed golden normalized-evidence fixture (D-P6-6)
// ---------------------------------------------------------------------------

/**
 * The committed cross-platform normalized canonical evidence of ONE uninterrupted Travel
 * Planner planning Submission (the P1/P4 happy path: canonical input, the search Turn, three
 * Tool settlements, the plan Turn, one Settlement). `travel-planner-phase6.test.ts` asserts
 * the DN run equals this value and `travel-planner-dc.test.ts` asserts the DC run equals this
 * value, so the two platforms' canonical outcomes are byte-equivalent transitively — the P6
 * exit gate "Travel Planner produces equivalent canonical outcomes under DN and DC".
 *
 * Regenerate ONLY when the Travel Planner scenario itself changes, by printing either suite's
 * normalized value; both suites must then agree on the new golden.
 */
export const phase6TravelPlannerGoldenEvidence: Schema.Json = [
  {
    batchId: "conversation-created:{conversationId}",
    sequence: 1,
    record: {
      recordId: "conversation-created:{conversationId}",
      family: "conversation",
      schemaVersion: 1,
      createdAt: "{timestamp}",
      deploymentId: "{deploymentId}",
      payload: {
        _tag: "ConversationCreated",
        agentId: "travel-planner-phase-4",
        definitions: {
          agent: "{digest}",
          model: "{digest}",
          tools: "{digest}",
        },
      },
    },
  },
  {
    batchId: "submission-input:{submissionId}",
    sequence: 2,
    record: {
      recordId: "input:{submissionId}",
      family: "conversation",
      schemaVersion: 1,
      createdAt: "{timestamp}",
      deploymentId: "{deploymentId}",
      payload: {
        _tag: "UserInputRecorded",
        submissionId: "{submissionId}",
        kind: "user",
        runId: "run:{submissionId}",
        input: {
          request:
            "Plan a review-only London trip using the deterministic flight, lodging, and activity searches.",
          origin: "SFO",
          destination: "LHR",
          departOn: "2026-09-14",
          nights: 4,
          travelers: 2,
          budgetCents: 350000,
          currency: "USD",
        },
      },
    },
  },
  {
    batchId: "turn-response:run:{submissionId}:1",
    sequence: 3,
    record: {
      recordId: "model-response:run:{submissionId}:1",
      family: "conversation",
      schemaVersion: 1,
      createdAt: "{timestamp}",
      deploymentId: "{deploymentId}",
      payload: {
        _tag: "ModelResponseRecorded",
        runId: "run:{submissionId}",
        turnId: "turn:run:{submissionId}:1",
        turn: 1,
        inputTokens: 128,
        outputTokens: 96,
        runScopedPrefixLength: 2,
        modelUsage: [
          {
            provider: "scripted",
            model: "travel-planner-phase-4",
            inputTokens: {
              total: 128,
              uncached: 128,
              cacheRead: 0,
              cacheWrite: 0,
            },
            outputTokens: {
              total: 96,
              text: 96,
              reasoning: 0,
            },
            costMicrousd: 0,
          },
        ],
        messages: {
          content: [
            {
              options: {},
              role: "system",
              content:
                'You are the Effect Agent Travel Planner P1 interpreter fixture.\nThe user asked: Plan a review-only London trip using the deterministic flight, lodging, and activity searches.\nCall search_flights, search_lodging, and search_activities exactly once in one Tool batch.\nThen return only a JSON object of exactly this shape, no prose:\n{"itineraries": [{"title": "<short itinerary name>", "route": "<origin-destination>", "dates": "<date range>", "flight": "<flight description from the Tool result>", "lodging": "<lodging description from the Tool result>", "activities": ["<activity>", "..."], "estimatedTotalCents": <positive integer total in cents>, "currency": "USD", "quoteId": "<quoteId from the flight Tool result>", "assumptions": ["<assumption>", "..."], "unresolvedConstraints": [], "nextAction": "review"}]}\nUse the Tool results verbatim; activity results may legitimately be an empty array.\nThis is read-only planning. Require review before any mutation.',
            },
            {
              options: {},
              role: "user",
              content:
                '{"request":"Plan a review-only London trip using the deterministic flight, lodging, and activity searches.","origin":"SFO","destination":"LHR","departOn":"2026-09-14","nights":4,"travelers":2,"budgetCents":350000,"currency":"USD"}',
            },
            {
              options: {},
              role: "assistant",
              content: [
                {
                  options: {},
                  type: "tool-call",
                  id: "flight-call-1",
                  name: "search_flights",
                  params: {
                    origin: "SFO",
                    destination: "LHR",
                    departOn: "2026-09-14",
                    travelers: 2,
                  },
                  providerExecuted: false,
                },
                {
                  options: {},
                  type: "tool-call",
                  id: "lodging-call-1",
                  name: "search_lodging",
                  params: {
                    destination: "LHR",
                    departOn: "2026-09-14",
                    nights: 4,
                    travelers: 2,
                  },
                  providerExecuted: false,
                },
                {
                  options: {},
                  type: "tool-call",
                  id: "activity-call-1",
                  name: "search_activities",
                  params: {
                    destination: "LHR",
                    departOn: "2026-09-14",
                    nights: 4,
                    travelers: 2,
                  },
                  providerExecuted: false,
                },
              ],
            },
          ],
        },
        messagesDigest: "{digest}",
      },
    },
  },
  {
    batchId: "turn-results:run:{submissionId}:1",
    sequence: 4,
    record: {
      recordId: "tool-settled:run:{submissionId}:1:flight-call-1",
      family: "conversation",
      schemaVersion: 1,
      createdAt: "{timestamp}",
      deploymentId: "{deploymentId}",
      payload: {
        _tag: "ToolCallSettled",
        runId: "run:{submissionId}",
        toolCallId: "flight-call-1",
        toolName: "search_flights",
        result: {
          quoteId: "quote-sfo-lhr-001",
          flight: "EA 218 · nonstop · SFO 18:40 → LHR 13:05+1",
          estimatedCents: 180000,
          currency: "USD",
        },
        isFailure: false,
      },
    },
  },
  {
    batchId: "turn-results:run:{submissionId}:1",
    sequence: 5,
    record: {
      recordId: "tool-settled:run:{submissionId}:1:lodging-call-1",
      family: "conversation",
      schemaVersion: 1,
      createdAt: "{timestamp}",
      deploymentId: "{deploymentId}",
      payload: {
        _tag: "ToolCallSettled",
        runId: "run:{submissionId}",
        toolCallId: "lodging-call-1",
        toolName: "search_lodging",
        result: {
          lodging: "Bloomsbury House · refundable studio · 4 nights",
          estimatedCents: 104000,
          currency: "USD",
        },
        isFailure: false,
      },
    },
  },
  {
    batchId: "turn-results:run:{submissionId}:1",
    sequence: 6,
    record: {
      recordId: "tool-settled:run:{submissionId}:1:activity-call-1",
      family: "conversation",
      schemaVersion: 1,
      createdAt: "{timestamp}",
      deploymentId: "{deploymentId}",
      payload: {
        _tag: "ToolCallSettled",
        runId: "run:{submissionId}",
        toolCallId: "activity-call-1",
        toolName: "search_activities",
        result: {
          activities: ["British Museum timed entry", "Thames evening walk"],
        },
        isFailure: false,
      },
    },
  },
  {
    batchId: "turn:run:{submissionId}:2",
    sequence: 7,
    record: {
      recordId: "model-response:run:{submissionId}:2",
      family: "conversation",
      schemaVersion: 1,
      createdAt: "{timestamp}",
      deploymentId: "{deploymentId}",
      payload: {
        _tag: "ModelResponseRecorded",
        runId: "run:{submissionId}",
        turnId: "turn:run:{submissionId}:2",
        turn: 2,
        inputTokens: 128,
        outputTokens: 96,
        modelUsage: [
          {
            provider: "scripted",
            model: "travel-planner-phase-4",
            inputTokens: {
              total: 128,
              uncached: 128,
              cacheRead: 0,
              cacheWrite: 0,
            },
            outputTokens: {
              total: 96,
              text: 96,
              reasoning: 0,
            },
            costMicrousd: 0,
          },
        ],
        messages: {
          content: [
            {
              options: {},
              role: "assistant",
              content:
                '{"itineraries":[{"title":"Westward light, eastbound overnight","route":"San Francisco → London","dates":"14–19 September 2026","flight":"EA 218 · nonstop · SFO 18:40 → LHR 13:05+1","lodging":"Bloomsbury House · refundable studio · 4 nights","activities":["British Museum timed entry","Thames evening walk"],"estimatedTotalCents":284000,"currency":"USD","quoteId":"quote-sfo-lhr-001","assumptions":["Two travelers sharing one studio","Quote is read-only availability, not a reservation"],"unresolvedConstraints":["Traveler names and accessibility requests are intentionally omitted"],"nextAction":"review"}]}',
            },
          ],
        },
        messagesDigest: "{digest}",
      },
    },
  },
  {
    batchId: "turn:run:{submissionId}:2",
    sequence: 8,
    record: {
      recordId: "run-completed:run:{submissionId}",
      family: "conversation",
      schemaVersion: 1,
      createdAt: "{timestamp}",
      deploymentId: "{deploymentId}",
      payload: {
        _tag: "RunCompleted",
        runId: "run:{submissionId}",
        output: {
          itineraries: [
            {
              title: "Westward light, eastbound overnight",
              route: "San Francisco → London",
              dates: "14–19 September 2026",
              flight: "EA 218 · nonstop · SFO 18:40 → LHR 13:05+1",
              lodging: "Bloomsbury House · refundable studio · 4 nights",
              activities: ["British Museum timed entry", "Thames evening walk"],
              estimatedTotalCents: 284000,
              currency: "USD",
              quoteId: "quote-sfo-lhr-001",
              assumptions: [
                "Two travelers sharing one studio",
                "Quote is read-only availability, not a reservation",
              ],
              unresolvedConstraints: [
                "Traveler names and accessibility requests are intentionally omitted",
              ],
              nextAction: "review",
            },
          ],
        },
      },
    },
  },
  {
    batchId: "submission-settlement:{submissionId}",
    sequence: 9,
    record: {
      recordId: "settlement:{submissionId}",
      family: "conversation",
      schemaVersion: 1,
      createdAt: "{timestamp}",
      deploymentId: "{deploymentId}",
      payload: {
        _tag: "SubmissionSettled",
        submissionId: "{submissionId}",
        settlementId: "settlement:{submissionId}",
        receiptId: "{receiptId}",
        outcome: "completed",
        runId: "run:{submissionId}",
        result: {
          itineraries: [
            {
              title: "Westward light, eastbound overnight",
              route: "San Francisco → London",
              dates: "14–19 September 2026",
              flight: "EA 218 · nonstop · SFO 18:40 → LHR 13:05+1",
              lodging: "Bloomsbury House · refundable studio · 4 nights",
              activities: ["British Museum timed entry", "Thames evening walk"],
              estimatedTotalCents: 284000,
              currency: "USD",
              quoteId: "quote-sfo-lhr-001",
              assumptions: [
                "Two travelers sharing one studio",
                "Quote is read-only availability, not a reservation",
              ],
              unresolvedConstraints: [
                "Traveler names and accessibility requests are intentionally omitted",
              ],
              nextAction: "review",
            },
          ],
        },
        usageSummary: {
          modelCalls: 2,
          inputTokens: {
            total: 256,
            uncached: 256,
            cacheRead: 0,
            cacheWrite: 0,
          },
          outputTokens: {
            total: 192,
            text: 192,
            reasoning: 0,
          },
          costMicrousd: 0,
          byModel: [
            {
              provider: "scripted",
              model: "travel-planner-phase-4",
              modelCalls: 2,
              inputTokens: {
                total: 256,
                uncached: 256,
                cacheRead: 0,
                cacheWrite: 0,
              },
              outputTokens: {
                total: 192,
                text: 192,
                reasoning: 0,
              },
              costMicrousd: 0,
            },
          ],
        },
      },
    },
  },
];
