import { SubagentReservationsMemoryLive } from "@effect-agent/capabilities";
import { Agent } from "@effect-agent/core";
import {
  DurableWorkerBinding,
  possessionChildAdmissionAuthorizerLayer,
  possessionOperationAuthorizerLayer,
  type ResolvedBinding,
  type ToolReconciler,
} from "@effect-agent/session";
import {
  DestinationGuide,
  DestinationResearcher,
  DestinationResearcherToolkitLayer,
  DeterministicIdGeneratorLayer,
  ResearchDispatchGate,
  SupplierBookingDesk,
  SupplierBookingRecord,
  SupplierUnavailable,
  TravelCoordinator,
  TravelPlannerPhase4,
  TravelPlannerPhase5,
  TravelSupplierReconcilerLayer,
  cancelBookingIdempotencyKey,
  destinationLookup,
  durableDestinationResearchHandlersLayer,
  encodedDestinationReport,
  phase4TravelPlannerDefinitionDigests,
  phase4TravelPlannerWorkerLayer,
  phase5TravelPlannerDefinitionDigests,
  phase5TravelPlannerWorkerLayer,
  phase6BookingRef,
  phase6BookingToolCallId,
  phase6ChildLookupCallId,
  phase6CoordinatorModel,
  phase6PlannerModel,
  phase6ResearchDestination,
  phase6TravelPlannerDeploymentId,
  phase6TravelPlannerProducerPrefix,
  s2CoordinatorDigests,
  s2ResearcherDigests,
  supplierBookingRefFor,
  type BookingRef,
  type SupplierBookRequest,
} from "@effect-agent/testing";
import { Effect, Layer, Option, Stream } from "effect";
import { LanguageModel, Model, type Response as AiResponse } from "effect/unstable/ai";

import { makeConversationObjectClass, type ConversationObjectOptions } from "../src/index.ts";
import { runtimeEvictionFailpoint, storageEvictionFailpoint } from "./fixtures.ts";

/**
 * Explicit workerd-safe Phase 6 fixture state. Worker-root `Ref`/`Deferred` values cannot be
 * consumed inside a Durable Object I/O context, so this root owns only plain external truth;
 * Effects are constructed for execution inside the addressed Object.
 */
interface CloudflarePhase6Harness {
  readonly bindings: Effect.Effect<ReadonlyArray<ResolvedBinding>>;
  readonly supplierDesk: SupplierBookingDesk["Service"];
  readonly supplierDeskLayer: Layer.Layer<SupplierBookingDesk>;
  readonly supplierReconcilerLayer: Layer.Layer<ToolReconciler>;
  readonly releaseResearcherGate: Effect.Effect<void>;
  readonly resetResearcherGate: Effect.Effect<void>;
  readonly guideInvocationCount: Effect.Effect<number>;
}

const usage = { inputTokens: { total: 128 }, outputTokens: { total: 96 } };

const promptAwareModel = (
  name: string,
  decide: (promptJson: string) => Stream.Stream<AiResponse.StreamPartEncoded>,
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

const researcherLookupParts: ReadonlyArray<AiResponse.StreamPartEncoded> = [
  {
    type: "tool-call",
    id: phase6ChildLookupCallId,
    name: "lookup_destination",
    params: { destination: phase6ResearchDestination },
    providerExecuted: false,
  },
  { type: "finish", reason: "tool-calls", usage },
];

const researcherReportParts: ReadonlyArray<AiResponse.StreamPartEncoded> = [
  { type: "text-start", id: "destination-report" },
  {
    type: "text-delta",
    id: "destination-report",
    delta: encodedDestinationReport(phase6ResearchDestination),
  },
  { type: "text-end", id: "destination-report" },
  { type: "finish", reason: "stop", usage },
];

const latestBookingMarker = (promptJson: string): string => {
  const matches = [...promptJson.matchAll(/\[case:([^\]]+)\]/g)];
  return matches.at(-1)?.[1] ?? "unknown-case";
};

/**
 * Workerd queue fixture variant of the Phase 6 booking model. A Conversation can carry more
 * than one booking Submission, so it selects the latest input marker instead of the first marker
 * retained in canonical history. Each queued Submission therefore declares its own approval.
 */
const cloudflareBookingModel = promptAwareModel(
  "travel-planner-phase-5-cloudflare",
  (promptJson) => {
    const marker = latestBookingMarker(promptJson);
    if (!promptJson.includes(phase6BookingToolCallId(marker))) {
      return Stream.fromIterable<AiResponse.StreamPartEncoded>([
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
        { type: "finish", reason: "tool-calls", usage },
      ]);
    }
    return Stream.fromIterable<AiResponse.StreamPartEncoded>([
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
      { type: "finish", reason: "stop", usage },
    ]);
  },
);

const makeCloudflarePhase6Harness = (): CloudflarePhase6Harness => {
  let researcherGateReleased = false;
  let guideInvocations = 0;
  const bookings = new Map<string, SupplierBookingRecord>();
  const counts = new Map<string, number>();

  const supplierDesk = SupplierBookingDesk.of({
    book: (request: SupplierBookRequest) =>
      Effect.sync(() => {
        counts.set(request.idempotencyKey, (counts.get(request.idempotencyKey) ?? 0) + 1);
        const existing = bookings.get(request.idempotencyKey);
        if (existing !== undefined) return existing;
        const record = SupplierBookingRecord.make({
          bookingRef: supplierBookingRefFor(request.idempotencyKey),
          idempotencyKey: request.idempotencyKey,
          operation: request.operation,
          detail: request.detail,
          status: "confirmed",
        });
        bookings.set(request.idempotencyKey, record);
        return record;
      }),
    cancel: (bookingRef: BookingRef) =>
      Effect.suspend(() => {
        const key = cancelBookingIdempotencyKey(bookingRef);
        counts.set(key, (counts.get(key) ?? 0) + 1);
        const entry = [...bookings.entries()].find(
          ([, record]) => record.bookingRef === bookingRef,
        );
        if (entry === undefined) {
          return Effect.fail(
            SupplierUnavailable.make({ message: `No supplier booking exists for ${bookingRef}` }),
          );
        }
        const [storeKey, existing] = entry;
        const cancelled = SupplierBookingRecord.make({ ...existing, status: "cancelled" });
        bookings.set(storeKey, cancelled);
        return Effect.succeed(cancelled);
      }),
    lookup: (idempotencyKey: string) =>
      Effect.sync(() => Option.fromNullishOr(bookings.get(idempotencyKey))),
    bookings: Effect.sync(() => [...bookings.values()]),
    callCount: (idempotencyKey: string) => Effect.sync(() => counts.get(idempotencyKey) ?? 0),
    holdAfterWrite: () =>
      Effect.die(
        new Error(
          "Cloudflare Phase 6 uses DO failpoints, not supplier hold-window crash injection.",
        ),
      ),
  });
  const supplierDeskLayer = Layer.succeed(SupplierBookingDesk, supplierDesk);
  const supplierReconcilerLayer: Layer.Layer<ToolReconciler> = TravelSupplierReconcilerLayer.pipe(
    Layer.provide(supplierDeskLayer),
  );

  const researcherModel = promptAwareModel("destination-researcher-p6-cloudflare", (promptJson) => {
    if (promptJson.includes(phase6ChildLookupCallId)) {
      return Stream.fromIterable(researcherReportParts);
    }
    return researcherGateReleased
      ? Stream.fromIterable(researcherLookupParts)
      : Stream.fromEffectDrain(Effect.never);
  });
  const countingGuideLayer = Layer.succeed(
    DestinationGuide,
    DestinationGuide.of({
      lookup: (query) =>
        Effect.sync(() => {
          guideInvocations += 1;
        }).pipe(Effect.andThen(destinationLookup(query))),
    }),
  );

  const bindings = Effect.gen(function* () {
    const planner = yield* DurableWorkerBinding.make(
      Agent.withModel(TravelPlannerPhase4, phase6PlannerModel),
      phase4TravelPlannerDefinitionDigests,
    ).pipe(Effect.provide(phase4TravelPlannerWorkerLayer));
    const booking = yield* DurableWorkerBinding.make(
      Agent.withModel(TravelPlannerPhase5, cloudflareBookingModel),
      phase5TravelPlannerDefinitionDigests,
    ).pipe(
      Effect.provide(phase5TravelPlannerWorkerLayer.pipe(Layer.provideMerge(supplierDeskLayer))),
    );
    const researcherBinding = Agent.withModel(DestinationResearcher, researcherModel);
    const childToolkitLayer = DestinationResearcherToolkitLayer.pipe(
      Layer.provideMerge(countingGuideLayer),
    );
    const coordinator = yield* DurableWorkerBinding.make(
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
    const researcher = yield* DurableWorkerBinding.make(
      researcherBinding,
      s2ResearcherDigests,
    ).pipe(Effect.provide(childToolkitLayer));
    return [planner, booking, coordinator, researcher];
  });

  return {
    bindings,
    supplierDesk,
    supplierDeskLayer,
    supplierReconcilerLayer,
    releaseResearcherGate: Effect.sync(() => {
      researcherGateReleased = true;
    }),
    resetResearcherGate: Effect.sync(() => {
      researcherGateReleased = false;
      guideInvocations = 0;
    }),
    guideInvocationCount: Effect.sync(() => guideInvocations),
  };
};

/** Explicit worker composition-root fixture; each test resets the state it owns. */
export const travelPlannerHarness = makeCloudflarePhase6Harness();

const authorizationLayer = Layer.merge(
  possessionOperationAuthorizerLayer,
  possessionChildAdmissionAuthorizerLayer,
);

const baseOptions: ConversationObjectOptions = {
  namespaceBinding: "CONVERSATIONS",
  deploymentId: phase6TravelPlannerDeploymentId,
  producerPrefix: phase6TravelPlannerProducerPrefix,
  ownershipLeaseDuration: 1_000,
  leaseRenewalInterval: 100,
  wakeScanInterval: 100,
  settlementPollInterval: 25,
  abortPollInterval: 25,
  alarmBackoffBase: 10,
  alarmBackoffCap: 100,
  observationPollInterval: 10,
  bindings: travelPlannerHarness.bindings,
  toolReconciler: travelPlannerHarness.supplierReconcilerLayer,
  storageFailpoint: storageEvictionFailpoint,
  runtimeFailpoint: runtimeEvictionFailpoint,
};

export class TravelPlannerConversationObject extends makeConversationObjectClass(
  baseOptions,
  authorizationLayer,
) {}

export class TravelPlannerLimitedObject extends makeConversationObjectClass(
  {
    ...baseOptions,
    namespaceBinding: "LIMITED",
    maxQueueDepthPerLane: 2,
    maxInputBytes: 512,
  },
  authorizationLayer,
) {}

export default {
  fetch(): Response {
    return new Response("effect-agent travel planner DC test worker");
  },
};
