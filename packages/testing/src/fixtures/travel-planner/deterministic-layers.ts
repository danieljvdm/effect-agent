import { Context, Deferred, Effect, Layer, Ref, Schema } from "effect";
import { ConversationId, IdGenerator, RunId, TurnId } from "@effect-agent/core";
import {
  ActivityCatalog,
  ActivitySearchResult,
  ActivityUnavailable,
  FlightCatalog,
  FlightOption,
  FlightUnavailable,
  LodgingCatalog,
  LodgingOption,
  LodgingUnavailable,
  QuoteId,
  TravelGuidance,
  TravelPlannerToolkit,
  TravelPlannerToolkitLayer,
} from "./definition.ts";

export class CatalogLifecycleCounts extends Schema.Class<CatalogLifecycleCounts>(
  "CatalogLifecycleCounts",
)({
  acquired: Schema.Natural,
  finalized: Schema.Natural,
}) {}
export class CatalogLifecycle extends Context.Service<
  CatalogLifecycle,
  {
    readonly markAcquired: Effect.Effect<void>;
    readonly markFinalized: Effect.Effect<void>;
    readonly counts: Effect.Effect<CatalogLifecycleCounts>;
  }
>()("@effect-agent/testing/travel-planner/CatalogLifecycle") {
  static readonly layerNoDeps = Layer.effect(
    this,
    Effect.gen(function* () {
      const acquired = yield* Ref.make(0);
      const finalized = yield* Ref.make(0);
      return CatalogLifecycle.of({
        markAcquired: Ref.update(acquired, (n) => n + 1),
        markFinalized: Ref.update(finalized, (n) => n + 1),
        counts: Effect.all({ acquired: Ref.get(acquired), finalized: Ref.get(finalized) }).pipe(
          Effect.map((counts) => CatalogLifecycleCounts.make(counts)),
        ),
      });
    }),
  );
}

const flight = FlightOption.make({
  quoteId: Schema.decodeSync(QuoteId)("quote-sfo-lhr-001"),
  flight: "EA 218 · nonstop · SFO 18:40 → LHR 13:05+1",
  estimatedCents: 180_000,
  currency: "USD",
});
const lodging = LodgingOption.make({
  lodging: "Bloomsbury House · refundable studio · 4 nights",
  estimatedCents: 104_000,
  currency: "USD",
});
const activities = ActivitySearchResult.make({
  activities: ["British Museum timed entry", "Thames evening walk"],
});

/**
 * Deterministic controls for a Tool batch whose completions are released in a
 * caller-selected order. This is intentionally a test fixture: it uses no
 * clock or sleep and lets engine scheduler tests prove parallel starts and
 * declaration-order prompt materialization.
 */
export interface TravelPlannerCompletionControls {
  readonly flightStarted: Effect.Effect<void>;
  readonly lodgingStarted: Effect.Effect<void>;
  readonly activityStarted: Effect.Effect<void>;
  readonly releaseFlight: Effect.Effect<void>;
  readonly releaseLodging: Effect.Effect<void>;
  readonly releaseActivity: Effect.Effect<void>;
}

export const ReverseCompletionToolkitLayer = Effect.gen(function* () {
  const flightStarted = yield* Deferred.make<void>();
  const lodgingStarted = yield* Deferred.make<void>();
  const activityStarted = yield* Deferred.make<void>();
  const releaseFlight = yield* Deferred.make<void>();
  const releaseLodging = yield* Deferred.make<void>();
  const releaseActivity = yield* Deferred.make<void>();
  const awaitRelease = <A>(
    started: Deferred.Deferred<void>,
    release: Deferred.Deferred<void>,
    value: A,
  ) =>
    Deferred.succeed(started, undefined).pipe(
      Effect.andThen(Deferred.await(release)),
      Effect.as(value),
    );
  return {
    controls: {
      flightStarted: Deferred.await(flightStarted),
      lodgingStarted: Deferred.await(lodgingStarted),
      activityStarted: Deferred.await(activityStarted),
      releaseFlight: Deferred.succeed(releaseFlight, undefined).pipe(Effect.asVoid),
      releaseLodging: Deferred.succeed(releaseLodging, undefined).pipe(Effect.asVoid),
      releaseActivity: Deferred.succeed(releaseActivity, undefined).pipe(Effect.asVoid),
    },
    layer: TravelPlannerToolkit.toLayer({
      search_flights: () => awaitRelease(flightStarted, releaseFlight, flight),
      search_lodging: () => awaitRelease(lodgingStarted, releaseLodging, lodging),
      search_activities: () => awaitRelease(activityStarted, releaseActivity, activities),
    }),
  };
});

export const FlightCatalogLayer = Layer.effect(
  FlightCatalog,
  Effect.gen(function* () {
    const lifecycle = yield* CatalogLifecycle;
    yield* Effect.acquireRelease(lifecycle.markAcquired, () => lifecycle.markFinalized);
    return FlightCatalog.of({
      search: (query) =>
        query.origin === query.destination
          ? Effect.fail(
              FlightUnavailable.make({
                query: `${query.origin}-${query.destination}`,
                message: "Origin and destination must differ.",
              }),
            )
          : Effect.succeed(flight),
    });
  }),
);
export const LodgingCatalogLayer = Layer.effect(
  LodgingCatalog,
  Effect.gen(function* () {
    const lifecycle = yield* CatalogLifecycle;
    yield* Effect.acquireRelease(lifecycle.markAcquired, () => lifecycle.markFinalized);
    return LodgingCatalog.of({
      search: (query) =>
        query.nights < 1
          ? Effect.fail(
              LodgingUnavailable.make({
                query: query.destination,
                message: "At least one night is required.",
              }),
            )
          : Effect.succeed(lodging),
    });
  }),
);
export const ActivityCatalogLayer = Layer.effect(
  ActivityCatalog,
  Effect.gen(function* () {
    const lifecycle = yield* CatalogLifecycle;
    yield* Effect.acquireRelease(lifecycle.markAcquired, () => lifecycle.markFinalized);
    return ActivityCatalog.of({
      search: (query) =>
        query.destination === ""
          ? Effect.fail(
              ActivityUnavailable.make({
                query: query.destination,
                message: "Destination is required.",
              }),
            )
          : Effect.succeed(activities),
    });
  }),
);
export const TravelGuidanceLayer = Layer.succeed(
  TravelGuidance,
  TravelGuidance.of({
    instructions: (input) =>
      Effect.succeed(
        [
          "You are the Effect Agent Travel Planner P1 interpreter fixture.",
          `The user asked: ${input.request}`,
          "Call search_flights, search_lodging, and search_activities exactly once in one Tool batch.",
          "Return only a JSON object with an itineraries array. Use the Tool results verbatim; activity results may legitimately be an empty array.",
          "This is read-only planning. Require review before any mutation.",
        ].join("\n"),
      ),
  }),
);
export const DeterministicIdGeneratorLayer = Layer.effect(
  IdGenerator,
  Effect.gen(function* () {
    const conversation = yield* Ref.make(0);
    const run = yield* Ref.make(0);
    const turn = yield* Ref.make(0);
    return IdGenerator.of({
      nextConversationId: Ref.updateAndGet(conversation, (n) => n + 1).pipe(
        Effect.map((n) => Schema.decodeSync(ConversationId)(`conversation-${n}`)),
      ),
      nextRunId: Ref.updateAndGet(run, (n) => n + 1).pipe(
        Effect.map((n) => Schema.decodeSync(RunId)(`run-${n}`)),
      ),
      nextTurnId: Ref.updateAndGet(turn, (n) => n + 1).pipe(
        Effect.map((n) => Schema.decodeSync(TurnId)(`turn-${n}`)),
      ),
    });
  }),
);
export const TravelPlannerRuntimeLayer = Layer.mergeAll(
  TravelPlannerToolkitLayer,
  FlightCatalogLayer,
  LodgingCatalogLayer,
  ActivityCatalogLayer,
  TravelGuidanceLayer,
  DeterministicIdGeneratorLayer,
).pipe(Layer.provide(CatalogLifecycle.layerNoDeps));
