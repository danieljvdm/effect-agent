import { Context, Effect, Layer, Ref, Schema } from "effect";
import { ConversationId, IdGenerator, RunId, TurnId } from "@effect-agent/core";

import {
  AvailabilityCatalog,
  AvailabilityUnavailable,
  type AvailabilityOption,
  QuoteId,
  TravelGuidance,
  TravelPlannerToolkitLayer,
} from "./definition.ts";

export const CatalogLifecycleCounts = Schema.Struct({
  acquired: Schema.Natural,
  finalized: Schema.Natural,
});
export type CatalogLifecycleCounts = typeof CatalogLifecycleCounts.Type;

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
        markAcquired: Ref.update(acquired, (count) => count + 1),
        markFinalized: Ref.update(finalized, (count) => count + 1),
        counts: Effect.all({
          acquired: Ref.get(acquired),
          finalized: Ref.get(finalized),
        }),
      });
    }),
  );
}

const deterministicOption: AvailabilityOption = {
  quoteId: Schema.decodeSync(QuoteId)("quote-sfo-lhr-001"),
  flight: "EA 218 · nonstop · SFO 18:40 → LHR 13:05+1",
  lodging: "Bloomsbury House · refundable studio · 4 nights",
  estimatedTotalCents: 284_000,
  currency: "USD",
};

export const AvailabilityCatalogLayer = Layer.effect(
  AvailabilityCatalog,
  Effect.gen(function* () {
    const lifecycle = yield* CatalogLifecycle;
    yield* Effect.acquireRelease(lifecycle.markAcquired, () => lifecycle.markFinalized);

    return AvailabilityCatalog.of({
      search: Effect.fn("AvailabilityCatalog.search")(function* (query) {
        if (query.origin === query.destination) {
          return yield* new AvailabilityUnavailable({
            route: `${query.origin}-${query.destination}`,
            message: "Origin and destination must differ.",
          });
        }
        return deterministicOption;
      }),
    });
  }),
);

export const TravelGuidanceLayer = Layer.succeed(
  TravelGuidance,
  TravelGuidance.of({
    instructions: (input) =>
      Effect.succeed(
        [
          "You are the Effect Agent Travel Planner design proof.",
          `The user asked: ${input.request}`,
          "Call search_availability exactly once, then return only JSON matching the requested output schema.",
          `Keep the ${input.origin} → ${input.destination} trip under $${(input.budgetCents / 100).toFixed(0)} ${input.currency}.`,
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
      nextConversationId: Ref.updateAndGet(conversation, (value) => value + 1).pipe(
        Effect.map((value) => Schema.decodeSync(ConversationId)(`conversation-${value}`)),
      ),
      nextRunId: Ref.updateAndGet(run, (value) => value + 1).pipe(
        Effect.map((value) => Schema.decodeSync(RunId)(`run-${value}`)),
      ),
      nextTurnId: Ref.updateAndGet(turn, (value) => value + 1).pipe(
        Effect.map((value) => Schema.decodeSync(TurnId)(`turn-${value}`)),
      ),
    });
  }),
);

/** Browser-safe services for running the deterministic Travel Planner fixture. */
export const TravelPlannerRuntimeLayer = Layer.mergeAll(
  TravelPlannerToolkitLayer,
  AvailabilityCatalogLayer,
  TravelGuidanceLayer,
  DeterministicIdGeneratorLayer,
).pipe(Layer.provide(CatalogLifecycle.layerNoDeps));
