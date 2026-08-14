import { Agent, type RunEvent } from "@effect-agent/core";
import { AgentRuntime } from "@effect-agent/engine";
import { describe, expect, it } from "@effect/vitest";
import { Cause, Deferred, Effect, Exit, Fiber, Layer, Option, Ref, Schema, Stream } from "effect";
import { Model } from "effect/unstable/ai";

import {
  ActivityCatalog,
  ActivityCatalogLayer,
  ActivitySearchResult,
  CatalogLifecycle,
  DeterministicIdGeneratorLayer,
  expectedTravelPlan,
  FlightCatalog,
  FlightCatalogLayer,
  FlightUnavailable,
  phase1HappyPathTurns,
  phase1Trip,
  LodgingCatalogLayer,
  ReverseCompletionToolkitLayer,
  ScriptedModel,
  type ScriptedTurnInput,
  TravelGuidanceLayer,
  TravelPlan,
  TravelPlanner,
  TravelPlannerToolkitLayer,
} from "../src/index.ts";

const makeScriptedAgent = (turns: ReadonlyArray<ScriptedTurnInput>) =>
  Agent.withModel(
    TravelPlanner,
    Model.make("scripted", "travel-planner-phase-1", ScriptedModel.layer(turns)),
  );

const TravelRuntimeLayer = Layer.mergeAll(
  TravelPlannerToolkitLayer,
  FlightCatalogLayer,
  LodgingCatalogLayer,
  ActivityCatalogLayer,
  TravelGuidanceLayer,
  DeterministicIdGeneratorLayer,
);

const provideTravelLayers = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(Effect.provide(TravelRuntimeLayer));

const failureFrom = <E>(exit: Exit.Exit<unknown, E>): E => {
  expect(Exit.isFailure(exit)).toBe(true);
  if (Exit.isSuccess(exit)) {
    throw new Error("Expected the Effect to fail");
  }
  const failure = Cause.findErrorOption(exit.cause);
  expect(Option.isSome(failure)).toBe(true);
  if (Option.isNone(failure)) {
    throw new Error("Expected a typed failure in the Cause");
  }
  return failure.value;
};

describe("TEST-014 P1 Travel Planner reference application (E)", () => {
  it.effect("runs the offline two-Turn bounded Tool batch through run and stream", () => {
    let sawToolResultOnSecondTurn = false;
    const assertedTurns: ReadonlyArray<ScriptedTurnInput> = [
      {
        ...phase1HappyPathTurns[0],
        assertRequest: (request) => {
          expect(request.tools.map((tool) => tool.name)).toEqual([
            "search_flights",
            "search_lodging",
            "search_activities",
          ]);
          expect(request.prompt.content.map((message) => message.role)).toEqual(["system", "user"]);
        },
      },
      {
        ...phase1HappyPathTurns[1],
        assertRequest: (request) => {
          const roles = request.prompt.content.map((message) => message.role);
          sawToolResultOnSecondTurn = roles.includes("tool");
          const prompt = JSON.stringify(request.prompt.content);
          expect(prompt).toContain("quote-sfo-lhr-001");
          expect(prompt.indexOf("EA 218")).toBeLessThan(prompt.indexOf("Bloomsbury House"));
          expect(prompt.indexOf("Bloomsbury House")).toBeLessThan(prompt.indexOf("British Museum"));
        },
      },
    ];

    return Effect.gen(function* () {
      const streamEvidence = yield* Effect.gen(function* () {
        const lifecycle = yield* CatalogLifecycle;
        const events = yield* provideTravelLayers(
          AgentRuntime.stream(makeScriptedAgent(assertedTurns), phase1Trip).pipe(Stream.runCollect),
        ).pipe(Effect.scoped);
        const counts = yield* lifecycle.counts;
        return { counts, events: Array.from(events) };
      }).pipe(Effect.provide(CatalogLifecycle.layerNoDeps));

      const runEvidence = yield* Effect.gen(function* () {
        const lifecycle = yield* CatalogLifecycle;
        const result = yield* provideTravelLayers(
          AgentRuntime.run(makeScriptedAgent(phase1HappyPathTurns), phase1Trip),
        ).pipe(Effect.scoped);
        const counts = yield* lifecycle.counts;
        return { counts, result };
      }).pipe(Effect.provide(CatalogLifecycle.layerNoDeps));

      const completed = streamEvidence.events.find((event) => event._tag === "RunCompleted");

      expect(completed?._tag).toBe("RunCompleted");
      if (completed?._tag !== "RunCompleted") {
        throw new Error("RunCompleted was not emitted");
      }

      const candidatePlan: unknown = completed.output;
      const decodedPlan = yield* Schema.decodeUnknownEffect(TravelPlan)(candidatePlan);
      expect(sawToolResultOnSecondTurn).toBe(true);
      expect(decodedPlan).toEqual(expectedTravelPlan);
      expect(runEvidence.result).toMatchObject({
        output: expectedTravelPlan,
        conversationId: completed.conversationId,
        runId: completed.runId,
        turns: completed.turns,
        finishReason: completed.finishReason,
      });
      expect(streamEvidence.events.filter((event) => event._tag === "TurnStarted")).toHaveLength(2);
      expect(
        streamEvidence.events.filter((event) => event._tag === "ToolCallSucceeded"),
      ).toHaveLength(3);
      expect(streamEvidence.counts).toEqual({ acquired: 3, finalized: 3 });
      expect(runEvidence.counts).toEqual({ acquired: 3, finalized: 3 });
    });
  });

  it.effect("finalizes the model stream and catalog resource on interruption", () =>
    Effect.gen(function* () {
      const evidence = yield* Effect.gen(function* () {
        const lifecycle = yield* CatalogLifecycle;
        const started = yield* Deferred.make<void>();
        const modelFinalized = yield* Deferred.make<void>();
        const turns: ReadonlyArray<ScriptedTurnInput> = [
          {
            _tag: "Stream",
            parts: [],
            termination: { _tag: "Hang" },
            onStreamStart: Deferred.succeed(started, undefined),
            onStreamFinalize: Deferred.succeed(modelFinalized, undefined),
          },
        ];

        const fiber = yield* provideTravelLayers(
          AgentRuntime.run(makeScriptedAgent(turns), phase1Trip),
        ).pipe(Effect.scoped, Effect.forkChild);

        yield* Deferred.await(started);
        yield* Fiber.interrupt(fiber);

        return {
          catalog: yield* lifecycle.counts,
          modelFinalized: yield* Deferred.isDone(modelFinalized),
        };
      }).pipe(Effect.provide(CatalogLifecycle.layerNoDeps));

      expect(evidence.modelFinalized).toBe(true);
      expect(evidence.catalog).toEqual({ acquired: 3, finalized: 3 });
    }),
  );

  it.effect("accepts an empty activity result as a successful Tool outcome", () => {
    const encodedPlan = Schema.encodeSync(TravelPlan)(expectedTravelPlan);
    const emptyPlan = Schema.decodeSync(TravelPlan)({
      itineraries: encodedPlan.itineraries.map((itinerary) => ({
        ...itinerary,
        activities: [],
      })),
    });
    const baseFinalTurn = phase1HappyPathTurns[1];
    if (baseFinalTurn._tag !== "Stream") {
      throw new Error("Expected the Phase 1 final fixture Turn to stream");
    }
    const finalTurn: ScriptedTurnInput = {
      ...baseFinalTurn,
      parts: baseFinalTurn.parts.map((part): (typeof baseFinalTurn.parts)[number] =>
        part.type === "text-delta"
          ? {
              ...part,
              delta: JSON.stringify(Schema.encodeSync(TravelPlan)(emptyPlan)),
            }
          : part,
      ),
      assertRequest: (request) => {
        expect(JSON.stringify(request.prompt.content)).toContain('"activities":[]');
      },
    };
    const emptyActivityLayer = Layer.succeed(
      ActivityCatalog,
      ActivityCatalog.of({
        search: () => Effect.succeed(Schema.decodeSync(ActivitySearchResult)({ activities: [] })),
      }),
    );
    const layer = Layer.mergeAll(
      TravelPlannerToolkitLayer,
      FlightCatalogLayer,
      LodgingCatalogLayer,
      emptyActivityLayer,
      TravelGuidanceLayer,
      DeterministicIdGeneratorLayer,
    ).pipe(Layer.provide(CatalogLifecycle.layerNoDeps));

    return AgentRuntime.run(
      makeScriptedAgent([phase1HappyPathTurns[0], finalTurn]),
      phase1Trip,
    ).pipe(
      Effect.provide(layer),
      Effect.scoped,
      Effect.tap((result) => Effect.sync(() => expect(result.output).toEqual(emptyPlan))),
    );
  });

  it.effect("keeps a typed flight failure in E after terminal Tool events", () =>
    Effect.gen(function* () {
      const observed = yield* Ref.make<ReadonlyArray<RunEvent>>([]);
      const unavailable = FlightUnavailable.make({
        query: "SFO-LHR",
        message: "The deterministic supplier is unavailable.",
      });
      const failingFlightLayer = Layer.succeed(
        FlightCatalog,
        FlightCatalog.of({
          search: () => Effect.fail(unavailable),
        }),
      );
      const layer = Layer.mergeAll(
        TravelPlannerToolkitLayer,
        failingFlightLayer,
        LodgingCatalogLayer,
        ActivityCatalogLayer,
        TravelGuidanceLayer,
        DeterministicIdGeneratorLayer,
      ).pipe(Layer.provide(CatalogLifecycle.layerNoDeps));

      const exit = yield* AgentRuntime.stream(
        makeScriptedAgent(phase1HappyPathTurns),
        phase1Trip,
      ).pipe(
        Stream.tap((event) => Ref.update(observed, (events) => [...events, event])),
        Stream.runDrain,
        Effect.provide(layer),
        Effect.scoped,
        Effect.exit,
      );
      const failure = failureFrom(exit);
      const events = yield* Ref.get(observed);

      expect(failure).toEqual(unavailable);
      expect(
        events.some(
          (event) => event._tag === "ToolCallStarted" && event.toolName === "search_flights",
        ),
      ).toBe(true);
      expect(
        events.some(
          (event) =>
            event._tag === "ToolCallFailed" &&
            event.toolName === "search_flights" &&
            event.errorTag === "FlightUnavailable",
        ),
      ).toBe(true);
    }),
  );

  it.effect("keeps reverse Tool completion deterministic for the next model request", () =>
    Effect.gen(function* () {
      const controlled = yield* ReverseCompletionToolkitLayer;
      let nextPrompt = "";
      const turns: ReadonlyArray<ScriptedTurnInput> = [
        phase1HappyPathTurns[0],
        {
          ...phase1HappyPathTurns[1],
          assertRequest: (request) => {
            nextPrompt = JSON.stringify(request.prompt.content);
          },
        },
      ];
      const layer = Layer.mergeAll(
        controlled.layer,
        FlightCatalogLayer,
        LodgingCatalogLayer,
        ActivityCatalogLayer,
        TravelGuidanceLayer,
        DeterministicIdGeneratorLayer,
      ).pipe(Layer.provide(CatalogLifecycle.layerNoDeps));
      const fiber = yield* AgentRuntime.run(makeScriptedAgent(turns), phase1Trip).pipe(
        Effect.provide(layer),
        Effect.scoped,
        Effect.forkChild,
      );

      yield* Effect.all([
        controlled.controls.flightStarted,
        controlled.controls.lodgingStarted,
        controlled.controls.activityStarted,
      ]);
      yield* controlled.controls.releaseActivity;
      yield* controlled.controls.releaseLodging;
      yield* controlled.controls.releaseFlight;
      yield* Fiber.join(fiber);

      expect(nextPrompt.indexOf("EA 218")).toBeLessThan(nextPrompt.indexOf("Bloomsbury House"));
      expect(nextPrompt.indexOf("Bloomsbury House")).toBeLessThan(
        nextPrompt.indexOf("British Museum"),
      );
    }),
  );
});
