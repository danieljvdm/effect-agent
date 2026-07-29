import { describe, expect, it } from "@effect/vitest";

import { Deferred, Effect, Fiber, Layer, Schema, Stream } from "effect";
import { Model } from "effect/unstable/ai";

import { Agent } from "@effect-agent/core";
import { AgentRuntime } from "@effect-agent/engine";
import {
  AvailabilityCatalogLayer,
  CatalogLifecycle,
  DeterministicIdGeneratorLayer,
  expectedTravelPlan,
  phase0HappyPathTurns,
  phase0Trip,
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
    Model.make("scripted", "travel-planner-phase-0", ScriptedModel.layer(turns)),
  );

const TravelRuntimeLayer = Layer.mergeAll(
  TravelPlannerToolkitLayer,
  AvailabilityCatalogLayer,
  TravelGuidanceLayer,
  DeterministicIdGeneratorLayer,
);

const provideTravelLayers = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
  effect.pipe(Effect.provide(TravelRuntimeLayer));

describe("TEST-014 Phase 0 Travel Planner reference application", () => {
  it.effect("runs the offline two-Turn availability scenario through run and stream", () => {
    let sawToolResultOnSecondTurn = false;
    const assertedTurns: ReadonlyArray<ScriptedTurnInput> = [
      {
        ...phase0HappyPathTurns[0]!,
        assertRequest: (request) => {
          expect(request.tools.map((tool) => tool.name)).toEqual(["search_availability"]);
          expect(request.prompt.content.map((message) => message.role)).toEqual(["system", "user"]);
        },
      },
      {
        ...phase0HappyPathTurns[1]!,
        assertRequest: (request) => {
          const roles = request.prompt.content.map((message) => message.role);
          sawToolResultOnSecondTurn = roles.includes("tool");
          expect(JSON.stringify(request.prompt.content)).toContain("quote-sfo-lhr-001");
        },
      },
    ];

    return Effect.gen(function* () {
      const streamEvidence = yield* Effect.gen(function* () {
        const lifecycle = yield* CatalogLifecycle;
        const events = yield* provideTravelLayers(
          AgentRuntime.stream(makeScriptedAgent(assertedTurns), phase0Trip).pipe(Stream.runCollect),
        ).pipe(Effect.scoped);
        const counts = yield* lifecycle.counts;
        return { counts, events: Array.from(events) };
      }).pipe(Effect.provide(CatalogLifecycle.layerNoDeps));

      const runEvidence = yield* Effect.gen(function* () {
        const lifecycle = yield* CatalogLifecycle;
        const result = yield* provideTravelLayers(
          AgentRuntime.run(makeScriptedAgent(phase0HappyPathTurns), phase0Trip),
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
      ).toHaveLength(1);
      expect(streamEvidence.counts).toEqual({ acquired: 1, finalized: 1 });
      expect(runEvidence.counts).toEqual({ acquired: 1, finalized: 1 });
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
          AgentRuntime.run(makeScriptedAgent(turns), phase0Trip),
        ).pipe(Effect.scoped, Effect.forkChild);

        yield* Deferred.await(started);
        yield* Fiber.interrupt(fiber);

        return {
          catalog: yield* lifecycle.counts,
          modelFinalized: yield* Deferred.isDone(modelFinalized),
        };
      }).pipe(Effect.provide(CatalogLifecycle.layerNoDeps));

      expect(evidence.modelFinalized).toBe(true);
      expect(evidence.catalog).toEqual({ acquired: 1, finalized: 1 });
    }),
  );
});
