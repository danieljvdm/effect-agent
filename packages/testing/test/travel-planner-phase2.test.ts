import {
  FollowUpCommand,
  makeRunCommandQueue,
  makeUsageBudget,
  RunCommandQueueConfig,
  SteeringCommand,
  toRunBudgetHook,
  toRunInputHook,
} from "@effect-agent/capabilities";
import {
  Agent,
  AgentApprovalDenied,
  AgentApprovalPending,
  ConversationId,
  RunId,
} from "@effect-agent/core";
import { AgentRuntime } from "@effect-agent/engine";
import { describe, expect, it } from "@effect/vitest";
import { Cause, DateTime, Effect, Fiber, Layer, Option, Ref, Schema } from "effect";
import { Model, Prompt } from "effect/unstable/ai";

import {
  ActivityCatalogLayer,
  CatalogLifecycle,
  DeterministicIdGeneratorLayer,
  expectedTravelPlan,
  FlightCatalogLayer,
  ItineraryHold,
  ItineraryHoldGateway,
  LodgingCatalogLayer,
  phase1HappyPathTurns,
  phase1Trip,
  QuoteId,
  ReverseCompletionToolkitLayer,
  ScriptedModel,
  type ScriptedTurnInput,
  TravelGuidanceLayer,
  TravelPlanner,
  TravelPlannerPhase2,
  TravelPlannerPhase2ToolkitLayer,
  TravelPlannerRuntimeLayer,
} from "../src/index.ts";

const makeTravelAgent = (turns: ReadonlyArray<ScriptedTurnInput>) =>
  Agent.withModel(
    TravelPlanner,
    Model.make("scripted", "travel-planner-phase-2", ScriptedModel.layer(turns)),
  );

const makePhase2Agent = (turns: ReadonlyArray<ScriptedTurnInput>) =>
  Agent.withModel(
    TravelPlannerPhase2,
    Model.make("scripted", "travel-planner-phase-2", ScriptedModel.layer(turns)),
  );

const failureFrom = <E>(exit: import("effect").Exit.Exit<unknown, E>): E => {
  if (exit._tag === "Success") {
    throw new Error("Expected the Travel Planner run to fail");
  }
  const failure = Cause.findErrorOption(exit.cause);
  if (Option.isNone(failure)) {
    throw new Error("Expected a typed failure");
  }
  return failure.value;
};

describe("TEST-014 P2 Travel Planner operational capabilities (E)", () => {
  it.effect("applies a date-change steering command only after the active Tool batch settles", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const controlled = yield* ReverseCompletionToolkitLayer;
        const runId = yield* Schema.decodeEffect(RunId)("run-1");
        const conversationId = yield* Schema.decodeEffect(ConversationId)("conversation-1");
        const queue = yield* makeRunCommandQueue(
          runId,
          RunCommandQueueConfig.make({ capacity: 4 }),
        );
        let secondPrompt = "";
        const turns: ReadonlyArray<ScriptedTurnInput> = [
          phase1HappyPathTurns[0],
          {
            ...phase1HappyPathTurns[1],
            assertRequest: (request) => {
              secondPrompt = JSON.stringify(request.prompt.content);
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
        const fiber = yield* AgentRuntime.run(makeTravelAgent(turns), phase1Trip, {
          input: toRunInputHook(queue),
        }).pipe(Effect.provide(layer), Effect.forkChild);

        yield* Effect.all(
          [
            controlled.controls.flightStarted,
            controlled.controls.lodgingStarted,
            controlled.controls.activityStarted,
          ],
          { concurrency: "unbounded" },
        );
        yield* queue.offer(
          SteeringCommand.make({
            id: "date-change-1",
            runId,
            conversationId,
            author: "traveler",
            content: "Change the departure date to 2026-09-21.",
            createdAt: DateTime.makeUnsafe(0),
          }),
        );
        yield* Effect.all(
          [
            controlled.controls.releaseActivity,
            controlled.controls.releaseLodging,
            controlled.controls.releaseFlight,
          ],
          { concurrency: "unbounded" },
        );

        const result = yield* Fiber.join(fiber);
        expect(result.output).toEqual(expectedTravelPlan);
        expect(secondPrompt).toContain("2026-09-21");
        expect(secondPrompt).toContain("quote-sfo-lhr-001");
        // The steering message may appear only after every committed result of
        // the settled Tool batch: applied at the Turn seam, not injected
        // mid-batch.
        expect(secondPrompt.indexOf("quote-sfo-lhr-001")).toBeLessThan(
          secondPrompt.indexOf("2026-09-21"),
        );
        expect(secondPrompt.indexOf("Bloomsbury House")).toBeLessThan(
          secondPrompt.indexOf("2026-09-21"),
        );
        expect(secondPrompt.indexOf("British Museum")).toBeLessThan(
          secondPrompt.indexOf("2026-09-21"),
        );
      }),
    ),
  );

  it.effect("keeps a missing-preference follow-up out of the first model call", () =>
    Effect.scoped(
      Effect.gen(function* () {
        const runId = yield* Schema.decodeEffect(RunId)("run-1");
        const conversationId = yield* Schema.decodeEffect(ConversationId)("conversation-1");
        const queue = yield* makeRunCommandQueue(
          runId,
          RunCommandQueueConfig.make({ capacity: 2 }),
        );
        yield* queue.offer(
          FollowUpCommand.make({
            id: "preference-1",
            runId,
            conversationId,
            author: "traveler",
            content: "Prefer a quiet room away from the lift.",
            createdAt: DateTime.makeUnsafe(0),
          }),
        );
        const prompts: Array<string> = [];
        const stopTurn = phase1HappyPathTurns[1];
        const turns: ReadonlyArray<ScriptedTurnInput> = [
          {
            ...stopTurn,
            assertRequest: (request) => {
              prompts.push(JSON.stringify(request.prompt.content));
            },
          },
          {
            ...stopTurn,
            assertRequest: (request) => {
              prompts.push(JSON.stringify(request.prompt.content));
            },
          },
        ];

        const result = yield* AgentRuntime.run(makeTravelAgent(turns), phase1Trip, {
          input: toRunInputHook(queue),
        }).pipe(Effect.provide(TravelPlannerRuntimeLayer));

        expect(result.output).toEqual(expectedTravelPlan);
        expect(prompts).toHaveLength(2);
        expect(prompts[0]).not.toContain("quiet room");
        expect(prompts[1]).toContain("quiet room");
      }),
    ),
  );

  it.effect("rejects the search batch when the hierarchical usage budget is exhausted", () =>
    Effect.gen(function* () {
      const budget = yield* makeUsageBudget({
        maxInputTokens: 1,
      });
      const exit = yield* AgentRuntime.run(makeTravelAgent(phase1HappyPathTurns), phase1Trip, {
        budget: toRunBudgetHook(budget),
      }).pipe(Effect.provide(TravelPlannerRuntimeLayer), Effect.exit);

      expect(failureFrom(exit)).toMatchObject({
        _tag: "BudgetExceeded",
        limit: "input-tokens",
      });
    }),
  );

  it.effect("compacts only the model view while retaining growing official Travel history", () =>
    Effect.gen(function* () {
      const sourceSizes = yield* Ref.make<ReadonlyArray<number>>([]);
      const receivedPrompts: Array<string> = [];
      const turns: ReadonlyArray<ScriptedTurnInput> = phase1HappyPathTurns.map((turn) => ({
        ...turn,
        assertRequest: (request) => {
          // The compacted request keeps the model-visible output contract
          // (RUN-028) ahead of the compacted view, and the derived run-status
          // message (RUN-024) trails it.
          expect(request.prompt.content.map((message) => message.role)).toEqual([
            "system",
            "user",
            "user",
          ]);
          receivedPrompts.push(JSON.stringify(request.prompt.content));
        },
      }));
      yield* AgentRuntime.run(makeTravelAgent(turns), phase1Trip, {
        context: {
          prepare: ({ source }) =>
            Ref.update(sourceSizes, (sizes) => [...sizes, source.content.length]).pipe(
              Effect.as({ prompt: Prompt.make("Use the compacted Travel Planner context.") }),
            ),
        },
      }).pipe(Effect.provide(TravelPlannerRuntimeLayer));

      const sizes = yield* Ref.get(sourceSizes);
      expect(sizes).toHaveLength(2);
      expect(sizes[1]).toBeGreaterThan(sizes[0] ?? 0);
      // The model must receive exactly the prepared compacted prompt, never
      // the growing official history it replaces.
      expect(receivedPrompts).toHaveLength(2);
      for (const prompt of receivedPrompts) {
        expect(prompt).toContain("Use the compacted Travel Planner context.");
        expect(prompt).not.toContain(phase1Trip.request);
        expect(prompt).not.toContain("quote-sfo-lhr-001");
      }
    }),
  );

  it.effect("never starts a denied or unresolved itinerary-hold handler", () =>
    Effect.gen(function* () {
      const handlerStarts = yield* Ref.make(0);
      const holdLayer = Layer.succeed(
        ItineraryHoldGateway,
        ItineraryHoldGateway.of({
          hold: (request) =>
            Ref.update(handlerStarts, (count) => count + 1).pipe(
              Effect.as(
                ItineraryHold.make({
                  holdId: "hold-1",
                  quoteId: request.quoteId,
                  status: "held",
                }),
              ),
            ),
        }),
      );
      const runtimeLayer = Layer.mergeAll(
        TravelPlannerPhase2ToolkitLayer,
        FlightCatalogLayer,
        LodgingCatalogLayer,
        ActivityCatalogLayer,
        holdLayer,
        TravelGuidanceLayer,
        DeterministicIdGeneratorLayer,
      ).pipe(Layer.provide(CatalogLifecycle.layerNoDeps));
      const holdTurn: ScriptedTurnInput = {
        _tag: "Stream",
        parts: [
          {
            type: "tool-call",
            id: "hold-call-1",
            name: "hold_itinerary",
            params: {
              quoteId: yield* Schema.decodeEffect(QuoteId)("quote-sfo-lhr-001"),
              expiresInMinutes: 15,
            },
          },
          {
            type: "finish",
            reason: "tool-calls",
            usage: { inputTokens: { total: 32 }, outputTokens: { total: 16 } },
          },
        ],
        termination: { _tag: "Complete" },
      };

      for (const decision of [
        { _tag: "denied" as const, reason: "traveler declined" },
        { _tag: "unresolved" as const, reason: "traveler did not respond" },
      ]) {
        const exit = yield* AgentRuntime.run(makePhase2Agent([holdTurn]), phase1Trip, {
          approval: { request: () => Effect.succeed(decision) },
        }).pipe(Effect.provide(runtimeLayer), Effect.exit);
        const failure = failureFrom(exit);
        expect(
          failure instanceof AgentApprovalDenied || failure instanceof AgentApprovalPending,
        ).toBe(true);
      }

      expect(yield* Ref.get(handlerStarts)).toBe(0);
    }),
  );
});
