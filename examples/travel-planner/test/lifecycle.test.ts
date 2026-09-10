import { IdGenerator } from "@effect-agent/core/IdGenerator";
import * as AgentRuntime from "@effect-agent/engine/AgentRuntime";
import { ThreadHistory } from "@effect-agent/engine/ThreadHistory";
import { it } from "@effect/vitest";
import { Cause, Deferred, Effect, Exit, Fiber, Layer, Ref } from "effect";
import { TestClock } from "effect/testing";
import { expect } from "vite-plus/test";

import { TripToolsLive } from "../src/agent.ts";
import { PlannerError, TripSiteStore } from "../src/domain.ts";
// Retain these protocol/legacy-publication regressions against the admitted v5 definition.
import { previousResponsePlanner as planner } from "../src/server/planner.ts";
import { TripRepository } from "../src/server/trips.ts";
import { FixtureBrowserLive } from "./fixtures/browser.ts";
import { FixtureModel } from "./fixtures/models.ts";

it.effect("times out a stalled native tool and runs its finalizer", () =>
  Effect.gen(function* () {
    const entered = yield* Deferred.make<void>();
    const closed = yield* Ref.make(false);

    const unavailable = () =>
      new PlannerError({ code: "unavailable", message: "Unused test operation." });

    const repository = Layer.succeed(TripRepository, {
      list: Effect.acquireUseRelease(
        Deferred.succeed(entered, undefined),
        () => Effect.never,
        () => Ref.set(closed, true),
      ),
      listConversations: Effect.succeed([]),
      rememberConversation: unavailable,
      get: unavailable,
      conversationId: unavailable,
      save: unavailable,
      recordPublication: unavailable,
    });

    const sites = Layer.succeed(TripSiteStore, { publish: unavailable, load: unavailable });

    const fiber = yield* AgentRuntime.run(planner, {
      message: "Lisbon",
      selectedTripId: null,
      publication: null,
    }).pipe(
      Effect.provide([
        FixtureBrowserLive,
        TripToolsLive("test-conversation"),
        FixtureModel,
        IdGenerator.layer,
        ThreadHistory.layerTransient,
        repository,
        sites,
      ]),
      Effect.exit,
      Effect.forkChild,
    );

    yield* Deferred.await(entered);
    yield* TestClock.adjust("121 seconds");
    const exit = yield* Fiber.join(fiber);

    expect(Exit.isFailure(exit)).toBe(true);
    if (Exit.isFailure(exit))
      expect(Cause.findErrorOption(exit.cause)).toMatchObject({
        value: { _tag: "AgentPolicyError", limit: "duration" },
      });
    expect(yield* Ref.get(closed)).toBe(true);
  }),
);
