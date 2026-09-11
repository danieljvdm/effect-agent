import { it } from "@effect/vitest";
import { Context, Effect, Exit, Layer, Ref, Schema, Stream } from "effect";
import { AiError, LanguageModel, Response, Tool, Toolkit } from "effect/unstable/ai";
import { expect, expectTypeOf } from "vite-plus/test";

import { defaultPlannerSettings } from "../src/domain.ts";
import { PlannerAttempt, ProgressStore, type ProgressWriter } from "../src/server/progress.ts";
import { PublicOutputLive } from "../src/server/public-output.ts";

const outputLayer = (progress: ProgressWriter) =>
  PublicOutputLive.pipe(
    Layer.provide(
      Layer.succeed(PlannerAttempt, {
        billingOwner: Effect.succeed("fixture"),
        settings: Effect.succeed(defaultPlannerSettings),
        progress,
      }),
    ),
  );

it.effect(
  "native output preserves order, isolates private parts, and fences replaced attempts",
  () =>
    Effect.gen(function* () {
      const store = yield* ProgressStore;
      const old = yield* store.begin("work", "old");
      const current = yield* store.begin("work", "current");

      const parts = [
        Response.makePart("text-start", { id: "text" }),
        Response.makePart("text-delta", { id: "text", delta: "Looking at stays" }),
        Response.makePart("reasoning-start", { id: "reason" }),
        Response.makePart("reasoning-delta", { id: "reason", delta: "PRIVATE_REASONING" }),
        Response.makePart("tool-params-start", {
          id: "private",
          name: "save_trip",
          providerExecuted: false,
        }),
        Response.makePart("tool-params-delta", {
          id: "private",
          delta: '{"message":"PRIVATE_TOOL"}',
        }),
        Response.makePart("tool-params-start", {
          id: "answer",
          name: "deliver_response",
          providerExecuted: false,
        }),
        Response.makePart("tool-params-delta", { id: "answer", delta: '{"message":"A quiet ' }),
        Response.makePart("tool-params-delta", {
          id: "answer",
          delta: 'stay","content":{"private":"PRIVATE_CARD"}}',
        }),
      ];

      const model = yield* LanguageModel.make({
        generateText: () => Effect.die("stream only"),
        streamText: () => Stream.fromIterable(parts),
      });

      const received = yield* Stream.runCollect(LanguageModel.streamText({ prompt: "test" })).pipe(
        Effect.provide(outputLayer(current)),
        Effect.provideService(LanguageModel.LanguageModel, model),
      );

      expect(received.map((part) => part.type)).toEqual(parts.map((part) => part.type));
      expect((yield* store.read).text).toBe("A quiet stay");
      expect(JSON.stringify(yield* store.read)).not.toContain("PRIVATE");
      const before = yield* store.read;

      yield* Stream.runDrain(LanguageModel.streamText({ prompt: "stale" })).pipe(
        Effect.provide(outputLayer(old)),
        Effect.provideService(LanguageModel.LanguageModel, model),
      );
      expect(yield* store.read).toEqual(before);
    }).pipe(Effect.provide(ProgressStore.layer)),
);

it.effect(
  "native stream failure preserves the typed error and releases the provider resource",
  () =>
    Effect.gen(function* () {
      const store = yield* ProgressStore;
      const writer = yield* store.begin("work", "attempt");
      const finalized = yield* Ref.make(false);

      const failure = AiError.make({
        module: "fixture",
        method: "stream",
        reason: new AiError.InvalidRequestError({ description: "failed" }),
      });

      const model = yield* LanguageModel.make({
        generateText: () => Effect.fail(failure),
        streamText: () => Stream.fail(failure).pipe(Stream.ensuring(Ref.set(finalized, true))),
      });

      const observed = yield* LanguageModel.LanguageModel.pipe(
        Effect.provide(outputLayer(writer)),
        Effect.provideService(LanguageModel.LanguageModel, model),
      );

      const result = yield* Stream.runDrain(observed.streamText({ prompt: "test" })).pipe(
        Effect.exit,
      );

      expect(result).toEqual(Exit.fail(failure));
      expect(yield* Ref.get(finalized)).toBe(true);

      expectTypeOf<Layer.Services<typeof PublicOutputLive>>().toEqualTypeOf<
        LanguageModel.LanguageModel | PlannerAttempt
      >();
      expectTypeOf<Layer.Error<typeof PublicOutputLive>>().toEqualTypeOf<never>();

      class Dependency extends Context.Service<Dependency, { readonly value: string }>()(
        "voice-test/Dependency",
      ) {}

      const toolkit = Toolkit.make(
        Tool.make("inspect", { parameters: Schema.Struct({}), success: Schema.String }),
      );

      const options = { prompt: "test", toolkit: Effect.flatMap(Dependency, () => toolkit) };
      const original = model.streamText(options);
      const decorated = observed.streamText(options);

      expectTypeOf<Stream.Error<typeof decorated>>().toEqualTypeOf<Stream.Error<typeof original>>();
      expectTypeOf<Stream.Services<typeof decorated>>().toEqualTypeOf<
        Stream.Services<typeof original>
      >();
      expectTypeOf<
        Extract<Stream.Services<typeof decorated>, Dependency>
      >().toEqualTypeOf<Dependency>();
    }).pipe(Effect.provide(ProgressStore.layer)),
);
