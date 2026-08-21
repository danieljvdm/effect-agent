import { describe, expect, it } from "@effect/vitest";
import { Cause, Deferred, Effect, Exit, Fiber, Option, Schema, Stream } from "effect";
import { LanguageModel } from "effect/unstable/ai";

import { ScriptedModel, ScriptedTurn, type ScriptedTurnInput } from "../src/index.ts";

const usage = {
  inputTokens: {},
  outputTokens: {},
};

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

describe("TEST-002 scripted Effect AI LanguageModel", () => {
  {
    let asserted = false;
    const turns: ReadonlyArray<ScriptedTurnInput> = [
      {
        _tag: "Generate",
        parts: [
          { type: "text", text: "hello" },
          { type: "finish", reason: "stop", usage },
        ],
        assertRequest: (request) => {
          asserted = true;
          expect(request.prompt.content).toHaveLength(1);
          expect(request.tools).toEqual([]);
          expect(request.responseFormat).toEqual({ type: "text" });
          expect(request.previousResponseId).toBeUndefined();
          expect(request.incrementalPrompt).toBeUndefined();
        },
      },
    ];

    it.layer(ScriptedModel.layer(turns))((it) => {
      it.effect("consumes a finite generate queue and captures normalized requests", () =>
        Effect.gen(function* () {
          const response = yield* LanguageModel.generateText({
            prompt: "hello",
            disableToolCallResolution: true,
          });
          const scripted = yield* ScriptedModel;
          const requests = yield* scripted.requests;
          yield* scripted.assertExhausted;

          expect(asserted).toBe(true);
          expect(response.text).toBe("hello");
          expect(requests).toHaveLength(1);
          expect(requests[0]?.kind).toBe("generate");
        }),
      );
    });
  }

  it.layer(ScriptedModel.layer([]))((it) => {
    it.effect("fails deterministically when the script is exhausted", () =>
      Effect.gen(function* () {
        const exit = yield* LanguageModel.generateText({
          prompt: "unexpected",
          disableToolCallResolution: true,
        }).pipe(Effect.exit);
        const error = failureFrom(exit);

        expect(error._tag).toBe("AiError");
        expect(error.message).toContain("Script exhausted");
      }),
    );
  });

  {
    const turns: ReadonlyArray<ScriptedTurnInput> = [
      {
        _tag: "Stream",
        parts: [
          { type: "text-start", id: "answer" },
          { type: "text-delta", id: "answer", delta: "partial" },
        ],
        termination: {
          _tag: "Fail",
          description: "provider disconnected",
        },
      },
    ];

    it.layer(ScriptedModel.layer(turns))((it) => {
      it.effect("emits parts before a scripted stream failure", () =>
        Effect.gen(function* () {
          const seen: Array<string> = [];
          const exit = yield* LanguageModel.streamText({
            prompt: "stream",
            disableToolCallResolution: true,
          }).pipe(
            Stream.tap((part) =>
              Effect.sync(() => {
                seen.push(part.type);
              }),
            ),
            Stream.runDrain,
            Effect.exit,
          );
          const error = failureFrom(exit);

          expect(seen).toEqual(["text-start", "text-delta"]);
          expect(error._tag).toBe("AiError");
          expect(error.message).toContain("provider disconnected");
        }),
      );
    });
  }

  it.effect("runs a hanging stream finalizer when interrupted", () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const finalized = yield* Deferred.make<void>();
      const turns: ReadonlyArray<ScriptedTurnInput> = [
        {
          _tag: "Stream",
          parts: [],
          termination: { _tag: "Hang" },
          onStreamStart: Deferred.succeed(started, undefined),
          onStreamFinalize: Deferred.succeed(finalized, undefined),
        },
      ];

      const fiber = yield* LanguageModel.streamText({
        prompt: "wait",
        disableToolCallResolution: true,
      }).pipe(Stream.runDrain, Effect.provide(ScriptedModel.layer(turns)), Effect.forkChild);
      yield* Deferred.await(started);
      yield* Fiber.interrupt(fiber);

      expect(yield* Deferred.isDone(finalized)).toBe(true);
    }),
  );

  it("validates the serializable turn grammar", () => {
    const unknownPart: unknown = {
      _tag: "Stream",
      parts: [{ type: "not-an-effect-ai-part" }],
      termination: { _tag: "Complete" },
    };
    const incompletePart: unknown = {
      _tag: "Stream",
      parts: [{ type: "text-delta" }],
      termination: { _tag: "Complete" },
    };

    expect(() => Schema.decodeUnknownSync(ScriptedTurn)(unknownPart)).toThrow("Expected");
    expect(() => Schema.decodeUnknownSync(ScriptedTurn)(incompletePart)).toThrow("Missing key");
  });
});
