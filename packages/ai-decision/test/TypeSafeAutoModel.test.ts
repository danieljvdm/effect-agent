import { AutoModel } from "@effect-agent/ai-decision";
import { TypeSafeClient, TypeSafeDecisionModel } from "@effect/ai-typesafe";
import { expect, it } from "@effect/vitest";
import { Effect, Layer, Redacted, Schema, Stream } from "effect";
import { LanguageModel, Model } from "effect/unstable/ai";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";

it.effect.each([
  {
    probabilities: { fast: 0.333333, balanced: 0.333334, deep: 0.333333 },
    accepted: true,
    usage: { input_tokens: 150, output_tokens: 12 },
  },
  { probabilities: { fast: 0.333333, balanced: 0.333334, deep: 0.333333 }, accepted: true },
  { probabilities: { fast: 0.33, balanced: 0.33, deep: 0.33 }, accepted: false },
  { probabilities: { fast: 0.33, balanced: 0.35, deep: 0.33 }, accepted: false },
])(
  "uses native TypeSafe validation and retains accepted selection evidence %#",
  ({ probabilities, accepted, usage }) =>
    Effect.gen(function* () {
      let requests = 0;

      const model = Model.make(
        "fixture",
        "native",
        Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.die("Selection must not generate text"),
            streamText: () => Stream.die("Selection must not stream text"),
          }),
        ),
      );

      const auto = AutoModel.make({
        version: "profiles-v1",
        models: {
          fast: { model, description: "Fast" },
          balanced: { model, description: "Balanced" },
          deep: { model, description: "Deep" },
        },
      });

      const live = TypeSafeDecisionModel.model("jev-latest").pipe(
        Layer.provide(TypeSafeClient.layer({ apiKey: Redacted.make("test-key") })),
        Layer.provide(
          Layer.succeed(
            HttpClient.HttpClient,
            HttpClient.make((request) =>
              Effect.sync(() => {
                requests++;
                expect(request.url).toBe("https://api.typesafe.ai/v1/systemone");
                if (request.body._tag !== "Uint8Array") throw new Error("Expected a JSON body");

                const body = Schema.decodeSync(Schema.fromJsonString(Schema.Unknown))(
                  new TextDecoder().decode(request.body.body),
                );

                expect(body).toMatchObject({
                  model: "jev-latest",
                  state: { task: "Investigate a bug", tools: ["read_file", "test"] },
                  questions: {
                    model: {
                      type: "choice",
                      criteria: { fast: "Fast", balanced: "Balanced", deep: "Deep" },
                    },
                  },
                });

                return HttpClientResponse.fromWeb(
                  request,
                  new Response(
                    JSON.stringify({
                      model: "jev-resolved",
                      answers: {
                        model: {
                          type: "choice",
                          choice: "balanced",
                          probabilities,
                          confidence: 0.33,
                        },
                      },
                      usage,
                    }),
                    { headers: { "content-type": "application/json" } },
                  ),
                );
              }),
            ),
          ),
        ),
      );

      const selection = auto
        .select({
          threadId: "child-thread",
          state: { task: "Investigate a bug", tools: ["read_file", "test"] },
        })
        .pipe(Effect.provide(live));

      if (!accepted) {
        const error = yield* selection.pipe(Effect.flip);

        expect(error.reason._tag).toBe("InvalidOutputError");
        expect(requests).toBe(1);

        return;
      }
      const selected = yield* selection;

      const json = yield* Schema.encodeEffect(Schema.fromJsonString(AutoModel.SelectionRecord))(
        selected.record,
      );

      const stored = yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Unknown))(json);
      const restored = yield* auto.restore("child-thread", stored);

      expect(requests).toBe(1);
      expect(restored.model).toBe(model);
      expect(restored.record.version).toBe(2);
      expect(restored.record.profileId).toBe("balanced");
      expect(restored.record.decision).toMatchObject({
        answers: {
          model: {
            label: "balanced",
            confidence: 0.33,
            probabilities: { fast: 0.333333, balanced: 0.333334, deep: 0.333333 },
          },
        },
      });
      expect(restored.record.decision.usage).toEqual(
        usage === undefined ? {} : { inputTokens: 150, outputTokens: 12 },
      );
    }),
);
