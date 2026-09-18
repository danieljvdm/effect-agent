import { AutoModel } from "@effect-agent/ai-decision";
import { TypeSafeClient, TypeSafeDecisionModel } from "@effect-agent/ai-typesafe";
import { expect, it } from "@effect/vitest";
import { Effect, Layer, Redacted, Schema, Stream } from "effect";
import { LanguageModel, Model } from "effect/unstable/ai";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";

it.effect("uses Jev Choice directly and retains rounded evidence when restoring a thread", () =>
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
      Layer.provide(TypeSafeClient.layer),
      Layer.provide(Layer.succeed(TypeSafeClient.Config, { apiKey: Redacted.make("test-key") })),
      Layer.provide(
        Layer.succeed(
          HttpClient.HttpClient,
          HttpClient.make((request) =>
            Effect.sync(() => {
              requests++;
              expect(request.url).toBe("https://api.typesafe.ai/v1/systemone");
              if (request.body._tag !== "Uint8Array") throw new Error("Expected a JSON body");

              const body = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown))(
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
                        probabilities: { fast: 0.33, balanced: 0.33, deep: 0.33 },
                        confidence: 0.33,
                      },
                    },
                    usage: { input_tokens: 150, output_tokens: 12 },
                  }),
                  { headers: { "content-type": "application/json" } },
                ),
              );
            }),
          ),
        ),
      ),
    );

    const selected = yield* auto
      .select({
        threadId: "child-thread",
        state: { task: "Investigate a bug", tools: ["read_file", "test"] },
      })
      .pipe(Effect.provide(live));

    const json = yield* Schema.encodeEffect(Schema.fromJsonString(AutoModel.SelectionRecord))(
      selected.record,
    );

    const stored = yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Unknown))(json);
    const restored = yield* auto.restore("child-thread", stored);

    expect(requests).toBe(1);
    expect(restored.model).toBe(model);
    expect(restored.record.profileId).toBe("balanced");
    expect(restored.record.decision).toMatchObject({
      provider: "typesafe",
      model: "jev-resolved",
      answers: { model: { probabilities: { fast: 0.33, balanced: 0.33, deep: 0.33 } } },
      usage: { inputTokens: 150, outputTokens: 12 },
      providerMetadata: { typesafe: { confidence: { model: 0.33 } } },
    });
  }),
);
