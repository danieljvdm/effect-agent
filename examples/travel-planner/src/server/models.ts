import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { Config, Effect, Layer, Result, Schema, Stream, type Redacted } from "effect";
import { AiError, LanguageModel, Model } from "effect/unstable/ai";
import { FetchHttpClient } from "effect/unstable/http";

import { PlannerError, type PlannerSettings } from "../domain.ts";
import { PlannerAttempt, type ProgressWriter } from "./progress.ts";
import { responseTextPreview } from "./response-stream.ts";

const PublicProviderEvent = Schema.Union([
  Schema.Struct({ type: Schema.Literal("response.output_text.delta"), delta: Schema.String }),
  Schema.Struct({
    type: Schema.Literals(["response.output_item.added", "response.output_item.done"]),
    item: Schema.Union([
      Schema.Struct({
        type: Schema.Literal("web_search_call"),
        id: Schema.String,
        status: Schema.String,
      }),
      Schema.Struct({
        type: Schema.Literal("function_call"),
        id: Schema.String,
        name: Schema.String,
      }),
    ]),
  }),
  Schema.Struct({
    type: Schema.Literal("response.function_call_arguments.delta"),
    item_id: Schema.String,
    delta: Schema.String,
  }),
]);

/** Observe typed public SSE events without altering the stream consumed by Effect AI. */
export const observeOpenAi = (
  client: OpenAiClient.Service,
  writer: ProgressWriter,
): OpenAiClient.Service => ({
  ...client,
  createResponseStream: (request) =>
    Effect.suspend(() => {
      let responseCall: string | undefined;
      const preview = responseTextPreview();

      return writer.newResponse.pipe(
        Effect.andThen(client.createResponseStream(request)),
        Effect.map(
          ([response, events]) =>
            [
              response,
              events.pipe(
                Stream.tap((event) => {
                  const decoded = Schema.decodeUnknownOption(PublicProviderEvent)(event);

                  if (decoded._tag === "None") return Effect.void;
                  const visible = decoded.value;

                  if (visible.type === "response.output_text.delta")
                    return writer.text(visible.delta);
                  if (visible.type === "response.function_call_arguments.delta") {
                    if (visible.item_id !== responseCall) return Effect.void;
                    const delta = preview(visible.delta);

                    return delta.length > 0 ? writer.text(delta) : Effect.void;
                  }
                  if (
                    visible.type === "response.output_item.added" ||
                    visible.type === "response.output_item.done"
                  ) {
                    if (visible.item.type === "function_call") {
                      if (
                        visible.type === "response.output_item.added" &&
                        visible.item.name === "deliver_response" &&
                        responseCall === undefined
                      ) {
                        responseCall = visible.item.id;

                        return writer.newResponse;
                      }

                      return Effect.void;
                    }

                    return writer.tool(
                      visible.item.id,
                      "Searching the web",
                      visible.type === "response.output_item.added"
                        ? "running"
                        : visible.item.status === "completed"
                          ? "complete"
                          : visible.item.status === "failed"
                            ? "failed"
                            : "incomplete",
                    );
                  }

                  return Effect.void;
                }),
              ),
            ] as const,
        ),
      );
    }),
});

export const selectedModelConfig = (settings: PlannerSettings) =>
  ({
    store: false,
    max_output_tokens: 16_384,
    // OpenAI ignores additional built-in attempts after this per-response allowance.
    max_tool_calls: 4,
    // Built-in search limits do not constrain native function calls or completion batches.
    parallel_tool_calls: false,
    reasoning: { effort: settings.reasoningEffort },
    service_tier: settings.fast ? "fast" : "default",
  }) as const;

const unavailableModel = (error: PlannerError) => {
  const failure = AiError.make({
    module: "TravelPlanner",
    method: "selectModel",
    reason: new AiError.InvalidRequestError({ description: error.message }),
  });

  return Model.make(
    "openai",
    "unavailable",
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.fail(failure),
        streamText: () => Stream.fail(failure),
      }),
    ),
  );
};

/** Settings are read once from the trusted claimed Submission, not from model prompts. */
export const selectableModel = (apiKey: Redacted.Redacted<string>) =>
  Layer.unwrap(
    Effect.gen(function* () {
      const attempt = yield* PlannerAttempt;
      const selected = yield* Effect.result(attempt.settings);

      if (Result.isFailure(selected)) return unavailableModel(selected.failure);
      const settings = selected.success;

      const client = Layer.effect(
        OpenAiClient.OpenAiClient,
        Effect.map(OpenAiClient.make({ apiKey }), (base) => observeOpenAi(base, attempt.progress)),
      ).pipe(Layer.provide(FetchHttpClient.layer));

      return OpenAiLanguageModel.model(settings.model, selectedModelConfig(settings)).pipe(
        Layer.provide(client),
      );
    }),
  );

/** Secrets are read only at host assembly; credentials never enter model identity or records. */
export const liveModel = Effect.gen(function* () {
  const apiKey = yield* Config.schema(
    Schema.Redacted(Schema.NonEmptyString),
    "OPENAI_API_KEY",
  ).pipe(
    Effect.mapError(
      () =>
        new PlannerError({
          code: "unavailable",
          message: "Configure OPENAI_API_KEY to use the planner.",
        }),
    ),
  );

  const name = yield* Config.nonEmptyString("OPENAI_MODEL").pipe(
    Config.withDefault("gpt-5.6-luna"),
  );

  const config = {
    store: false,
    max_output_tokens: 4096,
    max_tool_calls: 1,
    reasoning: { effort: "low" },
  } as const;

  return {
    model: OpenAiLanguageModel.model(name, config).pipe(
      Layer.provide(OpenAiClient.layer({ apiKey }).pipe(Layer.provide(FetchHttpClient.layer))),
    ),
    identity: JSON.stringify({ provider: "openai", name, ...config }),
    label: name,
    selectable: selectableModel(apiKey),
  };
});
