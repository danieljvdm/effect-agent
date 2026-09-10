import type * as Agent from "@effect-agent/core/Agent";
import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import { Config, Effect, Layer, Result, Schema, Stream, Redacted } from "effect";
import { AiError, LanguageModel, Model } from "effect/unstable/ai";
import {
  FetchHttpClient,
  HttpClient,
  HttpClientError,
  HttpClientRequest,
} from "effect/unstable/http";

import { type PlannerError, type PlannerSettings } from "../domain.ts";
import { credentialForOwner, type CredentialHost } from "./credentials.ts";
import { recordDiagnostic } from "./diagnostics.ts";
import { PlannerAttempt, type ProgressWriter } from "./progress.ts";
import { observePublicOutput } from "./public-output.ts";

const PublicProviderEvent = Schema.Union([
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
]);

/** Observe typed public SSE events without altering the stream consumed by Effect AI. */
export const observeOpenAi = (
  client: OpenAiClient.Service,
  writer: ProgressWriter,
): OpenAiClient.Service => ({
  ...client,
  createResponseStream: (request) =>
    Effect.suspend(() => {
      return client.createResponseStream(request).pipe(
        Effect.tapCause((cause) =>
          recordDiagnostic("OpenAI: response request failed", {
            request: {
              model: request.model,
              reasoning: request.reasoning,
              serviceTier: request.service_tier,
              maxOutputTokens: request.max_output_tokens,
              maxToolCalls: request.max_tool_calls,
            },
            cause,
          }),
        ),
        Effect.map(
          ([response, events]) =>
            [
              response,
              events.pipe(
                Stream.tapCause((cause) =>
                  recordDiagnostic("OpenAI: response stream failed", {
                    model: request.model,
                    response: { status: response.status, headers: response.headers },
                    cause,
                  }),
                ),
                Stream.tap((event) => {
                  if (
                    event.type === "error" ||
                    event.type === "response.failed" ||
                    event.type === "response.incomplete"
                  )
                    return recordDiagnostic(`OpenAI: ${event.type}`, event);
                  const decoded = Schema.decodeUnknownOption(PublicProviderEvent)(event);

                  if (decoded._tag === "None") return Effect.void;
                  const visible = decoded.value;

                  if (
                    visible.type === "response.output_item.added" ||
                    visible.type === "response.output_item.done"
                  ) {
                    if (visible.item.type === "function_call") {
                      return Effect.void;
                    }

                    const progress = writer.tool(
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

                    return visible.type === "response.output_item.done" &&
                      visible.item.status !== "completed"
                      ? recordDiagnostic(`OpenAI web search: ${visible.item.status}`, event, {
                          toolCallId: visible.item.id,
                        }).pipe(Effect.andThen(progress))
                      : progress;
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

/** Resolve on each HTTP request so removal/rotation also affects running durable workers.
 * An already dispatched provider request may finish; there is no host-key fallback.
 */
export const credentialClient = (key: Effect.Effect<Redacted.Redacted<string>, PlannerError>) =>
  OpenAiClient.make({
    transformClient: (client) =>
      HttpClient.mapRequestEffect(client, (request) =>
        key.pipe(
          Effect.map((apiKey) => HttpClientRequest.bearerToken(request, Redacted.value(apiKey))),
          Effect.mapError(
            (error) =>
              new HttpClientError.HttpClientError({
                reason: new HttpClientError.TransportError({ request, description: error.message }),
              }),
          ),
        ),
      ),
  });

/** Settings are read once from the trusted claimed Submission, not from model prompts. */
export const selectableModel = (
  resolve:
    | Redacted.Redacted<string>
    | ((
        attempt: PlannerAttempt["Service"],
      ) => Effect.Effect<Redacted.Redacted<string>, PlannerError>),
) =>
  Layer.unwrap(
    Effect.gen(function* () {
      const attempt = yield* PlannerAttempt;
      const selected = yield* Effect.result(attempt.settings);

      if (Result.isFailure(selected)) return unavailableModel(selected.failure);
      const settings = selected.success;

      const client = Layer.effect(
        OpenAiClient.OpenAiClient,
        Effect.map(
          credentialClient(
            Redacted.isRedacted(resolve) ? Effect.succeed(resolve) : resolve(attempt),
          ),
          (base) => observeOpenAi(base, attempt.progress),
        ),
      ).pipe(Layer.provide(FetchHttpClient.layer));

      const model = OpenAiLanguageModel.model(settings.model, selectedModelConfig(settings)).pipe(
        Layer.provide(client),
      );

      return Layer.effect(
        LanguageModel.LanguageModel,
        Effect.map(LanguageModel.LanguageModel, (base) =>
          observePublicOutput(base, attempt.progress),
        ),
      ).pipe(Layer.provideMerge(model));
    }),
  );

/** Production uses only the key belonging to the verified account. No deployment API key. */
export const liveModel = (
  env: CredentialHost,
): Effect.Effect<
  {
    readonly model: Layer.Layer<Agent.ModelServices, never, PlannerAttempt>;
    readonly selectable: Layer.Layer<Agent.ModelServices, never, PlannerAttempt>;
    readonly identity: string;
    readonly label: string;
  },
  Config.ConfigError
> =>
  Effect.gen(function* () {
    const resolve = (attempt: PlannerAttempt["Service"]) =>
      attempt.billingOwner.pipe(Effect.flatMap((owner) => credentialForOwner(env, owner)));

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
      // Preserve the legacy definition identity while changing only secret resolution.
      model: Layer.unwrap(
        Effect.map(PlannerAttempt, (attempt) =>
          OpenAiLanguageModel.model(name, config).pipe(
            Layer.provide(
              Layer.effect(OpenAiClient.OpenAiClient, credentialClient(resolve(attempt))).pipe(
                Layer.provide(FetchHttpClient.layer),
              ),
            ),
          ),
        ),
      ),
      identity: JSON.stringify({ provider: "openai", name, ...config }),
      label: name,
      selectable: selectableModel(resolve),
    };
  });
