/**
 * An Effect HttpClient integration for TypeSafe's System One evaluations.
 *
 * @since 0.1.0
 */
import * as EffectConfig from "effect/Config";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import { flow } from "effect/Function";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import * as Schema from "effect/Schema";
import * as AiError from "effect/unstable/ai/AiError";
import * as Headers from "effect/unstable/http/Headers";
import * as HttpClient from "effect/unstable/http/HttpClient";
import * as HttpClientRequest from "effect/unstable/http/HttpClientRequest";
import * as HttpClientResponse from "effect/unstable/http/HttpClientResponse";

import * as Errors from "./internal/errors.ts";
import { responseFor } from "./internal/schema.ts";
import * as TypeSafeSchema from "./TypeSafeSchema.ts";

/** @category services
 * @since 0.1.0
 */
export interface Service {
  readonly client: HttpClient.HttpClient;

  /**
   * Evaluate mixed questions against one state. Answers are validated against
   * the submitted IDs, types, and criteria before receiving their inferred type.
   * There are no automatic retries or timeouts; compose those with Effect.
   */
  readonly evaluate: <const Q extends TypeSafeSchema.Questions>(
    options: TypeSafeSchema.EvaluateRequest<Q>,
  ) => Effect.Effect<TypeSafeSchema.EvaluateResponse<Q>, AiError.AiError>;
}

/** @category services
 * @since 0.1.0
 */
export class TypeSafeClient extends Context.Service<TypeSafeClient, Service>()(
  "@effect-agent/ai-typesafe/TypeSafeClient",
) {}

const defaultApiUrl = "https://api.typesafe.ai/v1";

/**
 * Configuration captured when the client is acquired. Supply this service from
 * application configuration or use `Config.layer` to load environment values.
 *
 * @category services
 * @since 0.1.0
 */
export class Config extends Context.Service<
  Config,
  {
    readonly apiKey: Redacted.Redacted<string>;
    /** Base URL, including the API version. Defaults to https://api.typesafe.ai/v1. */
    readonly apiUrl?: string | undefined;
  }
>()("@effect-agent/ai-typesafe/TypeSafeClient/Config") {
  /**
   * Load `TYPESAFE_API_KEY` and optional `TYPESAFE_API_URL` through Effect Config.
   * Missing credentials fail with ConfigError before any HTTP request.
   */
  static readonly layer: Layer.Layer<Config, EffectConfig.ConfigError> = Layer.effect(
    Config,
    EffectConfig.all({
      apiKey: EffectConfig.Redacted("TYPESAFE_API_KEY"),
      apiUrl: EffectConfig.String("TYPESAFE_API_URL").pipe(EffectConfig.withDefault(defaultApiUrl)),
    }),
  );
}

const encodeRequest = Schema.encodeEffect(Schema.fromJsonString(TypeSafeSchema.EvaluateRequest));

/**
 * Capture Config and a platform-neutral HttpClient at construction.
 * Acquiring the service does not send a request. Apply HTTP policies to the
 * supplied HttpClient; its request middleware receives authenticated, absolute URLs.
 *
 * @category constructors
 * @since 0.1.0
 */
export const make: Effect.Effect<Service, never, Config | HttpClient.HttpClient> = Effect.gen(
  function* () {
    const config = yield* Config;
    const apiKey = Redacted.value(config.apiKey);

    const redact = (text: string) =>
      apiKey.length === 0 ? text : text.replaceAll(apiKey, "<redacted>");

    const client = (yield* HttpClient.HttpClient).pipe(
      HttpClient.mapRequestInput(
        flow(
          HttpClientRequest.prependUrl(config.apiUrl ?? defaultApiUrl),
          HttpClientRequest.bearerToken(config.apiKey),
          HttpClientRequest.acceptJson,
        ),
      ),
      HttpClient.filterStatusOk,
    );

    const evaluate = Effect.fnUntraced(function* <const Q extends TypeSafeSchema.Questions>(
      options: TypeSafeSchema.EvaluateRequest<Q>,
    ): Effect.fn.Return<TypeSafeSchema.EvaluateResponse<Q>, AiError.AiError> {
      const body = yield* encodeRequest(options).pipe(
        Effect.mapError((error) =>
          Errors.make(new AiError.InvalidRequestError({ description: redact(error.message) })),
        ),
      );

      const schema = responseFor(options.questions);

      return yield* client
        .execute(
          HttpClientRequest.post("/systemone").pipe(
            HttpClientRequest.bodyText(body, "application/json"),
          ),
        )
        .pipe(
          Effect.flatMap(HttpClientResponse.schemaBodyJson(schema, { onExcessProperty: "error" })),
          Effect.catchTags({
            HttpClientError: (error) => Errors.mapHttpClientError(error, redact),
            SchemaError: (error) => Effect.fail(Errors.mapSchemaError(error, redact)),
          }),
          Effect.updateService(Headers.CurrentRedactedNames, (names) => [
            ...names,
            "authorization",
          ]),
        );
    });

    return TypeSafeClient.of({ client, evaluate });
  },
);

/** @category layers
 * @since 0.1.0
 */
export const layer: Layer.Layer<TypeSafeClient, never, Config | HttpClient.HttpClient> =
  Layer.effect(TypeSafeClient, make);
