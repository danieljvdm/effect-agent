/**
 * An Effect HttpClient integration for TypeSafe's System One evaluations.
 *
 * @since 0.1.0
 */
import * as Config from "effect/Config";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import { flow, identity } from "effect/Function";
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

/** @category options
 * @since 0.1.0
 */
export interface Options {
  readonly apiKey: Redacted.Redacted<string>;
  /** Base URL, including the API version. Defaults to https://api.typesafe.ai/v1. */
  readonly apiUrl?: string | undefined;
  /** Applied after authentication and status filtering, for explicit HTTP policies. */
  readonly transformClient?: ((client: HttpClient.HttpClient) => HttpClient.HttpClient) | undefined;
}

const encodeRequest = Schema.encodeEffect(Schema.fromJsonString(TypeSafeSchema.EvaluateRequest));

/**
 * Construct the client using a supplied, platform-neutral HttpClient.
 * Acquiring the service does not send a request.
 *
 * @category constructors
 * @since 0.1.0
 */
export const make = Effect.fnUntraced(function* (
  options: Options,
): Effect.fn.Return<Service, never, HttpClient.HttpClient> {
  const apiKey = Redacted.value(options.apiKey);

  const redact = (text: string) =>
    apiKey.length === 0 ? text : text.replaceAll(apiKey, "<redacted>");

  const client = (yield* HttpClient.HttpClient).pipe(
    HttpClient.mapRequest(
      flow(
        HttpClientRequest.prependUrl(options.apiUrl ?? "https://api.typesafe.ai/v1"),
        HttpClientRequest.bearerToken(options.apiKey),
        HttpClientRequest.acceptJson,
      ),
    ),
    HttpClient.filterStatusOk,
    options.transformClient ?? identity,
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
        Effect.updateService(Headers.CurrentRedactedNames, (names) => [...names, "authorization"]),
      );
  });

  return TypeSafeClient.of({ client, evaluate });
});

/** @category layers
 * @since 0.1.0
 */
export const layer = (
  options: Options,
): Layer.Layer<TypeSafeClient, never, HttpClient.HttpClient> =>
  Layer.effect(TypeSafeClient, make(options));

/**
 * Configure the client with Effect Config. The API key defaults to
 * `Config.Redacted("TYPESAFE_API_KEY")`.
 *
 * @category layers
 * @since 0.1.0
 */
export const layerConfig = (options?: {
  readonly apiKey?: Config.Config<Redacted.Redacted<string>> | undefined;
  readonly apiUrl?: Config.Config<string> | undefined;
  readonly transformClient?: Options["transformClient"];
}): Layer.Layer<TypeSafeClient, Config.ConfigError, HttpClient.HttpClient> =>
  Layer.effect(
    TypeSafeClient,
    Effect.gen(function* () {
      return yield* make({
        apiKey: yield* options?.apiKey ?? Config.Redacted("TYPESAFE_API_KEY"),
        apiUrl: options?.apiUrl === undefined ? undefined : yield* options.apiUrl,
        transformClient: options?.transformClient,
      });
    }),
  );
