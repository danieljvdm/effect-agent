import { Context, Effect, Layer, Option, Redactable, Redacted, Schema } from "effect";
import {
  FetchHttpClient,
  Headers,
  HttpClient,
  HttpClientError,
  HttpClientRequest,
} from "effect/unstable/http";

const Segment = Schema.NonEmptyString.check(
  Schema.isMaxLength(256),
  Schema.isPattern(/^[a-zA-Z0-9_-]+$/),
);

const decodeSegment = Schema.decodeUnknownSync(Segment);

/** Options understood by upstream Effect AI clients; no model or provider wrapper is created. */
export interface ClientOptions {
  readonly apiUrl: string;
  readonly transformClient: (client: HttpClient.HttpClient) => HttpClient.HttpClient;
}

export interface ProviderOptions {
  readonly accountId: string;
  readonly gatewayId: string;
  /** Cloudflare's provider path, such as openai, anthropic, google-ai-studio, or perplexity-ai. */
  readonly provider: string;
  /** Omit only for an unauthenticated gateway with a separately supplied provider key. */
  readonly apiToken?: Redacted.Redacted<string>;
}

export interface RestOptions {
  readonly accountId: string;
  readonly gatewayId: string;
  /** Cloudflare API token with Workers AI Read permission. */
  readonly apiToken: Redacted.Redacted<string>;
  /** Matches the paths appended by the upstream Effect client. */
  readonly protocol: "responses" | "chat-completions" | "messages";
}

/** Choose exactly one route: a native provider path or an account REST protocol. */
export type RouteOptions =
  | (ProviderOptions & { readonly protocol?: never })
  | (RestOptions & { readonly provider?: never });

const redactGatewayToken = (headers: Headers.Headers, tokenHeader: string): void => {
  // HTTP tracing can copy headers after preprocessing. Attach the public redaction
  // protocol to the final request as well, before a provider builds error details.
  Object.defineProperty(headers, Redactable.symbolRedactable, {
    configurable: true,
    value: (context: Context.Context<never>) =>
      Headers.redact(headers, [...Context.get(context, Headers.CurrentRedactedNames), tokenHeader]),
  });
};

const clientOptions = (
  apiUrl: string,
  tokenHeader: "authorization" | "cf-aig-authorization",
  apiToken: Redacted.Redacted<string> | undefined,
  gatewayId?: string,
): ClientOptions =>
  Object.freeze({
    apiUrl,
    transformClient: (client: HttpClient.HttpClient) =>
      client.pipe(
        HttpClient.mapRequestEffect((request) => {
          const url = URL.parse(request.url);

          // Check the normalized URL too: dot segments must not escape this account or gateway.
          if (
            url === null ||
            url.username !== "" ||
            url.password !== "" ||
            !(url.href === apiUrl || url.href.startsWith(`${apiUrl}/`))
          ) {
            return Effect.fail(
              new HttpClientError.HttpClientError({
                reason: new HttpClientError.InvalidUrlError({
                  request,
                  description: "Request is outside the configured Cloudflare AI Gateway endpoint",
                }),
              }),
            );
          }
          let prepared = request;

          if (apiToken !== undefined) {
            prepared = HttpClientRequest.setHeader(
              prepared,
              tokenHeader,
              `Bearer ${Redacted.value(apiToken)}`,
            );
          }
          if (gatewayId !== undefined) {
            prepared = HttpClientRequest.setHeader(prepared, "cf-aig-gateway-id", gatewayId);
          }

          return Effect.succeed(prepared);
        }),
        HttpClient.transformResponse((effect) =>
          Effect.gen(function* () {
            const defaults = yield* Effect.serviceOption(FetchHttpClient.RequestInit);

            return yield* effect.pipe(
              Effect.provideService(FetchHttpClient.RequestInit, {
                ...Option.getOrElse(defaults, () => ({})),
                redirect: "error",
              }),
            );
          }),
        ),
        HttpClient.transformResponse((effect) =>
          effect.pipe(
            Effect.tap((response) =>
              Effect.sync(() => redactGatewayToken(response.request.headers, tokenHeader)),
            ),
            Effect.tapError((error) =>
              Effect.sync(() => redactGatewayToken(error.reason.request.headers, tokenHeader)),
            ),
            Effect.updateService(Headers.CurrentRedactedNames, (names) => [...names, tokenHeader]),
          ),
        ),
      ),
  });

/**
 * Provider-native proxy for model calls, streaming, embeddings, and hosted tools supported
 * by that provider. Pass the result to the upstream client's layer along with its apiKey
 * for BYOK-in-request, or omit apiKey for Gateway stored keys / Unified Billing.
 * Provider model names and request bodies pass through unchanged. This module is Node-safe.
 * Custom transforms and redirect policies must retain this endpoint and credential boundary.
 */
export const provider = (options: ProviderOptions): ClientOptions =>
  clientOptions(
    `https://gateway.ai.cloudflare.com/v1/${decodeSegment(options.accountId)}/${decodeSegment(options.gatewayId)}/${decodeSegment(options.provider)}`,
    "cf-aig-authorization",
    options.apiToken,
  );

/**
 * Cloudflare's account REST API (not the deprecated /compat API). Use provider-qualified
 * model names, e.g. openai/gpt-4.1 or anthropic/claude-haiku-4.5, and omit provider apiKey.
 * OpenAI clients append /responses or /chat/completions; Anthropic appends /v1/messages.
 * Compatibility remains the responsibility of the selected upstream client and model.
 */
export const rest = (options: RestOptions): ClientOptions =>
  clientOptions(
    `https://api.cloudflare.com/client/v4/accounts/${decodeSegment(options.accountId)}/ai${options.protocol === "messages" ? "" : "/v1"}`,
    "authorization",
    options.apiToken,
    decodeSegment(options.gatewayId),
  );

/**
 * Provide a Gateway-configured upstream client directly in a Layer pipeline.
 * Pass the client's `layer` factory, or a callback adding client-specific options.
 * The factory's errors and remaining services (such as HttpClient) stay visible.
 * Model selection and resource ownership remain with the supplied upstream Layers.
 *
 * @example
 * ```ts
 * AnthropicLanguageModel.model("claude-haiku-4-5").pipe(
 *   Gateway.provide(AnthropicClient.layer, { ...options, provider: "anthropic" }),
 * )
 * ```
 */
export const provide = <Client, E, R>(
  clientLayer: (options: ClientOptions) => Layer.Layer<Client, E, R>,
  options: RouteOptions,
) => Layer.provide(clientLayer(options.provider !== undefined ? provider(options) : rest(options)));
