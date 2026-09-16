import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Record from "effect/Record";
import * as Redacted from "effect/Redacted";
import type * as Schema from "effect/Schema";
import * as AiError from "effect/unstable/ai/AiError";
import * as Headers from "effect/unstable/http/Headers";
import type * as HttpClientError from "effect/unstable/http/HttpClientError";

export const make = (reason: AiError.AiErrorReason): AiError.AiError =>
  AiError.make({ module: "TypeSafeClient", method: "evaluate", reason });

export const mapSchemaError = (
  error: Schema.SchemaError,
  redact: (text: string) => string,
): AiError.AiError => {
  const reason = AiError.InvalidOutputError.fromSchemaError(error);

  return make(
    new AiError.InvalidOutputError({ ...reason, description: redact(reason.description) }),
  );
};

const redactRequest = (
  request: typeof AiError.HttpRequestDetails.Type,
  redact: (text: string) => string,
): typeof AiError.HttpRequestDetails.Type => ({
  ...request,
  url: redact(request.url),
  urlParams: request.urlParams.map(([key, value]) => [key, redact(value)]),
  hash: request.hash === undefined ? undefined : redact(request.hash),
  headers: Record.map(request.headers, (value) =>
    Redacted.isRedacted(value) ? "<redacted>" : redact(value),
  ),
});

export const mapHttpClientError = Effect.fnUntraced(function* (
  error: HttpClientError.HttpClientError,
  redact: (text: string) => string,
): Effect.fn.Return<never, AiError.AiError> {
  const reason = error.reason;

  switch (reason._tag) {
    case "TransportError":
    case "EncodeError":
    case "InvalidUrlError": {
      const network = AiError.NetworkError.fromRequestError(reason);

      return yield* make(
        new AiError.NetworkError({
          ...network,
          request: redactRequest(network.request, redact),
          description: network.description === undefined ? undefined : redact(network.description),
        }),
      );
    }
    case "DecodeError":
    case "EmptyBodyError":
      return yield* make(
        new AiError.InvalidOutputError({
          description: redact(reason.description ?? "Could not decode the TypeSafe response body"),
        }),
      );
    case "StatusCodeError": {
      const { request, response } = reason;
      const redactedNames = yield* Headers.CurrentRedactedNames;

      const headers = (value: Headers.Headers) =>
        Record.map(Headers.redact(value, redactedNames), (value) =>
          Redacted.isRedacted(value) ? "<redacted>" : redact(value),
        );

      const text = yield* Effect.option(response.text);
      const body = Option.isSome(text) ? redact(text.value) : undefined;

      const http: typeof AiError.HttpContext.Type = {
        request: {
          method: request.method,
          url: redact(request.url),
          urlParams: Array.from(request.urlParams, ([key, value]) => [key, redact(value)]),
          hash: Option.getOrUndefined(Option.map(request.hash, redact)),
          headers: headers(request.headers),
        },
        response: { status: response.status, headers: headers(response.headers) },
        body,
      };

      const description = AiError.buildErrorDescription({
        status: response.status,
        method: request.method,
        url: http.request.url,
        message: undefined,
        body,
      });

      return yield* make(
        response.status === 422
          ? new AiError.InvalidRequestError({ description, http })
          : AiError.reasonFromHttpStatus({ status: response.status, description, http }),
      );
    }
  }
});
