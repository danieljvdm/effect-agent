import { createServerFn } from "@tanstack/react-start";
import { Effect, Schema } from "effect";

import {
  OpenAiDemoRunFailure,
  OpenAiDemoRunRequest,
  OpenAiDemoRunResponse,
  type OpenAiDemoRunResponse as OpenAiDemoRunResponseValue,
} from "./contracts";

const errorTag = (error: unknown): string =>
  typeof error === "object" &&
  error !== null &&
  "_tag" in error &&
  typeof error._tag === "string" &&
  error._tag.length > 0
    ? error._tag
    : "OpenAiRunError";

const safeErrorMessage = (error: unknown): string => {
  const message =
    typeof error === "object" &&
    error !== null &&
    "message" in error &&
    typeof error.message === "string"
      ? error.message
      : "The live model run failed.";
  return message.replace(/\bsk-[A-Za-z0-9_-]+\b/g, "[redacted]").slice(0, 600);
};

const failureResponse = (error: unknown): OpenAiDemoRunResponseValue =>
  Schema.decodeSync(OpenAiDemoRunFailure)({
    _tag: "OpenAiDemoRunFailure",
    errorTag: errorTag(error),
    message: safeErrorMessage(error),
  });

/**
 * Runs the OpenAI profile on the Start server and returns only its encoded
 * semantic contract to the browser.
 */
export const runOpenAiDemo = createServerFn({ method: "POST" })
  .validator((input: typeof OpenAiDemoRunRequest.Encoded) =>
    Schema.decodeSync(OpenAiDemoRunRequest)(input),
  )
  .handler(async ({ data }) => {
    const { runOpenAiDemoOnServer } = await import("./run-openai.server");
    const response = await Effect.runPromise(
      runOpenAiDemoOnServer(data).pipe(
        Effect.match({
          onFailure: failureResponse,
          onSuccess: (success): OpenAiDemoRunResponseValue => success,
        }),
      ),
    );
    return Schema.encodeSync(OpenAiDemoRunResponse)(response);
  });
