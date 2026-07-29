import { Schema } from "effect";

import { RunEvent } from "@effect-agent/core";
import { TravelPlan } from "@effect-agent/testing";

/** Runtime profile selected by the browser bench. */
export const DemoMode = Schema.Literals(["deterministic", "openai"]);
export type DemoMode = typeof DemoMode.Type;

/** One request submitted from the browser bench. */
export const DemoRunSelection = Schema.Struct({
  mode: DemoMode,
  request: Schema.String.check(Schema.isTrimmed(), Schema.isNonEmpty(), Schema.isMaxLength(8_000)),
});
export type DemoRunSelection = typeof DemoRunSelection.Type;

/** Server input for the credentialed OpenAI profile. */
export const OpenAiDemoRunRequest = Schema.Struct({
  request: DemoRunSelection.fields.request,
});
export type OpenAiDemoRunRequest = typeof OpenAiDemoRunRequest.Type;

/** Successful, Schema-encoded result from a live provider run. */
export const OpenAiDemoRunSuccess = Schema.TaggedStruct("OpenAiDemoRunSuccess", {
  model: Schema.Literal("gpt-5.6-luna"),
  events: Schema.Array(RunEvent),
  output: TravelPlan,
});
export type OpenAiDemoRunSuccess = typeof OpenAiDemoRunSuccess.Type;

/** Sanitized failure returned across the server-function boundary. */
export const OpenAiDemoRunFailure = Schema.TaggedStruct("OpenAiDemoRunFailure", {
  errorTag: Schema.NonEmptyString,
  message: Schema.String,
});
export type OpenAiDemoRunFailure = typeof OpenAiDemoRunFailure.Type;

/** Complete response contract for the credentialed OpenAI profile. */
export const OpenAiDemoRunResponse = Schema.Union([OpenAiDemoRunSuccess, OpenAiDemoRunFailure]);
export type OpenAiDemoRunResponse = typeof OpenAiDemoRunResponse.Type;

/** Expected client-side RPC or response-decoding failure. */
export class DemoRequestError extends Schema.TaggedErrorClass<DemoRequestError>()(
  "DemoRequestError",
  {
    message: Schema.String,
  },
) {}
