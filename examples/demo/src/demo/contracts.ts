import { Schema } from "effect";

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
