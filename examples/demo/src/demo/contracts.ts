import { Schema } from "effect";
import { ChatInput } from "./general-chat";

/** Runtime profile selected by the browser bench. */
export const DemoMode = Schema.Literals(["deterministic", "openai"]);
export type DemoMode = typeof DemoMode.Type;

/** One request submitted from the browser bench. */
export class DemoRunSelection extends Schema.Class<DemoRunSelection>("DemoRunSelection")({
  mode: DemoMode,
  message: ChatInput.fields.message,
}) {}
