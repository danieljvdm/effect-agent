import { Schema } from "effect";

import { ChatInput } from "./general-chat";

/** Runtime profile selected by the browser bench. */
export const DemoMode = Schema.Literals(["deterministic", "openai"]);
export type DemoMode = typeof DemoMode.Type;

/** One visible prior message supplied as model context for the next chat Run. */
export class DemoChatHistoryMessage extends Schema.Class<DemoChatHistoryMessage>(
  "DemoChatHistoryMessage",
)({
  role: Schema.Literals(["user", "assistant"]),
  content: ChatInput.fields.message,
}) {}

/** One chat turn submitted from the browser bench with bounded prior context. */
export class DemoRunSelection extends Schema.Class<DemoRunSelection>("DemoRunSelection")({
  mode: DemoMode,
  message: ChatInput.fields.message,
  history: Schema.Array(DemoChatHistoryMessage).check(Schema.isMaxLength(40)),
}) {}
