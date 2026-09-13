import { Schema } from "effect";
import { Tool, Toolkit } from "effect/unstable/ai";

import { Emitter, UpdateError } from "../AgentUpdates.ts";

/** Definition-owned native Tool. Runtime supplies its handler under the same identity. */
export const updateTool = <S extends Schema.Top>(schema: S) =>
  Tool.make("emit_update", {
    description: "Emit a structured intermediate update. This does not complete the task.",
    parameters: Schema.Struct({ value: schema }),
    success: Schema.Void,
    failure: UpdateError,
    failureMode: "return",
  }).addDependency(Emitter);

export type UpdateToolkit<
  T extends Toolkit.Any,
  S extends Schema.Top | undefined,
> = S extends Schema.Top
  ? Toolkit.Toolkit<T["tools"] & { readonly emit_update: ReturnType<typeof updateTool<S>> }>
  : T;

export const withUpdateTool = (
  toolkit: Toolkit.Any,
  schema: Schema.Top | undefined,
): Toolkit.Any => {
  if (schema === undefined) return toolkit;
  if (Object.hasOwn(toolkit.tools, "emit_update"))
    throw new TypeError("emit_update is reserved for Agent updates");

  return Toolkit.make(...Object.values(toolkit.tools), updateTool(schema));
};
