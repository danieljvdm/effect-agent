import type * as Agent from "@effect-agent/core/Agent";
import { Prompt, Tool } from "effect/unstable/ai";

/**
 * Model-visible final-output contract (RUN-028).
 *
 * For ordinary text completion, the interpreter's only output-conformance
 * point is `decodeFinalOutput`, which validates the final text after the
 * model has already finished. This module renders that Schema to JSON Schema
 * with the same Effect AI derivation the providers use for Tool parameters
 * and states it as one framework-owned system message on every model request.
 * A required completion Tool instead gets a native-tool directive: its Tool
 * parameter Schema is already carried by the provider request, and ordinary
 * final text is not an allowed completion path.
 *
 * The contract is a per-request projection of the immutable definition —
 * exactly like the Tool schemas the request already carries. It is inserted
 * at model-request materialization (after context preparation) and never
 * into official history, so canonical records, run events, and the committed
 * DN/DC golden are unchanged.
 *
 * Reversal: deleting this module, its single `makeTurn` call site, the
 * `RunContextRequest.outputContract` field, and its test file restores the
 * prior behavior exactly.
 */

/** Rendering outcome for one definition's output Schema. */
type OutputContract =
  | {
      readonly _tag: "rendered";
      /** The complete system-message text: directive plus the derived JSON Schema. */
      readonly message: string;
    }
  | {
      readonly _tag: "unrenderable";
      /** Why Effect AI's JSON-Schema derivation rejected the output Schema. */
      readonly reason: string;
    };

const contractDirective = (definition: Agent.AnyDefinition): string =>
  definition.completion === undefined
    ? "Final output contract: when the task is complete, the final assistant message must be only " +
      "JSON that is valid against this JSON Schema — no prose, no Markdown code fences, nothing " +
      "before or after the JSON."
    : `Final output contract: when the task is complete without calling the "${definition.completion.tool}" completion Tool, the final assistant message must be only ` +
      "JSON that is valid against this JSON Schema — no prose, no Markdown code fences, nothing " +
      `before or after the JSON. When calling the "${definition.completion.tool}" completion Tool, never place this private Agent output JSON in any Tool argument; follow the Tool's parameter schema instead. The engine projects the successful completion Tool result into the Agent output.`;

const requiredCompletionDirective = (tool: string): string =>
  `Final output contract: complete only by calling the required completion Tool ${JSON.stringify(tool)} ` +
  "as the sole Tool Call in its batch. Do not emit an ordinary final assistant text answer. " +
  "The Tool's canonical parameters and successful result are projected and validated as the Agent output.";

/**
 * Render the model-visible final-output contract for one definition. The
 * derivation is pure and cheap relative to a model call (providers derive
 * every Tool's JSON Schema per request the same way), so no cache is kept.
 * An output Schema the Effect AI derivation cannot represent is reported as
 * `unrenderable`; the caller falls back to the prior behavior — the contract
 * is guidance, and a Schema that decodes but does not render must not become
 * a new failure mode.
 */
export const outputSchemaContract = (definition: Agent.AnyDefinition): OutputContract => {
  if (definition.completion?.required === true) {
    return {
      _tag: "rendered",
      message: requiredCompletionDirective(definition.completion.tool),
    };
  }
  try {
    const jsonSchema = Tool.getJsonSchemaFromSchema(definition.output);

    return {
      _tag: "rendered",
      message: `${contractDirective(definition)}\n\n${JSON.stringify(jsonSchema, undefined, 2)}`,
    };
  } catch (cause) {
    return {
      _tag: "unrenderable",
      reason: cause instanceof Error ? cause.message : String(cause),
    };
  }
};

/**
 * Insert the contract immediately after the request prompt's last system
 * message (position 0 when none exists), extending the last contiguous
 * system block. Placement is normative: the Anthropic provider replaces its
 * top-level `system` parameter per contiguous system group, so only the last
 * block survives there — an isolated trailing contract message would discard
 * the author's instructions, and a contract inside an earlier block (for
 * example a resumed Thread's original instructions ahead of this Run's
 * evaluated instructions) would itself be discarded. Extending the last
 * block keeps author content and contract together on every provider and
 * preserves the author's per-message cache-control annotations.
 */
export const insertOutputContract = (prompt: Prompt.Prompt, message: string): Prompt.Prompt => {
  const content = prompt.content;
  let insertAt = 0;

  for (let index = 0; index < content.length; index += 1) {
    if (content[index]?.role === "system") {
      insertAt = index + 1;
    }
  }

  return Prompt.fromMessages([
    ...content.slice(0, insertAt),
    Prompt.makeMessage("system", { content: message }),
    ...content.slice(insertAt),
  ]);
};
