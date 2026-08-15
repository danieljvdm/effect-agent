import type { Agent } from "@effect-agent/core";
import { Prompt, Tool } from "effect/unstable/ai";

/**
 * Model-visible final-output contract (ADR-0020 / D-038, both **Proposed**).
 *
 * The interpreter's only output-conformance point is `decodeFinalOutput`,
 * which validates the final text after the model has already finished; the
 * model itself is never told the Agent's output Schema (issue #41). This
 * module renders that Schema to JSON Schema with the same Effect AI
 * derivation the providers use for Tool parameters and states it as one
 * framework-owned system message on every model request.
 *
 * The contract is a per-request projection of the immutable definition —
 * exactly like the Tool schemas the request already carries. It is inserted
 * at model-request materialization (after context preparation) and never
 * into official history, so canonical records, run events, and the committed
 * DN/DC golden are unchanged.
 *
 * Proposed default under owner review: deleting this module, its single
 * `makeTurn` call site, and its test file restores the prior behavior
 * exactly.
 */

/** Rendering outcome for one definition's output Schema, memoized per definition. */
export type OutputContract =
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

const contractDirective =
  "Final output contract: when the task is complete, the final assistant message must be only " +
  "JSON that is valid against this JSON Schema — no prose, no Markdown code fences, nothing " +
  "before or after the JSON.";

const renderedContracts = new WeakMap<Agent.AnyDefinition, OutputContract>();

/**
 * Render (and memoize) the model-visible final-output contract for one
 * definition. An output Schema the Effect AI derivation cannot represent is
 * reported as `unrenderable`; the caller falls back to the prior behavior —
 * the contract is guidance, and a Schema that decodes but does not render
 * must not become a new failure mode (ADR-0020 decision 3).
 */
export const outputSchemaContract = (definition: Agent.AnyDefinition): OutputContract => {
  const cached = renderedContracts.get(definition);
  if (cached !== undefined) {
    return cached;
  }
  let contract: OutputContract;
  try {
    const jsonSchema = Tool.getJsonSchemaFromSchema(definition.output);
    contract = {
      _tag: "rendered",
      message: `${contractDirective}\n\n${JSON.stringify(jsonSchema, undefined, 2)}`,
    };
  } catch (cause) {
    contract = {
      _tag: "unrenderable",
      reason: cause instanceof Error ? cause.message : String(cause),
    };
  }
  renderedContracts.set(definition, contract);
  return contract;
};

/**
 * Insert the contract immediately after the request prompt's last system
 * message (position 0 when none exists), extending the last contiguous
 * system block. Placement is normative: the Anthropic provider replaces its
 * top-level `system` parameter per contiguous system group, so only the last
 * block survives there — an isolated trailing contract message would discard
 * the author's instructions, and a contract inside an earlier block (for
 * example a resumed Conversation's original instructions ahead of this Run's
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
