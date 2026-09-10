import type * as Agent from "@effect-agent/core/Agent";
import { Schema } from "effect";
import { Prompt, Tool } from "effect/unstable/ai";

const outputFormatAnnotation = "@effect-agent/engine/Output/format";

/**
 * Declare ordinary assistant text as an Agent's final-output wire format. Apply this to the
 * complete output Schema after composing transformations. Its encoded type must be string;
 * decoding, checks, transformations, and service requirements remain owned by that Schema.
 * Text is preserved verbatim, including whitespace, quotes, and an empty reply when allowed.
 * Required completion Tools still take precedence. Unmarked Schemas use JSON final output.
 *
 * @example
 * ```ts
 * output: Output.text(Schema.String.check(Schema.isMaxLength(20_000)))
 * ```
 */
export const textOutput = <S extends Schema.Top & { readonly Encoded: string }>(
  schema: S,
): S["Rebuild"] => schema.annotate({ [outputFormatAnnotation]: "text" });

export const isTextOutput = (schema: Schema.Top): boolean =>
  Schema.resolveAnnotations(schema)?.[outputFormatAnnotation] === "text";

/**
 * Model-visible final-output contract (RUN-028).
 *
 * For ordinary text completion, the interpreter's only output-conformance
 * point is `decodeFinalOutput`, which validates the final text after the
 * model has already finished. This module renders that Schema's wire contract
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
 */

/** Rendering outcome for one definition's output Schema. */
type OutputContract =
  | {
      readonly _tag: "rendered";
      /** The complete system-message text: directive plus the derived JSON Schema. */
      readonly message: string;
      readonly part: Prompt.SystemMessage;
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
      `before or after the JSON. When calling the "${definition.completion.tool}" completion Tool, never place this private Agent output JSON in any Tool argument; follow the Tool's parameter schema instead. Call it alone, after receiving any other Tool results. The engine projects the successful completion Tool result into the Agent output.`;

const requiredCompletionDirective = (tool: string, hasAlternatives: boolean): string =>
  `Final output contract: ${hasAlternatives ? "otherwise complete" : "complete only"} by calling the required completion Tool ${JSON.stringify(tool)} ` +
  "as the sole Tool Call in its batch. Do not emit an ordinary final assistant text answer. " +
  "The Tool's canonical parameters and successful result are projected and validated as the Agent output.";

/**
 * Render the model-visible final-output contract for one definition. The
 * derivation and message identity are stable for an immutable definition.
 * An output Schema the Effect AI derivation cannot represent is reported as
 * `unrenderable`; the caller falls back to the prior behavior — the contract
 * is guidance, and a Schema that decodes but does not render must not become
 * a new failure mode.
 */
const rendered = (message: string): OutputContract => ({
  _tag: "rendered",
  message,
  part: Prompt.makeMessage("system", { content: message }),
});

const renderOutputSchemaContract = (definition: Agent.AnyDefinition): OutputContract => {
  if (definition.completion?.required === true) {
    return rendered(
      requiredCompletionDirective(
        definition.completion.tool,
        (definition.completionFromTools?.length ?? 0) > 0,
      ),
    );
  }
  if (isTextOutput(definition.output)) {
    return rendered(
      "Final output contract: write the final reply as ordinary assistant text, without JSON wrapping. " +
        "An empty reply is valid only when allowed by the output Schema and the task instructions." +
        (definition.completion === undefined
          ? ""
          : ` When calling the "${definition.completion.tool}" completion Tool, follow its parameter schema instead and call it alone, after receiving any other Tool results; the engine projects its successful result into the Agent output.`),
    );
  }
  try {
    const jsonSchema = Tool.getJsonSchemaFromSchema(definition.output);

    return rendered(
      `${contractDirective(definition)}\n\n${JSON.stringify(jsonSchema, undefined, 2)}`,
    );
  } catch (cause) {
    return {
      _tag: "unrenderable",
      reason: cause instanceof Error ? cause.message : String(cause),
    };
  }
};

// Native incremental-response tracking recognizes message objects, not rendered text.
// Weak keys retain neither discarded definitions nor their Schema graphs.
const outputContracts = new WeakMap<Agent.AnyDefinition, OutputContract>();

export const outputSchemaContract = (definition: Agent.AnyDefinition): OutputContract => {
  const cached = outputContracts.get(definition);

  if (cached !== undefined) return cached;
  const base = renderOutputSchemaContract(definition);
  const alternatives = definition.completionFromTools ?? [];

  const contract =
    base._tag === "rendered" && alternatives.length > 0
      ? rendered(
          `The following action Tools may complete the Run from their successful canonical result: ${alternatives.map((declaration) => JSON.stringify(declaration.tool)).join(", ")}. ` +
            "Call such a Tool alone, following its parameter schema. It may complete the Run only when it satisfies the whole request. " +
            "An incomplete or pending result continues the Run. These actions are unavailable during finalization after budget exhaustion.\n\n" +
            base.message,
        )
      : base;

  outputContracts.set(definition, contract);

  return contract;
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
export const insertOutputContract = (
  prompt: Prompt.Prompt,
  message: Prompt.SystemMessage,
): Prompt.Prompt => {
  const content = prompt.content;
  let insertAt = 0;

  for (let index = 0; index < content.length; index += 1) {
    if (content[index]?.role === "system") {
      insertAt = index + 1;
    }
  }

  return Prompt.fromMessages([...content.slice(0, insertAt), message, ...content.slice(insertAt)]);
};
