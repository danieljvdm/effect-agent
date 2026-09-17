import { OpenAiSchema } from "@effect/ai-openai";
import { Effect, Schema } from "effect";
import { AiError, Tool } from "effect/unstable/ai";
import { toCodecOpenAI } from "effect/unstable/ai/OpenAiStructuredOutput";

type Request = typeof OpenAiSchema.CreateResponse.Encoded;
type Definition = typeof OpenAiSchema.Tool.Encoded;

const invalid = (description: string) =>
  AiError.AiError.make({
    module: "ToolSelectionBenchmark",
    method: "stableTools",
    reason: AiError.InvalidRequestError.make({ description }),
  });

/** Use the same public schema transformer as the pinned OpenAI language-model adapter. */
export const encodeTools = Effect.fnUntraced(function* (tools: ReadonlyArray<Tool.Any>) {
  const definitions = yield* Effect.try({
    try: () =>
      tools.map((tool) => ({
        type: "function",
        name: tool.name,
        parameters: Tool.getJsonSchema(tool, { transformer: toCodecOpenAI }),
        strict: Tool.getStrictMode(tool) ?? true,
        description: Tool.getDescription(tool),
      })),
    catch: () => invalid("Could not encode the fixture catalogue"),
  });

  return yield* Schema.decodeUnknownEffect(Schema.Array(OpenAiSchema.Tool))(definitions).pipe(
    Effect.mapError(() => invalid("Malformed fixture definitions")),
  );
});

const fingerprint = (tool: Definition) =>
  tool.type === "function"
    ? JSON.stringify([tool.name, tool.description, tool.parameters, tool.strict])
    : undefined;

/**
 * Experimental transport only: all supplied definitions must already belong to the
 * authorized fixture catalogue. Runtime exposure and handler checks remain unchanged.
 * Fail closed if the model adapter requests a different schema or unsupported choice.
 */
export const withStableTools = Effect.fnUntraced(function* (
  payload: Request,
  catalogue: ReadonlyArray<Definition>,
): Effect.fn.Return<Request, AiError.AiError> {
  const definitions = new Map<string, string | undefined>();

  for (const tool of catalogue) {
    if (tool.type !== "function" || definitions.has(tool.name))
      return yield* invalid("Stable catalogue must contain unique function tools");
    definitions.set(tool.name, fingerprint(tool));
  }
  const names: Array<string> = [];

  for (const tool of payload.tools ?? []) {
    if (
      tool.type !== "function" ||
      !definitions.has(tool.name) ||
      definitions.get(tool.name) !== fingerprint(tool) ||
      names.includes(tool.name)
    )
      return yield* invalid(
        "Callable tool is absent, duplicated or changed in the stable catalogue",
      );
    names.push(tool.name);
  }
  const choice = payload.tool_choice;

  if (typeof choice === "object") {
    if (choice.type !== "function" || !names.includes(choice.name))
      return yield* invalid("Unsupported or unavailable forced tool choice");

    return { ...payload, tools: catalogue, tool_choice: choice };
  }
  if (choice === "required" && names.length === 0)
    return yield* invalid("Cannot require an empty callable subset");

  return {
    ...payload,
    tools: catalogue,
    tool_choice:
      choice === "none" || names.length === 0
        ? "none"
        : {
            type: "allowed_tools",
            mode: choice === "required" ? "required" : "auto",
            tools: names.map((name) => ({ type: "function", name })),
          },
  };
});

/**
 * Keep availability updates in an append-only transport overlay. Replacing an
 * ephemeral note each turn would discard that part of the reusable history.
 * This overlay is scoped to one benchmark sample, not durable thread storage.
 */
export const makeAvailabilityNotes = () => {
  let previous: ReadonlyArray<typeof OpenAiSchema.InputItem.Encoded> = [];
  let previousNames: string | undefined;
  const notes: Array<{ readonly after: number; readonly text: string }> = [];

  return Effect.fnUntraced(function* (
    payload: Request,
    names: ReadonlyArray<string>,
  ): Effect.fn.Return<Request, AiError.AiError> {
    const input = payload.input;

    if (input === undefined || typeof input === "string" || input.length === 0)
      return yield* invalid("Availability notes require a structured conversation");
    if (
      input.length < previous.length ||
      previous.some((item, index) => JSON.stringify(item) !== JSON.stringify(input[index]))
    )
      return yield* invalid("Availability notes require append-only source messages");

    const namesKey = JSON.stringify(names);

    if (namesKey !== previousNames) {
      notes.push({
        after: input.length,
        text:
          `Tool availability update: the currently callable functions are ${names.length === 0 ? "none" : names.join(", ")}. ` +
          "Other tool definitions remain visible but are currently disabled." +
          (names.includes("discover_tools")
            ? " Use discover_tools to enable a needed capability before calling it."
            : ""),
      });
      previousNames = namesKey;
    }
    previous = input;

    const annotated: Array<typeof OpenAiSchema.InputItem.Encoded> = [];

    for (const [index, item] of input.entries()) {
      annotated.push(item);
      for (const note of notes) {
        if (note.after === index + 1)
          annotated.push({
            role: "developer",
            content: [{ type: "input_text", text: note.text }],
          });
      }
    }

    return { ...payload, input: annotated };
  });
};
