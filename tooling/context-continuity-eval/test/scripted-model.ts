import { Effect, Option, Schema } from "effect";

import { makeScenario } from "../src/scenario.ts";

/** Test-only deterministic provider, never bundled into the live worker. */
export const scriptedResponse = Effect.fn("fixture.scriptedResponse")(function* (
  json: string,
  phase: number,
  ordinal: number,
  countedInput: number,
  model: string,
  seed: number,
  original: string | undefined,
) {
  const scenario = makeScenario(seed);

  const Payload = Schema.Struct({
    input: Schema.optionalKey(
      Schema.Union([
        Schema.String,
        Schema.Array(
          Schema.Struct({
            type: Schema.optionalKey(Schema.String),
            output: Schema.optionalKey(Schema.Unknown),
          }),
        ),
      ]),
    ),
  });

  const payload = yield* Schema.decodeEffect(Schema.fromJsonString(Payload))(json);
  const expected = scenario[phase];

  if (expected === undefined) return yield* Effect.die("No scripted phase");

  const results =
    typeof payload.input === "string"
      ? []
      : (payload.input ?? []).flatMap((item) =>
          item.type === "function_call_output" && typeof item.output === "string"
            ? [item.output]
            : [],
        );

  const Notes = Schema.Struct({ revision: Schema.NullOr(Schema.String), text: Schema.String });

  const notes = results
    .map((value) => Schema.decodeOption(Schema.fromJsonString(Notes))(value))
    .filter(Option.isSome)
    .at(-1)?.value;

  const Hit = Schema.Struct({ recordId: Schema.String, text: Schema.String });

  const hits = results.flatMap((value) => {
    const decoded = Schema.decodeOption(Schema.fromJsonString(Schema.Array(Hit)))(value);

    return Option.isSome(decoded) ? decoded.value : [];
  });

  const source = hits.find((hit) =>
    hit.text.includes(expected.receipt?.code ?? "missing"),
  )?.recordId;

  original ??= source;

  const actions = [
    { name: "read_notes", args: {} },
    {
      name: "write_notes",
      args: { text: JSON.stringify(expected.expected), expectedRevision: notes?.revision ?? null },
    },
    ...(phase === 0
      ? []
      : [
          { name: "inspect_manifest", args: { update: phase, page: 0 } },
          { name: "inspect_manifest", args: { update: phase, page: 1 } },
          { name: "read_notes", args: {} },
        ]),
    ...(expected.receipt === null
      ? []
      : [
          { name: "search_context_windows", args: { query: expected.receipt.label, limit: 3 } },
          { name: "read_context_window", args: { recordId: source ?? "missing", maxChars: 5000 } },
          {
            name: "read_context_window",
            args: { recordId: original ?? "missing", maxChars: 5000 },
          },
        ]),
  ];

  const action = actions[ordinal];

  const text = JSON.stringify({
    ...expected.expected,
    receipts:
      expected.receipt === null ? [] : [{ ...expected.receipt, recordId: original ?? "missing" }],
  });

  const item =
    action === undefined
      ? {
          type: "message",
          id: `msg_${phase}_${ordinal}`,
          role: "assistant",
          status: "completed",
          content: [{ type: "output_text", text, annotations: [] }],
        }
      : {
          type: "function_call",
          id: `fc_${phase}_${ordinal}`,
          call_id: `call_${phase}_${ordinal}`,
          name: action.name,
          arguments: JSON.stringify(action.args),
          status: "completed",
        };

  const events = [
    { type: "response.output_item.added", output_index: 0, item },
    ...(action === undefined
      ? [
          {
            type: "response.output_text.delta",
            item_id: item.id,
            output_index: 0,
            content_index: 0,
            delta: text,
          },
        ]
      : [
          {
            type: "response.function_call_arguments.done",
            output_index: 0,
            item_id: item.id,
            arguments: JSON.stringify(action.args),
          },
        ]),
    { type: "response.output_item.done", output_index: 0, item },
    {
      type: "response.completed",
      response: {
        id: `resp_${phase}_${ordinal}`,
        object: "response",
        model,
        created_at: 1,
        status: "completed",
        service_tier: "default",
        output: [item],
        usage: {
          input_tokens: countedInput,
          output_tokens: 100,
          total_tokens: countedInput + 100,
          input_tokens_details: { cached_tokens: 0 },
          output_tokens_details: { reasoning_tokens: 0 },
        },
      },
    },
  ];

  return {
    original,
    response: new Response(
      events
        .map(
          (event, sequence_number) => `data: ${JSON.stringify({ ...event, sequence_number })}\n\n`,
        )
        .join(""),
      { headers: { "content-type": "text/event-stream" } },
    ),
  };
});
