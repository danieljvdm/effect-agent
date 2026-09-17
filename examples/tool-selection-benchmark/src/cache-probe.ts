import { Cause, Clock, Effect, Exit, Schema, Stream } from "effect";

import { Parameters, referenceContext, tools } from "./fixture.ts";
import { type Arm, type ContextSize, instrument, Sample } from "./measurement.ts";
import { encodeTools } from "./stable-tools.ts";

/** Four identical prompts: A/A/A/A or A/B/C/A callable subsets. No tool handlers run. */
export const runProbe = Effect.fn("ToolSelectionBenchmark.probe")(function* (
  arm: Arm,
  repetition: number,
  context: typeof ContextSize.Type,
  nonce: string,
) {
  const encoded = yield* encodeTools(tools);

  // Salt the first tool's description, before any cacheable shared prefix, once
  // per trial. A cache key alone would not establish a cold rendered prefix.
  const catalogue = encoded.map((tool, index) =>
    index === 0 && tool.type === "function"
      ? { ...tool, description: `Trial ${nonce}. ${tool.description}` }
      : tool,
  );

  const metered = yield* instrument(8, {
    maxCalls: 4,
    ...(arm === "probe-allowed" ? { stableTools: catalogue } : {}),
  });

  const started = yield* Clock.currentTimeMillis;

  const exit = yield* Effect.gen(function* () {
    for (const position of [0, 1, 2, 0]) {
      const offset = arm === "probe-fixed" ? 2 : 2 + position * 6;

      const [_, stream] = yield* metered.client.createResponseStream({
        model: "gpt-6-astra",
        max_output_tokens: 2_048,
        reasoning: { effort: "low" },
        store: false,
        service_tier: "default",
        tools: [...catalogue.slice(0, 2), ...catalogue.slice(offset, offset + 6)],
        tool_choice: "required",
        input: [
          {
            role: "developer",
            content:
              "Retrieve the requested fresh record with exactly one tool call. " +
              (context === "reference" ? referenceContext : ""),
          },
          { role: "user", content: "Get the shipping record for order ORD-104." },
        ],
      });

      yield* Stream.runDrain(stream);
    }
  }).pipe(Effect.timeout("150 seconds"), Effect.exit);

  const successfulCalls = metered.calls.filter((call) => {
    if (call.status !== "response.completed" || call.toolCalls.length !== 1) return false;
    const toolCall = call.toolCalls[0]!;
    const decoded = Schema.decodeExit(Schema.fromJsonString(Parameters))(toolCall.arguments);

    return (
      toolCall.name === "get_order_shipping" &&
      Exit.isSuccess(decoded) &&
      decoded.value.id === "ORD-104"
    );
  });

  const success = Exit.isSuccess(exit) && successfulCalls.length === 4;

  return Sample.make({
    arm,
    task: "cache-probe",
    repetition,
    context,
    forcedMiss: false,
    elapsedMs: (yield* Clock.currentTimeMillis) - started,
    success,
    answer: null,
    failure: Exit.isFailure(exit)
      ? Cause.pretty(exit.cause)
      : success
        ? null
        : "Expected exactly one correct shipping call per request",
    modelCalls: metered.calls,
    decisionCalls: metered.decisions,
    toolCalls: [],
    selections: [],
  });
});
