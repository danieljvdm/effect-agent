import { DecisionSchema } from "@effect-agent/ai-decision";
import { OpenAiClient } from "@effect/ai-openai";
import { Clock, Effect, Schema, Stream } from "effect";
import { AiError } from "effect/unstable/ai";

export const Arm = Schema.Literals(["all-50", "fixed-8-discovery", "jev-8-discovery"]);
export type Arm = typeof Arm.Type;

export const Usage = Schema.Struct({
  input_tokens: Schema.Natural,
  output_tokens: Schema.Natural,
  input_tokens_details: Schema.Struct({ cached_tokens: Schema.Natural }),
  output_tokens_details: Schema.Struct({ reasoning_tokens: Schema.Natural }),
});

export const ModelCall = Schema.Struct({
  startedAt: Schema.Finite,
  completedAt: Schema.NullOr(Schema.Finite),
  firstDeltaAt: Schema.NullOr(Schema.Finite),
  tools: Schema.Array(Schema.String),
  toolCalls: Schema.Array(Schema.Struct({ name: Schema.String, arguments: Schema.String })),
  requestJson: Schema.String,
  model: Schema.NullOr(Schema.String),
  status: Schema.NullOr(Schema.String),
  usage: Schema.NullOr(Usage),
});

export const Selection = Schema.Struct({
  elapsedMs: Schema.Finite,
  selected: Schema.Array(Schema.String),
  evaluation: Schema.NullOr(
    Schema.Struct({
      model: Schema.String,
      usage: DecisionSchema.Usage,
    }),
  ),
});

export const Sample = Schema.Struct({
  arm: Arm,
  task: Schema.String,
  repetition: Schema.Natural,
  elapsedMs: Schema.Finite,
  success: Schema.Boolean,
  answer: Schema.NullOr(Schema.String),
  failure: Schema.NullOr(Schema.String),
  modelCalls: Schema.Array(ModelCall),
  toolCalls: Schema.Array(
    Schema.Struct({ name: Schema.String, id: Schema.String, at: Schema.Finite }),
  ),
  selections: Schema.Array(Selection),
});

export class BenchmarkError extends Schema.TaggedError<BenchmarkError>()("BenchmarkError", {
  message: Schema.String,
}) {}

const Completion = Schema.Struct({
  model: Schema.String,
  usage: Usage,
  output: Schema.Array(
    Schema.Struct({
      type: Schema.String,
      name: Schema.optional(Schema.String),
      arguments: Schema.optional(Schema.String),
    }),
  ),
});

const refuse = (description: string) =>
  AiError.AiError.make({
    module: "ToolSelectionBenchmark",
    method: "provider",
    reason: AiError.InvalidRequestError.make({ description }),
  });

// No token-count preflight or retries: timings include exactly the production path.
// Request count, request bytes, output tokens and elapsed time bound this experiment.
export const instrument = Effect.fn("ToolSelectionBenchmark.instrument")(function* () {
  const native = yield* OpenAiClient.OpenAiClient;
  const calls: Array<typeof ModelCall.Type> = [];

  const client = OpenAiClient.OpenAiClient.of({
    ...native,
    createResponse: () => refuse("Expected streaming requests"),
    createResponseStream: Effect.fn("ToolSelectionBenchmark.response")(function* (payload) {
      const requestJson = JSON.stringify(payload);

      if (calls.length >= 6 || new TextEncoder().encode(requestJson).byteLength > 65_536)
        return yield* refuse("Model-call or request-byte bound exceeded");
      if (
        payload.model !== "gpt-6-astra" ||
        payload.max_output_tokens !== 2_048 ||
        payload.store !== false
      )
        return yield* refuse("Unexpected model configuration");

      const index = calls.length;

      let call: typeof ModelCall.Type = {
        startedAt: yield* Clock.currentTimeMillis,
        completedAt: null,
        firstDeltaAt: null,
        tools:
          payload.tools?.flatMap((tool) => (tool.type === "function" ? [tool.name] : [])) ?? [],
        requestJson,
        toolCalls: [],
        model: null,
        status: null,
        usage: null,
      };

      calls.push(call);
      const [response, stream] = yield* native.createResponseStream(payload);

      return [
        response,
        stream.pipe(
          Stream.tap(
            Effect.fnUntraced(function* (event) {
              const now = yield* Clock.currentTimeMillis;

              if (call.firstDeltaAt === null && event.type.endsWith(".delta")) {
                call = { ...call, firstDeltaAt: now };
                calls[index] = call;
              }
              if (
                event.type === "response.completed" ||
                event.type === "response.incomplete" ||
                event.type === "response.failed"
              ) {
                const completion = yield* Schema.decodeUnknownEffect(Completion)(
                  event.response,
                ).pipe(Effect.mapError(() => refuse("Missing or malformed provider usage")));

                call = {
                  ...call,
                  completedAt: now,
                  model: completion.model,
                  status: event.type,
                  usage: completion.usage,
                  toolCalls: completion.output.flatMap((item) =>
                    item.type === "function_call" &&
                    item.name !== undefined &&
                    item.arguments !== undefined
                      ? [{ name: item.name, arguments: item.arguments }]
                      : [],
                  ),
                };
                calls[index] = call;
              }
            }),
          ),
        ),
      ] as const;
    }),
  });

  return { client, calls };
});
