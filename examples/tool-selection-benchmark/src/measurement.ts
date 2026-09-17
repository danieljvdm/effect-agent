import { DecisionModel, DecisionSchema } from "@effect-agent/ai-decision";
import { OpenAiClient, type OpenAiSchema } from "@effect/ai-openai";
import { Cause, Clock, Context, Effect, Exit, Schema, Stream } from "effect";
import { AiError } from "effect/unstable/ai";

import { makeAvailabilityNotes, withStableTools } from "./stable-tools.ts";

export const Arm = Schema.Literals([
  "all-50",
  "fixed-8-keyword",
  "jev-8-keyword",
  "fixed-8-jev",
  "jev-8-jev",
  "all-50-discovery",
  "stable-fixed-8-jev",
  "stable-jev-8-jev",
  "probe-fixed",
  "probe-filtered",
  "probe-allowed",
  "informed-fixed-8-jev",
  "informed-jev-8-jev",
]);

export type Arm = typeof Arm.Type;
export const arms: ReadonlyArray<Arm> = Arm.literals.slice(0, 5);
export const informedArms: ReadonlyArray<Arm> = Arm.literals.slice(11);
export const cacheArms: ReadonlyArray<Arm> = [...Arm.literals.slice(0, 8), ...informedArms];
export const probeArms: ReadonlyArray<Arm> = Arm.literals.slice(8, 11);
export const Suite = Schema.Literals(["discovery", "cache", "probe", "informed"]);
export const ContextSize = Schema.Literals(["short", "reference"]);

export const Usage = Schema.Struct({
  input_tokens: Schema.Natural,
  output_tokens: Schema.Natural,
  input_tokens_details: Schema.Struct({
    cached_tokens: Schema.Natural,
    cache_write_tokens: Schema.optionalKey(Schema.Natural),
  }),
  output_tokens_details: Schema.Struct({ reasoning_tokens: Schema.Natural }),
});

export const ModelCall = Schema.Struct({
  startedAt: Schema.Finite,
  completedAt: Schema.NullOr(Schema.Finite),
  firstDeltaAt: Schema.NullOr(Schema.Finite),
  tools: Schema.Array(Schema.String),
  callableTools: Schema.Array(Schema.String),
  toolCalls: Schema.Array(Schema.Struct({ name: Schema.String, arguments: Schema.String })),
  requestJson: Schema.String,
  model: Schema.NullOr(Schema.String),
  status: Schema.NullOr(Schema.String),
  usage: Schema.NullOr(Usage),
});

export const DecisionCall = Schema.Struct({
  phase: Schema.Literals(["initial", "discovery"]),
  startedAt: Schema.Finite,
  completedAt: Schema.NullOr(Schema.Finite),
  request: DecisionSchema.EvaluateRequest,
  response: Schema.NullOr(DecisionSchema.EvaluateResponse),
  failure: Schema.NullOr(Schema.String),
});

export const Selection = Schema.Struct({
  elapsedMs: Schema.Finite,
  selected: Schema.Array(Schema.String),
});

export const Sample = Schema.Struct({
  arm: Arm,
  task: Schema.String,
  repetition: Schema.Natural,
  context: ContextSize,
  forcedMiss: Schema.Boolean,
  elapsedMs: Schema.Finite,
  success: Schema.Boolean,
  answer: Schema.NullOr(Schema.Array(Schema.String)),
  failure: Schema.NullOr(Schema.String),
  modelCalls: Schema.Array(ModelCall),
  decisionCalls: Schema.Array(DecisionCall),
  toolCalls: Schema.Array(
    Schema.Struct({ name: Schema.String, id: Schema.String, at: Schema.Finite }),
  ),
  selections: Schema.Array(Selection),
});

export class BenchmarkError extends Schema.TaggedError<BenchmarkError>()("BenchmarkError", {
  message: Schema.String,
}) {}

export const refuse = (description: string) =>
  AiError.AiError.make({
    module: "ToolSelectionBenchmark",
    method: "provider",
    reason: AiError.InvalidRequestError.make({ description }),
  });

/** Append-only checkpoints retain in-flight paid calls even if the process is interrupted. */
export class Journal extends Context.Service<
  Journal,
  {
    readonly record: (kind: string, payload: string) => Effect.Effect<void, AiError.AiError>;
  }
>()("ToolSelectionBenchmark/Journal") {}

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

// No token-count preflight or retries. Capture providers and checkpoint sink once per sample.
export const instrument = Effect.fn("ToolSelectionBenchmark.instrument")(function* (
  expectedInitialTools: number,
  options: {
    readonly stableTools?: ReadonlyArray<typeof OpenAiSchema.Tool.Encoded>;
    readonly maxCalls?: number;
    readonly availabilityNotes?: boolean;
  } = {},
) {
  const native = yield* OpenAiClient.OpenAiClient;
  const nativeDecision = yield* DecisionModel.DecisionModel;
  const journal = yield* Journal;
  const calls: Array<typeof ModelCall.Type> = [];
  const decisions: Array<typeof DecisionCall.Type> = [];
  const annotateAvailability = makeAvailabilityNotes();

  const saveModel = (call: typeof ModelCall.Type) =>
    journal.record("model", Schema.encodeSync(Schema.fromJsonString(ModelCall))(call));

  const saveDecision = (call: typeof DecisionCall.Type) =>
    journal.record("decision", Schema.encodeSync(Schema.fromJsonString(DecisionCall))(call));

  const decision = yield* DecisionModel.make({
    evaluate: Effect.fn("ToolSelectionBenchmark.decision")(function* (request) {
      if (decisions.length >= (options.maxCalls ?? 6) + 1)
        return yield* refuse("Decision-call bound exceeded");
      const index = decisions.length;

      let call: typeof DecisionCall.Type = {
        phase: typeof request.state === "string" ? "initial" : "discovery",
        startedAt: yield* Clock.currentTimeMillis,
        completedAt: null,
        request,
        response: null,
        failure: null,
      };

      decisions.push(call);
      yield* saveDecision(call);

      return yield* nativeDecision.evaluate(request).pipe(
        Effect.onExit((exit) =>
          Effect.gen(function* () {
            call = {
              ...call,
              completedAt: yield* Clock.currentTimeMillis,
              response: Exit.isSuccess(exit)
                ? yield* Schema.decodeUnknownEffect(DecisionSchema.EvaluateResponse)(
                    exit.value,
                  ).pipe(Effect.mapError(() => refuse("Malformed decision response evidence")))
                : null,
              failure: Exit.isFailure(exit) ? Cause.pretty(exit.cause) : null,
            };
            decisions[index] = call;
            yield* saveDecision(call);
          }),
        ),
      );
    }),
  });

  const client = OpenAiClient.OpenAiClient.of({
    ...native,
    createResponse: () => refuse("Expected streaming requests"),
    createResponseStream: Effect.fn("ToolSelectionBenchmark.response")(function* (original) {
      if (calls.length === 0 && original.tools?.length !== expectedInitialTools)
        return yield* refuse("Initial tool count does not match the benchmark arm");

      const stable = options.stableTools
        ? yield* withStableTools(original, options.stableTools)
        : original;

      const callableTools =
        original.tool_choice === "none"
          ? []
          : (original.tools?.flatMap((tool) => (tool.type === "function" ? [tool.name] : [])) ??
            []);

      const payload = options.availabilityNotes
        ? yield* annotateAvailability(stable, callableTools)
        : stable;

      const requestJson = JSON.stringify(payload);

      if (
        calls.length >= (options.maxCalls ?? 6) ||
        new TextEncoder().encode(requestJson).byteLength > 262_144
      )
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
        callableTools,
        requestJson,
        toolCalls: [],
        model: null,
        status: null,
        usage: null,
      };

      calls.push(call);
      yield* saveModel(call);
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
                yield* saveModel(call);
              }
            }),
          ),
        ),
      ] as const;
    }),
  });

  return { client, decision, calls, decisions };
});
