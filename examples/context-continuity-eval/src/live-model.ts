import { OpenAiClient, OpenAiSchema } from "@effect/ai-openai";
import { Clock, Effect, Ref, Schema, Semaphore, Stream } from "effect";
import { AiError } from "effect/unstable/ai";
import { HttpBody, HttpClientResponse } from "effect/unstable/http";

import { type EvaluationError, type ModelUsage } from "./contracts.ts";
import { MAX_COST_MICROUSD } from "./profiles.ts";

export const MODEL_IDS = [
  "gpt-6-astra",
  "gpt-5.6-sol",
  "gpt-5.6-terra",
  "gpt-5.6-luna",
  "gpt-5.4-mini",
] as const;

export const ModelId = Schema.Literals(MODEL_IDS);
export type ModelId = typeof ModelId.Type;
export const ReasoningEffort = Schema.Literals(["low", "medium", "high"]);

// USD per million tokens = microdollars per token. Standard, short-context pricing,
// checked 2026-09-08: https://developers.openai.com/api/docs/pricing
// Charge uncached input at the higher cache-write rate for a conservative estimate.
const prices: Readonly<Record<ModelId, { input: number; cached: number; output: number }>> = {
  "gpt-6-astra": { input: 12.5, cached: 1, output: 50 },
  "gpt-5.6-sol": { input: 5, cached: 0.4, output: 20 },
  "gpt-5.6-terra": { input: 2.5, cached: 0.2, output: 12 },
  "gpt-5.6-luna": { input: 0.25, cached: 0.02, output: 1.2 },
  "gpt-5.4-mini": { input: 0.75, cached: 0.075, output: 4.5 },
};

export const MAX_OUTPUT_TOKENS = 4_096;
export const MAX_INPUT_TOKENS = 32_000;
export const MAX_MODEL_CALLS = 200;

/** Planning only: uncached 80%-full requests, one per window, excluding output and repeated reads. */
export const productionCostPlan = (contextTokens: number) => ({
  assumedUncachedInputTokens: 12 * Math.floor(contextTokens * 0.8),
  estimateIsInvoice: false,
  models: MODEL_IDS.map((model) => ({
    model,
    inputOnlyMicrousd: Math.ceil(12 * Math.floor(contextTokens * 0.8) * prices[model].input),
  })),
});

export const RequestAudit = Schema.Struct({
  kind: Schema.Literals(["request", "response"]),
  request: Schema.Natural,
  phase: Schema.Natural,
  inputTokens: Schema.Natural,
  outputTokens: Schema.Natural,
  /** Exact synthetic provider request, or a bounded response summary. No HTTP headers. */
  json: Schema.String,
});

export type RequestAudit = typeof RequestAudit.Type;

const TokenCount = Schema.Struct({
  object: Schema.Literal("response.input_tokens"),
  input_tokens: Schema.Natural,
});

const Usage = Schema.Struct({
  input_tokens: Schema.Natural,
  output_tokens: Schema.Natural,
  total_tokens: Schema.Natural,
  input_tokens_details: Schema.Struct({ cached_tokens: Schema.Natural }),
}).check(
  Schema.makeFilter(
    (value) =>
      value.total_tokens === value.input_tokens + value.output_tokens &&
      value.input_tokens_details.cached_tokens <= value.input_tokens,
    { expected: "complete, consistent token usage" },
  ),
);

type Payload = typeof OpenAiSchema.CreateResponse.Encoded;
interface Reservation {
  readonly id: number;
  readonly phase: number;
  readonly tokens: number;
  readonly cost: number;
}
interface Spending {
  readonly closed: boolean;
  readonly failure: string | null;
  readonly calls: number;
  readonly completedCalls: number;
  readonly input: number;
  readonly output: number;
  readonly maxInput: number;
  readonly cost: number;
  readonly pending: ReadonlyMap<number, Reservation>;
  readonly models: ReadonlySet<string>;
}

/** One suite-wide budget, retained across every runtime/SQLite reacquisition. No inference retries. */
export const makeLiveClient = Effect.fn("ContextContinuity.makeLiveClient")(function* (options: {
  readonly model: ModelId;
  readonly maxCostMicrousd: number;
  readonly phase: Ref.Ref<number>;
  readonly audit: (event: RequestAudit) => Effect.Effect<void, EvaluationError>;
  readonly initialUsage?: ModelUsage;
}) {
  const native = yield* OpenAiClient.OpenAiClient;
  const price = prices[options.model];

  const state = yield* Ref.make<Spending>({
    closed:
      (options.initialUsage?.reservedCostMicrousd ?? 0) > 0 ||
      options.initialUsage?.calls !== options.initialUsage?.completedCalls,
    failure:
      (options.initialUsage?.reservedCostMicrousd ?? 0) > 0
        ? "Unresolved provider reservation survived recovery"
        : null,
    calls: options.initialUsage?.calls ?? 0,
    completedCalls: options.initialUsage?.completedCalls ?? 0,
    input: options.initialUsage?.inputTokens ?? 0,
    output: options.initialUsage?.outputTokens ?? 0,
    maxInput: options.initialUsage?.maxInputTokens ?? 0,
    cost: options.initialUsage?.estimatedCostMicrousd ?? 0,
    pending: new Map(),
    models: new Set(options.initialUsage?.returnedModels),
  });

  const semaphore = yield* Semaphore.make(1);

  const outstanding = (value: Spending) =>
    [...value.pending.values()].reduce(
      (sum, r) => sum + r.cost,
      options.initialUsage?.reservedCostMicrousd ?? 0,
    );

  const refuse = (description: string) =>
    Ref.update(state, (s) => ({ ...s, closed: true, failure: s.failure ?? description })).pipe(
      Effect.andThen(
        AiError.make({
          module: "ContextContinuityEval",
          method: "provider",
          reason: AiError.InvalidRequestError.make({ description }),
        }),
      ),
    );

  const audit = (event: RequestAudit) =>
    options
      .audit(event)
      .pipe(Effect.catch(() => refuse("Could not preserve evaluation request evidence")));

  const admit = Effect.fn("ContextContinuity.admit")(function* (original: Payload) {
    if (options.maxCostMicrousd > MAX_COST_MICROUSD || options.maxCostMicrousd <= 0)
      return yield* refuse("Evaluation spending ceiling must be positive and no greater than $10");
    if (options.model === "gpt-5.6-sol" && (yield* Clock.currentTimeMillis) >= 1_795_305_600_000)
      return yield* refuse("Refresh the Sol pricing card after its guaranteed promotional period");
    if (
      original.model !== options.model ||
      original.store !== false ||
      original.previous_response_id !== undefined ||
      original.conversation !== undefined ||
      original.background === true ||
      original.service_tier !== "default" ||
      original.max_output_tokens !== MAX_OUTPUT_TOKENS ||
      original.tools?.some((tool) => tool.type !== "function")
    ) {
      return yield* refuse("Request escaped the stateless, bounded evaluation configuration");
    }
    const before = yield* Ref.get(state);

    if (before.closed || before.calls >= MAX_MODEL_CALLS)
      return yield* refuse("Evaluation stopped or reached its model-call limit");
    const payload: Payload = { ...original, truncation: "disabled" };

    // Count the actual native request before paid inference, including Tool/output schemas.
    const tokens = yield* native.client
      .post("/responses/input_tokens", {
        body: HttpBody.jsonUnsafe({
          model: payload.model,
          input: payload.input,
          instructions: payload.instructions,
          tools: payload.tools,
          tool_choice: payload.tool_choice,
          reasoning: payload.reasoning,
          text: payload.text,
          truncation: "disabled",
        }),
      })
      .pipe(
        Effect.flatMap(HttpClientResponse.schemaBodyJson(TokenCount)),
        Effect.map((value) => value.input_tokens),
        Effect.timeout("15 seconds"),
        Effect.catch(() => refuse("Input-token preflight failed; no inference dispatched")),
      );

    if (tokens > MAX_INPUT_TOKENS)
      return yield* refuse("Outgoing input exceeded the evaluation's 32000-token bound");
    const cost = Math.ceil(tokens * price.input + MAX_OUTPUT_TOKENS * price.output);

    if (before.cost + outstanding(before) + cost > options.maxCostMicrousd)
      return yield* refuse("Insufficient evaluation budget for this entire request");

    const reservation: Reservation = {
      id: before.calls + 1,
      phase: yield* Ref.get(options.phase),
      tokens,
      cost,
    };

    yield* Ref.set(state, {
      ...before,
      calls: reservation.id,
      maxInput: Math.max(before.maxInput, tokens),
      pending: new Map([...before.pending, [reservation.id, reservation]]),
    });
    yield* audit({
      kind: "request",
      request: reservation.id,
      phase: reservation.phase,
      inputTokens: tokens,
      outputTokens: 0,
      json: JSON.stringify(payload),
    });

    return { payload, reservation };
  }, semaphore.withPermits(1));

  const settle = Effect.fn("ContextContinuity.settle")(function* (
    reservation: Reservation,
    response: OpenAiSchema.Response,
    status: "completed" | "incomplete" | "failed",
  ) {
    const usage = yield* Schema.decodeUnknownEffect(Usage)(response.usage).pipe(
      Effect.catch(() => refuse("Provider returned unmetered usage; retain the reservation")),
    );

    const cost = Math.ceil(
      (usage.input_tokens - usage.input_tokens_details.cached_tokens) * price.input +
        usage.input_tokens_details.cached_tokens * price.cached +
        usage.output_tokens * price.output,
    );

    if (
      usage.input_tokens !== reservation.tokens ||
      usage.output_tokens > MAX_OUTPUT_TOKENS ||
      cost > reservation.cost ||
      (response.service_tier !== undefined && response.service_tier !== "default")
    )
      return yield* refuse("Provider usage or service tier escaped the preflight reservation");
    const before = yield* Ref.get(state);

    if (!before.pending.has(reservation.id)) return yield* refuse("Duplicate provider completion");
    const pending = new Map(before.pending);

    pending.delete(reservation.id);
    yield* Ref.set(state, {
      ...before,
      pending,
      completedCalls: before.completedCalls + 1,
      cost: before.cost + cost,
      input: before.input + usage.input_tokens,
      output: before.output + usage.output_tokens,
      models: new Set([...before.models, response.model]),
    });
    yield* audit({
      kind: "response",
      request: reservation.id,
      phase: reservation.phase,
      inputTokens: usage.input_tokens,
      outputTokens: usage.output_tokens,
      json: JSON.stringify({ model: response.model, status, usage }),
    });
    if (status !== "completed") return yield* refuse("Provider response did not complete");
  });

  const client = OpenAiClient.OpenAiClient.of({
    ...native,
    createResponse: () =>
      refuse("Unexpected non-streaming request in the durable continuity evaluation"),
    createResponseStream: Effect.fn("ContextContinuity.createResponseStream")(function* (original) {
      const { payload, reservation } = yield* admit(original);

      const [response, stream] = yield* native.createResponseStream(payload).pipe(
        Effect.timeout("3 minutes"),
        Effect.catch(() => refuse("Provider stream could not start; retain the reservation")),
      );

      return [
        response,
        stream.pipe(
          Stream.timeout("3 minutes"),
          Stream.tap((event) => {
            if (
              event.type !== "response.completed" &&
              event.type !== "response.incomplete" &&
              event.type !== "response.failed"
            )
              return Effect.void;

            return Schema.decodeUnknownEffect(OpenAiSchema.Response)(event.response).pipe(
              Effect.catch(() => refuse("Invalid provider completion")),
              Effect.flatMap((value) =>
                settle(
                  reservation,
                  value,
                  event.type === "response.completed"
                    ? "completed"
                    : event.type === "response.incomplete"
                      ? "incomplete"
                      : "failed",
                ),
              ),
            );
          }),
          Stream.catch(() =>
            Stream.fromEffect(refuse("Provider stream failed; retain unsettled usage")),
          ),
          Stream.ensuring(
            Ref.update(state, (s) => ({
              ...s,
              closed: s.closed || s.pending.has(reservation.id),
            })),
          ),
        ),
      ] as const;
    }),
  });

  const snapshot: Effect.Effect<ModelUsage> = Ref.get(state).pipe(
    Effect.map((s) => ({
      calls: s.calls,
      completedCalls: s.completedCalls,
      inputTokens: s.input,
      outputTokens: s.output,
      maxInputTokens: s.maxInput,
      estimatedCostMicrousd: s.cost,
      reservedCostMicrousd: outstanding(s),
      returnedModels: [...s.models],
    })),
  );

  return { client, snapshot, failure: Ref.get(state).pipe(Effect.map((s) => s.failure)) };
});
