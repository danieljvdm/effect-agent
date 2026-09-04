import { type RunCostEstimator } from "@effect-agent/engine/RunOptions";
import {
  type ReviewCostControl,
  ReviewCostSnapshot,
  type ReviewStage,
  type ReviewStrategy,
  ReviewUsage,
} from "@effect-agent/pr-review/Review";
import { OpenAiClient, OpenAiSchema } from "@effect/ai-openai";
import { Clock, Effect, Exit, Ref, Schema, Semaphore, Stream } from "effect";
import { AiError, type Response } from "effect/unstable/ai";
import { HttpBody, HttpClientError, HttpClientResponse } from "effect/unstable/http";

/** Strictly below one dollar, including outstanding reservations. Not configurable upward. */
export const REVIEW_COST_LIMIT_MICROUSD = 999_999;
export const REVIEW_DISCOVERY_COST_LIMIT_MICROUSD = 699_999;
export const REVIEW_VERIFICATION_RESERVE_MICROUSD = 300_000;
export const REVIEW_MAX_INPUT_TOKENS = 128_000;
export const REVIEW_MAX_OUTPUT_TOKENS = 32_000;
export const REVIEW_PRICING_VERSION = "openai-standard-2026-08-30";
// Refresh the rate card before Sol's guaranteed promotional window ends.
export const REVIEW_PRICING_VALID_UNTIL = 1_795_305_600_000; // 2026-11-22T00:00:00Z

interface Pricing {
  readonly label: string;
  readonly url: string;
  readonly input: number;
  readonly read: number;
  readonly write: number;
  readonly output: number;
}

// Hundredths of a microdollar per token. Direct OpenAI, standard tier, <=128k input.
// https://developers.openai.com/api/docs/pricing
const sol: Pricing = {
  label: "GPT-5.6 Sol",
  url: "https://developers.openai.com/api/docs/models/gpt-5.6-sol",
  input: 400,
  read: 40,
  write: 500,
  output: 2_000,
};

const modelPricing: Readonly<Record<string, Pricing>> = {
  "gpt-5.6": sol,
  "gpt-5.6-sol": sol,
  "gpt-5.6-terra": {
    label: "GPT-5.6 Terra",
    url: "https://developers.openai.com/api/docs/models/gpt-5.6-terra",
    input: 200,
    read: 20,
    write: 250,
    output: 1_200,
  },
  "gpt-5.6-luna": {
    label: "GPT-5.6 Luna",
    url: "https://developers.openai.com/api/docs/models/gpt-5.6-luna",
    input: 20,
    read: 2,
    write: 25,
    output: 120,
  },
};

export const reviewModelPricing = (model: string): Pricing | undefined =>
  Object.hasOwn(modelPricing, model) ? modelPricing[model] : undefined;

/** Raw Effect OpenAI usage includes cache writes in uncached; charge each token once. */
export const estimateGpt56CostMicrousd = (
  model: string,
  usage: Response.Usage,
): number | undefined => {
  const pricing = reviewModelPricing(model);

  if (pricing === undefined) return undefined;
  const read = usage.inputTokens.cacheRead ?? 0;
  const total = usage.inputTokens.total ?? (usage.inputTokens.uncached ?? 0) + read;
  const uncached = Math.max(0, total - read);
  const write = Math.min(uncached, usage.inputTokens.cacheWrite ?? 0);

  return Math.ceil(
    ((uncached - write) * pricing.input +
      read * pricing.read +
      write * pricing.write +
      (usage.outputTokens.total ?? 0) * pricing.output) /
      100,
  );
};

export const reviewCostEstimator =
  (model: string): RunCostEstimator =>
  (usage) =>
    Effect.succeed({
      costMicrousd: estimateGpt56CostMicrousd(model, usage) ?? 0,
      serviceTier: "default",
      pricingVersion: REVIEW_PRICING_VERSION,
    });

const CacheBreakpoint = Schema.Struct({ mode: Schema.Literal("explicit") });

const CacheOptions = Schema.Struct({
  mode: Schema.Literal("explicit"),
  ttl: Schema.Literal("30m"),
});

export const REVIEW_CACHE_POLICY = CacheOptions.make({ mode: "explicit", ttl: "30m" });

const InputTokenCount = Schema.Struct({
  object: Schema.Literal("response.input_tokens"),
  input_tokens: Schema.Natural,
});

const ChargedUsage = Schema.Struct({
  input_tokens: Schema.Natural,
  output_tokens: Schema.Natural,
  total_tokens: Schema.Natural,
  input_tokens_details: Schema.Struct({
    cached_tokens: Schema.Natural,
    cache_write_tokens: Schema.Natural,
  }),
}).check(
  Schema.makeFilter(
    (usage) =>
      usage.input_tokens + usage.output_tokens === usage.total_tokens &&
      usage.input_tokens_details.cached_tokens + usage.input_tokens_details.cache_write_tokens <=
        usage.input_tokens,
    { title: "Disjoint, complete provider usage" },
  ),
);

type Payload = typeof OpenAiSchema.CreateResponse.Encoded;
const breakpoint = CacheBreakpoint.make({ mode: "explicit" });

const cacheContent = (content: string | ReadonlyArray<OpenAiSchema.InputContent>, mark = true) => {
  const parts =
    typeof content === "string" ? [{ type: "input_text" as const, text: content }] : content;

  return parts.map((part, index) =>
    mark && index === parts.length - 1 ? { ...part, prompt_cache_breakpoint: breakpoint } : part,
  );
};

/**
 * rc.111's native client serializes its payload without stripping extra fields.
 * Decorate that supported boundary; keep upstream message/tool encoding and SSE decoding.
 * Breakpoints stay on earlier messages as history grows. Status is outgoing-only.
 */
export const withReviewPromptCache = (payload: Payload, key: string) => ({
  ...payload,
  prompt_cache_key: key,
  prompt_cache_options: REVIEW_CACHE_POLICY,
  input:
    typeof payload.input === "string"
      ? [{ role: "user" as const, content: cacheContent(payload.input) }]
      : payload.input?.map((item, index, items) => {
          if ("role" in item && item.role !== "assistant") {
            const text =
              typeof item.content === "string"
                ? item.content
                : item.content.length === 1 && item.content[0]?.type === "input_text"
                  ? item.content[0].text
                  : "";

            if (item.role === "user" && text.startsWith("<run-status>")) {
              return item;
            }

            return { ...item, content: cacheContent(item.content) };
          }
          if (item.type === "function_call_output") {
            // One boundary per committed batch keeps earlier useful boundaries
            // inside the provider's breakpoint lookup window on wide batches.
            return {
              ...item,
              output: cacheContent(item.output, items[index + 1]?.type !== "function_call_output"),
            };
          }

          return item;
        }),
});

const admissionError = (description: string) =>
  AiError.make({
    module: "ReviewOpenAi",
    method: "admit",
    reason: AiError.InvalidRequestError.make({ description }),
  });

interface Reservation {
  readonly id: number;
  readonly stage: ReviewStage;
  readonly inputTokens: number;
  readonly outputTokens: number;
  readonly microusd: number;
  readonly outputLimitedByCost: boolean;
}

interface StageSpending {
  readonly stopped: boolean;
  readonly inputLimitExceeded: boolean;
  readonly modelCalls: number;
  readonly input: number;
  readonly read: number;
  readonly write: number;
  readonly output: number;
  readonly cost: number;
}

interface Spending {
  readonly stage: ReviewStage;
  readonly closed: boolean;
  readonly stages: Readonly<Record<ReviewStage, StageSpending>>;
  readonly pending: ReadonlyMap<number, Reservation>;
}

const emptyStage = (): StageSpending => ({
  stopped: false,
  inputLimitExceeded: false,
  modelCalls: 0,
  input: 0,
  read: 0,
  write: 0,
  output: 0,
  cost: 0,
});

const totals = (state: Spending) => {
  const discovery = state.stages.discovery;
  const verification = state.stages.verification;

  return {
    modelCalls: discovery.modelCalls + verification.modelCalls,
    input: discovery.input + verification.input,
    read: discovery.read + verification.read,
    write: discovery.write + verification.write,
    output: discovery.output + verification.output,
    cost: discovery.cost + verification.cost,
  };
};

const reservedCost = (state: Spending, stage?: ReviewStage) =>
  [...state.pending.values()].reduce(
    (total, item) => total + (stage === undefined || stage === item.stage ? item.microusd : 0),
    0,
  );

/** One ephemeral review's client and spending ledger. Never share it between review attempts. */
export const makeReviewOpenAi = Effect.fn("makeReviewOpenAi")(function* (options: {
  readonly client: OpenAiClient.Service;
  readonly model: string;
  readonly cacheKey: string;
  readonly strategy?: ReviewStrategy;
}) {
  const pricing = reviewModelPricing(options.model);

  if (pricing === undefined) {
    return yield* admissionError("The review model has no verified standard-tier price.");
  }

  const state = yield* Ref.make<Spending>({
    stage: "discovery",
    closed: false,
    stages: { discovery: emptyStage(), verification: emptyStage() },
    pending: new Map(),
  });

  const admissions = yield* Semaphore.make(1);
  const close = Ref.update(state, (current) => ({ ...current, closed: true }));

  const refuse = (message: string) =>
    close.pipe(Effect.andThen(Effect.fail(admissionError(message))));

  const stopStage = (stage: ReviewStage, inputLimitExceeded = false) =>
    Ref.update(state, (current) => ({
      ...current,
      stages: {
        ...current.stages,
        [stage]: {
          ...current.stages[stage],
          stopped: !inputLimitExceeded || current.stages[stage].stopped,
          inputLimitExceeded: inputLimitExceeded || current.stages[stage].inputLimitExceeded,
        },
      },
    }));

  const stageLimit = (stage: ReviewStage) =>
    options.strategy === "verified" && stage === "discovery"
      ? REVIEW_DISCOVERY_COST_LIMIT_MICROUSD
      : REVIEW_COST_LIMIT_MICROUSD;

  const countAttempt = Effect.fn("ReviewOpenAi.countAttempt")(function* (payload: Payload) {
    // This endpoint does no inference. Count the exact outgoing token-affecting
    // fields, without truncation or mutable server-side conversation.
    const response = yield* options.client.client.post("/responses/input_tokens", {
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
    });

    return (yield* HttpClientResponse.schemaBodyJson(InputTokenCount)(response)).input_tokens;
  }, Effect.timeout("10 seconds"));

  const transientCountFailure = (error: Effect.Error<ReturnType<typeof countAttempt>>): boolean =>
    error._tag === "TimeoutError" ||
    (HttpClientError.isHttpClientError(error) &&
      (error.reason._tag === "TransportError" ||
        [408, 429, 500, 502, 503, 504].includes(error.response?.status ?? 0)));

  const count = Effect.fn("ReviewOpenAi.count")(function* (payload: Payload) {
    let attempt = 0;

    return yield* Effect.suspend(() => {
      attempt += 1;

      return countAttempt(payload).pipe(
        Effect.tapError((error) =>
          Effect.logWarning("Review preflight failed", {
            phase: "input-token-count",
            attempt,
            failureType: error._tag,
            ...(HttpClientError.isHttpClientError(error)
              ? { reason: error.reason._tag, status: error.response?.status }
              : {}),
            retrying: attempt === 1 && transientCountFailure(error),
          }),
        ),
      );
    }).pipe(Effect.retry({ times: 1, while: transientCountFailure }));
  });

  const admit = Effect.fn("ReviewOpenAi.admit")(function* (original: Payload) {
    const now = yield* Clock.currentTimeMillis;

    if (
      now >= REVIEW_PRICING_VALID_UNTIL ||
      original.model !== options.model ||
      original.service_tier !== "default" ||
      original.store !== false ||
      original.conversation !== undefined ||
      original.previous_response_id !== undefined ||
      original.background === true ||
      original.modalities !== undefined ||
      original.tools?.some((tool) => tool.type !== "function") ||
      !Number.isSafeInteger(original.max_output_tokens) ||
      (original.max_output_tokens ?? 0) < 16 ||
      (original.max_output_tokens ?? 0) > REVIEW_MAX_OUTPUT_TOKENS
    ) {
      return yield* refuse("The review request is outside the verified pricing contract.");
    }
    const before = yield* Ref.get(state);

    if (before.closed) return yield* refuse("Review spending admission has already stopped.");
    if (before.stages[before.stage].stopped || before.stages[before.stage].inputLimitExceeded)
      return yield* admissionError("This review stage has already stopped.");
    const beforeTotals = totals(before);
    const allowance = stageLimit(before.stage);
    const balance = allowance - beforeTotals.cost - reservedCost(before);

    // Outgoing-only host feedback. Count these exact bytes before reserving or
    // dispatching; withReviewPromptCache leaves run-status outside the cache.
    // The output allowance is determined after this count, so do not advertise
    // a token allowance calculated for an earlier or smaller prompt.
    const spendingStatus = [
      "<run-status>",
      `Review balance before this request: $${(balance / 1_000_000).toFixed(6)} of the $${(allowance / 1_000_000).toFixed(6)} ceiling. Estimated charges: $${(beforeTotals.cost / 1_000_000).toFixed(6)}. Outstanding reservations: $${(reservedCost(before) / 1_000_000).toFixed(6)}.`,
      `This request must first reserve its entire input at the full cache-miss rate of $${(pricing.write / 100).toFixed(2)} per million tokens; only the remainder can fund reasoning and output at $${(pricing.output / 100).toFixed(2)} per million tokens. Cache hits reduce the settled charge, not the required reservation.`,
      "</run-status>",
    ].join("\n");

    const payload: Payload = withReviewPromptCache(
      {
        ...original,
        truncation: "disabled",
        input: [
          ...(typeof original.input === "string"
            ? [{ role: "user" as const, content: original.input }]
            : (original.input ?? [])),
          { role: "user", content: spendingStatus },
        ],
      },
      options.cacheKey,
    );

    const inputTokens = yield* count(payload).pipe(
      Effect.catch(() => refuse("Unable to count the review input before paid inference.")),
    );

    if (inputTokens > REVIEW_MAX_INPUT_TOKENS) {
      yield* stopStage(before.stage, true);
      yield* Effect.logInfo("Review input-token limit reached before dispatch", {
        stage: before.stage,
        modelCalls: beforeTotals.modelCalls,
        inputTokens,
        inputTokenLimit: REVIEW_MAX_INPUT_TOKENS,
      });

      return yield* admissionError(
        "The counted review input exceeds the 128,000-token price boundary.",
      );
    }
    const requestedOutputTokens = original.max_output_tokens ?? REVIEW_MAX_OUTPUT_TOKENS;

    const outputTokens = Math.min(
      requestedOutputTokens,
      Math.floor((balance * 100 - inputTokens * pricing.write) / pricing.output),
    );

    if (outputTokens < 16) {
      yield* stopStage(before.stage);
      yield* Effect.logInfo("Review spending limit reached before dispatch", {
        stage: before.stage,
        modelCalls: beforeTotals.modelCalls,
        inputTokens,
        remainingCostMicrousd: balance,
        minimumRequestCostMicrousd: Math.ceil(
          (inputTokens * pricing.write + 16 * pricing.output) / 100,
        ),
        costLimitMicrousd: REVIEW_COST_LIMIT_MICROUSD,
        stageCostLimitMicrousd: allowance,
      });

      return yield* admissionError("No paid request fits this stage's remaining allowance.");
    }
    // A smaller output allowance still permits research. Only a refused request or a
    // response truncated by this cost limit stops the review; tool choice stays native.
    const outputLimitedByCost = outputTokens < requestedOutputTokens;
    const microusd = Math.ceil((inputTokens * pricing.write + outputTokens * pricing.output) / 100);

    const reservation: Reservation = {
      id: beforeTotals.modelCalls + 1,
      stage: before.stage,
      inputTokens,
      outputTokens,
      microusd,
      outputLimitedByCost,
    };

    const current = yield* Ref.get(state);

    if (
      current.closed ||
      current.stage !== before.stage ||
      totals(current).cost + reservedCost(current) + microusd > allowance
    ) {
      return yield* refuse("The review's remaining spending allowance changed before dispatch.");
    }
    yield* Ref.update(state, (current) => ({
      ...current,
      stages: {
        ...current.stages,
        [reservation.stage]: {
          ...current.stages[reservation.stage],
          modelCalls: current.stages[reservation.stage].modelCalls + 1,
        },
      },
      pending: new Map([...current.pending, [reservation.id, reservation]]),
    }));
    yield* Effect.logInfo("Review request admitted", {
      modelCall: reservation.id,
      stage: reservation.stage,
      toolDefinitions: payload.tools?.length ?? 0,
      inputTokens,
      requestedMaxOutputTokens: requestedOutputTokens,
      maxOutputTokens: outputTokens,
      outputLimitedByCost,
      reservedCostMicrousd: microusd,
      remainingCostMicrousd: balance - microusd,
      costLimitMicrousd: REVIEW_COST_LIMIT_MICROUSD,
      stageCostLimitMicrousd: allowance,
      cacheMode: "explicit",
      serviceTier: "default",
      pricingVersion: REVIEW_PRICING_VERSION,
    });

    return { payload: { ...payload, max_output_tokens: outputTokens }, reservation };
  }, admissions.withPermit);

  const settle = Effect.fn("ReviewOpenAi.settle")(function* (
    reservation: Reservation,
    response: OpenAiSchema.Response,
  ) {
    // Providers can emit the same terminal response more than once. A settled
    // request cannot charge again or invalidate its already accepted accounting.
    if (!(yield* Ref.get(state)).pending.has(reservation.id)) return;

    const usage = yield* Schema.decodeUnknownEffect(ChargedUsage)(response.usage).pipe(
      Effect.catch(() =>
        refuse("Provider usage is missing or invalid; retain its full reservation."),
      ),
    );

    const canonicalModel = options.model === "gpt-5.6" ? "gpt-5.6-sol" : options.model;

    if (
      !(
        response.model === options.model ||
        response.model === canonicalModel ||
        response.model.startsWith(`${canonicalModel}-`)
      ) ||
      response.service_tier !== "default" ||
      usage.input_tokens > reservation.inputTokens ||
      usage.output_tokens > reservation.outputTokens
    ) {
      return yield* refuse("Provider response violated the counted standard-tier price contract.");
    }
    const read = usage.input_tokens_details.cached_tokens;
    const write = usage.input_tokens_details.cache_write_tokens;
    const ordinary = usage.input_tokens - read - write;

    const cost = Math.ceil(
      (ordinary * pricing.input +
        read * pricing.read +
        write * pricing.write +
        usage.output_tokens * pricing.output) /
        100,
    );

    const outputLimitReached =
      reservation.outputLimitedByCost &&
      response.incomplete_details?.reason === "max_output_tokens";

    const updated = yield* Ref.modify(state, (current) => {
      if (!current.pending.has(reservation.id)) return [false, current] as const;
      const pending = new Map(current.pending);
      const stage = current.stages[reservation.stage];

      pending.delete(reservation.id);

      return [
        true,
        {
          ...current,
          pending,
          stages: {
            ...current.stages,
            [reservation.stage]: {
              ...stage,
              stopped: stage.stopped || outputLimitReached,
              input: stage.input + usage.input_tokens,
              read: stage.read + read,
              write: stage.write + write,
              output: stage.output + usage.output_tokens,
              cost: stage.cost + cost,
            },
          },
        },
      ] as const;
    });

    if (!updated) return;
    const current = yield* Ref.get(state);
    const total = totals(current);

    yield* Effect.logInfo("Review model usage", {
      modelCall: reservation.id,
      stage: reservation.stage,
      functionCalls: response.output.filter((item) => item.type === "function_call").length,
      completionCalls: response.output.filter(
        (item) =>
          item.type === "function_call" &&
          (item.name === "submit_review" || item.name === "submit_verification"),
      ).length,
      inputTokens: usage.input_tokens,
      uncachedInputTokens: ordinary,
      cachedInputTokens: read,
      cacheWriteInputTokens: write,
      outputTokens: usage.output_tokens,
      outputLimitReached,
      cacheHitRatio: usage.input_tokens === 0 ? 0 : read / usage.input_tokens,
      estimatedCostMicrousd: cost,
      cumulativeCostMicrousd: total.cost,
      reservedCostMicrousd: reservedCost(current),
      remainingCostMicrousd: REVIEW_COST_LIMIT_MICROUSD - total.cost - reservedCost(current),
    });
  });

  const usageSnapshot = (amount: ReturnType<typeof totals>, reservedCostMicrousd: number) =>
    ReviewUsage.make({
      inputTokens: amount.input,
      uncachedInputTokens: amount.input - amount.read - amount.write,
      cachedInputTokens: amount.read,
      cacheWriteInputTokens: amount.write,
      outputTokens: amount.output,
      estimatedCostMicrousd: amount.cost,
      reservedCostMicrousd,
    });

  const costControl: ReviewCostControl = {
    beginStage: (stage) =>
      Ref.update(state, (current) => {
        if (stage === current.stage) return current;
        if (current.stage === "verification" || options.strategy !== "verified")
          return { ...current, closed: true };

        return { ...current, stage };
      }).pipe(admissions.withPermit),
    snapshot: Effect.map(Ref.get(state), (current) =>
      ReviewCostSnapshot.make({
        stage: current.stage,
        globalStopped: current.closed,
        stopped: current.stages[current.stage].stopped,
        ...(current.stages[current.stage].inputLimitExceeded ? { inputLimitExceeded: true } : {}),
        modelCalls: totals(current).modelCalls,
        usage: usageSnapshot(totals(current), reservedCost(current)),
        stages: (["discovery", "verification"] as const).map((stage) => ({
          stage,
          stopped: current.stages[stage].stopped,
          ...(current.stages[stage].inputLimitExceeded
            ? { inputLimitExceeded: true as const }
            : {}),
          modelCalls: current.stages[stage].modelCalls,
          usage: usageSnapshot(current.stages[stage], reservedCost(current, stage)),
        })),
      }),
    ),
  };

  const client = OpenAiClient.OpenAiClient.of({
    ...options.client,
    createResponse: Effect.fn("ReviewOpenAi.createResponse")(
      function* (original) {
        const { payload, reservation } = yield* admit(original);

        const result = yield* options.client
          .createResponse(payload)
          .pipe(Effect.catch(() => refuse("OpenAI request failed; retain its full reservation.")));

        yield* settle(reservation, result[0]);

        return result;
      },
      Effect.onExit((exit) =>
        Exit.hasDies(exit) || Exit.hasInterrupts(exit) ? close : Effect.void,
      ),
    ),
    createResponseStream: Effect.fn("ReviewOpenAi.createResponseStream")(
      function* (original) {
        const { payload, reservation } = yield* admit(original);

        const [response, stream] = yield* options.client
          .createResponseStream(payload)
          .pipe(Effect.catch(() => refuse("OpenAI request failed; retain its full reservation.")));

        return [
          response,
          stream.pipe(
            Stream.tap((event) => {
              if (
                event.type !== "response.completed" &&
                event.type !== "response.incomplete" &&
                event.type !== "response.failed"
              )
                return Effect.void;

              return Schema.decodeUnknownEffect(OpenAiSchema.Response)(event.response).pipe(
                Effect.mapError(() =>
                  admissionError("Invalid provider completion; retain its reservation."),
                ),
                Effect.flatMap((completed) => settle(reservation, completed)),
              );
            }),
            Stream.catch(() =>
              Stream.fromEffect(refuse("OpenAI stream failed; retain any unsettled reservation.")),
            ),
            Stream.ensuring(
              Effect.flatMap(Ref.get(state), (current) =>
                current.pending.has(reservation.id) ? close : Effect.void,
              ),
            ),
            Stream.onExit((exit) => (Exit.isFailure(exit) ? close : Effect.void)),
          ),
        ] as const;
      },
      Effect.onExit((exit) =>
        Exit.hasDies(exit) || Exit.hasInterrupts(exit) ? close : Effect.void,
      ),
    ),
  });

  return { client, costControl };
});
