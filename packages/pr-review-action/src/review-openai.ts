import {
  type ReviewCostControl,
  ReviewCostSnapshot,
  ReviewUsage,
  type RunCostEstimator,
} from "@effect-agent/pr-review";
import { OpenAiClient, OpenAiSchema } from "@effect/ai-openai";
import { Clock, Effect, Exit, Ref, Schema, Semaphore, Stream } from "effect";
import { AiError, type Response } from "effect/unstable/ai";
import { HttpBody, HttpClientResponse } from "effect/unstable/http";

/** Strictly below one dollar, including outstanding reservations. Not configurable upward. */
export const REVIEW_COST_LIMIT_MICROUSD = 999_999;
const MAX_INPUT_TOKENS = 128_000;
const MAX_OUTPUT_TOKENS = 32_000;
const PRICING_VERSION = "openai-standard-2026-08-30";
// Refresh the rate card before Sol's guaranteed promotional window ends.
const PRICING_VALID_UNTIL = 1_795_305_600_000; // 2026-11-22T00:00:00Z

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
      pricingVersion: PRICING_VERSION,
    });

const CacheBreakpoint = Schema.Struct({ mode: Schema.Literal("explicit") });
const CacheOptions = Schema.Struct({
  mode: Schema.Literal("explicit"),
  ttl: Schema.Literal("30m"),
});
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
  prompt_cache_options: CacheOptions.make({ mode: "explicit", ttl: "30m" }),
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
  readonly inputTokens: number;
  readonly outputTokens: number;
  readonly microusd: number;
  readonly outputLimitedByCost: boolean;
}

interface Spending {
  readonly stopped: boolean;
  readonly closed: boolean;
  readonly modelCalls: number;
  readonly pending: ReadonlyMap<number, Reservation>;
  readonly input: number;
  readonly read: number;
  readonly write: number;
  readonly output: number;
  readonly cost: number;
}

const reservedCost = (state: Spending) =>
  [...state.pending.values()].reduce((total, item) => total + item.microusd, 0);

/** One ephemeral review's client and spending ledger. Never share it between review attempts. */
export const makeReviewOpenAi = Effect.fn("makeReviewOpenAi")(function* (options: {
  readonly client: OpenAiClient.Service;
  readonly model: string;
  readonly cacheKey: string;
}) {
  const pricing = reviewModelPricing(options.model);
  if (pricing === undefined) {
    return yield* admissionError("The review model has no verified standard-tier price.");
  }
  const state = yield* Ref.make<Spending>({
    stopped: false,
    closed: false,
    modelCalls: 0,
    pending: new Map(),
    input: 0,
    read: 0,
    write: 0,
    output: 0,
    cost: 0,
  });
  const admissions = yield* Semaphore.make(1);
  const close = Ref.update(state, (current) => ({ ...current, closed: true }));
  const refuse = (message: string) =>
    close.pipe(Effect.andThen(Effect.fail(admissionError(message))));
  const count = Effect.fn("ReviewOpenAi.count")(function* (payload: Payload) {
    // Token-affecting fields emitted by the pinned native encoder. No inference, retry,
    // truncation, provider tools, or mutable server-side conversation is used.
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
  });
  const admit = Effect.fn("ReviewOpenAi.admit")(function* (original: Payload) {
    const now = yield* Clock.currentTimeMillis;
    if (
      now >= PRICING_VALID_UNTIL ||
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
      (original.max_output_tokens ?? 0) > MAX_OUTPUT_TOKENS
    ) {
      return yield* refuse("The review request is outside the verified pricing contract.");
    }
    const before = yield* Ref.get(state);
    if (before.closed) return yield* refuse("Review spending admission has already stopped.");
    const balance = REVIEW_COST_LIMIT_MICROUSD - before.cost - reservedCost(before);
    // Outgoing-only host feedback. Count these exact bytes before reserving or
    // dispatching; withReviewPromptCache leaves run-status outside the cache.
    // The output allowance is determined after this count, so do not advertise
    // a token allowance calculated for an earlier or smaller prompt.
    const spendingStatus = [
      "<run-status>",
      `Review balance before this request: $${(balance / 1_000_000).toFixed(6)} of the $${(REVIEW_COST_LIMIT_MICROUSD / 1_000_000).toFixed(6)} ceiling. Estimated charges: $${(before.cost / 1_000_000).toFixed(6)}. Outstanding reservations: $${(reservedCost(before) / 1_000_000).toFixed(6)}.`,
      `This request must first reserve its entire input at the full cache-miss rate of $${(pricing.write / 100).toFixed(2)} per million tokens; only the remainder can fund reasoning and output at $${(pricing.output / 100).toFixed(2)} per million tokens. Cache hits reduce the settled charge, not the required reservation.`,
      "More source grows future requests and shrinks their output allowance. Finish once the supplied patches and concrete defect questions are assessed. If any supplied patch remains unassessed, submit incomplete rather than claiming completion.",
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
    if (inputTokens > MAX_INPUT_TOKENS) {
      return yield* refuse("The counted review input exceeds the 128,000-token price boundary.");
    }
    const requestedOutputTokens = original.max_output_tokens ?? MAX_OUTPUT_TOKENS;
    const outputTokens = Math.min(
      requestedOutputTokens,
      Math.floor((balance * 100 - inputTokens * pricing.write) / pricing.output),
    );
    if (outputTokens < 16) {
      yield* Ref.update(state, (current) => ({ ...current, stopped: true }));
      yield* Effect.logInfo("Review spending limit reached before dispatch", {
        modelCalls: before.modelCalls,
        inputTokens,
        remainingCostMicrousd: balance,
        minimumRequestCostMicrousd: Math.ceil(
          (inputTokens * pricing.write + 16 * pricing.output) / 100,
        ),
        costLimitMicrousd: REVIEW_COST_LIMIT_MICROUSD,
      });
      return yield* refuse("No paid request fits; deliver recorded findings without inference.");
    }
    // A smaller output allowance still permits research. Only a refused request or a
    // response truncated by this cost limit stops the review; tool choice stays native.
    const outputLimitedByCost = outputTokens < requestedOutputTokens;
    const microusd = Math.ceil((inputTokens * pricing.write + outputTokens * pricing.output) / 100);
    const reservation: Reservation = {
      id: before.modelCalls + 1,
      inputTokens,
      outputTokens,
      microusd,
      outputLimitedByCost,
    };
    const current = yield* Ref.get(state);
    if (
      current.closed ||
      current.cost + reservedCost(current) + microusd > REVIEW_COST_LIMIT_MICROUSD
    ) {
      return yield* refuse("The review's remaining spending allowance changed before dispatch.");
    }
    yield* Ref.update(state, (current) => ({
      ...current,
      modelCalls: reservation.id,
      pending: new Map([...current.pending, [reservation.id, reservation]]),
    }));
    yield* Effect.logInfo("Review request admitted", {
      modelCall: reservation.id,
      toolDefinitions: payload.tools?.length ?? 0,
      inputTokens,
      requestedMaxOutputTokens: requestedOutputTokens,
      maxOutputTokens: outputTokens,
      outputLimitedByCost,
      reservedCostMicrousd: microusd,
      remainingCostMicrousd: balance - microusd,
      costLimitMicrousd: REVIEW_COST_LIMIT_MICROUSD,
      cacheMode: "explicit",
      serviceTier: "default",
      pricingVersion: PRICING_VERSION,
    });
    return { payload: { ...payload, max_output_tokens: outputTokens }, reservation };
  }, admissions.withPermit);

  const settle = Effect.fn("ReviewOpenAi.settle")(function* (
    reservation: Reservation,
    response: OpenAiSchema.Response,
  ) {
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
      pending.delete(reservation.id);
      return [
        true,
        {
          ...current,
          pending,
          stopped: current.stopped || outputLimitReached,
          closed: current.closed || outputLimitReached,
          input: current.input + usage.input_tokens,
          read: current.read + read,
          write: current.write + write,
          output: current.output + usage.output_tokens,
          cost: current.cost + cost,
        },
      ] as const;
    });
    if (!updated) return;
    const totals = yield* Ref.get(state);
    yield* Effect.logInfo("Review model usage", {
      modelCall: reservation.id,
      functionCalls: response.output.filter((item) => item.type === "function_call").length,
      completionCalls: response.output.filter(
        (item) => item.type === "function_call" && item.name === "submit_review",
      ).length,
      inputTokens: usage.input_tokens,
      uncachedInputTokens: ordinary,
      cachedInputTokens: read,
      cacheWriteInputTokens: write,
      outputTokens: usage.output_tokens,
      outputLimitReached,
      cacheHitRatio: usage.input_tokens === 0 ? 0 : read / usage.input_tokens,
      estimatedCostMicrousd: cost,
      cumulativeCostMicrousd: totals.cost,
      reservedCostMicrousd: reservedCost(totals),
      remainingCostMicrousd: REVIEW_COST_LIMIT_MICROUSD - totals.cost - reservedCost(totals),
    });
  });

  const costControl: ReviewCostControl = {
    snapshot: Effect.map(Ref.get(state), (current) =>
      ReviewCostSnapshot.make({
        stopped: current.stopped,
        modelCalls: current.modelCalls,
        usage: ReviewUsage.make({
          inputTokens: current.input,
          uncachedInputTokens: current.input - current.read - current.write,
          cachedInputTokens: current.read,
          cacheWriteInputTokens: current.write,
          outputTokens: current.output,
          estimatedCostMicrousd: current.cost,
          reservedCostMicrousd: reservedCost(current),
        }),
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
      Effect.onExit((exit) => (Exit.isFailure(exit) ? close : Effect.void)),
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
          ),
        ] as const;
      },
      Effect.onExit((exit) => (Exit.isFailure(exit) ? close : Effect.void)),
    ),
  });
  return { client, costControl };
});
