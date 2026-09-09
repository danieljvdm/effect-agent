import { IdGenerator } from "@effect-agent/core/IdGenerator";
import * as AgentRuntime from "@effect-agent/engine/AgentRuntime";
import {
  RunContextPreparationPassthrough,
  toolFailureObserverLayer,
} from "@effect-agent/engine/RunOptions";
import { ThreadHistory } from "@effect-agent/engine/ThreadHistory";
import { CloudflareCodeMode } from "@effect-agent/platform-cloudflare/CloudflareCodeMode";
import { OpenAiClient, OpenAiLanguageModel, OpenAiSchema } from "@effect/ai-openai";
import { Clock, Effect, Exit, Layer, Redacted, References, Schema, Stream } from "effect";
import { AiError } from "effect/unstable/ai";
import { FetchHttpClient } from "effect/unstable/http";

import {
  type Audit,
  BenchmarkError,
  Output,
  Request,
  type Result,
  settings,
} from "./discovery-benchmark-contracts.ts";
import {
  agent,
  codeMode,
  discovery,
  expected,
  FixtureObserver,
  handlers,
  question,
  requiredNames,
  scripted,
} from "./discovery-benchmark-fixture.ts";

const bytes = (value: unknown) => new TextEncoder().encode(JSON.stringify(value)).length;

const Usage = Schema.Struct({
  input_tokens: Schema.Natural,
  output_tokens: Schema.Natural,
  input_tokens_details: Schema.Struct({ cached_tokens: Schema.Natural }),
  output_tokens_details: Schema.optionalKey(Schema.Struct({ reasoning_tokens: Schema.Natural })),
});

interface Environment {
  readonly LOADER: WorkerLoader;
  readonly OPENAI_API_KEY?: string;
}

/** All arms execute in this same local workerd host. Generated programs receive only broker capabilities. */
const run = Effect.fn("ToolBenchmark.run")(function* (request: Request, env: Environment) {
  const started = yield* Clock.currentTimeMillis;
  let firstUsefulActionMillis: number | null = null;
  let firstUsefulResultMillis: number | null = null;
  let output: typeof Output.Type | null = null;
  let modelCalls = 0;
  let modelFinalizers = 0;
  let discoveryCalls = 0;
  let codeCalls = 0;
  let costMicrousd = 0;
  let pendingMicrousd = 0;
  let scopeClosed = false;
  let providerFailure: string | null = null;
  const businessCalls: Array<string> = [];
  const audits: Array<Audit> = [];
  const failures: Array<string> = [];
  const needed = requiredNames(request);

  const finalized = Effect.sync(() => {
    modelFinalizers++;
  });

  const observer = FixtureObserver.of({
    seed: request.seed,
    fail: (name) =>
      Effect.sync(() => {
        failures.push(`${name}: invalid business read`);
      }),
    start: Effect.fn("benchmark.handlerStart")(function* (name) {
      if (needed.includes(name) && firstUsefulActionMillis === null)
        firstUsefulActionMillis = (yield* Clock.currentTimeMillis) - started;
    }),
    end: Effect.fn("benchmark.handlerEnd")(function* (name) {
      businessCalls.push(name);
      if (needed.includes(name) && firstUsefulResultMillis === null)
        firstUsefulResultMillis = (yield* Clock.currentTimeMillis) - started;
    }),
  });

  const handlerLayer = handlers.pipe(Layer.provide(Layer.succeed(FixtureObserver, observer)));

  const services = Layer.mergeAll(
    handlerLayer,
    discovery.handlers,
    CloudflareCodeMode.layer(codeMode, { loader: env.LOADER, handlers: handlerLayer }),
    IdGenerator.layer,
    ThreadHistory.layerTransient,
    RunContextPreparationPassthrough,
    toolFailureObserverLayer({
      observe: (observation) =>
        Effect.sync(() => {
          failures.push(`${observation.toolName}: ${observation._tag}`);
        }),
    }),
  );

  const refuse = (description: string) => {
    providerFailure = description;

    return AiError.make({
      module: "ToolBenchmark",
      method: "provider",
      reason: AiError.InvalidRequestError.make({ description }),
    });
  };

  const auditedClient = Layer.effect(
    OpenAiClient.OpenAiClient,
    Effect.gen(function* () {
      const native = yield* OpenAiClient.OpenAiClient;

      return OpenAiClient.OpenAiClient.of({
        ...native,
        createResponse: () => Effect.fail(refuse("Unexpected non-streaming inference")),
        createResponseStream: Effect.fn("benchmark.provider")(function* (payload) {
          if (
            payload.model !== request.model ||
            payload.service_tier !== "default" ||
            payload.store !== false ||
            payload.max_output_tokens !== 2_048 ||
            payload.previous_response_id !== undefined ||
            payload.conversation !== undefined ||
            payload.tools?.some((tool) => tool.type !== "function")
          )
            return yield* refuse("Provider request changed the fixed benchmark configuration");
          const requestBytes = bytes(payload);
          // Deliberately conservative local reservation, without an extra token-count HTTP request.
          // This is a byte-based bound, not a tokenizer; actual usage must fit it or the suite stops.
          const reservedInputTokens = requestBytes * 2 + 8_192;

          const reservation = Math.ceil(
            reservedInputTokens * request.prices.input + 2_048 * request.prices.output,
          );

          if (
            requestBytes > 512 * 1024 ||
            costMicrousd + pendingMicrousd + reservation > request.remainingMicrousd
          )
            return yield* refuse(
              "Insufficient remaining budget for conservative request reservation",
            );
          const ordinal = audits.length;

          const audit: Audit = {
            ordinal,
            toolNames: (payload.tools ?? []).map((tool) =>
              tool.type === "function" ? tool.name : tool.type,
            ),
            toolBytes: bytes(payload.tools ?? []),
            requestBytes,
            reservedInputTokens,
            inputTokens: null,
            cachedInputTokens: null,
            outputTokens: null,
            reasoningTokens: null,
            returnedModel: null,
            returnedTier: null,
            completed: false,
          };

          audits.push(audit);
          pendingMicrousd += reservation;
          const [response, stream] = yield* native.createResponseStream(payload);

          return [
            response,
            stream.pipe(
              Stream.tap(
                Effect.fn("benchmark.usage")(function* (event) {
                  if (
                    event.type !== "response.completed" &&
                    event.type !== "response.failed" &&
                    event.type !== "response.incomplete"
                  )
                    return;

                  const completed = yield* Schema.decodeUnknownEffect(OpenAiSchema.Response)(
                    event.response,
                  ).pipe(Effect.mapError(() => refuse("Invalid provider completion")));

                  const usage = yield* Schema.decodeUnknownEffect(Usage)(completed.usage).pipe(
                    Effect.mapError(() => refuse("Missing or invalid provider token usage")),
                  );

                  if (audits[ordinal]?.completed)
                    return yield* refuse("Duplicate provider completion");

                  const cost = Math.ceil(
                    (usage.input_tokens - usage.input_tokens_details.cached_tokens) *
                      request.prices.input +
                      usage.input_tokens_details.cached_tokens * request.prices.cachedInput +
                      usage.output_tokens * request.prices.output,
                  );

                  audits[ordinal] = {
                    ...audit,
                    inputTokens: usage.input_tokens,
                    cachedInputTokens: usage.input_tokens_details.cached_tokens,
                    outputTokens: usage.output_tokens,
                    reasoningTokens: usage.output_tokens_details?.reasoning_tokens ?? null,
                    returnedModel: completed.model,
                    returnedTier: completed.service_tier ?? null,
                    completed: event.type === "response.completed",
                  };
                  if (
                    usage.input_tokens_details.cached_tokens > usage.input_tokens ||
                    usage.input_tokens > reservedInputTokens ||
                    usage.output_tokens > 2_048 ||
                    cost > reservation ||
                    (completed.service_tier !== undefined &&
                      completed.service_tier !== null &&
                      completed.service_tier !== "default")
                  )
                    return yield* refuse(
                      "Provider usage or tier escaped reservation; retain unresolved budget",
                    );
                  costMicrousd += cost;
                  pendingMicrousd -= reservation;
                  if (event.type !== "response.completed")
                    return yield* refuse("Provider did not complete its response");
                }),
              ),
              Stream.ensuring(finalized),
            ),
          ] as const;
        }),
      });
    }),
  ).pipe(
    Layer.provide(
      OpenAiClient.layer({ apiKey: Redacted.make(env.OPENAI_API_KEY ?? "") }).pipe(
        Layer.provide(FetchHttpClient.layer),
      ),
    ),
  );

  const liveModel = OpenAiLanguageModel.model(request.model, settings).pipe(
    Layer.provide(auditedClient),
  );

  const model = request.live ? liveModel : scripted(request, finalized);

  const execution = Effect.gen(function* () {
    yield* Effect.addFinalizer(() =>
      Effect.sync(() => {
        scopeClosed = true;
      }),
    );
    yield* AgentRuntime.stream(agent(request.mode), question(request)).pipe(
      Stream.runForEach((event) => {
        if (event._tag === "ModelStarted") modelCalls++;
        if (event._tag === "ToolCallSucceeded" && event.toolName === "discover_tools")
          discoveryCalls++;
        if (event._tag === "ToolCallSucceeded" && event.toolName === "run_javascript") codeCalls++;
        if (event._tag === "ToolCallFailed")
          failures.push(`${event.toolName}: ${event.errorTag}: ${event.message}`);

        return event._tag === "RunCompleted"
          ? Schema.decodeUnknownEffect(Output)(event.output).pipe(
              Effect.tap((value) =>
                Effect.sync(() => {
                  output = value;
                }),
              ),
              Effect.asVoid,
            )
          : Effect.void;
      }),
    );
  }).pipe(
    Effect.provide(Layer.merge(services, model)),
    Effect.provideService(References.MinimumLogLevel, "None"),
    Effect.scoped,
    Effect.exit,
  );

  const exit = yield* execution;

  if (Exit.isFailure(exit))
    failures.push(providerFailure ?? "Agent execution failed or was interrupted");
  const answer = expected(request);

  if (output === null || JSON.stringify(output) !== JSON.stringify(answer))
    failures.push("Output did not match exact amount and required receipts");
  if (!needed.every((name) => businessCalls.includes(name)))
    failures.push("Missing successful required business reads");
  if (request.mode === "code-mode" && request.workload !== "common" && codeCalls === 0)
    failures.push("Code Mode did not execute successfully");
  if (modelCalls !== modelFinalizers || !scopeClosed)
    failures.push("Resource finalizers did not complete");
  if (pendingMicrousd > 0) failures.push("Provider reservation remains unresolved");

  return {
    passed: failures.length === 0,
    failure: failures.length === 0 ? null : failures.join("; "),
    output,
    firstUsefulActionMillis,
    firstUsefulResultMillis,
    modelCalls,
    modelFinalizers,
    businessCalls,
    discoveryCalls,
    codeCalls,
    audits,
    costMicrousd,
    pendingMicrousd,
    scopeClosed,
  } satisfies Result;
});

export default {
  async fetch(httpRequest: globalThis.Request, env: Environment): Promise<Response> {
    const input = await httpRequest.json();

    return Effect.runPromise(
      Schema.decodeUnknownEffect(Request)(input).pipe(
        Effect.flatMap((request) =>
          request.live && !env.OPENAI_API_KEY
            ? Effect.fail(
                BenchmarkError.make({ message: "OPENAI_API_KEY is required for live inference" }),
              )
            : run(request, env),
        ),
        Effect.map((result) => Response.json(result)),
        Effect.catch(() =>
          Effect.succeed(
            Response.json(
              { error: "Invalid benchmark request or missing credential" },
              { status: 400 },
            ),
          ),
        ),
      ),
    );
  },
};
