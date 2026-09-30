import { expect, layer } from "@effect/vitest";
import { Cause, Clock, Duration, Effect, Exit, Fiber, Layer, Schema, Stream } from "effect";
import * as Agent from "effect-agent/agent";
import { AgentPolicy } from "effect-agent/agent-policy";
import * as AgentRuntime from "effect-agent/agent-runtime";
import { IdGenerator } from "effect-agent/id-generator";
import { RunId, ThreadId, TurnId } from "effect-agent/identifiers";
import { ThreadHistory } from "effect-agent/thread-history";
import { TestClock } from "effect/testing";
import { AiError, LanguageModel, Model, type Response, Tool, Toolkit } from "effect/unstable/ai";

let threadSequence = 0;

const identifiers = Layer.succeed(IdGenerator, {
  nextThreadId: Effect.sync(() =>
    Schema.decodeSync(ThreadId)(`model-retry-thread-${++threadSequence}`),
  ),
  nextRunId: Effect.succeed(Schema.decodeSync(RunId)("model-retry-run")),
  nextTurnId: Effect.succeed(Schema.decodeSync(TurnId)("model-retry-turn")),
});

const usage = { inputTokens: {}, outputTokens: {} };
const metadata: Response.StreamPartEncoded = { type: "response-metadata", id: "resp-1" };

const rateLimitedPart: Response.StreamPartEncoded = {
  type: "error",
  error: { code: 429, message: "model is temporarily rate-limited upstream" },
};

const answer: ReadonlyArray<Response.StreamPartEncoded> = [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: '"done"' },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage },
];

type Attempt = ReadonlyArray<Response.StreamPartEncoded> | AiError.AiError;

const HostedCode = Tool.providerDefined({
  id: "test.code_interpreter",
  customName: "HostedCode",
  providerName: "code_interpreter",
})(undefined);

const runScripted = (
  attempts: ReadonlyArray<Attempt>,
  modelRetries?: number,
  hostedTool?: ReturnType<typeof HostedCode.annotate>,
) =>
  Effect.gen(function* () {
    const callTimes: Array<number> = [];

    const model = Model.make(
      "scripted",
      "model-retry",
      Layer.effect(
        LanguageModel.LanguageModel,
        LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: () =>
            Stream.unwrap(
              Effect.gen(function* () {
                const attempt = attempts[Math.min(callTimes.length, attempts.length - 1)]!;

                callTimes.push(yield* Clock.currentTimeMillis);

                return AiError.isAiError(attempt)
                  ? Stream.fail(attempt)
                  : Stream.fromIterable(attempt);
              }),
            ),
        }),
      ),
    );

    const agent = Agent.withModel(
      Agent.make("model-retry", {
        input: Schema.String,
        output: Schema.String,
        instructions: "Answer.",
        toolkit: hostedTool === undefined ? Toolkit.empty : Toolkit.make(hostedTool),
        policy: AgentPolicy.make({
          maxTurns: 2,
          maxToolCalls: 1,
          maxDuration: "10 minutes",
          toolConcurrency: 1,
          ...(modelRetries === undefined ? {} : { modelRetries }),
        }),
      }),
      model,
    );

    const fiber = yield* Effect.forkChild(Stream.runDrain(AgentRuntime.stream(agent, "begin")));

    // Backoff sleeps start at different points in the run; advance until it settles.
    yield* TestClock.adjust("1 second").pipe(
      Effect.repeat({ until: () => fiber.pollUnsafe() !== undefined }),
    );
    const exit = yield* Fiber.await(fiber);

    return { exit, calls: callTimes.length, callTimes };
  });

const failureTag = (exit: Exit.Exit<unknown, unknown>) =>
  Exit.isFailure(exit) ? Cause.squash(exit.cause) : undefined;

layer(Layer.mergeAll(identifiers, ThreadHistory.layer))("model call retries", (it) => {
  it.effect.each([{ code: 429 }, { status: 503 }])(
    "retries a transient error part before any content (%j)",
    (error) =>
      Effect.gen(function* () {
        const { exit, calls } = yield* runScripted(
          [[metadata, { type: "error", error }], answer],
          2,
        );

        expect(Exit.isSuccess(exit)).toBe(true);
        expect(calls).toBe(2);
      }),
  );

  it.effect("retries a retryable provider failure", () =>
    Effect.gen(function* () {
      const rateLimited = AiError.make({
        module: "scripted",
        method: "streamText",
        reason: new AiError.RateLimitError({}),
      });

      const { exit, calls } = yield* runScripted([rateLimited, rateLimited, answer], 2);

      expect(Exit.isSuccess(exit)).toBe(true);
      expect(calls).toBe(3);
    }),
  );

  // Regressions in 50f22b5: retrying before content did not establish hosted-tool
  // replay safety, preserve longer provider cooldowns, or validate HTTP status codes.
  it.effect.each([false, true])(
    "replays hosted tools only with readonly permission (readonly=%s)",
    (readonly) =>
      Effect.gen(function* () {
        const transportError = AiError.make({
          module: "scripted",
          method: "streamText",
          reason: new AiError.NetworkError({
            reason: "TransportError",
            request: {
              method: "POST",
              url: "https://example.invalid/model",
              urlParams: [],
              hash: undefined,
              headers: {},
            },
            description: "Connection lost after hosted execution, before response parts",
          }),
        });

        const { exit, calls } = yield* runScripted(
          [transportError, answer],
          1,
          readonly ? HostedCode.annotate(Tool.Readonly, true) : HostedCode,
        );

        expect(calls).toBe(readonly ? 2 : 1);
        expect(failureTag(exit)).toBe(readonly ? undefined : transportError);
      }),
  );

  it.effect("honors retryAfter beyond the exponential backoff cap", () =>
    Effect.gen(function* () {
      const rateLimited = AiError.make({
        module: "scripted",
        method: "streamText",
        reason: new AiError.RateLimitError({ retryAfter: Duration.minutes(2) }),
      });

      const { exit, callTimes } = yield* runScripted([rateLimited, answer], 1);

      expect(Exit.isSuccess(exit)).toBe(true);
      expect(callTimes).toHaveLength(2);
      expect(callTimes[1]! - callTimes[0]!).toBeGreaterThanOrEqual(120_000);
    }),
  );

  it.effect("does not interpret application error codes as HTTP statuses", () =>
    Effect.gen(function* () {
      const { exit, calls } = yield* runScripted(
        [[{ type: "error", error: { code: 7001 } }], answer],
        1,
      );

      expect(calls).toBe(1);
      expect(failureTag(exit)).toMatchObject({ _tag: "ModelProtocolError" });
    }),
  );

  it.effect("fails unchanged when retries are off or exhausted", () =>
    Effect.gen(function* () {
      const off = yield* runScripted([[metadata, rateLimitedPart], answer]);
      const exhausted = yield* runScripted([[metadata, rateLimitedPart]], 1);

      expect(off.calls).toBe(1);
      expect(failureTag(off.exit)).toMatchObject({ _tag: "ModelProtocolError" });
      expect(exhausted.calls).toBe(2);
      expect(failureTag(exhausted.exit)).toMatchObject({ _tag: "ModelProtocolError" });
    }),
  );

  it.effect("does not retry once content has streamed", () =>
    Effect.gen(function* () {
      const { exit, calls } = yield* runScripted(
        [[metadata, { type: "text-start", id: "answer" }, rateLimitedPart], answer],
        2,
      );

      expect(calls).toBe(1);
      expect(failureTag(exit)).toMatchObject({ _tag: "ModelProtocolError" });
    }),
  );
});
