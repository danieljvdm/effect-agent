import { DecisionModel } from "@effect-agent/ai-decision";
import { Cause, Deferred, Effect, Exit, Fiber, Layer, Option, Schema, Stream } from "effect";
import { CompactionPolicy } from "effect-agent/agent-policy";
import {
  CompactionError,
  ContextCompactor,
  type CompactionDecision,
  type CompactionRequest,
} from "effect-agent/context-compactor";
import { RunId, ThreadId } from "effect-agent/identifiers";
import { TestClock } from "effect/testing";
import { AiError, Prompt } from "effect/unstable/ai";
import { expect, expectTypeOf, it } from "vite-plus/test";

import { layerSelective, SelectionState } from "../src/selective-compactor.ts";
import { fixtureHistory, runProbe, scriptedDecisionLayer } from "../src/selective-spike.ts";

const request: CompactionRequest<never, never> = {
  source: fixtureHistory,
  threadId: ThreadId.make("selection-test"),
  runId: RunId.make("selection-test"),
  turn: 1,
  trigger: "pressure",
  modelCallAllowed: true,
  targetTokens: 1_400,
  policy: CompactionPolicy.make({ mode: "prune", keepRecentTokens: 1 }),
  state: {
    protectedStart: 0,
    protectedEnd: 1,
    clearedThrough: 0,
    replacement: undefined,
    lastCompactionTurn: 0,
    overflowRetryTurn: 0,
    lastViewLength: -1,
  },
  summarize: () => Effect.die("Unexpected summary"),
  evaluate: (operation) => operation.pipe(Effect.map((result) => result.value)),
};

it("compares the same history through the native engine and keeps exact evidence selectively", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const age = yield* runProbe("age");
      const selective = yield* runProbe("selective");

      expect(age).toMatchObject({
        receiptAvailable: false,
        newestAvailable: true,
        retainedHistoryIntact: true,
        clearedResults: ["receipt", "noise"],
      });
      expect(selective).toMatchObject({
        receiptAvailable: true,
        newestAvailable: true,
        retainedHistoryIntact: true,
        clearedResults: ["noise"],
      });
      expect(selective.selectorUsage).toHaveLength(1);
      expect(selective.selectorUsage[0]).toMatchObject({
        purpose: "compaction",
        inputTokens: { total: 200 },
        outputTokens: { total: 2 },
      });
    }).pipe(Effect.provide(scriptedDecisionLayer)),
  );
});

it("pins application-selected tools and supplies bounded result evidence to the classifier", async () => {
  let calls = 0;

  const provider = Layer.effect(
    DecisionModel.DecisionModel,
    DecisionModel.make({
      evaluate: (input) =>
        Effect.sync(() => {
          calls++;
          expect(JSON.stringify(input.state)).toContain("Superseded directory listing");
          expect(Object.keys(input.questions)).toHaveLength(1);
          expect(JSON.stringify(input.questions)).not.toContain("book_trip");
          expect(JSON.stringify(input.questions)).not.toContain("booking_status");

          return {
            provider: "test",
            model: "selector",
            usage: { inputTokens: 1, outputTokens: 1 },
            answers: Object.fromEntries(
              Object.keys(input.questions).map((id) => [
                id,
                { type: "probability", probability: 0 },
              ]),
            ),
          };
        }),
    }),
  );

  const selected = layerSelective({ pinnedTools: ["book_trip"] }).pipe(
    Layer.provide(ContextCompactor.layerRollover),
    Layer.provide(provider),
  );

  const decisions = await Effect.runPromise(
    Effect.gen(function* () {
      const compactor = yield* ContextCompactor;

      return yield* compactor.compact(request).pipe(Stream.runCollect);
    }).pipe(Effect.provide(selected)),
  );

  expect(calls).toBe(1);
  expect(decisions).toEqual([
    { kind: "clear-tool-results", through: 5, results: [{ messageIndex: 4, toolCallId: "noise" }] },
  ]);
});

it("includes tool inputs, excluded activity and the latest task when older conversation is abridged", async () => {
  let observed = false;

  const source = Prompt.fromMessages([
    Prompt.systemMessage({ content: "Preserve immutable publication receipts." }),
    Prompt.userMessage({
      content: [Prompt.textPart({ text: "Investigate the previous release. ".repeat(800) })],
    }),
    Prompt.makeMessage("assistant", {
      content: [
        Prompt.makePart("tool-call", {
          id: "read",
          name: "read_file",
          params: { path: "/receipts/production.json" },
          providerExecuted: false,
        }),
      ],
    }),
    Prompt.makeMessage("tool", {
      content: [
        Prompt.makePart("tool-result", {
          id: "read",
          name: "read_file",
          result: "receipt data ".repeat(200),
          isFailure: false,
          providerExecuted: false,
        }),
      ],
    }),
    Prompt.makeMessage("assistant", {
      content: [
        Prompt.makePart("tool-call", {
          id: "update",
          name: "publish",
          params: { release: "next" },
          providerExecuted: false,
        }),
      ],
    }),
    Prompt.makeMessage("tool", {
      content: [
        Prompt.makePart("tool-result", {
          id: "update",
          name: "publish",
          result: "Deployment refused: certificate expired",
          isFailure: true,
          providerExecuted: false,
        }),
      ],
    }),
    Prompt.userMessage({
      content: [
        Prompt.textPart({
          text: "Use the production receipt; the replacement publication failed.",
        }),
      ],
    }),
  ]);

  const provider = Layer.effect(
    DecisionModel.DecisionModel,
    DecisionModel.make({
      evaluate: (input) =>
        Effect.gen(function* () {
          const state = yield* Schema.decodeUnknownEffect(SelectionState)(input.state).pipe(
            Effect.orDie,
          );

          expect(state.task).toContain("replacement publication failed");
          expect(state.task).toContain("immutable publication receipts");
          expect(state.conversation).toContain("certificate expired");
          expect(state.results).toHaveLength(1);
          expect(state.results[0]).toMatchObject({
            input: '{"path":"/receipts/production.json"}',
            truncated: true,
          });
          observed = true;

          return {
            provider: "test",
            model: "context",
            usage: { inputTokens: 1, outputTokens: 1 },
            answers: Object.fromEntries(
              Object.keys(input.questions).map((id) => [
                id,
                { type: "probability", probability: 1 },
              ]),
            ),
          };
        }),
    }),
  );

  await Effect.runPromise(
    Effect.gen(function* () {
      const compactor = yield* ContextCompactor;

      yield* compactor.compact({ ...request, source }).pipe(Stream.runDrain);
    }).pipe(
      Effect.provide(
        layerSelective().pipe(Layer.provide(ContextCompactor.layer), Layer.provide(provider)),
      ),
    ),
  );
  expect(observed).toBe(true);
});

it("fits the complete Unicode request and leaves candidates beyond the bound unselected", async () => {
  const messages: Array<Prompt.Message> = [];

  for (let index = 0; index < 34; index++) {
    messages.push(
      Prompt.makeMessage("assistant", {
        content: [
          Prompt.makePart("tool-call", {
            id: `r${index}`,
            name: "read_file",
            params: { path: "資料".repeat(800) },
            providerExecuted: false,
          }),
        ],
      }),
      Prompt.makeMessage("tool", {
        content: [
          Prompt.makePart("tool-result", {
            id: `r${index}`,
            name: "read_file",
            result: "予約情報".repeat(2_000),
            isFailure: false,
            providerExecuted: false,
          }),
        ],
      }),
    );
  }
  let queried = 0;

  const provider = Layer.effect(
    DecisionModel.DecisionModel,
    DecisionModel.make({
      evaluate: (input) =>
        Effect.sync(() => {
          expect(new TextEncoder().encode(JSON.stringify(input)).byteLength).toBeLessThanOrEqual(
            48_000,
          );
          queried = Object.keys(input.questions).length;
          expect(queried).toBeGreaterThan(0);
          expect(queried).toBeLessThanOrEqual(32);

          return {
            provider: "test",
            model: "bounded",
            usage: { inputTokens: 1, outputTokens: 1 },
            answers: Object.fromEntries(
              Object.keys(input.questions).map((id) => [
                id,
                { type: "probability", probability: 0 },
              ]),
            ),
          };
        }),
    }),
  );

  const decisions = await Effect.runPromise(
    Effect.gen(function* () {
      const compactor = yield* ContextCompactor;

      return yield* compactor
        .compact({ ...request, source: Prompt.fromMessages(messages) })
        .pipe(Stream.runCollect);
    }).pipe(
      Effect.provide(
        layerSelective().pipe(Layer.provide(ContextCompactor.layer), Layer.provide(provider)),
      ),
    ),
  );

  expect(decisions).toHaveLength(1);
  expect(decisions[0]?.kind).toBe("clear-tool-results");
  if (decisions[0]?.kind === "clear-tool-results") {
    expect(decisions[0].results).toHaveLength(queried);
    expect(decisions[0].results?.some((result) => result.toolCallId === "r33")).toBe(false);
  }
});

for (const mode of ["invalid", "failure", "defect", "timeout", "interruption"] as const) {
  it(`preserves the view and finalizes classifier resources on ${mode}`, async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const started = yield* Deferred.make<void>();
        let finalized = 0;
        const emitted: Array<CompactionDecision> = [];
        const before = JSON.stringify(fixtureHistory);

        const outcome =
          mode === "invalid"
            ? Effect.succeed({
                provider: "test",
                model: "selector",
                usage: { inputTokens: 1, outputTokens: 1 },
                answers: {},
              })
            : mode === "failure"
              ? Effect.fail(
                  new AiError.AiError({
                    module: "test",
                    method: "evaluate",
                    reason: new AiError.UnknownError({ description: "offline" }),
                  }),
                )
              : mode === "defect"
                ? Effect.die("classifier defect")
                : Effect.never;

        const provider = Layer.effect(
          DecisionModel.DecisionModel,
          DecisionModel.make({
            evaluate: () =>
              Effect.acquireRelease(Deferred.succeed(started, undefined), () =>
                Effect.sync(() => {
                  finalized++;
                }),
              ).pipe(Effect.andThen(outcome)),
          }),
        );

        const selected = layerSelective().pipe(
          Layer.provide(ContextCompactor.layerRollover),
          Layer.provide(provider),
        );

        const fiber = yield* Effect.gen(function* () {
          const compactor = yield* ContextCompactor;

          yield* compactor.compact(request).pipe(
            Stream.runForEach((decision) =>
              Effect.sync(() => {
                emitted.push(decision);
              }),
            ),
          );
        }).pipe(Effect.provide(selected), Effect.forkChild);

        yield* Deferred.await(started);
        if (mode === "timeout") yield* TestClock.adjust("6 seconds");
        if (mode === "interruption") yield* Fiber.interrupt(fiber);
        const exit = yield* Fiber.await(fiber);

        expect(Exit.isFailure(exit)).toBe(true);
        if (Exit.isFailure(exit)) {
          if (mode === "defect") expect(Cause.hasDies(exit.cause)).toBe(true);
          else if (mode === "interruption") expect(Cause.hasInterrupts(exit.cause)).toBe(true);
          else {
            const error = Cause.findErrorOption(exit.cause);

            expect(Option.isSome(error) && Schema.is(CompactionError)(error.value)).toBe(true);
          }
        }
        expect(finalized).toBe(1);
        expect(emitted).toEqual([]);
        expect(JSON.stringify(fixtureHistory)).toBe(before);
      }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
    );
  });
}

it("uses replacement after the engine consumes this Turn's selection allowance", async () => {
  let evaluations = 0;
  let replacements = 0;

  const provider = Layer.effect(
    DecisionModel.DecisionModel,
    DecisionModel.make({
      evaluate: () =>
        Effect.sync(() => {
          evaluations++;
          throw new Error("Unexpected second evaluation");
        }),
    }),
  );

  const fallback = Layer.succeed(ContextCompactor, {
    estimate: () => 100,
    compact: () => {
      replacements++;

      return Stream.succeed({ kind: "rollover", through: 5 });
    },
  });

  const selected = layerSelective().pipe(Layer.provide(fallback), Layer.provide(provider));

  const decisions = await Effect.runPromise(
    Effect.gen(function* () {
      const compactor = yield* ContextCompactor;
      const withoutEvaluation = { ...request };

      delete withoutEvaluation.evaluate;

      return yield* compactor
        .compact({
          ...withoutEvaluation,
          policy: CompactionPolicy.make({ mode: "prune-then-summarize" }),
        })
        .pipe(Stream.runCollect);
    }).pipe(Effect.provide(selected)),
  );

  expect(evaluations).toBe(0);
  expect(replacements).toBe(1);
  expect(decisions).toEqual([{ kind: "rollover", through: 5 }]);
});

it("keeps decision-provider requirements and compaction errors visible", () => {
  const make = layerSelective();

  expectTypeOf<Layer.Services<typeof make>>().toEqualTypeOf<
    DecisionModel.DecisionModel | ContextCompactor
  >();
  expectTypeOf<Layer.Error<typeof make>>().toEqualTypeOf<CompactionError>();
  expectTypeOf<
    Effect.Services<ReturnType<typeof runProbe>>
  >().toEqualTypeOf<DecisionModel.DecisionModel>();
});
