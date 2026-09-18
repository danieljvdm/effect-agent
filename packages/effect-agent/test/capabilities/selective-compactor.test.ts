import { DecisionModel, DecisionQuery } from "@effect-agent/ai-decision";
import {
  Cause,
  Context,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Option,
  Schema,
  Stream,
} from "effect";
import { SelectiveCompactor } from "effect-agent";
import { CompactionPolicy } from "effect-agent/agent-policy";
import {
  CompactionError,
  CompactionEvaluator,
  ContextCompactor,
  type CompactionDecision,
  type CompactionRequest,
} from "effect-agent/context-compactor";
import { RunId, ThreadId } from "effect-agent/identifiers";
import * as DirectSelectiveCompactor from "effect-agent/selective-compactor";
import { TestClock } from "effect/testing";
import { AiError, Prompt } from "effect/unstable/ai";
import { expect, expectTypeOf, it } from "vite-plus/test";

const round = (id: string, name: string, result: string): ReadonlyArray<Prompt.Message> => [
  Prompt.makeMessage("assistant", {
    content: [Prompt.makePart("tool-call", { id, name, params: {}, providerExecuted: false })],
  }),
  Prompt.makeMessage("tool", {
    content: [
      Prompt.makePart("tool-result", {
        id,
        name,
        result,
        isFailure: false,
        providerExecuted: false,
      }),
    ],
  }),
];

const fixtureHistory = Prompt.fromMessages([
  Prompt.userMessage({ content: [Prompt.textPart({ text: "Prepare the trip report." })] }),
  ...round("receipt", "book_trip", "Preserve the booking receipt. ".repeat(40)),
  ...round("noise", "list_files", "Superseded directory listing. ".repeat(200)),
  ...round("newest", "booking_status", "The booking is confirmed. ".repeat(40)),
]);

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
};

const unmeteredEvaluator = CompactionEvaluator().of({
  available: true,
  evaluate: (operation) => operation.pipe(Effect.map((result) => result.value)),
});

class QuestionPolicy extends Context.Service<
  QuestionPolicy,
  { readonly instructions: Effect.Effect<string, CompactionError> }
>()("test/SelectiveCompactor/QuestionPolicy") {}

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

  const selected = SelectiveCompactor.layer({ pinnedTools: ["book_trip"] }).pipe(
    Layer.provide(ContextCompactor.layerRollover),
    Layer.provide(provider),
  );

  const decisions = await Effect.runPromise(
    Effect.gen(function* () {
      const compactor = yield* ContextCompactor;

      return yield* compactor.compact(request).pipe(Stream.runCollect);
    }).pipe(
      Effect.provideService(CompactionEvaluator(), unmeteredEvaluator),
      Effect.provide(selected),
    ),
  );

  expect(calls).toBe(1);
  expect(decisions).toEqual([
    { kind: "clear-tool-results", through: 5, results: [{ messageIndex: 4, toolCallId: "noise" }] },
  ]);
});

it("captures custom question services and closes preparation resources before inference", async () => {
  let prepared = 0;
  let finalized = 0;
  const instructions = "Keep this result as supporting evidence for the trip report.";
  const criteria = { true: "Needed for the report", false: "Superseded evidence" };

  const provider = Layer.effect(
    DecisionModel.DecisionModel,
    DecisionModel.make({
      evaluate: (input) =>
        Effect.sync(() => {
          expect(prepared).toBe(1);
          expect(finalized).toBe(1);
          expect(Object.values(input.questions)).toEqual([
            { type: "probability", instructions, criteria },
          ]);

          return {
            provider: "test",
            model: "custom-question",
            usage: { inputTokens: 1, outputTokens: 1 },
            answers: Object.fromEntries(
              Object.keys(input.questions).map((id) => [
                id,
                { type: "probability", probability: 0.05 },
              ]),
            ),
          };
        }),
    }),
  );

  const selected = SelectiveCompactor.layer({
    pinnedTools: ["book_trip"],
    question: Effect.fn(function* ({ result, state }) {
      const policy = yield* QuestionPolicy;
      const instructions = yield* policy.instructions;

      expect(result.tool).toBe("list_files");
      expect(state.task).toBe("Prepare the trip report.");
      yield* Effect.acquireRelease(
        Effect.sync(() => {
          prepared++;
        }),
        () =>
          Effect.sync(() => {
            finalized++;
          }),
      );

      return DecisionQuery.probability({ instructions, criteria });
    }),
  }).pipe(
    Layer.provide(
      Layer.mergeAll(
        ContextCompactor.layerRollover,
        provider,
        Layer.succeed(QuestionPolicy, { instructions: Effect.succeed(instructions) }),
      ),
    ),
  );

  const decisions = await Effect.runPromise(
    Effect.gen(function* () {
      const compactor = yield* ContextCompactor;

      expect(prepared).toBe(0);

      return yield* compactor.compact(request).pipe(Stream.runCollect);
    }).pipe(
      Effect.provideService(CompactionEvaluator(), unmeteredEvaluator),
      Effect.provide(selected),
    ),
  );

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
          const state = yield* Schema.decodeUnknownEffect(SelectiveCompactor.SelectionState)(
            input.state,
          ).pipe(Effect.orDie);

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
      Effect.provideService(CompactionEvaluator(), unmeteredEvaluator),
      Effect.provide(
        SelectiveCompactor.layer().pipe(
          Layer.provide(ContextCompactor.layer),
          Layer.provide(provider),
        ),
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
  let prepared = 0;

  const provider = Layer.effect(
    DecisionModel.DecisionModel,
    DecisionModel.make({
      evaluate: (input) =>
        Effect.sync(() => {
          expect(new TextEncoder().encode(JSON.stringify(input)).byteLength).toBeLessThanOrEqual(
            48_000,
          );
          queried = Object.keys(input.questions).length;
          expect(prepared).toBe(32);
          expect(queried).toBeGreaterThan(0);
          expect(queried).toBeLessThan(32);

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
      Effect.provideService(CompactionEvaluator(), unmeteredEvaluator),
      Effect.provide(
        SelectiveCompactor.layer({
          question: ({ result }) =>
            Effect.sync(() => {
              prepared++;

              return DecisionQuery.probability({
                instructions: `Keep ${result.id} as evidence: ${"参考資料".repeat(1_000)}`,
              });
            }),
        }).pipe(Layer.provide(ContextCompactor.layer), Layer.provide(provider)),
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

it("surfaces task evidence at varied interior positions while keeping bounded excerpts and original bodies", async () => {
  const task = "Reconcile the warehouse ledger locator and remaining units.";

  const evidence = [
    "ledger locator=parcel-482",
    "ledger locator=parcel-715",
    "ledger locator=parcel-936",
  ];

  const bodies = evidence.map(
    (fact, index) =>
      "Completed unrelated diagnostic inventory.\n" +
      "ordinary diagnostic row\n".repeat(50 + index * 131) +
      fact +
      "\n" +
      "ordinary diagnostic row\n".repeat(400 - index * 131) +
      "End of archived observations.",
  );

  const source = Prompt.fromMessages([
    Prompt.userMessage({ content: [Prompt.textPart({ text: task })] }),
    ...bodies.flatMap((body, index) => round(`buried-${index}`, "read_inventory", body)),
    ...round("newest", "status", "Current allocation remains pending."),
  ]);

  const original = JSON.stringify(source);
  let observed = false;

  const provider = Layer.effect(
    DecisionModel.DecisionModel,
    DecisionModel.make({
      evaluate: (input) =>
        Effect.gen(function* () {
          const state = yield* Schema.decodeUnknownEffect(SelectiveCompactor.SelectionState)(
            input.state,
          ).pipe(Effect.orDie);

          expect(state.results).toHaveLength(3);
          for (const [index, fact] of evidence.entries()) {
            const candidate = state.results.find(
              (result) => result.toolCallId === `buried-${index}`,
            );

            expect(candidate?.excerpt).toContain(fact);
            expect(candidate?.excerpt).toContain("Completed unrelated diagnostic inventory.");
            expect(candidate?.excerpt).toContain("End of archived observations.");
            expect(candidate?.excerpt.length).toBeLessThanOrEqual(800);
            expect(candidate?.truncated).toBe(true);
          }
          observed = true;

          return {
            provider: "test",
            model: "task-evidence",
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

  const decisions = await Effect.runPromise(
    Effect.gen(function* () {
      const compactor = yield* ContextCompactor;

      return yield* compactor.compact({ ...request, source }).pipe(Stream.runCollect);
    }).pipe(
      Effect.provideService(CompactionEvaluator(), unmeteredEvaluator),
      Effect.provide(
        SelectiveCompactor.layer().pipe(
          Layer.provide(ContextCompactor.layer),
          Layer.provide(provider),
        ),
      ),
    ),
  );

  expect(observed).toBe(true);
  expect(decisions).toEqual([]);
  expect(JSON.stringify(source)).toBe(original);
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

        const selected = SelectiveCompactor.layer().pipe(
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
        }).pipe(
          Effect.provideService(CompactionEvaluator(), unmeteredEvaluator),
          Effect.provide(selected),
          Effect.forkChild,
        );

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

it("rejects a non-probability question before reserving an evaluation", async () => {
  const selected = SelectiveCompactor.layer({
    // @ts-expect-error JavaScript consumers must also be prevented from changing the answer kind.
    question: () =>
      Effect.succeed(DecisionQuery.score({ instructions: "Relevance", levels: ["low", "high"] })),
  }).pipe(
    Layer.provide(
      Layer.merge(
        ContextCompactor.layerRollover,
        Layer.effect(
          DecisionModel.DecisionModel,
          DecisionModel.make({
            evaluate: () => Effect.die("Unexpected classifier call"),
          }),
        ),
      ),
    ),
  );

  const exit = await Effect.runPromiseExit(
    Effect.gen(function* () {
      const compactor = yield* ContextCompactor;

      return yield* compactor.compact(request).pipe(Stream.runCollect);
    }).pipe(
      Effect.provideService(CompactionEvaluator(), {
        available: true,
        evaluate: () => Effect.die("Unexpected evaluation reservation"),
      }),
      Effect.provide(selected),
    ),
  );

  const error = Exit.isFailure(exit) ? Cause.findErrorOption(exit.cause) : Option.none();

  expect(error).toMatchObject(
    Option.some({
      _tag: "CompactionError",
      message: "Invalid compaction probability question",
    }),
  );
});

for (const mode of ["failure", "defect", "timeout", "interruption"] as const) {
  it(`finalizes question preparation on ${mode} before reservation, inference or pruning`, async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const started = yield* Deferred.make<void>();
        const failure = CompactionError.make({ message: "Retention policy unavailable" });
        let finalized = 0;
        const emitted: Array<CompactionDecision> = [];

        const outcome =
          mode === "failure"
            ? Effect.fail(failure)
            : mode === "defect"
              ? Effect.die("question defect")
              : Effect.never;

        const provider = Layer.effect(
          DecisionModel.DecisionModel,
          DecisionModel.make({ evaluate: () => Effect.die("Unexpected classifier call") }),
        );

        const selected = SelectiveCompactor.layer({
          question: () =>
            Effect.acquireRelease(Deferred.succeed(started, undefined), () =>
              Effect.sync(() => {
                finalized++;
              }),
            ).pipe(Effect.andThen(outcome)),
        }).pipe(Layer.provide(Layer.merge(ContextCompactor.layerRollover, provider)));

        const fiber = yield* Effect.gen(function* () {
          const compactor = yield* ContextCompactor;

          yield* compactor.compact(request).pipe(
            Stream.runForEach((decision) =>
              Effect.sync(() => {
                emitted.push(decision);
              }),
            ),
          );
        }).pipe(
          Effect.provideService(CompactionEvaluator(), {
            available: true,
            evaluate: () => Effect.die("Unexpected evaluation reservation"),
          }),
          Effect.provide(selected),
          Effect.forkChild,
        );

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
            if (mode === "failure" && Option.isSome(error)) expect(error.value).toBe(failure);
          }
        }
        expect(finalized).toBe(1);
        expect(emitted).toEqual([]);
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

  const selected = SelectiveCompactor.layer({
    question: () => Effect.die("Unexpected question after the selection allowance was consumed"),
  }).pipe(Layer.provide(fallback), Layer.provide(provider));

  const decisions = await Effect.runPromise(
    Effect.gen(function* () {
      const compactor = yield* ContextCompactor;

      return yield* compactor
        .compact({
          ...request,
          policy: CompactionPolicy.make({ mode: "prune-then-summarize" }),
        })
        .pipe(Stream.runCollect);
    }).pipe(
      Effect.provideService(CompactionEvaluator(), {
        available: false,
        evaluate: () => Effect.die("Unexpected unavailable evaluation"),
      }),
      Effect.provide(selected),
    ),
  );

  expect(evaluations).toBe(0);
  expect(replacements).toBe(1);
  expect(decisions).toEqual([{ kind: "rollover", through: 5 }]);
});

it("keeps decision-provider requirements and compaction errors visible", () => {
  const make = SelectiveCompactor.layer();

  expectTypeOf<Layer.Services<typeof make>>().toEqualTypeOf<
    DecisionModel.DecisionModel | ContextCompactor
  >();
  expectTypeOf<Layer.Error<typeof make>>().toEqualTypeOf<CompactionError>();
  const withFallback = make.pipe(Layer.provide(ContextCompactor.layerRollover));

  expectTypeOf<Layer.Services<typeof withFallback>>().toEqualTypeOf<DecisionModel.DecisionModel>();
  expectTypeOf<Layer.Success<typeof withFallback>>().toEqualTypeOf<ContextCompactor>();
  expectTypeOf(SelectiveCompactor.layer).toEqualTypeOf(DirectSelectiveCompactor.layer);

  const custom = SelectiveCompactor.layer({
    question: Effect.fn(function* () {
      const policy = yield* QuestionPolicy;
      const instructions = yield* policy.instructions;

      yield* Effect.acquireRelease(Effect.void, () => Effect.void);

      return DecisionQuery.probability({ instructions });
    }),
  });

  expectTypeOf<Layer.Services<typeof custom>>().toEqualTypeOf<
    DecisionModel.DecisionModel | ContextCompactor | QuestionPolicy
  >();
  expectTypeOf<Layer.Error<typeof custom>>().toEqualTypeOf<CompactionError>();
});
