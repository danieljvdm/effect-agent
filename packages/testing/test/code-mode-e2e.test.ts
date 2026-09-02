import { CodeMode } from "@effect-agent/capabilities";
import { Agent, AgentPolicy, ThreadId, IdGenerator, RunId, TurnId } from "@effect-agent/core";
import {
  ThreadHistory,
  RunContextPreparationPassthrough,
  AgentRuntime,
  ToolExecutionClass,
  toolFailureObserverLayer,
  type ToolFailureObservation,
} from "@effect-agent/engine";
import { MemoryThreadStoreLive, MemorySubmissionLedgerLive } from "@effect-agent/storage-memory";
import { inProcessCodeExecutorLayer } from "@effect-agent/testing/code-executor";
import {
  ThreadRead,
  ThreadStore,
  DefinitionDigests,
  DeploymentId,
  Digest,
  DurableAgentRuntime,
  DurableRuntimeConfig,
  DurableRuntimeFailpoint,
  IdempotencyKey,
  Principal,
  ProducerId,
  ToolReconciler,
  WakeScheduler,
} from "@effect-agent/thread";
import { NodeCrypto } from "@effect/platform-node";
import { expect, layer } from "@effect/vitest";
import { Cause, Effect, Layer, Logger, Ref, References, Schema, Stream } from "effect";
import { LanguageModel, Model, Tool, Toolkit, type Response } from "effect/unstable/ai";

import {
  warehouseDbLayer,
  warehouseDemoSeed,
  warehouseHandlersLayer,
  warehouseQueryTool,
} from "./fixtures/warehouse.ts";

const usage = { inputTokens: {}, outputTokens: {} };

const identifiers = Layer.succeed(IdGenerator, {
  nextThreadId: Effect.succeed(Schema.decodeSync(ThreadId)("thread-cm-e2e")),
  nextRunId: Effect.succeed(Schema.decodeSync(RunId)("run-cm-e2e")),
  nextTurnId: Effect.succeed(Schema.decodeSync(TurnId)("turn-cm-e2e")),
});

const QueryWarehouse = Tool.make("query_warehouse", {
  description: "Run one read-only SQL query against the invoice warehouse",
  parameters: Schema.Struct({ sql: Schema.String }),
  success: Schema.Struct({
    rows: Schema.Array(Schema.Struct({ customer: Schema.String, revenue: Schema.Int })),
    truncated: Schema.Boolean,
  }),
}).annotate(ToolExecutionClass, "readonly");

const pageOne = [
  { customer: "acme", revenue: 4_200 },
  { customer: "tiny", revenue: 120 },
];
const pageTwo = [
  { customer: "globex", revenue: 2_500 },
  { customer: "small", revenue: 90 },
];

const runScenario = (options: { readonly code: string; readonly maxToolCalls: number }) =>
  Effect.gen(function* () {
    const codeMode = CodeMode.make("run_javascript", {
      description: "Execute JavaScript that may query the warehouse",
      tools: { warehouse: { query: QueryWarehouse } },
    });
    const agent = Agent.make("code-mode-e2e", {
      input: Schema.Struct({ question: Schema.String }),
      output: Schema.Struct({ answer: Schema.String }),
      instructions: "Answer with run_javascript.",
      toolkit: Toolkit.make(codeMode.tool),
      policy: AgentPolicy.make({
        maxTurns: 2,
        maxToolCalls: options.maxToolCalls,
        maxDuration: "30 seconds",
        toolConcurrency: 1,
      }),
    });
    const observedToolResults = yield* Ref.make<
      ReadonlyArray<{ readonly result: unknown; readonly isFailure: boolean }>
    >([]);
    const model = Model.make(
      "scripted",
      "code-mode-e2e",
      Layer.effect(
        LanguageModel.LanguageModel,
        Effect.gen(function* () {
          const turn = yield* Ref.make(0);
          return yield* LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: ({ prompt }) =>
              Stream.unwrap(
                Ref.getAndUpdate(turn, (value) => value + 1).pipe(
                  Effect.tap(() =>
                    Ref.update(observedToolResults, (all) => [
                      ...all,
                      ...prompt.content
                        .filter((message) => message.role === "tool")
                        .flatMap((message) => message.content)
                        .filter((part) => part.type === "tool-result")
                        .map((part) => ({
                          result: part.result,
                          isFailure: part.isFailure === true,
                        })),
                    ]),
                  ),
                  Effect.map((value) =>
                    Stream.fromIterable<Response.StreamPartEncoded>(
                      value === 0
                        ? [
                            {
                              type: "tool-call",
                              id: "code-1",
                              name: "run_javascript",
                              params: { code: options.code },
                              providerExecuted: false,
                            },
                            { type: "finish", reason: "tool-calls", usage },
                          ]
                        : [
                            { type: "text-start", id: "answer" },
                            { type: "text-delta", id: "answer", delta: '{"answer":"done"}' },
                            { type: "text-end", id: "answer" },
                            { type: "finish", reason: "stop", usage },
                          ],
                    ),
                  ),
                ),
              ),
          });
        }),
      ),
    );
    const handlerCalls = yield* Ref.make<ReadonlyArray<string>>([]);
    const handlerLayer = codeMode.handlers.pipe(
      Layer.provide(
        Toolkit.make(QueryWarehouse).toLayer({
          query_warehouse: ({ sql }) =>
            Ref.update(handlerCalls, (all) => [...all, sql]).pipe(
              Effect.as({
                rows: sql.includes("page = 1") ? pageOne : pageTwo,
                truncated: false,
              }),
            ),
        }),
      ),
      Layer.provide(inProcessCodeExecutorLayer),
    );
    const result = yield* AgentRuntime.run(
      Agent.withModel(agent, model),
      { question: "top customers" },
      {},
    ).pipe(Effect.provide(handlerLayer), Effect.scoped);
    return {
      answer: result.output,
      toolResults: yield* Ref.get(observedToolResults),
      handlerCalls: yield* Ref.get(handlerCalls),
    };
  });

// The suite opts out of the injected test services because the in-process
// executor's wall-clock deadline runs on the live Clock (see the substitute
// suite for the rationale).
const testLayer = Layer.mergeAll(
  identifiers,
  ThreadHistory.layerTransient,
  RunContextPreparationPassthrough,
);

layer(testLayer, { excludeTestServices: true })("Code Mode end to end", (it) => {
  it.effect(
    "RUN-036 CAP-016 observes the inner Cause while CodeModeFailure recovers to a completed durable Run",
    () => {
      class LookupFailure extends Schema.TaggedError<LookupFailure>()("LookupFailure", {
        message: Schema.String,
        privateDetail: Schema.String,
      }) {}
      const original = LookupFailure.make({
        message: "Lookup unavailable",
        privateDetail: "CODE_MODE_CAUSE_SECRET",
      });
      const Lookup = Tool.make("lookup", {
        parameters: Schema.Struct({ value: Schema.Int }),
        success: Schema.String,
        failure: LookupFailure,
      }).annotate(ToolExecutionClass, "readonly");
      const Declared = Tool.make("declared", {
        parameters: Schema.Struct({ value: Schema.Int }),
        success: Schema.String,
        failure: LookupFailure,
        failureMode: "return",
      }).annotate(ToolExecutionClass, "readonly");
      const codeMode = CodeMode.make("search_workspace", {
        description: "Search the workspace",
        tools: { workspace: { declared: Declared, lookup: Lookup } },
      });
      const definition = Agent.make("code-mode-observer-regression", {
        input: Schema.String,
        output: Schema.String,
        instructions: "Search, then answer.",
        toolkit: Toolkit.make(codeMode.tool),
        policy: AgentPolicy.make({
          maxTurns: 2,
          maxToolCalls: 4,
          maxDuration: "30 seconds",
          toolConcurrency: 1,
        }),
      });
      const observations: Array<ToolFailureObservation> = [];
      const logs: Array<unknown> = [];
      const logger = Logger.make<unknown, void>(({ message, cause, fiber }) => {
        logs.push({
          message,
          cause: Cause.pretty(cause),
          annotations: fiber.getRef(References.CurrentLogAnnotations),
        });
      });
      const runtimeLayer = DurableAgentRuntime.layer.pipe(
        Layer.provideMerge(
          Layer.mergeAll(
            MemoryThreadStoreLive,
            MemorySubmissionLedgerLive,
            WakeScheduler.layerNoop,
            DurableRuntimeFailpoint.layer,
            ToolReconciler.uncertain,
            DurableRuntimeConfig.layer({
              deploymentId: DeploymentId.make("observer-regression"),
              producerId: ProducerId.make("observer-regression"),
            }),
            toolFailureObserverLayer({
              observe: (observation) =>
                Effect.sync(() => {
                  observations.push(observation);
                }),
            }),
          ),
        ),
        Layer.provide(NodeCrypto.layer),
      );
      const handlers = codeMode.handlers.pipe(
        Layer.provide([
          Toolkit.make(Declared, Lookup).toLayer({
            declared: () =>
              Effect.fail(
                LookupFailure.make({
                  message: "Expected lookup failure",
                  privateDetail: "CODE_MODE_DECLARED_SECRET",
                }),
              ),
            lookup: () => Effect.fail(original),
          }),
          inProcessCodeExecutorLayer,
        ]),
      );
      return Effect.gen(function* () {
        const turn = yield* Ref.make(0);
        const model = Model.make(
          "scripted",
          "observer-regression",
          Layer.effect(
            LanguageModel.LanguageModel,
            LanguageModel.make({
              generateText: () => Effect.succeed([]),
              streamText: () =>
                Stream.unwrap(
                  Ref.getAndUpdate(turn, (n) => n + 1).pipe(
                    Effect.map((n) =>
                      Stream.fromIterable<Response.StreamPartEncoded>(
                        n === 0
                          ? [
                              {
                                type: "tool-call",
                                id: "search-1",
                                name: "search_workspace",
                                params: {
                                  code: `async () => {
                                    try { await workspace.declared({ value: 1 }) } catch {}
                                    return await workspace.lookup({ value: 1 })
                                  }`,
                                },
                                providerExecuted: false,
                              },
                              { type: "finish", reason: "tool-calls", usage },
                            ]
                          : [
                              { type: "text-start", id: "answer" },
                              { type: "text-delta", id: "answer", delta: '"fallback answer"' },
                              { type: "text-end", id: "answer" },
                              { type: "finish", reason: "stop", usage },
                            ],
                      ),
                    ),
                  ),
                ),
            }),
          ),
        );
        const agent = Agent.withModel(definition, model);
        const runtime = yield* DurableAgentRuntime;
        const threadId = ThreadId.make("code-mode-observer");
        const digest = Digest.make("a".repeat(64));
        const receipt = yield* runtime.submit(agent, "search", {
          threadId,
          principal: Principal.make("test"),
          idempotencyKey: IdempotencyKey.make("search"),
          definitions: DefinitionDigests.make({ agent: digest, model: digest, tools: digest }),
        });
        yield* runtime.processThread(agent, threadId).pipe(Effect.provide(handlers));
        const settlement = yield* runtime.awaitSettlement(receipt);
        expect(settlement.outcome).toBe("completed");
        expect(observations).toHaveLength(3);
        expect(observations[0]).toMatchObject({
          _tag: "ProgrammaticToolFailure",
          kind: "declared-failure",
          tag: "LookupFailure",
          parentToolCallId: "search-1",
          toolCallId: "search-1#0",
        });
        for (const field of ["cause", "message", "result", "encodedResult"])
          expect(observations[0]).not.toHaveProperty(field);
        expect(observations[1]).toMatchObject({
          _tag: "ProgrammaticToolFailure",
          kind: "handler-error",
          tag: "LookupFailure",
          parentToolCallId: "search-1",
          toolCallId: "search-1#1",
        });
        expect(observations[1]?.cause?.reasons.filter(Cause.isFailReason)[0]?.error).toBe(original);
        expect(observations[1]).not.toHaveProperty("message");
        expect(observations[2]).toMatchObject({
          _tag: "ModelToolFailure",
          kind: "declared-failure",
          tag: "CodeModeFailure",
          toolCallId: "search-1",
        });
        expect(observations[2]).not.toHaveProperty("cause");
        expect(observations[2]).not.toHaveProperty("message");
        const store = yield* ThreadStore;
        const records = yield* store
          .read(ThreadRead.make({ threadId, limit: 1_024 }))
          .pipe(Stream.runCollect);
        const tools = records.filter(
          (envelope) => envelope.record.payload._tag === "ToolCallSettled",
        );
        expect(tools).toHaveLength(1);
        expect(tools[0]?.record.payload).toMatchObject({
          toolCallId: "search-1",
          isFailure: true,
          result: { _tag: "CodeModeFailure" },
        });
        expect(JSON.stringify({ records, logs })).not.toContain("CODE_MODE_CAUSE_SECRET");
        expect(JSON.stringify({ records, logs })).not.toContain("CODE_MODE_DECLARED_SECRET");
        expect(JSON.stringify(records)).not.toContain("search-1#0");
        expect(JSON.stringify(records)).not.toContain("search-1#1");
      }).pipe(Effect.provide([runtimeLayer, Logger.layer([logger])]));
    },
  );

  it.effect(
    "CAP-014 a generated program composes allowlisted Tools through the broker and executor",
    () =>
      Effect.gen(function* () {
        const outcome = yield* runScenario({
          maxToolCalls: 4,
          code: `async () => {
            const first = await warehouse.query({ sql: "SELECT * FROM invoices WHERE page = 1" });
            const second = await warehouse.query({ sql: "SELECT * FROM invoices WHERE page = 2" });
            const rows = [...first.rows, ...second.rows].filter((row) => row.revenue > 1000);
            console.log("kept", rows.length, "rows");
            return { top: rows.map((row) => row.customer), count: rows.length };
          }`,
        });
        expect(outcome.answer).toEqual({ answer: "done" });
        expect(outcome.handlerCalls).toEqual([
          "SELECT * FROM invoices WHERE page = 1",
          "SELECT * FROM invoices WHERE page = 2",
        ]);
        expect(outcome.toolResults).toHaveLength(1);
        expect(outcome.toolResults[0].isFailure).toBe(false);
        expect(outcome.toolResults[0].result).toMatchObject({
          result: { top: ["acme", "globex"], count: 2 },
          logs: ["kept 2 rows"],
        });
      }),
  );

  it.effect(
    "RUN-017 a generated program observes mid-pass budget exhaustion as a catchable envelope",
    () =>
      Effect.gen(function* () {
        // maxToolCalls 2 = the outer call plus exactly one inner call: the
        // second query fails mid-pass and the program branches on the
        // envelope instead of the Run aborting.
        const outcome = yield* runScenario({
          maxToolCalls: 2,
          code: `async () => {
            const first = await warehouse.query({ sql: "SELECT * FROM invoices WHERE page = 1" });
            try {
              await warehouse.query({ sql: "SELECT * FROM invoices WHERE page = 2" });
              return { exhausted: false };
            } catch (envelope) {
              return { exhausted: true, tag: envelope._tag, firstRows: first.rows.length };
            }
          }`,
        });
        expect(outcome.handlerCalls).toEqual(["SELECT * FROM invoices WHERE page = 1"]);
        expect(outcome.toolResults[0].isFailure).toBe(false);
        expect(outcome.toolResults[0].result).toMatchObject({
          result: { exhausted: true, tag: "AgentPolicyError", firstRows: 2 },
        });
      }),
  );

  it.effect(
    "SEC-015 a generated program filters the real read-only warehouse into a bounded answer",
    () =>
      Effect.gen(function* () {
        // The C3 reference scenario (plan §8.2): real SQLite behind the
        // tenant-scoped read-only fixture; the executor sees only namespace
        // methods — no connection, credential, or network authority. The
        // program pulls a broad result, filters locally, and also proves the
        // database authority end to end by catching a denied UPDATE.
        const codeMode = CodeMode.make("run_javascript", {
          description: "Execute JavaScript over the read-only warehouse",
          tools: { warehouse: { query: warehouseQueryTool } },
        });
        const agent = Agent.make("code-mode-c3", {
          input: Schema.Struct({ question: Schema.String }),
          output: Schema.Struct({ answer: Schema.String }),
          instructions: "Answer with run_javascript.",
          toolkit: Toolkit.make(codeMode.tool),
          policy: AgentPolicy.make({
            maxTurns: 2,
            maxToolCalls: 4,
            maxDuration: "30 seconds",
            toolConcurrency: 1,
          }),
        });
        const observed = yield* Ref.make<ReadonlyArray<unknown>>([]);
        const model = Model.make(
          "scripted",
          "code-mode-c3",
          Layer.effect(
            LanguageModel.LanguageModel,
            Effect.gen(function* () {
              const turn = yield* Ref.make(0);
              const code = `async () => {
                const broad = await warehouse.query({
                  sql: "SELECT customer, revenue FROM invoice_summary ORDER BY revenue DESC",
                });
                const big = broad.rows.filter((row) => row.revenue > 5000);
                let writeDenied = false;
                try {
                  await warehouse.query({ sql: "UPDATE invoice_summary SET revenue = 0" });
                } catch (envelope) {
                  writeDenied = envelope._tag === "WarehouseQueryDenied";
                }
                console.log("kept", big.length, "of", broad.rows.length);
                return { top: big.map((row) => row.customer), writeDenied };
              }`;
              return yield* LanguageModel.make({
                generateText: () => Effect.succeed([]),
                streamText: ({ prompt }) =>
                  Stream.unwrap(
                    Ref.getAndUpdate(turn, (value) => value + 1).pipe(
                      Effect.tap(() =>
                        Ref.update(observed, (all) => [
                          ...all,
                          ...prompt.content
                            .filter((message) => message.role === "tool")
                            .flatMap((message) => message.content)
                            .filter((part) => part.type === "tool-result")
                            .map((part) => part.result),
                        ]),
                      ),
                      Effect.map((value) =>
                        Stream.fromIterable<Response.StreamPartEncoded>(
                          value === 0
                            ? [
                                {
                                  type: "tool-call",
                                  id: "code-c3",
                                  name: "run_javascript",
                                  params: { code },
                                  providerExecuted: false,
                                },
                                { type: "finish", reason: "tool-calls", usage },
                              ]
                            : [
                                { type: "text-start", id: "answer" },
                                { type: "text-delta", id: "answer", delta: '{"answer":"done"}' },
                                { type: "text-end", id: "answer" },
                                { type: "finish", reason: "stop", usage },
                              ],
                        ),
                      ),
                    ),
                  ),
              });
            }),
          ),
        );
        const handlerLayer = codeMode.handlers.pipe(
          Layer.provide(warehouseHandlersLayer),
          Layer.provide(warehouseDbLayer({ tenant: "acme", seed: warehouseDemoSeed })),
          Layer.provide(inProcessCodeExecutorLayer),
        );
        const result = yield* AgentRuntime.run(
          Agent.withModel(agent, model),
          { question: "top customers" },
          {},
        ).pipe(Effect.provide(handlerLayer), Effect.scoped);
        expect(result.output).toEqual({ answer: "done" });
        const [toolResult] = yield* Ref.get(observed);
        expect(toolResult).toMatchObject({
          result: {
            top: ["Stellar Freight", "Nimbus Analytics", "Harbor Lights Ltd"],
            writeDenied: true,
          },
          logs: ["kept 3 of 4"],
        });
      }),
  );
});
