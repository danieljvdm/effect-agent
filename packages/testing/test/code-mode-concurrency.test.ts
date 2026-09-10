import * as CodeMode from "@effect-agent/capabilities/CodeMode";
import * as Agent from "@effect-agent/core/Agent";
import { ThreadId } from "@effect-agent/core/Identifiers";
import { IdGenerator } from "@effect-agent/core/IdGenerator";
import * as AgentRuntime from "@effect-agent/engine/AgentRuntime";
import { RunContextPreparationPassthrough } from "@effect-agent/engine/RunOptions";
import { ThreadHistory } from "@effect-agent/engine/ThreadHistory";
import { CodeExecutionLimits } from "@effect-agent/sandbox/CodeExecutor";
import { MemorySubmissionLedgerLive } from "@effect-agent/storage-memory/MemorySubmissionLedger";
import { MemoryThreadStoreLive } from "@effect-agent/storage-memory/MemoryThreadStore";
import { inProcessCodeExecutorLayer } from "@effect-agent/testing/CodeExecutorSubstitute";
import {
  DurableAgentRuntime,
  DurableRuntimeConfig,
} from "@effect-agent/thread/DurableAgentRuntime";
import { DurableRuntimeFailpoint } from "@effect-agent/thread/DurableFailpoint";
import { DefinitionDigests, DeploymentId, Digest, ProducerId } from "@effect-agent/thread/Records";
import { IdempotencyKey, Principal } from "@effect-agent/thread/SubmissionLedger";
import { ToolReconciler } from "@effect-agent/thread/ToolReconciler";
import { WakeScheduler } from "@effect-agent/thread/WakeScheduler";
import { NodeCrypto } from "@effect/platform-node";
import { expect, layer } from "@effect/vitest";
import { Cause, Deferred, Duration, Effect, Exit, Fiber, Layer, Schema, Stream } from "effect";
import { LanguageModel, Model, Tool, Toolkit, type Response } from "effect/unstable/ai";

class WriteFailure extends Schema.TaggedError<WriteFailure>()("WriteFailure", {}) {}

const Write = Tool.make("write", {
  parameters: Schema.Struct({ id: Schema.Int }),
  success: Schema.Int,
  failure: WriteFailure,
  failureMode: "return",
});

const scenario = (
  code: string,
  handler: (id: number) => Effect.Effect<number, WriteFailure>,
  options: { readonly wallMillis?: number; readonly maxEgressBytes?: number } = {},
) => {
  const reports: Array<CodeMode.CodeModePassReport> = [];
  const results: Array<unknown> = [];

  const mode = CodeMode.make("run_code", {
    description: "Write the selected records",
    tools: { tools: { write: Write } },
    maxEgressBytes: options.maxEgressBytes,
    limits: CodeExecutionLimits.make({
      maxSourceBytes: 16_384,
      maxWallTime: Duration.millis(options.wallMillis ?? 2_000),
      maxLogBytes: 1_024,
      maxResultBytes: 16_384,
      maxHostCalls: 16,
      maxHostCallArgumentBytes: 1_024,
      maxHostCallResultBytes: 1_024,
      maxHostCallConcurrency: 2,
    }),
    onPassExit: (report) =>
      Effect.sync(() => {
        reports.push(report);
      }),
  });

  const definition = Agent.make("writes", {
    input: Schema.String,
    output: Schema.String,
    instructions: "Run the program then finish.",
    toolkit: Toolkit.make(mode.tool),
    policy: { maxTurns: 2, maxToolCalls: 20, maxDuration: "5 seconds", toolConcurrency: 1 },
  });

  const usage = { inputTokens: {}, outputTokens: {} };

  const model = Model.make(
    "scripted",
    "writes",
    Layer.effect(
      LanguageModel.LanguageModel,
      Effect.gen(function* () {
        let turn = 0;

        return yield* LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: ({ prompt }) =>
            Stream.unwrap(
              Effect.sync(() => {
                results.push(
                  ...prompt.content
                    .filter((m) => m.role === "tool")
                    .flatMap((m) => m.content)
                    .filter((p) => p.type === "tool-result")
                    .map((p) => p.result),
                );

                const parts: ReadonlyArray<Response.StreamPartEncoded> =
                  turn++ === 0
                    ? [
                        { type: "tool-call", id: "program", name: "run_code", params: { code } },
                        { type: "finish", reason: "tool-calls", usage },
                      ]
                    : [
                        { type: "text-start", id: "answer" },
                        { type: "text-delta", id: "answer", delta: '"done"' },
                        { type: "text-end", id: "answer" },
                        { type: "finish", reason: "stop", usage },
                      ];

                return Stream.fromIterable(parts);
              }),
            ),
        });
      }),
    ),
  );

  const handlers = mode.handlers.pipe(
    Layer.provide([
      Toolkit.make(Write).toLayer({ write: ({ id }) => handler(id) }),
      inProcessCodeExecutorLayer,
    ]),
  );

  const agent = Agent.withModel(definition, model);

  return {
    reports,
    results,
    agent,
    handlers,
    run: AgentRuntime.run(agent, "go").pipe(Effect.provide(handlers), Effect.scoped),
  };
};

layer(
  Layer.mergeAll(IdGenerator.layer, ThreadHistory.layerTransient, RunContextPreparationPassthrough),
  { excludeTestServices: true },
)("Code Mode writes and concurrency", (it) => {
  it.effect(
    "runs dependencies in order and independent writes with a finite concurrency limit",
    () =>
      Effect.gen(function* () {
        const completed: Array<number> = [];
        let active = 0;
        let peak = 0;

        const test = scenario(
          `async () => {
      const project = await tools.write({ id: 0 });
      const tasks = await Promise.all([1, 2, 3, 4, 5].map(id => tools.write({ id })));
      return { project, tasks };
    }`,
          (id) =>
            Effect.gen(function* () {
              if (id > 0) expect(completed).toContain(0);
              active++;
              peak = Math.max(peak, active);
              yield* Effect.sleep("5 millis");
              completed.push(id);

              return id;
            }).pipe(
              Effect.ensuring(
                Effect.sync(() => {
                  active--;
                }),
              ),
            ),
        );

        yield* test.run;
        expect(peak).toBe(2);
        expect(active).toBe(0);
        expect(test.results[0]).toMatchObject({ result: { project: 0, tasks: [1, 2, 3, 4, 5] } });
        expect(test.reports[0]?.calls.map((call) => call.status)).toEqual(
          Array(6).fill("succeeded"),
        );
      }),
  );

  it.effect(
    "reports completed writes, declared failures, and interrupted siblings in invocation order",
    () =>
      Effect.gen(function* () {
        const started = yield* Deferred.make<void>();
        let finalized = false;

        const test = scenario(
          `async () => {
      await tools.write({ id: 0 });
      return await Promise.all([tools.write({ id: 1 }), tools.write({ id: 2 })]);
    }`,
          (id) =>
            id === 0
              ? Effect.succeed(id)
              : id === 1
                ? Deferred.succeed(started, undefined).pipe(
                    Effect.andThen(Effect.never),
                    Effect.ensuring(
                      Effect.sync(() => {
                        finalized = true;
                      }),
                    ),
                  )
                : Deferred.await(started).pipe(Effect.andThen(Effect.fail(new WriteFailure({})))),
        );

        yield* test.run;
        expect(finalized).toBe(true);
        expect(test.reports[0]?.status).toBe("failed");
        expect(test.reports[0]?.calls.map((call) => call.status)).toEqual([
          "succeeded",
          "uncertain",
          "failed",
        ]);
        expect(test.results[0]).toMatchObject({
          _tag: "CodeModeFailure",
          calls: test.reports[0]?.calls,
          omittedCalls: 0,
        });
      }),
  );

  it.effect("preserves interruption and reports uncertain writes after finalization", () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      let finalized = false;

      const test = scenario(`async () => await tools.write({ id: 1 })`, () =>
        Deferred.succeed(started, undefined).pipe(
          Effect.andThen(Effect.never),
          Effect.ensuring(
            Effect.sync(() => {
              finalized = true;
            }),
          ),
        ),
      );

      const fiber = yield* test.run.pipe(Effect.forkChild);

      yield* Deferred.await(started);
      yield* Fiber.interrupt(fiber);
      const exit = yield* Fiber.await(fiber);

      expect(Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause)).toBe(true);
      expect(finalized).toBe(true);
      expect(test.reports).toMatchObject([
        { status: "interrupted", calls: [{ status: "uncertain" }] },
      ]);
    }),
  );

  it.effect("reports timeout without replaying or claiming a started write failed", () =>
    Effect.gen(function* () {
      let starts = 0;

      const test = scenario(
        `async () => await tools.write({ id: 1 })`,
        () =>
          Effect.sync(() => {
            starts++;
          }).pipe(Effect.andThen(Effect.never)),
        { wallMillis: 30 },
      );

      yield* test.run;
      expect(starts).toBe(1);
      expect(test.results[0]).toMatchObject({
        errorTag: "CodeExecutionTimeoutError",
        calls: [{ status: "uncertain" }],
      });
    }),
  );

  it.effect("keeps defects as defects and reports their uncertain write", () =>
    Effect.gen(function* () {
      const test = scenario(`async () => await tools.write({ id: 1 })`, () =>
        Effect.die("handler defect"),
      );

      const exit = yield* test.run.pipe(Effect.exit);

      expect(Exit.isFailure(exit)).toBe(true);
      expect(test.reports).toMatchObject([{ status: "defect", calls: [{ status: "uncertain" }] }]);
    }),
  );

  it.effect("bounds failure evidence and explicitly reports omitted calls", () =>
    Effect.gen(function* () {
      const test = scenario(
        `async () => {
      for (let id = 0; id < 8; id++) await tools.write({ id });
      throw 'x'.repeat(2000);
    }`,
        Effect.succeed,
        { maxEgressBytes: 256 },
      );

      yield* test.run;
      const failure = Schema.decodeUnknownSync(CodeMode.CodeModeFailure)(test.results[0]);

      expect(failure.omittedCalls).toBeGreaterThan(0);
      expect(new TextEncoder().encode(JSON.stringify(failure)).byteLength).toBeLessThanOrEqual(256);
      expect(test.reports[0]?.calls).toHaveLength(8);
    }),
  );
  it.effect("does not replay a program interrupted after a write under durable recovery", () =>
    Effect.gen(function* () {
      let writes = 0;

      const test = scenario(
        `async () => {
      await tools.write({ id: 0 });
      return await tools.write({ id: 1 });
    }`,
        (id) => (id === 0 ? Effect.sync(() => ++writes) : Effect.interrupt),
      );

      const runtimeLayer = DurableAgentRuntime.layer.pipe(
        Layer.provideMerge(
          Layer.mergeAll(
            MemoryThreadStoreLive,
            MemorySubmissionLedgerLive,
            WakeScheduler.layerNoop,
            DurableRuntimeFailpoint.layer,
            ToolReconciler.uncertain,
            DurableRuntimeConfig.layer({
              deploymentId: DeploymentId.make("code-mode"),
              producerId: ProducerId.make("code-mode"),
            }),
          ),
        ),
        Layer.provide(NodeCrypto.layer),
      );

      yield* Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const threadId = ThreadId.make("interrupted-writes");
        const digest = Digest.make("a".repeat(64));

        const receipt = yield* runtime.submit(test.agent, "go", {
          threadId,
          principal: Principal.make("test"),
          idempotencyKey: IdempotencyKey.make("write"),
          definitions: DefinitionDigests.make({ agent: digest, model: digest, tools: digest }),
        });

        const killed = yield* runtime
          .processThread(test.agent, threadId)
          .pipe(Effect.provide(test.handlers), Effect.exit);

        expect(Exit.isFailure(killed)).toBe(true);
        expect(writes).toBe(1);
        const reports = yield* runtime.runRecovery;

        expect(
          reports.find((report) => report.submissionId === receipt.submissionId)?.decision._tag,
        ).toBe("MarkUnknown");
        yield* runtime.processThread(test.agent, threadId).pipe(Effect.provide(test.handlers));
        expect(writes).toBe(1);
      }).pipe(Effect.provide(runtimeLayer), Effect.scoped);
    }),
  );
});
