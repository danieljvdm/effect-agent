import { MemorySubmissionLedgerLive } from "@effect-agent/storage-memory/memory-submission-ledger";
import { MemoryThreadStoreLive } from "@effect-agent/storage-memory/memory-thread-store";
import { inProcessCodeExecutorLayer } from "@effect-agent/testing/code-executor-substitute";
import { NodeCrypto } from "@effect/platform-node";
import { expect, layer } from "@effect/vitest";
import { Deferred, Duration, Effect, Exit, Layer, Schema, Stream } from "effect";
import * as Agent from "effect-agent/agent";
import * as AgentRuntime from "effect-agent/agent-runtime";
import { CodeExecutionLimits } from "effect-agent/code-executor";
import * as CodeMode from "effect-agent/code-mode";
import { DurableAgentRuntime, DurableRuntimeConfig } from "effect-agent/durable-agent-runtime";
import { DurableRuntimeFailpoint } from "effect-agent/durable-failpoint";
import { ThreadId } from "effect-agent/identifiers";
import { DefinitionDigests, DeploymentId, Digest, ProducerId } from "effect-agent/records";
import { RunContextPreparationPassthrough } from "effect-agent/run-options";
import { IdempotencyKey, Principal } from "effect-agent/submission-ledger";
import { ThreadHistory } from "effect-agent/thread-history";
import { ToolReconciler } from "effect-agent/tool-reconciler";
import { WakeScheduler } from "effect-agent/wake-scheduler";
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
  options: { readonly wallMillis?: number } = {},
) => {
  const reports: Array<CodeMode.CodeModePassReport> = [];
  const results: Array<unknown> = [];

  const mode = CodeMode.make("run_code", {
    description: "Write the selected records",
    tools: { tools: { write: Write } },
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

layer(Layer.mergeAll(ThreadHistory.layer, RunContextPreparationPassthrough), {
  excludeTestServices: true,
})("Code Mode writes and concurrency", (it) => {
  it.effect("reports completed writes, declared failures, and interrupted siblings", () =>
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
      expect(test.reports[0]?.calls.map((call) => call.status)).toEqual(
        expect.arrayContaining(["succeeded", "uncertain", "failed"]),
      );
      expect(test.results[0]).toMatchObject({
        _tag: "CodeModeFailure",
        calls: test.reports[0]?.calls,
        omittedCalls: 0,
      });
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
        const reports = (yield* runtime.runRecovery()).reports;

        expect(
          reports.find((report) => report.submissionId === receipt.submissionId)?.decision._tag,
        ).toBe("MarkUnknown");
        yield* runtime.processThread(test.agent, threadId).pipe(Effect.provide(test.handlers));
        expect(writes).toBe(1);
      }).pipe(Effect.provide(runtimeLayer), Effect.scoped);
    }),
  );
});
