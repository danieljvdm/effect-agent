import * as Subagent from "@effect-agent/capabilities/Subagent";
import * as Agent from "@effect-agent/core/Agent";
import { ThreadId } from "@effect-agent/core/Identifiers";
import type { Receipt } from "@effect-agent/core/Receipt";
import { WorkerError } from "@effect-agent/core/Worker";
import { SubagentHost } from "@effect-agent/engine/SubagentHost";
import * as NodeHost from "@effect-agent/platform-node/NodeDurableHost";
import { DurableAgentRuntime } from "@effect-agent/thread/DurableAgentRuntime";
import { DefinitionDigestInput } from "@effect-agent/thread/Records";
import { IdempotencyKey, Principal } from "@effect-agent/thread/SubmissionLedger";
import { ThreadExportRequest, ThreadStore } from "@effect-agent/thread/ThreadStore";
import { WorkerHostAuthorizer } from "@effect-agent/thread/WorkerHost";
import { NodeFileSystem } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Context, Effect, Exit, Fiber, FileSystem, Layer, Schema, Scope, Stream } from "effect";
import { TestClock } from "effect/testing";
import { LanguageModel, Model, Toolkit, type Response } from "effect/unstable/ai";

const principal = Schema.decodeSync(Principal)("worker-owner");
const sourceThreadId = Schema.decodeSync(ThreadId)("background-source");
const key = Schema.decodeSync(IdempotencyKey);
const definitions = DefinitionDigestInput.make({ agent: "background-v1", model: "v1", tools: [] });

const parts: ReadonlyArray<Response.StreamPartEncoded> = [
  { type: "text-start", id: "reply" },
  { type: "text-delta", id: "reply", delta: '{"answer":"done"}' },
  { type: "text-end", id: "reply" },
  { type: "finish", reason: "stop", usage: { inputTokens: {}, outputTokens: {} } },
];

const agent = (id: string) =>
  Agent.withModel(
    Agent.make(id, {
      input: Schema.Struct({ question: Schema.String }),
      output: Schema.Struct({ answer: Schema.String }),
      instructions: "Answer as JSON.",
      toolkit: Toolkit.empty,
      policy: { maxTurns: 20, maxToolCalls: 20, maxDuration: "1 minute", toolConcurrency: 2 },
    }),
    Model.make(
      "scripted",
      id,
      Layer.effect(
        LanguageModel.LanguageModel,
        LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: () => Stream.fromIterable(parts),
        }),
      ),
    ),
  );

const source = agent("background-source-agent");
const target = agent("background-target-agent");

const declaration = Subagent.make("research", {
  target: target.definition,
  success: target.definition.output,
  projectResult: (output) => Effect.succeed(output),
  policy: Subagent.SubagentPolicy.make({
    maxChildren: 8,
    maxConcurrency: 2,
    maxTurns: 1,
    maxToolCalls: 1,
    maxDuration: "1 second",
  }),
});

const authority = Layer.succeed(WorkerHostAuthorizer)({
  authorize: (request) =>
    request.principal === principal && request.sourceThreadId === sourceThreadId
      ? Effect.succeed(principal)
      : WorkerError.make({ operation: request.operation, reason: "denied" }),
});

const withFacet = <A, E>(
  facet: SubagentHost["Service"],
  effect: Effect.Effect<A, E, SubagentHost>,
) => effect.pipe(Effect.provideService(SubagentHost, facet));

const untilSettled = (
  facet: SubagentHost["Service"],
  started: { readonly worker: Subagent.Worker<"research">; readonly receipt: Receipt },
) =>
  Effect.gen(function* () {
    for (let attempt = 0; attempt < 200; attempt++) {
      const result = yield* withFacet(
        facet,
        Subagent.inspect(declaration, started.worker, started.receipt),
      );

      if (result._tag === "Settled") return result;
      yield* TestClock.adjust(10);
      yield* Effect.yieldNow;
    }

    return yield* Effect.die("Worker receipt did not settle");
  });

it.effect(
  "retains worker ownership across a Node restart and accepts another input after both agents settle",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "background-worker-" });

        const options = {
          filename: `${directory}/runtime.sqlite`,
          deploymentId: "background-v1",
          producerId: "background-node",
          workerConcurrency: 1,
          wakeScanInterval: 10,
          settlementPollInterval: 10,
        };

        const registrations = [
          { agent: source, definitions },
          { agent: target, definitions },
        ];

        const firstScope = yield* Scope.make();

        yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void));

        const first = yield* Layer.build(
          NodeHost.NodeDurableHost.layerRegistered(registrations, options).pipe(
            Layer.provide(authority),
          ),
        ).pipe(Scope.provide(firstScope));

        const runtime = Context.get(first, DurableAgentRuntime);

        const sourceReceipt = yield* runtime.submitRegistered(
          source,
          { question: "prepare workspace" },
          {
            threadId: sourceThreadId,
            principal,
            idempotencyKey: key("source"),
          },
        );

        yield* runtime.processThreadResolved(sourceThreadId);
        expect((yield* runtime.awaitSettlement(sourceReceipt)).outcome).toBe("completed");
        const facet = yield* runtime.workerHost({ sourceThreadId, principal });

        const started = yield* withFacet(
          facet,
          Subagent.start(declaration, { question: "first" }, { idempotencyKey: key("first") }),
        );

        expect(
          yield* withFacet(
            facet,
            Subagent.start(declaration, { question: "first" }, { idempotencyKey: key("first") }),
          ),
        ).toEqual(started);
        expect(
          yield* withFacet(
            facet,
            Subagent.start(declaration, { question: "changed" }, { idempotencyKey: key("first") }),
          ).pipe(Effect.result),
        ).toMatchObject({ _tag: "Failure", failure: { reason: "idempotency-conflict" } });
        expect(
          yield* withFacet(facet, Subagent.inspect(declaration, started.worker, started.receipt)),
        ).toEqual({ _tag: "Pending", receipt: started.receipt });

        const waiter = yield* withFacet(
          facet,
          Subagent.await(declaration, started.worker, started.receipt),
        ).pipe(Effect.forkChild);

        yield* Fiber.interrupt(waiter);
        expect(
          yield* withFacet(facet, Subagent.inspect(declaration, started.worker, started.receipt)),
        ).toEqual({ _tag: "Pending", receipt: started.receipt });
        yield* Scope.close(firstScope, Exit.void);

        const second = yield* Layer.build(
          NodeHost.layer(registrations, options).pipe(Layer.provide(authority)),
        );

        const reopened = Context.get(second, DurableAgentRuntime);
        const owner = yield* reopened.workerHost({ sourceThreadId, principal });
        const firstResult = yield* untilSettled(owner, started);

        expect(firstResult).toMatchObject({ outcome: "completed", result: { answer: "done" } });

        const nextReceipt = yield* withFacet(
          owner,
          Subagent.followUp(
            declaration,
            started.worker,
            { question: "second" },
            { idempotencyKey: key("second") },
          ),
        );

        expect(nextReceipt.threadId).toBe(started.worker.threadId);
        expect(nextReceipt.submissionId).not.toBe(started.receipt.submissionId);

        const secondResult = yield* untilSettled(owner, {
          worker: started.worker,
          receipt: nextReceipt,
        });

        expect(secondResult).toMatchObject({ outcome: "completed", result: { answer: "done" } });
        expect(secondResult.runId).not.toBe(firstResult.runId);
        expect(yield* withFacet(owner, Subagent.list(declaration))).toEqual({
          items: [{ worker: started.worker, latestReceipt: nextReceipt, state: "idle" }],
          next: null,
        });

        const sourceLog = yield* Context.get(second, ThreadStore).export(
          ThreadExportRequest.make({ threadId: sourceThreadId }),
        );

        expect(
          sourceLog.records.filter(({ record }) => record.payload._tag === "WorkerInputCompleted"),
        ).toHaveLength(2);

        const childLog = yield* Context.get(second, ThreadStore).export(
          ThreadExportRequest.make({ threadId: started.worker.threadId }),
        );

        expect(
          childLog.records.filter(({ record }) => record.payload._tag === "WorkerOriginRecorded"),
        ).toHaveLength(1);
        expect(
          yield* reopened
            .submitRegistered(
              target,
              { question: "bypass" },
              { threadId: started.worker.threadId, principal, idempotencyKey: key("bypass") },
            )
            .pipe(Effect.result),
        ).toMatchObject({ _tag: "Failure" });
        expect(
          yield* reopened
            .workerHost({ sourceThreadId, principal: Schema.decodeSync(Principal)("stranger") })
            .pipe(Effect.result),
        ).toMatchObject({ _tag: "Failure", failure: { reason: "denied" } });
      }),
    ).pipe(Effect.provide(NodeFileSystem.layer)),
  15_000,
);
