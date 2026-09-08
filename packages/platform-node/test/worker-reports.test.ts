import * as Subagent from "@effect-agent/capabilities/Subagent";
import * as Agent from "@effect-agent/core/Agent";
import { ThreadId } from "@effect-agent/core/Identifiers";
import { WorkerError } from "@effect-agent/core/Worker";
import { SubagentHost } from "@effect-agent/engine/SubagentHost";
import * as NodeHost from "@effect-agent/platform-node/NodeDurableHost";
import { DurableAgentRuntime } from "@effect-agent/thread/DurableAgentRuntime";
import { DurableRuntimeFailpointError } from "@effect-agent/thread/DurableFailpoint";
import { MessageDeliveryStore } from "@effect-agent/thread/MessageDelivery";
import { DefinitionDigestInput } from "@effect-agent/thread/Records";
import { IdempotencyKey, Principal } from "@effect-agent/thread/SubmissionLedger";
import { ThreadExportRequest, ThreadStore } from "@effect-agent/thread/ThreadStore";
import { WorkerHostAuthorizer } from "@effect-agent/thread/WorkerHost";
import { NodeFileSystem } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Context, Effect, Exit, FileSystem, Layer, Ref, Schema, Scope, Stream } from "effect";
import { LanguageModel, Model, Toolkit, type Response } from "effect/unstable/ai";

const principal = Schema.decodeSync(Principal)("report-owner");
const sourceThreadId = Schema.decodeSync(ThreadId)("report-source");
const key = Schema.decodeSync(IdempotencyKey);
const definitions = DefinitionDigestInput.make({ agent: "report-v1", model: "v1", tools: [] });
const input = Schema.Struct({ question: Schema.String });
const output = Schema.Struct({ answer: Schema.String });

const parts: ReadonlyArray<Response.StreamPartEncoded> = [
  { type: "text-start", id: "reply" },
  { type: "text-delta", id: "reply", delta: '{"answer":"done"}' },
  { type: "text-end", id: "reply" },
  { type: "finish", reason: "stop", usage: { inputTokens: {}, outputTokens: {} } },
];

const agent = (id: string) =>
  Agent.withModel(
    Agent.make(id, {
      input,
      output,
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

it.live(
  "recovers one frozen report for joined child inputs after both Node lanes restart",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "worker-report-" });
        const source = agent("report-source-agent");
        const child = agent("report-child-agent");

        const declaration = Subagent.make("research", {
          target: child.definition,
          success: output,
          projectResult: (value) => Effect.succeed(value),
          policy: Subagent.SubagentPolicy.make({
            maxChildren: 8,
            maxConcurrency: 2,
            maxTurns: 2,
            maxToolCalls: 1,
            maxDuration: "2 seconds",
          }),
        });

        const projected = yield* Ref.make(0);

        const reporting = Subagent.reporting(declaration, {
          input,
          prepare: (report) =>
            Ref.update(projected, (count) => count + 1).pipe(
              Effect.as({
                question:
                  report.outcome === "completed"
                    ? `report:${report.runId}:${report.result.answer}`
                    : "report:failed",
              }),
            ),
        });

        const registrations = [
          { agent: source, definitions, reporting: [reporting] },
          { agent: child, definitions },
        ];

        const authority = Layer.succeed(WorkerHostAuthorizer)({
          authorize: (request) =>
            request.principal === principal
              ? Effect.succeed(principal)
              : WorkerError.make({ operation: request.operation, reason: "denied" }),
        });

        const options = {
          filename: `${directory}/runtime.sqlite`,
          deploymentId: "reports-v1",
          producerId: "report-node",
          workerConcurrency: 1,
          wakeScanInterval: 10,
          settlementPollInterval: 10,
        };

        const firstScope = yield* Scope.make();

        yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void));

        const first = yield* Layer.build(
          NodeHost.NodeDurableHost.layerRegistered(registrations, {
            ...options,
            runtimeFailpoint: (location) =>
              location === "worker:after-report-append"
                ? DurableRuntimeFailpointError.make({ location })
                : Effect.void,
          }).pipe(Layer.provide(authority)),
        ).pipe(Scope.provide(firstScope));

        const runtime = Context.get(first, DurableAgentRuntime);

        const sourceReceipt = yield* runtime.submitRegistered(
          source,
          { question: "launch complete" },
          {
            threadId: sourceThreadId,
            principal,
            idempotencyKey: key("source"),
          },
        );

        yield* runtime.processThreadResolved(sourceThreadId);
        const sourceSettlement = yield* runtime.awaitSettlement(sourceReceipt);

        expect(sourceSettlement.outcome).toBe("completed");
        const host = yield* runtime.workerHost({ sourceThreadId, principal });

        const started = yield* Subagent.start(
          declaration,
          { question: "host input" },
          { idempotencyKey: key("first") },
        ).pipe(Effect.provideService(SubagentHost, host));

        const joined = yield* Subagent.followUp(
          declaration,
          started.worker,
          { question: "joined input" },
          { idempotencyKey: key("joined") },
        ).pipe(Effect.provideService(SubagentHost, host));

        yield* runtime.processThreadResolved(started.worker.threadId).pipe(Effect.result);

        const firstLog = yield* Context.get(first, ThreadStore).export(
          ThreadExportRequest.make({ threadId: started.worker.threadId }),
        );

        const decisions = firstLog.records.flatMap(({ record }) =>
          record.payload._tag === "WorkerReportPrepared" ? [record.payload] : [],
        );

        expect(decisions).toHaveLength(1);
        expect(yield* Ref.get(projected)).toBe(1);
        expect(
          (yield* Context.get(first, MessageDeliveryStore).list({
            ownerThreadId: started.worker.threadId,
            limit: 100,
          })).items,
        ).toHaveLength(0);
        yield* Scope.close(firstScope, Exit.void);

        // No live source Run or retained wake fiber survives this complete host restart.
        const second = yield* Layer.build(
          NodeHost.layer(registrations, options).pipe(Layer.provide(authority)),
        );

        const reopened = Context.get(second, DurableAgentRuntime);
        const store = Context.get(second, ThreadStore);
        const deliveries = Context.get(second, MessageDeliveryStore);

        yield* Effect.gen(function* () {
          for (;;) {
            const rows = yield* deliveries.list({
              ownerThreadId: started.worker.threadId,
              limit: 100,
            });

            if (rows.items.length === 1 && rows.items[0]?.status === "processed") return;
            yield* Effect.sleep("10 millis");
          }
        }).pipe(Effect.timeout("10 seconds"));
        const firstResult = yield* reopened.awaitSettlement(started.receipt);
        const joinedResult = yield* reopened.awaitSettlement(joined);

        expect(firstResult.outcome).toBe("completed");
        expect(joinedResult.outcome).toBe("completed");

        const sourceLog = yield* store.export(
          ThreadExportRequest.make({ threadId: sourceThreadId }),
        );

        const inputs = sourceLog.records.flatMap(({ record }) =>
          record.payload._tag === "UserInputRecorded" ? [record.payload] : [],
        );

        expect(inputs).toHaveLength(2);
        expect(inputs[1]?.input).toEqual({ question: `report:${decisions[0]?.runId}:done` });
        expect(inputs[1]?.runId).not.toBe(inputs[0]?.runId);

        const settlements = sourceLog.records.flatMap(({ record }) =>
          record.payload._tag === "SubmissionSettled" ? [record.payload] : [],
        );

        expect(settlements).toHaveLength(2);
        expect(settlements.every((settlement) => settlement.outcome === "completed")).toBe(true);

        const childLog = yield* store.export(
          ThreadExportRequest.make({ threadId: started.worker.threadId }),
        );

        const childSettlements = childLog.records.flatMap(({ record }) =>
          record.payload._tag === "SubmissionSettled" ? [record.payload] : [],
        );

        expect(childSettlements).toHaveLength(2);
        expect(childSettlements.map((settlement) => settlement.runId)).toEqual([
          decisions[0]?.runId,
          decisions[0]?.runId,
        ]);
        expect(
          childLog.records.flatMap(({ record }) =>
            record.payload._tag === "WorkerReportPrepared" ? [record.payload] : [],
          ),
        ).toEqual(decisions);
        expect(yield* Ref.get(projected)).toBe(1);
        expect(
          (yield* deliveries.list({ ownerThreadId: started.worker.threadId, limit: 100 })).items,
        ).toHaveLength(1);
      }),
    ).pipe(Effect.provide(NodeFileSystem.layer)),
  15_000,
);
