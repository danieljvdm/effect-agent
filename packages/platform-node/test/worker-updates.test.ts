import * as NodeHost from "@effect-agent/platform-node/node-durable-host";
import { NodeFileSystem } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Context, Deferred, Effect, Exit, FileSystem, Layer, Schema, Scope, Stream } from "effect";
import * as Agent from "effect-agent/agent";
import { DurableAgentRuntime } from "effect-agent/durable-agent-runtime";
import { ThreadId } from "effect-agent/identifiers";
import { MessageDeliveryStore } from "effect-agent/message-delivery";
import { DefinitionDigestInput } from "effect-agent/records";
import * as Subagent from "effect-agent/subagent";
import { SubagentHost } from "effect-agent/subagent-host";
import { AbortCommand, IdempotencyKey, Principal } from "effect-agent/submission-ledger";
import { ThreadExportRequest, ThreadStore } from "effect-agent/thread-store";
import { WorkerError, WorkerUpdate } from "effect-agent/worker";
import { WorkerHostAuthorizer } from "effect-agent/worker-host";
import { LanguageModel, Model, Toolkit, type Response } from "effect/unstable/ai";

const input = Schema.Struct({ question: Schema.String });
const output = Schema.Struct({ answer: Schema.String });

const areaConcern = Schema.Struct({
  _tag: Schema.Literal("AreaConcern"),
  area: Schema.String,
  recommendation: Schema.String,
});

const concern = {
  _tag: "AreaConcern" as const,
  area: "Johannesburg CBD",
  recommendation: "Rosebank",
};

const principal = Schema.decodeSync(Principal)("update-owner");
const threadId = Schema.decodeSync(ThreadId)("johannesburg-parent");
const key = Schema.decodeSync(IdempotencyKey);
const usage = { inputTokens: {}, outputTokens: {} };

const finish = (answer: string): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: JSON.stringify({ answer }) },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage },
];

const calls = (
  ...tools: ReadonlyArray<{ name: string; params: unknown }>
): ReadonlyArray<Response.StreamPartEncoded> => [
  ...tools.map(({ name, params }, index) => ({
    type: "tool-call" as const,
    id: `${name}-${index}-call`,
    name,
    params,
    providerExecuted: false,
  })),
  { type: "finish", reason: "tool-calls", usage },
];

const model = (name: string, streamText: Parameters<typeof LanguageModel.make>[0]["streamText"]) =>
  Model.make(
    "scripted",
    name,
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({ generateText: () => Effect.succeed([]), streamText }),
    ),
  );

const authority = Layer.succeed(WorkerHostAuthorizer, {
  authorize: (request) =>
    request.principal === principal
      ? Effect.succeed(principal)
      : WorkerError.make({ operation: request.operation, reason: "denied" }),
});

for (const [parentState, failpoint] of [
  ["completed", "update:after-canonical-append"],
  ["aborted", "update:after-delivery-insert"],
] as const) {
  it.live(
    `retains delivered updates after lost acknowledgement at ${failpoint}, restart, and a ${parentState} parent`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const directory = yield* fs.makeTempDirectoryScoped({ prefix: "update-restart-" });
          const lostAck = yield* Deferred.make<void>();
          let childModelCalls = 0;

          const child = Agent.make("restart-hotel", {
            input,
            output,
            updates: areaConcern,
            instructions: "Research hotels and report concerns.",
            toolkit: Toolkit.empty,
            policy: { maxTurns: 2, maxToolCalls: 1, maxDuration: "30 seconds" },
          });

          const declaration = Subagent.make("restart-hotel", { target: child });
          let selected = true;
          let selections = 0;

          const background = Subagent.background(child, {
            start: true,
            reportToParent: true,
            reportUpdate: () => {
              selections++;

              return selected;
            },
          });

          const source = Agent.withModel(
            Agent.make("restart-parent", {
              input,
              output,
              instructions: "Discuss findings.",
              toolkit: background.toolkit,
              policy: { maxTurns: 10, maxToolCalls: 10, maxDuration: "1 minute" },
            }),
            model("restart-parent", () => Stream.fromIterable(finish("I have the finding."))),
          );

          const childBinding = Agent.withModel(
            child,
            model("restart-child", () => {
              childModelCalls++;

              return Stream.fromIterable(
                calls({ name: "emit_update", params: { value: concern } }),
              );
            }),
          );

          const registrations = [source, childBinding].map((agent) => ({
            agent,
            definitions: DefinitionDigestInput.make({
              agent: agent.definition.id,
              model: "v1",
              tools: Object.keys(agent.definition.toolkit.tools),
            }),
          }));

          const options = {
            filename: `${directory}/runtime.sqlite`,
            deploymentId: "restart-updates",
            producerId: "node",
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
                location === failpoint
                  ? Deferred.succeed(lostAck, undefined).pipe(Effect.andThen(Effect.never))
                  : Effect.void,
            }).pipe(Layer.provide([authority, background.layer])),
          ).pipe(Scope.provide(firstScope));

          const runtime = Context.get(first, DurableAgentRuntime);

          const sourceReceipt = yield* runtime.submitRegistered(
            source,
            { question: "Johannesburg" },
            { threadId, principal, idempotencyKey: key("source") },
          );

          if (parentState === "completed") yield* runtime.processThreadResolved(threadId);

          const owner = yield* runtime.workerHost({
            sourceThreadId: threadId,
            principal,
            sourceSubmissionId: sourceReceipt.submissionId,
          });

          const started = yield* Subagent.start(
            declaration,
            { question: "Check the CBD" },
            { idempotencyKey: key("hotel") },
          ).pipe(Effect.provideService(SubagentHost, owner));

          if (parentState === "aborted") {
            yield* runtime.abort(
              AbortCommand.make({
                submissionId: sourceReceipt.submissionId,
                author: principal,
                reason: "cancel parent",
              }),
            );
            yield* runtime.processThreadResolved(threadId);
          }
          expect((yield* runtime.awaitSettlement(sourceReceipt)).outcome).toBe(parentState);
          yield* runtime
            .processThreadResolved(started.worker.threadId)
            .pipe(Effect.forkIn(firstScope));
          yield* Deferred.await(lostAck);

          const before = yield* Context.get(first, ThreadStore).export(
            ThreadExportRequest.make({ threadId: started.worker.threadId }),
          );

          const accepted = before.records.flatMap(({ record }) =>
            record.payload._tag === "AgentUpdateEmitted" ? [record.payload.update] : [],
          );

          expect(accepted).toHaveLength(1);
          expect(
            before.records.filter(({ record }) => record.payload._tag === "ToolCallSettled"),
          ).toHaveLength(0);
          yield* Scope.close(firstScope, Exit.void);
          selected = false;

          const second = yield* Layer.build(
            NodeHost.layer(registrations, options).pipe(
              Layer.provide([authority, background.layer]),
            ),
          );

          const reopened = Context.get(second, DurableAgentRuntime);
          const store = Context.get(second, ThreadStore);
          const deliveries = Context.get(second, MessageDeliveryStore);
          const read = (id: ThreadId) => store.export(ThreadExportRequest.make({ threadId: id }));

          yield* Effect.gen(function* () {
            for (;;) {
              const rows = yield* deliveries.list({
                ownerThreadId: started.worker.threadId,
                limit: 100,
              });

              if (
                rows.items.length === 1 &&
                rows.items[0]?.receipt !== null &&
                rows.items[0]?.receipt !== undefined
              ) {
                yield* reopened.awaitSettlement(rows.items[0].receipt);

                return;
              }
              yield* Effect.sleep(10);
            }
          }).pipe(Effect.timeout("5 seconds"));
          const parentLog = yield* read(threadId);

          const messages = parentLog.records.flatMap(({ record }) =>
            record.payload._tag === "UserInputRecorded" &&
            Schema.is(WorkerUpdate)(record.payload.messageAdmission)
              ? [record.payload.messageAdmission]
              : [],
          );

          expect(messages).toHaveLength(1);
          expect(messages[0]).toMatchObject({ worker: started.worker, update: accepted[0] });
          expect(selections).toBe(1);
          const blocked = yield* read(started.worker.threadId);

          expect(
            blocked.records.filter(({ record }) => record.payload._tag === "AgentUpdateEmitted"),
          ).toHaveLength(1);
          expect(
            blocked.records.filter(({ record }) => record.payload._tag === "RunCompleted"),
          ).toHaveLength(0);
          expect(childModelCalls).toBe(1);
          expect(
            blocked.records.filter(({ record }) => record.payload._tag === "ToolCallUnknown"),
          ).toHaveLength(1);
          yield* reopened.abort(
            AbortCommand.make({
              submissionId: started.delivery.receipt!.submissionId,
              author: principal,
              reason: "resolve interrupted worker",
            }),
          );
          expect((yield* reopened.awaitSettlement(started.delivery.receipt!)).outcome).toBe(
            "aborted",
          );
          const finalChild = yield* read(started.worker.threadId);

          expect(
            finalChild.records.flatMap(({ record }) =>
              record.payload._tag === "AgentUpdateEmitted" ? [record.payload.update] : [],
            ),
          ).toEqual(accepted);
          expect((yield* reopened.awaitSettlement(sourceReceipt)).outcome).toBe(parentState);
          expect(childModelCalls).toBe(1);
        }),
      ).pipe(Effect.provide(NodeFileSystem.layer)),
    15_000,
  );
}
