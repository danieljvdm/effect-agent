import * as Subagent from "@effect-agent/capabilities/Subagent";
import { SubagentReservationsMemoryLive } from "@effect-agent/capabilities/SubagentReservations";
import * as Agent from "@effect-agent/core/Agent";
import { ThreadId, ToolCallId } from "@effect-agent/core/Identifiers";
import { IdGenerator } from "@effect-agent/core/IdGenerator";
import { NodeDurableAgentRuntime } from "@effect-agent/platform-node/NodeDurableAgentRuntime";
import { DurableAgentRuntime } from "@effect-agent/thread/DurableAgentRuntime";
import { DefinitionDigestInput } from "@effect-agent/thread/Records";
import { childThreadIdFor } from "@effect-agent/thread/RunJournal";
import { IdempotencyKey, Principal } from "@effect-agent/thread/SubmissionLedger";
import { ThreadExportRequest, ThreadStore } from "@effect-agent/thread/ThreadStore";
import { NodeFileSystem } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import {
  Context,
  Deferred,
  Effect,
  Fiber,
  FileSystem,
  Layer,
  Option,
  Ref,
  Schema,
  Stream,
} from "effect";
import { LanguageModel, Model, Toolkit, type Response } from "effect/unstable/ai";

const callId = Schema.decodeSync(ToolCallId)("scout-call");

const finalParts: ReadonlyArray<Response.StreamPartEncoded> = [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: '"done"' },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage: { inputTokens: {}, outputTokens: {} } },
];

const delegateParts: ReadonlyArray<Response.StreamPartEncoded> = [
  { type: "tool-call", id: callId, name: "scout", params: "research", providerExecuted: false },
  { type: "finish", reason: "tool-calls", usage: { inputTokens: {}, outputTokens: {} } },
];

const policy = {
  maxTurns: 2,
  maxToolCalls: 1,
  maxDuration: "20 seconds",
  toolConcurrency: 1,
} as const;

for (const boundary of ["ThreadCreated", "SubagentLineageRecorded"] as const) {
  it.effect(
    `finishes attached child establishment when competing claims fence its ${boundary} append`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;

          const directory = yield* fs.makeTempDirectoryScoped({
            prefix: "subagent-establishment-race-",
          });

          const beforeAppend = yield* Deferred.make<void>();
          const resumeAppend = yield* Deferred.make<void>();
          const parentCalls = yield* Ref.make(0);
          const childCalls = yield* Ref.make(0);
          let admitted = false;
          let childAppends = 0;

          const model = (name: string, calls: Ref.Ref<number>, delegates: boolean) =>
            Model.make(
              "scripted",
              name,
              Layer.effect(
                LanguageModel.LanguageModel,
                LanguageModel.make({
                  generateText: () => Effect.succeed([]),
                  streamText: () =>
                    Stream.unwrap(
                      Ref.getAndUpdate(calls, (count) => count + 1).pipe(
                        Effect.map((count) =>
                          Stream.fromIterable(
                            delegates && count === 0 ? delegateParts : finalParts,
                          ),
                        ),
                      ),
                    ),
                }),
              ),
            );

          const child = Agent.withModel(
            Agent.make("establishment-scout", {
              input: Schema.String,
              output: Schema.String,
              instructions: "Answer as JSON.",
              toolkit: Toolkit.empty,
              policy,
            }),
            model("scout", childCalls, false),
          );

          const declaration = Subagent.make("scout", {
            target: child.definition,
            policy: Subagent.SubagentPolicy.make({
              maxChildren: 1,
              maxConcurrency: 1,
              maxTurns: 2,
              maxToolCalls: 1,
              maxDuration: "20 seconds",
              maxResultBytes: 1024,
            }),
          });

          const parent = Agent.withModel(
            Agent.make("establishment-parent", {
              input: Schema.String,
              output: Schema.String,
              instructions: "Attach a scout, then answer as JSON.",
              toolkit: Toolkit.make(declaration.tool),
              policy,
            }),
            model("parent", parentCalls, true),
          );

          const handlers = Subagent.SubagentRuntime.layer(declaration, child).pipe(
            Layer.provide([SubagentReservationsMemoryLive, IdGenerator.layer]),
          );

          const context = yield* Layer.build(
            NodeDurableAgentRuntime.layerRegistered(
              [parent, child].map((agent) => ({
                agent,
                definitions: DefinitionDigestInput.make({
                  agent: agent.definition.id,
                  model: "race-v1",
                  tools: Object.keys(agent.definition.toolkit.tools),
                }),
              })),
              {
                filename: `${directory}/runtime.sqlite`,
                deploymentId: "race-v1",
                producerId: "race-test",
                runtimeFailpoint: (point) =>
                  Effect.sync(() => {
                    if (point === "subagent:after-admit") admitted = true;
                  }),
                storageFailpoint: (point) =>
                  Effect.gen(function* () {
                    if (point !== "append:before" || !admitted) return;
                    childAppends += 1;
                    if (childAppends !== (boundary === "ThreadCreated" ? 1 : 2)) return;
                    admitted = false;
                    yield* Deferred.succeed(beforeAppend, undefined);
                    yield* Deferred.await(resumeAppend);
                  }),
              },
            ).pipe(Layer.provide(handlers)),
          );

          const runtime = Context.get(context, DurableAgentRuntime);
          const store = Context.get(context, ThreadStore);

          const receipt = yield* runtime.submitRegistered(parent, "research", {
            threadId: Schema.decodeSync(ThreadId)("parent"),
            principal: Schema.decodeSync(Principal)("owner"),
            idempotencyKey: Schema.decodeSync(IdempotencyKey)("parent-input"),
          });

          const establishing = yield* runtime
            .processThreadResolved(receipt.threadId)
            .pipe(Effect.forkChild);

          yield* Deferred.await(beforeAppend);
          const childThreadId = childThreadIdFor(receipt.submissionId, callId);

          // SQLite atomically advances the Thread fence on every claim, including admitted
          // children which correctly defer execution until their parent records lineage.
          for (let contender = 0; contender < 3; contender += 1) {
            expect(Option.isNone(yield* runtime.processThreadHead(childThreadId))).toBe(true);
          }
          expect(yield* Ref.get(childCalls)).toBe(0);
          yield* Deferred.succeed(resumeAppend, undefined);
          expect(yield* Fiber.join(establishing)).toEqual([]);

          const established = yield* store.export(
            ThreadExportRequest.make({ threadId: childThreadId }),
          );

          expect(established.records.map(({ record }) => record.payload._tag)).toEqual([
            "ThreadCreated",
            "SubagentLineageRecorded",
          ]);
          expect(
            (yield* runtime.processThreadResolved(childThreadId)).map(
              (settlement) => settlement.outcome,
            ),
          ).toEqual(["completed"]);
          expect(
            (yield* runtime.processThreadResolved(receipt.threadId)).map(
              (settlement) => settlement.outcome,
            ),
          ).toEqual(["completed"]);
          expect(yield* Ref.get(childCalls)).toBe(1);
          expect(yield* Ref.get(parentCalls)).toBe(2);

          const completed = yield* store.export(
            ThreadExportRequest.make({ threadId: receipt.threadId }),
          );

          expect(
            completed.records.filter(({ record }) => record.payload._tag === "SubagentStarted"),
          ).toHaveLength(1);
          expect(
            completed.records.filter(({ record }) => record.payload._tag === "SubagentJoined"),
          ).toHaveLength(1);
          expect((yield* runtime.submissionStatus(receipt))._tag).toBe("settled");
        }),
      ).pipe(Effect.provide(NodeFileSystem.layer)),
  );
}
