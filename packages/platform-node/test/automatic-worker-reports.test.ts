import * as Subagent from "@effect-agent/capabilities/Subagent";
import * as Agent from "@effect-agent/core/Agent";
import { ThreadId } from "@effect-agent/core/Identifiers";
import { WorkerCompletion, WorkerError } from "@effect-agent/core/Worker";
import { SubagentHost } from "@effect-agent/engine/SubagentHost";
import * as NodeHost from "@effect-agent/platform-node/NodeDurableHost";
import { DurableAgentRuntime } from "@effect-agent/thread/DurableAgentRuntime";
import { MessageDeliveryStore } from "@effect-agent/thread/MessageDelivery";
import { DefinitionDigestInput } from "@effect-agent/thread/Records";
import { AbortCommand, IdempotencyKey, Principal } from "@effect-agent/thread/SubmissionLedger";
import { ThreadExportRequest, ThreadStore } from "@effect-agent/thread/ThreadStore";
import { WorkerHostAuthorizer } from "@effect-agent/thread/WorkerHost";
import { NodeFileSystem } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Context, Deferred, Effect, Fiber, FileSystem, Layer, Ref, Schema, Stream } from "effect";
import { LanguageModel, Model, Toolkit, type Response } from "effect/unstable/ai";

const principal = Schema.decodeSync(Principal)("automatic-owner");
const threadId = Schema.decodeSync(ThreadId)("automatic-parent");
const key = Schema.decodeSync(IdempotencyKey);
const input = Schema.Struct({ question: Schema.String });
const output = Schema.Struct({ answer: Schema.String });

const finish = (answer: string): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: JSON.stringify({ answer }) },
  { type: "text-end", id: "answer" },
  {
    type: "finish",
    reason: "stop",
    usage: { inputTokens: { total: 5 }, outputTokens: { total: 1 } },
  },
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

for (const parentState of ["active", "completed", "aborted"] as const) {
  for (const childState of ["completed", "failed", "defect", "aborted", "partial"] as const) {
    it.live(
      `${childState} worker reports to ${parentState} parent with an ordinary input Schema`,
      () =>
        Effect.scoped(
          Effect.gen(function* () {
            const fs = yield* FileSystem.FileSystem;
            const directory = yield* fs.makeTempDirectoryScoped({ prefix: "automatic-report-" });
            const parentEntered = yield* Deferred.make<void>();
            const releaseParent = yield* Deferred.make<void>();
            const childEntered = yield* Deferred.make<void>();
            const prompts = yield* Ref.make<ReadonlyArray<string>>([]);
            let parentCalls = 0;
            let applicationPrompts = 0;

            const child = Agent.withModel(
              Agent.make("automatic-child", {
                input,
                output,
                instructions: "Research",
                toolkit: Toolkit.empty,
                policy: {
                  maxTurns: 1,
                  maxToolCalls: 1,
                  maxDuration: "10 seconds",
                  ...(childState === "partial"
                    ? { tokenBudget: 1, onExhaustion: "final-answer" }
                    : {}),
                },
              }),
              model("child", () => {
                if (childState === "failed")
                  return Stream.fromIterable(
                    finish("finding").map((part) =>
                      part.type === "text-delta" ? { ...part, delta: "{}" } : part,
                    ),
                  );
                if (childState === "defect") return Stream.die("private child diagnostic");
                if (childState === "aborted")
                  return Stream.fromEffectDrain(
                    Deferred.succeed(childEntered, undefined).pipe(Effect.andThen(Effect.never)),
                  );

                return Stream.fromIterable(finish("finding"));
              }),
            );

            const declaration = Subagent.make("research", { target: child.definition });

            const background = Subagent.background(declaration, {
              start: true,
              followUp: true,
              reportToParent: true,
            });

            const parent = Agent.withModel(
              Agent.make("automatic-parent-agent", {
                input,
                output,
                instructions: ({ question }) => `Discuss ${question}; handle completion messages.`,
                inputPrompt: ({ question }) => {
                  applicationPrompts++;

                  return question;
                },
                toolkit: background.toolkit,
                policy: {
                  maxTurns: 10,
                  maxToolCalls: 10,
                  maxDuration: "30 seconds",
                  toolConcurrency: 2,
                },
              }),
              model("parent", ({ prompt }) =>
                Stream.unwrap(
                  Effect.gen(function* () {
                    yield* Ref.update(prompts, (seen) => [...seen, JSON.stringify(prompt)]);
                    const call = parentCalls++;

                    if (call === 0)
                      return Stream.fromIterable<Response.StreamPartEncoded>([
                        {
                          type: "tool-call",
                          id: "start",
                          name: "research_start",
                          params: { question: "research" },
                          providerExecuted: false,
                        },
                        {
                          type: "finish",
                          reason: "tool-calls",
                          usage: { inputTokens: {}, outputTokens: {} },
                        },
                      ]);
                    if (call === 1)
                      return Stream.fromEffectDrain(
                        Deferred.succeed(parentEntered, undefined).pipe(
                          Effect.andThen(Deferred.await(releaseParent)),
                        ),
                      ).pipe(Stream.concat(Stream.fromIterable(finish("I can keep chatting"))));

                    return Stream.fromIterable(finish("Here are the findings"));
                  }),
                ),
              ),
            );

            const context = yield* Layer.build(
              NodeHost.NodeDurableHost.layerRegistered(
                [
                  {
                    agent: parent,
                    definitions: DefinitionDigestInput.make({
                      agent: "parent-v1",
                      model: "v1",
                      tools: ["research_start", "research_follow_up"],
                    }),
                  },
                  {
                    agent: child,
                    definitions: DefinitionDigestInput.make({
                      agent: "child-v1",
                      model: "v1",
                      tools: [],
                    }),
                  },
                ],
                {
                  filename: `${directory}/runtime.sqlite`,
                  deploymentId: "automatic-v1",
                  producerId: "node",
                  workerConcurrency: 1,
                  wakeScanInterval: 10,
                  settlementPollInterval: 10,
                },
              ).pipe(
                Layer.provide(background.layer),
                Layer.provide(
                  Layer.succeed(WorkerHostAuthorizer, {
                    authorize: (request) =>
                      request.principal === principal
                        ? Effect.succeed(principal)
                        : WorkerError.make({ operation: request.operation, reason: "denied" }),
                  }),
                ),
              ),
            );

            const runtime = Context.get(context, DurableAgentRuntime);
            const store = Context.get(context, ThreadStore);
            const deliveries = Context.get(context, MessageDeliveryStore);

            yield* Context.get(context, NodeHost.NodeDurableHost)
              .runWorkers(Effect.never)
              .pipe(Effect.forkChild);

            const receipt = yield* runtime.submitRegistered(
              parent,
              { question: "Lisbon" },
              { threadId, principal, idempotencyKey: key("parent") },
            );

            const parentFiber = yield* runtime
              .processThreadResolved(threadId)
              .pipe(Effect.forkChild);

            yield* Deferred.await(parentEntered);

            const owner = yield* runtime.workerHost({
              sourceThreadId: threadId,
              principal,
              sourceSubmissionId: receipt.submissionId,
            });

            const page = yield* Subagent.list(declaration).pipe(
              Effect.provideService(SubagentHost, owner),
            );

            const worker = page.items[0];

            if (worker === undefined || worker.latestReceipt === null)
              return yield* Effect.die("Missing worker");
            const workerReceipt = worker.latestReceipt;

            if (parentState === "completed") {
              yield* Deferred.succeed(releaseParent, undefined);
              yield* Fiber.join(parentFiber);
              expect((yield* runtime.awaitSettlement(receipt)).outcome).toBe("completed");
            } else if (parentState === "aborted") {
              yield* runtime.abort(
                AbortCommand.make({
                  submissionId: receipt.submissionId,
                  author: principal,
                  reason: "cancel parent",
                }),
              );
              yield* Fiber.join(parentFiber);
              expect((yield* runtime.awaitSettlement(receipt)).outcome).toBe("aborted");
            }

            const childFiber = yield* runtime
              .processThreadResolved(worker.worker.threadId)
              .pipe(Effect.exit, Effect.forkChild);

            if (childState === "aborted") {
              yield* Deferred.await(childEntered);
              yield* Subagent.cancel(declaration, worker.worker, workerReceipt).pipe(
                Effect.provideService(SubagentHost, owner),
              );
            }
            yield* Fiber.join(childFiber);
            if (childState === "defect") {
              const before = yield* store.export(
                ThreadExportRequest.make({ threadId: worker.worker.threadId }),
              );

              expect(
                before.records.filter(
                  ({ record }) => record.payload._tag === "WorkerReportPrepared",
                ),
              ).toHaveLength(0);
              yield* runtime.abort(
                AbortCommand.make({
                  submissionId: workerReceipt.submissionId,
                  author: principal,
                  reason: "resolve failed attempt",
                }),
              );
              yield* runtime.processThreadResolved(worker.worker.threadId);
            }
            expect((yield* runtime.awaitSettlement(workerReceipt)).outcome).toBe(
              childState === "partial"
                ? "completed"
                : childState === "defect"
                  ? "aborted"
                  : childState,
            );

            const delivery = yield* Effect.gen(function* () {
              for (;;) {
                const rows = yield* deliveries.list({
                  ownerThreadId: worker.worker.threadId,
                  limit: 100,
                });

                if (rows.items[0]?.receipt !== null && rows.items[0]?.receipt !== undefined)
                  return rows.items[0];
                yield* Effect.sleep(10);
              }
            }).pipe(Effect.timeout("3 seconds"));

            if (parentState === "active") {
              yield* Deferred.succeed(releaseParent, undefined);
              yield* Fiber.join(parentFiber);
            } else {
              yield* runtime.processThreadResolved(threadId);
            }
            const log = yield* store.export(ThreadExportRequest.make({ threadId }));

            const inputs = log.records.flatMap(({ record }) =>
              record.payload._tag === "UserInputRecorded" ? [record.payload] : [],
            );

            expect(inputs).toHaveLength(2);

            const message = yield* Schema.decodeUnknownEffect(WorkerCompletion)(
              inputs[1]?.messageAdmission,
            );

            expect(message.report).toMatchObject({
              worker: worker.worker,
              receipt: workerReceipt,
              outcome:
                childState === "partial"
                  ? "completed"
                  : childState === "defect"
                    ? "aborted"
                    : childState,
            });
            expect(message.budgetExhausted).toBe(childState === "partial");
            if (message.report.outcome === "completed")
              expect(message.report.result).toEqual({
                output: { answer: "finding" },
                budgetExhausted: childState === "partial",
              });
            else
              expect(message.report.failure.classification).toBe(
                childState === "aborted" || childState === "defect"
                  ? "child-aborted"
                  : "child-failed",
              );
            expect(inputs[1]?.input).toEqual({ question: "Lisbon" });
            expect(inputs[1]?.runId === inputs[0]?.runId).toBe(parentState === "active");
            expect(applicationPrompts).toBe(1);
            expect((yield* Ref.get(prompts)).at(-1)).toContain("WorkerCompletion");
            expect(JSON.stringify(message)).not.toContain("private child diagnostic");
            expect(delivery.envelope.input).toEqual({ question: "Lisbon" });
          }),
        ).pipe(Effect.provide(NodeFileSystem.layer)),
      10_000,
    );
  }
}
