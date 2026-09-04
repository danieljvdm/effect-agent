import * as Subagent from "@effect-agent/capabilities/Subagent";
import { SubagentPolicy, SubagentRuntime } from "@effect-agent/capabilities/Subagent";
import { SubagentReservationsMemoryLive } from "@effect-agent/capabilities/SubagentReservations";
import * as Agent from "@effect-agent/core/Agent";
import { AgentPolicy } from "@effect-agent/core/AgentPolicy";
import { ThreadId, ToolCallId } from "@effect-agent/core/Identifiers";
import { IdGenerator } from "@effect-agent/core/IdGenerator";
import { ToolExecutionClass } from "@effect-agent/engine/DurableStep";
import { NodeDurableAgentRuntime } from "@effect-agent/platform-node/NodeDurableAgentRuntime";
import { compileRegistrations, DurableWorkerBinding } from "@effect-agent/thread/AgentRegistration";
import { digestDefinitions } from "@effect-agent/thread/Digest";
import { DurableAgentRuntime } from "@effect-agent/thread/DurableAgentRuntime";
import {
  DurableRuntimeFailpointError,
  type DurableRuntimeFailpointLocation,
} from "@effect-agent/thread/DurableFailpoint";
import { DefinitionDigests, DefinitionDigestInput, Digest } from "@effect-agent/thread/Records";
import { childThreadIdFor } from "@effect-agent/thread/RunJournal";
import {
  ApprovalDecisionCommand,
  IdempotencyKey,
  Principal,
} from "@effect-agent/thread/SubmissionLedger";
import { ThreadRead, ThreadStore } from "@effect-agent/thread/ThreadStore";
import { NodeCrypto, NodeFileSystem } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Cause, Effect, Exit, FileSystem, Layer, Option, Schema, Stream } from "effect";
import { LanguageModel, Model, Tool, Toolkit, type Response } from "effect/unstable/ai";

const digest = Schema.decodeSync(Digest)("a".repeat(64));
const digests = DefinitionDigests.make({ agent: digest, model: digest, tools: digest });
const delegationCall = Schema.decodeSync(ToolCallId)("delegate-1");
const usage = { inputTokens: {}, outputTokens: {} };

class ProbeFailed extends Schema.TaggedError<ProbeFailed>()("ProbeFailed", {
  tag: Schema.String,
}) {}

const finalParts: ReadonlyArray<Response.StreamPartEncoded> = [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: '{"answer":"partial"}' },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage },
];

const toolTurn = (
  id: string,
  name: string,
  params: unknown,
): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "tool-call", id, name, params, providerExecuted: false },
  { type: "finish", reason: "tool-calls", usage },
];

const readLog = Effect.fn("AllowanceTest.readLog")(function* (threadId: ThreadId) {
  const store = yield* ThreadStore;

  return yield* Stream.runCollect(store.read(ThreadRead.make({ threadId, limit: 1024 })));
});

it.effect("shares a durable delegation pool across calls and SQLite reopen", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({ prefix: "subagent-shared-budget-" });

      const target = Agent.make("shared-budget-child", {
        input: Schema.String,
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Answer.",
        toolkit: Toolkit.empty,
      });

      const delegation = Subagent.define("delegate_shared_budget", {
        target,
        failureMode: "return",
      });

      const versions = DefinitionDigestInput.make({
        agent: "shared-budget-v1",
        model: "scripted-v1",
        tools: [],
      });

      const sharedDigests = yield* digestDefinitions(versions).pipe(
        Effect.provide(NodeCrypto.layer),
      );

      const childModel = Model.make(
        "scripted",
        "shared-child",
        Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: () => Stream.fromIterable(finalParts),
          }),
        ),
      );

      const parent = Agent.withModel(
        Agent.make("shared-budget-parent", {
          input: Schema.String,
          output: Schema.Struct({ answer: Schema.String }),
          instructions: "Delegate twice.",
          toolkit: Toolkit.make(delegation.tool),
          policy: { maxTurns: 3, maxToolCalls: 2, toolConcurrency: 1 },
        }),
        Model.make(
          "scripted",
          "shared-parent",
          Layer.effect(
            LanguageModel.LanguageModel,
            LanguageModel.make({
              generateText: () => Effect.succeed([]),
              streamText: (request) => {
                const count = request.prompt.content.filter(
                  (message) => message.role === "tool",
                ).length;

                return Stream.fromIterable(
                  count < 2 ? toolTurn(`shared-${count}`, delegation.name, "question") : finalParts,
                );
              },
            }),
          ),
        ),
      );

      const handlers = SubagentRuntime.layer(delegation, childModel, {
        durable: { targetDigests: sharedDigests },
      }).pipe(Layer.provide([SubagentReservationsMemoryLive, IdGenerator.layer]));

      const bindings = yield* compileRegistrations([
        { agent: parent, definitions: versions },
        { agent: target, model: childModel, definitions: versions },
      ]).pipe(Effect.provide([handlers, NodeCrypto.layer]));

      const parentId = Schema.decodeSync(ThreadId)("shared-budget-parent");

      const runtimeLayer = () =>
        NodeDurableAgentRuntime.layerWithBindings(bindings, {
          filename: `${directory}/shared.sqlite`,
          deploymentId: "shared-budget",
          producerId: "shared-budget",
        });

      yield* Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;

        const receipt = yield* runtime.submit(parent, "start", {
          threadId: parentId,
          principal: Schema.decodeSync(Principal)("test"),
          idempotencyKey: Schema.decodeSync(IdempotencyKey)("shared"),
          definitions: sharedDigests,
        });

        expect(yield* runtime.processThreadResolved(parentId)).toEqual([]);
        yield* runtime.processThreadResolved(
          childThreadIdFor(receipt.submissionId, Schema.decodeSync(ToolCallId)("shared-0")),
        );

        return receipt;
      }).pipe(Effect.provide(runtimeLayer()));
      yield* Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;

        expect((yield* runtime.processThreadResolved(parentId))[0]?.outcome).toBe("completed");
        const log = yield* readLog(parentId);

        expect(
          log.filter(({ record }) => record.payload._tag === "SubagentRequested"),
        ).toHaveLength(1);

        const settlements = log
          .filter(({ record }) => record.payload._tag === "ToolCallSettled")
          .map(({ record }) => record.payload);

        expect(settlements).toEqual(
          expect.arrayContaining([
            expect.objectContaining({
              result: { output: { answer: "partial" }, budgetExhausted: false },
            }),
            expect.objectContaining({
              result: expect.objectContaining({
                _tag: "SubagentExecutionFailure",
                errorTag: "SubagentBudgetExhausted",
              }),
            }),
          ]),
        );
      }).pipe(Effect.provide(runtimeLayer()));
    }),
  ).pipe(Effect.provide(NodeFileSystem.layer)),
);

it.effect(
  "persists child allowances through establishment faults, approval suspension, and SQLite reopen without replenishing usage",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "subagent-allowance-" });

        const rows = [
          {
            requested: undefined,
            definition: 8,
            reservation: 4,
            effective: 1,
            fault: "subagent:after-request-append",
          },
          {
            requested: 99,
            definition: 8,
            reservation: 3,
            effective: 3,
            fault: "subagent:after-admit",
          },
          {
            requested: 99,
            definition: 2,
            reservation: 4,
            effective: 2,
            fault: "subagent:after-child-ready",
          },
        ] as const;

        for (const [index, row] of rows.entries()) {
          const filename = `${directory}/${index}.sqlite`;
          let incarnation = 0;

          const withRuntime = <A, E, R>(
            effect: Effect.Effect<A, E, R>,
            fault?: DurableRuntimeFailpointLocation,
          ) =>
            effect.pipe(
              Effect.provide(
                NodeDurableAgentRuntime.layerWithBindings(bindings, {
                  filename,
                  deploymentId: "allowance-test",
                  producerId: `producer-${incarnation++}`,
                  runtimeFailpoint: (location) =>
                    location === fault
                      ? Effect.fail(DurableRuntimeFailpointError.make({ location }))
                      : Effect.void,
                }),
              ),
            );

          const starts: Array<number> = [];
          let offeredAllowance = 1;

          const tools = Toolkit.make(
            Tool.make("probe", {
              parameters: Schema.Struct({ index: Schema.Int }),
              success: Schema.String,
              needsApproval: ({ index }) => index === 1,
            }).annotate(ToolExecutionClass, "readonly"),
          );

          const handlers = tools.toLayer({
            probe: ({ index }) =>
              Effect.sync(() => {
                starts.push(index);

                return "found";
              }),
          });

          const childDefinition = Agent.make("allowance-child", {
            input: Schema.String,
            output: Schema.Struct({ answer: Schema.String }),
            instructions: "Probe until the budget is exhausted.",
            toolkit: tools,
            policy: { maxToolCalls: row.definition },
          });

          // Prompt-derived calls survive replacement runtimes without a live model counter.
          const child = Agent.withModel(
            childDefinition,
            Model.make(
              "scripted",
              "allowance-child",
              Layer.effect(
                LanguageModel.LanguageModel,
                LanguageModel.make({
                  generateText: () => Effect.succeed([]),
                  streamText: (request) => {
                    const count = request.prompt.content
                      .flatMap((message) => (message.role === "tool" ? message.content : []))
                      .filter(
                        (part) => part.type === "tool-result" && part.name === "probe",
                      ).length;

                    return Stream.fromIterable(
                      request.toolChoice === "none"
                        ? finalParts
                        : toolTurn(`probe-${count + 1}`, "probe", { index: count + 1 }),
                    );
                  },
                }),
              ),
            ),
          );

          const delegation = Subagent.define("delegate_probe", {
            target: childDefinition,
            description: "Run bounded probes.",
            parameters: Schema.Struct({ allowance: Schema.optionalKey(Schema.Number) }),
            success: Schema.Struct({ exhausted: Schema.Boolean }),
            failure: ProbeFailed,
            prepareInput: () => Effect.succeed("probe"),
            projectResult: (_, context) => Effect.succeed({ exhausted: context.budgetExhausted }),
            toolCallAllowance: {
              default: 1,
              fromParameters: ({ allowance }) => allowance ?? offeredAllowance,
            },
            policy: SubagentPolicy.make({
              maxChildren: 1,
              maxConcurrency: 1,
              maxTurns: 12,
              maxToolCalls: row.reservation,
              maxDuration: "5 minutes",
            }),
          });

          const parent = Agent.withModel(
            Agent.make("allowance-parent", {
              input: Schema.String,
              output: Schema.Struct({ answer: Schema.String }),
              instructions: "Delegate.",
              toolkit: Toolkit.make(delegation.tool),
              policy: AgentPolicy.make({
                maxTurns: 3,
                maxToolCalls: 1,
                maxDuration: "5 minutes",
                toolConcurrency: 1,
              }),
            }),
            Model.make(
              "scripted",
              "allowance-parent",
              Layer.effect(
                LanguageModel.LanguageModel,
                LanguageModel.make({
                  generateText: () => Effect.succeed([]),
                  streamText: (request) =>
                    Stream.fromIterable(
                      request.prompt.content.some((message) => message.role === "tool")
                        ? finalParts
                        : toolTurn(
                            delegationCall,
                            delegation.name,
                            row.requested === undefined ? {} : { allowance: row.requested },
                          ),
                    ),
                }),
              ),
            ),
          );

          const delegationLayer = SubagentRuntime.layer(delegation, child.model, {
            mapChildFailure: (failure) => new ProbeFailed({ tag: failure._tag }),
            durable: { targetDigests: digests },
          }).pipe(Layer.provide([handlers, SubagentReservationsMemoryLive, IdGenerator.layer]));

          const bindings = [
            yield* DurableWorkerBinding.make(parent, digests).pipe(Effect.provide(delegationLayer)),
            yield* DurableWorkerBinding.make(child, digests).pipe(Effect.provide(handlers)),
          ];

          const parentId = Schema.decodeSync(ThreadId)(`allowance-parent-${index}`);

          const receipt = yield* withRuntime(
            Effect.gen(function* () {
              const runtime = yield* DurableAgentRuntime;

              const receipt = yield* runtime.submit(parent, "probe", {
                threadId: parentId,
                principal: Schema.decodeSync(Principal)("allowance-test"),
                idempotencyKey: Schema.decodeSync(IdempotencyKey)("one"),
                definitions: digests,
              });

              const exit = yield* Effect.exit(runtime.processThreadResolved(parentId));

              expect(
                Exit.isFailure(exit) ? Cause.findErrorOption(exit.cause) : Option.none(),
              ).toEqual(Option.some(DurableRuntimeFailpointError.make({ location: row.fault })));

              return receipt;
            }),
            row.fault,
          );

          // Re-evaluating the delegation on the next Attempt must not raise its pinned limit.
          offeredAllowance = 99;
          const childId = childThreadIdFor(receipt.submissionId, delegationCall);

          const childSubmission = yield* withRuntime(
            Effect.gen(function* () {
              const runtime = yield* DurableAgentRuntime;

              // Complete admission from the request record, without a delegation handler.
              yield* runtime.runRecovery;
              yield* runtime.processThreadResolved(parentId);
              expect(yield* runtime.processThreadResolved(childId)).toEqual([]);
              expect(starts).toEqual([]);
              const explanations = yield* runtime.explainThread(childId);

              expect(explanations[0]?.evidence.approvalsPending).toHaveLength(1);
              const submission = explanations[0]?.submission;

              if (submission === undefined) return yield* Effect.die("Missing child Submission");

              return submission.submissionId;
            }),
          );

          yield* withRuntime(
            Effect.gen(function* () {
              const runtime = yield* DurableAgentRuntime;

              yield* runtime.resolveApproval(
                ApprovalDecisionCommand.make({
                  submissionId: childSubmission,
                  toolCallId: Schema.decodeSync(ToolCallId)("probe-1"),
                  decision: "approved",
                  resolver: "test",
                  reason: "first probe approved",
                }),
              );
              const exit = yield* Effect.exit(runtime.processThreadResolved(childId));

              expect(
                Exit.isFailure(exit) ? Cause.findErrorOption(exit.cause) : Option.none(),
              ).toEqual(
                Option.some(
                  DurableRuntimeFailpointError.make({ location: "turn:after-results-append" }),
                ),
              );
              expect(starts).toEqual([1]);
            }),
            "turn:after-results-append",
          );
          yield* withRuntime(
            Effect.gen(function* () {
              const runtime = yield* DurableAgentRuntime;

              yield* runtime.runRecovery;
              const settlements = yield* runtime.processThreadResolved(childId);

              expect(settlements[0]?.outcome).toBe("completed");
              expect(starts).toEqual(
                Array.from({ length: row.effective }, (_, index) => index + 1),
              );
              expect((yield* runtime.processThreadResolved(parentId))[0]?.outcome).toBe(
                "completed",
              );
              const parentLog = yield* readLog(parentId);
              const childLog = yield* readLog(childId);

              expect(
                childLog.find(({ record }) => record.payload._tag === "SubmissionSettled")?.record
                  .payload,
              ).toMatchObject({ finishReason: "budget-exhausted" });
              expect(
                parentLog.find(({ record }) => record.payload._tag === "SubagentRequested")?.record
                  .payload,
              ).toMatchObject({
                toolCallAllowance: row.effective,
                policy: { maxTurns: 3, toolConcurrency: 1 },
              });
              expect(
                childLog.find(({ record }) => record.payload._tag === "SubagentLineageRecorded")
                  ?.record.payload,
              ).toMatchObject({
                toolCallAllowance: row.effective,
                policy: { maxTurns: 3, toolConcurrency: 1 },
              });
              expect(
                parentLog.find(({ record }) => record.payload._tag === "ToolCallSettled")?.record
                  .payload,
              ).toMatchObject({ result: { exhausted: true } });
            }),
          );
        }
      }),
    ).pipe(Effect.provide(NodeFileSystem.layer)),
  30_000,
);
