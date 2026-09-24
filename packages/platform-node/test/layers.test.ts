import {
  NodeDurableAgentRuntime,
  type NodeDurableAgentRuntimeInitializationError,
  type NodeDurableAgentRuntimeOptions,
  type NodeDurableAgentRuntimeServices,
} from "@effect-agent/platform-node/node-durable-agent-runtime";
import { NodeDurableHost } from "@effect-agent/platform-node/node-durable-host";
import * as NodeHost from "@effect-agent/platform-node/node-durable-host";
import { NodeCrypto, NodeFileSystem } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import type { PlatformError } from "effect";
import {
  Cause,
  Context,
  Deferred,
  Effect,
  Exit,
  Fiber,
  FileSystem,
  Layer,
  Ref,
  Schema,
  Stream,
} from "effect";
import * as Agent from "effect-agent/agent";
import { AgentPolicy } from "effect-agent/agent-policy";
import { digestDefinitions } from "effect-agent/digest";
import { DurableAgentRuntime, type DurableSubmitOptions } from "effect-agent/durable-agent-runtime";
import {
  DurableRuntimeFailpoint,
  DurableRuntimeFailpointError,
} from "effect-agent/durable-failpoint";
import { ToolExecutionClass } from "effect-agent/durable-step";
import { ThreadId } from "effect-agent/identifiers";
import { DefinitionDigests, DefinitionDigestInput, Digest } from "effect-agent/records";
import { RunContextPreparation, RunToolAuthorization } from "effect-agent/run-options";
import {
  IdempotencyKey,
  Principal,
  SubmissionLedger,
  SubmissionLookupById,
} from "effect-agent/submission-ledger";
import { ThreadRead, ThreadStore } from "effect-agent/thread-store";
import { ToolReconciler } from "effect-agent/tool-reconciler";
import { TestClock } from "effect/testing";
import { LanguageModel, Model, Prompt, Tool, Toolkit, type Response } from "effect/unstable/ai";

const hostLayerProbe = NodeDurableHost.layer;

const SHA_A = Schema.decodeSync(Digest)("a".repeat(64));
const DIGESTS = DefinitionDigests.make({ agent: SHA_A, model: SHA_A, tools: SHA_A });
const PRINCIPAL = Schema.decodeSync(Principal)("principal-platform-node");
const decodeThreadId = Schema.decodeSync(ThreadId);
const decodeIdempotencyKey = Schema.decodeSync(IdempotencyKey);

const runtimeOptions = (
  filename: string,
  overrides?: Partial<NodeDurableAgentRuntimeOptions>,
): NodeDurableAgentRuntimeOptions => ({
  filename,
  deploymentId: "deployment-platform-node",
  producerId: "producer-platform-node",
  wakeScanInterval: 1_000,
  ...overrides,
});

const submitOptions = (threadId: string, idempotencyKey: string): DurableSubmitOptions => ({
  threadId: decodeThreadId(threadId),
  principal: PRINCIPAL,
  idempotencyKey: decodeIdempotencyKey(idempotencyKey),
  definitions: DIGESTS,
});

const usage = { inputTokens: {}, outputTokens: {} };

const finalParts = (text: string): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: text },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage },
];

const makeScriptedModel = Effect.fn("PlatformNodeTest.makeScriptedModel")(function* (
  script: (call: number, prompt: Prompt.Prompt) => ReadonlyArray<Response.StreamPartEncoded>,
) {
  const calls = yield* Ref.make(0);

  return Model.make(
    "scripted",
    "platform-node-test",
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: ({ prompt }) =>
          Stream.unwrap(
            Ref.getAndUpdate(calls, (call) => call + 1).pipe(
              Effect.map((call) => Stream.fromIterable(script(call, prompt))),
            ),
          ),
      }),
    ),
  );
});

const plannerDefinition = Agent.make("platform-node-planner", {
  input: Schema.Struct({ question: Schema.String }),
  output: Schema.Struct({ answer: Schema.String }),
  instructions: ({ question }) => `Answer ${question} as JSON.`,
  toolkit: Toolkit.empty,
  policy: AgentPolicy.make({
    maxTurns: 3,
    maxToolCalls: 2,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  }),
});

const withTemporaryDatabase = <A, E>(
  use: (filename: string) => Effect.Effect<A, E>,
): Effect.Effect<A, E | PlatformError.PlatformError> =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;

      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "effect-agent-platform-node-",
      });

      return yield* use(`${directory}/host.sqlite`);
    }),
  ).pipe(Effect.provide(NodeFileSystem.layer));

/** One host "process": the full DN stack over `filename`, closed (and drained) when `effect` ends. */
const withHost = <A, E, R>(
  options: NodeDurableAgentRuntimeOptions,
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<
  A,
  E | Layer.Error<typeof hostLayerProbe> | NodeDurableAgentRuntimeInitializationError,
  Exclude<R, NodeDurableHost | NodeDurableAgentRuntimeServices>
> => Effect.provide(effect, NodeDurableHost.layerStack(options));

const failureOf = <A, E>(exit: Exit.Exit<A, E>): unknown => {
  expect(Exit.isFailure(exit)).toBe(true);
  if (Exit.isSuccess(exit)) throw new Error("Expected the Effect to fail");

  return Cause.squash(exit.cause);
};

describe("NodeDurableAgentRuntime", () => {
  it.effect("supervises managed workers and releases their resources on failure", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        const ready = yield* Deferred.make<NodeDurableHost["Service"]>();
        const started = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const marks: Array<string> = [];
        const model = yield* makeScriptedModel(() => finalParts('{"answer":"unused"}'));

        class Resource extends Context.Service<Resource, string>()("test/ManagedHostResource") {}

        const agent = Agent.withModel(
          Agent.make("managed-worker", {
            input: plannerDefinition.input,
            output: plannerDefinition.output,
            instructions: () => Resource,
            toolkit: Toolkit.empty,
          }),
          model,
        );

        const definitions = DefinitionDigestInput.make({ agent: "v1", model: "v1", tools: "v1" });

        const digests = yield* digestDefinitions(definitions).pipe(
          Effect.provide(NodeCrypto.layer),
        );

        const live = NodeHost.layer(
          [{ agent, definitions }],
          runtimeOptions(filename, {
            runtimeFailpoint: (location) =>
              location === "claim:after-claim"
                ? Effect.gen(function* () {
                    yield* Deferred.succeed(started, undefined);
                    yield* Deferred.await(release);

                    return yield* DurableRuntimeFailpointError.make({ location });
                  }).pipe(
                    Effect.ensuring(
                      Effect.sync(() => {
                        marks.push("worker-finalized");
                      }),
                    ),
                  )
                : Effect.void,
          }),
        ).pipe(
          Layer.provide(
            Layer.effect(
              Resource,
              Effect.acquireRelease(Effect.succeed("Answer as JSON."), () =>
                Effect.sync(() => {
                  marks.push("resource-finalized");
                }),
              ),
            ),
          ),
        );

        const main = Effect.gen(function* () {
          const host = yield* NodeDurableHost;

          yield* Deferred.succeed(ready, host);
          yield* host.submit(
            agent,
            { question: "wait" },
            {
              ...submitOptions("managed-thread", "managed-input"),
              definitions: digests,
            },
          );
          yield* NodeHost.run.pipe(
            Effect.onError(() =>
              Effect.gen(function* () {
                expect(yield* host.admissionOpen).toBe(false);
                expect(
                  yield* host
                    .submit(
                      agent,
                      { question: "too late" },
                      {
                        ...submitOptions("late-thread", "late-input"),
                        definitions: digests,
                      },
                    )
                    .pipe(Effect.result),
                ).toMatchObject({
                  _tag: "Failure",
                  failure: { _tag: "AdmissionClosed" },
                });
              }),
            ),
          );
        }).pipe(Effect.provide(live));

        const fiber = yield* main.pipe(Effect.forkChild);

        const host = yield* Deferred.await(ready);

        yield* Deferred.await(started);
        yield* Deferred.succeed(release, undefined);
        const exit = yield* Fiber.await(fiber);

        expect(marks).toEqual(["worker-finalized", "resource-finalized"]);
        expect(yield* host.admissionOpen).toBe(false);
        expect(Exit.isFailure(exit)).toBe(true);
        expect(failureOf(exit)).toMatchObject({ _tag: "DurableRuntimeFailpointError" });
      }),
    ),
  );

  it.effect(
    "shares one bounded managed pool across observers and closes admission before draining it",
    () =>
      withTemporaryDatabase((filename) =>
        Effect.gen(function* () {
          const started = yield* Deferred.make<void>();
          const ready = yield* Deferred.make<NodeDurableHost["Service"]>();
          const claimed = yield* Ref.make(0);
          const finalized = yield* Ref.make(0);
          const model = yield* makeScriptedModel(() => finalParts('{"answer":"unused"}'));
          const agent = Agent.withModel(plannerDefinition, model);
          const definitions = DefinitionDigestInput.make({ agent: "v1", model: "v1", tools: "v1" });

          const digests = yield* digestDefinitions(definitions).pipe(
            Effect.provide(NodeCrypto.layer),
          );

          const live = NodeHost.layer(
            [{ agent, definitions }],
            runtimeOptions(filename, {
              workerConcurrency: 2,
              runtimeFailpoint: (location) =>
                location === "claim:after-claim"
                  ? Effect.gen(function* () {
                      const count = yield* Ref.updateAndGet(claimed, (n) => n + 1);

                      if (count === 2) yield* Deferred.succeed(started, undefined);

                      return yield* Effect.never;
                    }).pipe(
                      Effect.ensuring(
                        Effect.gen(function* () {
                          const host = yield* Deferred.await(ready);

                          expect(yield* host.admissionOpen).toBe(false);
                          yield* Ref.update(finalized, (n) => n + 1);
                        }),
                      ),
                    )
                  : Effect.void,
            }),
          );

          yield* Effect.gen(function* () {
            const host = yield* NodeDurableHost;

            yield* Deferred.succeed(ready, host);
            for (const id of ["first", "second", "third"]) {
              yield* host.submit(
                agent,
                { question: id },
                {
                  ...submitOptions(id, id),
                  definitions: digests,
                },
              );
            }
            yield* Deferred.await(started);
            const first = yield* NodeHost.run.pipe(Effect.forkChild);
            const second = yield* NodeHost.run.pipe(Effect.forkChild);

            yield* Fiber.interrupt(first);
            yield* Fiber.interrupt(second);
            expect(yield* host.admissionOpen).toBe(true);
            expect(yield* Ref.get(claimed)).toBe(2);
            expect(yield* Ref.get(finalized)).toBe(0);
          }).pipe(Effect.provide(live));
          expect(yield* Ref.get(finalized)).toBe(2);
        }),
      ),
  );

  for (const mode of ["timeout"] as const) {
    it.effect(`startup keeps admission closed after a recovery ${mode}`, () =>
      withTemporaryDatabase((filename) =>
        Effect.gen(function* () {
          const runtime = yield* DurableAgentRuntime;
          const store = yield* ThreadStore;
          const ledger = yield* SubmissionLedger;
          const entered = yield* Deferred.make<void>();
          let active = 0;
          let admitted = false;

          const receipt = yield* runtime.submit(
            { definition: plannerDefinition },
            { question: "keep the original accepted input" },
            submitOptions("startup-blocked", "original"),
          );

          const lookup = SubmissionLookupById.make({ submissionId: receipt.submissionId });
          const before = yield* ledger.lookup(lookup);

          const blockedStore = ThreadStore.of({
            ...store,
            read: () =>
              Stream.unwrap(
                Effect.acquireRelease(
                  Effect.sync(() => {
                    active++;
                  }),
                  () =>
                    Effect.sync(() => {
                      active--;
                    }),
                ).pipe(
                  Effect.andThen(Deferred.succeed(entered, undefined)),
                  Effect.andThen(Effect.never),
                ),
              ),
          });

          const blockedRuntime = Layer.fresh(DurableAgentRuntime.layer).pipe(
            Layer.provide(Layer.succeed(ThreadStore, blockedStore)),
            Layer.provide(
              Layer.mergeAll(
                NodeCrypto.layer,
                DurableRuntimeFailpoint.layer,
                ToolReconciler.uncertain,
              ),
            ),
          );

          const startup = yield* Effect.forkChild(
            Effect.gen(function* () {
              yield* NodeDurableHost;
              admitted = true;
            }).pipe(
              Effect.provide(NodeDurableHost.layer.pipe(Layer.provide(blockedRuntime))),
              Effect.exit,
            ),
          );

          yield* Deferred.await(entered);
          yield* TestClock.adjust("30 seconds");
          expect(failureOf(yield* Fiber.join(startup))).toMatchObject({
            _tag: "RecoveryBlocked",
            threadId: receipt.threadId,
            failure: { phase: "history", reason: mode },
          });
          expect(admitted).toBe(false);
          expect(active).toBe(0);
          expect(yield* ledger.lookup(lookup)).toEqual(before);
          expect(
            yield* runtime.submit(
              { definition: plannerDefinition },
              { question: "keep the original accepted input" },
              submitOptions("startup-blocked", "original"),
            ),
          ).toEqual(receipt);
        }).pipe(
          Effect.scoped,
          Effect.provide(NodeDurableAgentRuntime.layer(runtimeOptions(filename))),
        ),
      ),
    );
  }

  it.effect(
    "captures independent preparation and authorization Layers in each registered Node host",
    () =>
      withTemporaryDatabase((filename) =>
        Effect.gen(function* () {
          const marks: Array<string> = [];

          const tools = Toolkit.make(
            Tool.make("book", {
              parameters: Schema.Struct({}),
              success: Schema.String,
            }).annotate(ToolExecutionClass, "readonly"),
          );

          const model = yield* makeScriptedModel((call) => [
            {
              type: "tool-call",
              id: `book-${call}`,
              name: "book",
              params: {},
              providerExecuted: false,
            },
            { type: "finish", reason: "tool-calls", usage },
          ]);

          const agent = Agent.withModel(
            Agent.make("node-run-services", {
              input: Schema.String,
              output: Schema.String,
              instructions: "Book it.",
              toolkit: tools,
              policy: plannerDefinition.policy,
            }),
            model,
          );

          const handlers = tools.toLayer({
            book: () =>
              Effect.sync(() => {
                marks.push("handler");

                return "booked";
              }),
          });

          const definitions = DefinitionDigestInput.make({ agent: "v1", model: "v1", tools: "v1" });

          const definitionsDigest = yield* digestDefinitions(definitions).pipe(
            Effect.provide(NodeCrypto.layer),
          );

          const threadId = decodeThreadId("node-run-services");

          for (const incarnation of [1, 2]) {
            const runContext = Layer.succeed(RunContextPreparation, {
              hook: {
                prepare: ({ source }) =>
                  Effect.sync(() => {
                    if (incarnation === 2) expect(JSON.stringify(source)).toContain("booked");

                    return { prompt: source };
                  }),
              },
            });

            const toolAuthorization = Layer.succeed(RunToolAuthorization, {
              authorize: () =>
                Effect.sync(() => {
                  marks.push(`authorize:${incarnation}`);

                  return incarnation === 1
                    ? { _tag: "allowed" as const }
                    : { _tag: "denied" as const, reason: "revoked" };
                }),
            });

            const live = NodeDurableHost.layerRegistered(
              [{ agent, definitions }],
              runtimeOptions(filename, {
                runContext,
                toolAuthorization,
                runtimeFailpoint: (location) =>
                  incarnation === 1 && location === "turn:after-results-append"
                    ? Effect.fail(DurableRuntimeFailpointError.make({ location }))
                    : Effect.void,
              }),
            ).pipe(Layer.provide(handlers));

            yield* Effect.gen(function* () {
              const runtime = yield* DurableAgentRuntime;

              if (incarnation === 1) {
                yield* runtime.submit(agent, "book", {
                  ...submitOptions(threadId, "book"),
                  definitions: definitionsDigest,
                });

                const interrupted = yield* runtime
                  .processThreadResolved(threadId)
                  .pipe(Effect.exit);

                expect(failureOf(interrupted)).toHaveProperty(
                  "_tag",
                  "DurableRuntimeFailpointError",
                );
              } else {
                const settlements = yield* runtime
                  .processThreadResolved(threadId)
                  .pipe(Effect.provide(RunToolAuthorization.allowAll));

                expect(settlements[0]).toMatchObject({
                  outcome: "failed",
                  failure: { errorTag: "AgentToolAuthorizationDenied" },
                });
              }
            }).pipe(Effect.provide(live));
          }
          expect(marks.filter((mark) => /^(authorize|handler)/.test(mark))).toEqual([
            "authorize:1",
            "handler",
            "authorize:2",
          ]);
        }),
      ),
  );

  it.effect("keeps projected root and joined inputs private across Node host recovery", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        const sentinel = "HOST-ONLY-INPUT-SENTINEL";
        const requests: Array<Prompt.Prompt> = [];
        const authorizedInputs: Array<unknown> = [];
        const projectedInputs: Array<string> = [];
        const inputSchema = Schema.Struct({ question: Schema.String, hostOnly: Schema.String });

        const tools = Toolkit.make(
          Tool.make("lookup", {
            parameters: Schema.Struct({}),
            success: Schema.String,
          }).annotate(ToolExecutionClass, "readonly"),
        );

        const model = yield* makeScriptedModel((call, prompt) => {
          requests.push(prompt);
          expect(JSON.stringify(prompt)).not.toContain(sentinel);

          return call === 0
            ? [
                {
                  type: "tool-call",
                  id: "lookup-projected-input",
                  name: "lookup",
                  params: {},
                  providerExecuted: false,
                },
                { type: "finish", reason: "tool-calls", usage },
              ]
            : finalParts('{"answer":"done"}');
        });

        const agent = Agent.withModel(
          Agent.make("node-input-projection", {
            input: inputSchema,
            output: Schema.Struct({ answer: Schema.String }),
            instructions: "Answer the public question.",
            inputPrompt: (input) =>
              Effect.sync(() => {
                expect(input.hostOnly).toBe(sentinel);
                projectedInputs.push(input.question);

                return Prompt.make([
                  { role: "user", content: [{ type: "text", text: input.question }] },
                ]);
              }),
            toolkit: tools,
            policy: plannerDefinition.policy,
          }),
          model,
        );

        const threadId = decodeThreadId("node-input-projection");
        const rootInput = { question: "public root question", hostOnly: sentinel };
        const joinedInput = { question: "public joined question", hostOnly: sentinel };
        const handlers = tools.toLayer({ lookup: () => Effect.succeed("public result") });

        const toolAuthorization = Layer.succeed(RunToolAuthorization, {
          authorize: ({ input }) =>
            Effect.sync(() => {
              authorizedInputs.push(input);

              return { _tag: "allowed" as const };
            }),
        });

        for (const incarnation of [1, 2, 3]) {
          yield* withHost(
            runtimeOptions(filename, {
              toolAuthorization,
              runtimeFailpoint: (location) =>
                (incarnation === 1 && location === "join:after-canonical-append") ||
                (incarnation === 2 && location === "turn:after-response-append")
                  ? Effect.fail(DurableRuntimeFailpointError.make({ location }))
                  : Effect.void,
            }),
            Effect.gen(function* () {
              const runtime = yield* DurableAgentRuntime;

              if (incarnation === 1) {
                yield* runtime.submit(agent, rootInput, submitOptions(threadId, "root"));
                yield* runtime.submit(agent, joinedInput, submitOptions(threadId, "joined"));
              }
              const result = yield* Effect.exit(runtime.processThread(agent, threadId));

              if (incarnation < 3) {
                expect(failureOf(result)).toHaveProperty("_tag", "DurableRuntimeFailpointError");
              } else {
                expect(Exit.isSuccess(result)).toBe(true);
                const store = yield* ThreadStore;

                const records = yield* Stream.runCollect(
                  store.read(ThreadRead.make({ threadId, limit: 1_024 })),
                );

                const inputs = records.flatMap(({ record }) =>
                  record.payload._tag === "UserInputRecorded" ? [record.payload.input] : [],
                );

                expect(inputs).toEqual([rootInput, joinedInput]);

                const responses = records.filter(
                  ({ record }) => record.payload._tag === "ModelResponseRecorded",
                );

                expect(responses).toHaveLength(2);
                expect(JSON.stringify(responses)).not.toContain(sentinel);

                const settlements = records.flatMap(({ record }) =>
                  record.payload._tag === "SubmissionSettled" ? [record.payload.outcome] : [],
                );

                expect(settlements).toEqual(["completed", "completed"]);
              }
            }).pipe(Effect.provide(handlers)),
          );
        }
        expect(requests).toHaveLength(2);
        for (const request of requests) {
          expect(JSON.stringify(request)).toContain(rootInput.question);
          expect(JSON.stringify(request)).toContain(joinedInput.question);
        }
        expect(authorizedInputs).toEqual([rootInput]);
        expect(projectedInputs).toContain(joinedInput.question);
      }),
    ),
  );
});
