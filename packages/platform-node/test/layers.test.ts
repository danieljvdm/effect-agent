import { Agent, AgentPolicy, ThreadId } from "@effect-agent/core";
import type { SubmissionId } from "@effect-agent/core";
import {
  RunContextPreparation,
  RunToolAuthorization,
  ToolExecutionClass,
  toolFailureObserverLayer,
  type ToolFailureObservation,
} from "@effect-agent/engine";
import {
  CurrentSqliteStorageVersion,
  SqliteStorageCompatibilityError,
  type SqliteStorageInitializationError,
} from "@effect-agent/storage-sqlite";
import {
  AdmissionRequest,
  ClaimRequest,
  ThreadRead,
  ThreadStore,
  DefinitionDigests,
  Digest,
  digestJson,
  DurableAgentRuntime,
  DurableRuntimeConfig,
  DurableRuntimeFailpointError,
  IdempotencyKey,
  MarkReadyRequest,
  Principal,
  ProducerId,
  SubmissionLedger,
  SubmissionLookupById,
  WakeScheduler,
  type DurableSubmitOptions,
  type DurableWorkerFailure,
  type PersistedJson,
  type SubmissionState,
} from "@effect-agent/thread";
import { NodeCrypto, NodeFileSystem } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { describe, expect, it } from "@effect/vitest";
import type { PlatformError } from "effect";
import {
  Cause,
  Context,
  Crypto,
  Deferred,
  Duration,
  Effect,
  Exit,
  Fiber,
  FileSystem,
  Layer,
  Option,
  Ref,
  Schema,
  Scope,
  Stream,
} from "effect";
import { TestClock } from "effect/testing";
import { LanguageModel, Model, Prompt, Tool, Toolkit, type Response } from "effect/unstable/ai";
import * as SqlClientService from "effect/unstable/sql/SqlClient";

import {
  NodeDurableHost,
  NodeDurableRuntime,
  NodeDurableRuntimeConfig,
  type NodeDurableRuntimeInitializationError,
  type NodeDurableRuntimeOptions,
  type NodeDurableRuntimeServices,
} from "../src/index.ts";

type Equal<Left, Right> =
  (<Value>() => Value extends Left ? 1 : 2) extends <Value>() => Value extends Right ? 1 : 2
    ? (<Value>() => Value extends Right ? 1 : 2) extends <Value>() => Value extends Left ? 1 : 2
      ? true
      : false
    : false;
type Assert<Value extends true> = Value;

const runtimeLayerProbe = NodeDurableRuntime.layer({
  filename: "unused.sqlite",
  deploymentId: "deployment-proof",
  producerId: "producer-proof",
});
const hostLayerProbe = NodeDurableHost.layer();

class ContextSetupError extends Schema.TaggedError<ContextSetupError>()("ContextSetupError", {}) {}
class AuthorizationSetupError extends Schema.TaggedError<AuthorizationSetupError>()(
  "AuthorizationSetupError",
  {},
) {}
class ContextConfig extends Context.Service<ContextConfig, { readonly fail: boolean }>()(
  "test/ContextConfig",
) {}
class AuthorizationConfig extends Context.Service<
  AuthorizationConfig,
  { readonly fail: boolean }
>()("test/AuthorizationConfig") {}

const configuredContext = Layer.effect(
  RunContextPreparation,
  Effect.gen(function* () {
    yield* Crypto.Crypto;
    const config = yield* ContextConfig;
    if (config.fail) return yield* new ContextSetupError();
    return RunContextPreparation.of({});
  }),
);
const configuredAuthorization = Layer.effect(
  RunToolAuthorization,
  Effect.gen(function* () {
    yield* Crypto.Crypto;
    const config = yield* AuthorizationConfig;
    if (config.fail) return yield* new AuthorizationSetupError();
    return RunToolAuthorization.of({
      authorize: () => Effect.succeed({ _tag: "denied", reason: "test policy" }),
    });
  }),
);
type RuntimeLayerServicesProof = Assert<
  Equal<Layer.Success<typeof runtimeLayerProbe>, NodeDurableRuntimeServices>
>;
type RuntimeLayerErrorProof = Assert<
  Equal<Layer.Error<typeof runtimeLayerProbe>, NodeDurableRuntimeInitializationError>
>;
type RuntimeLayerRequirementsProof = Assert<Equal<Layer.Services<typeof runtimeLayerProbe>, never>>;
type RuntimeInitializationErrorProof = Assert<
  Equal<
    NodeDurableRuntimeInitializationError,
    | SqliteStorageInitializationError
    | Extract<NodeDurableRuntimeInitializationError, { readonly _tag: "NodePlatformConfigError" }>
  >
>;
type HostLayerRequirementsProof = Assert<
  Equal<Layer.Services<typeof hostLayerProbe>, DurableAgentRuntime | NodeDurableRuntimeConfig>
>;

const SHA_A = Schema.decodeSync(Digest)("a".repeat(64));
const DIGESTS = DefinitionDigests.make({ agent: SHA_A, model: SHA_A, tools: SHA_A });
const PRINCIPAL = Schema.decodeSync(Principal)("principal-platform-node");
const decodeThreadId = Schema.decodeSync(ThreadId);
const decodeIdempotencyKey = Schema.decodeSync(IdempotencyKey);
const decodeProducerId = Schema.decodeSync(ProducerId);
const decodeAgentId = Schema.decodeSync(AdmissionRequest.fields.agentId);
const decodeDeploymentId = Schema.decodeSync(AdmissionRequest.fields.deploymentId);

const runtimeOptions = (
  filename: string,
  overrides?: Partial<NodeDurableRuntimeOptions>,
): NodeDurableRuntimeOptions => ({
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

const withSql = <A, E>(filename: string, effect: Effect.Effect<A, E, SqlClientService.SqlClient>) =>
  Effect.provide(effect, SqliteClient.layer({ filename }));

/** One host "process": the full DN stack over `filename`, closed (and drained) when `effect` ends. */
const withHost = <A, E, R>(
  options: NodeDurableRuntimeOptions,
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<
  A,
  E | Layer.Error<typeof hostLayerProbe> | NodeDurableRuntimeInitializationError,
  Exclude<R, NodeDurableHost | NodeDurableRuntimeServices>
> => Effect.provide(effect, NodeDurableHost.layerStack(options));

const failureOf = <A, E>(exit: Exit.Exit<A, E>): unknown => {
  expect(Exit.isFailure(exit)).toBe(true);
  if (Exit.isSuccess(exit)) throw new Error("Expected the Effect to fail");
  return Cause.squash(exit.cause);
};

const lookupState = (
  submissionId: SubmissionId,
): Effect.Effect<SubmissionState, never, SubmissionLedger> =>
  Effect.gen(function* () {
    const ledger = yield* SubmissionLedger;
    const snapshot = yield* ledger
      .lookup(SubmissionLookupById.make({ submissionId }))
      .pipe(Effect.orDie);
    expect(Option.isSome(snapshot)).toBe(true);
    if (Option.isNone(snapshot)) throw new Error("Expected the Submission to exist");
    return snapshot.value.state;
  });

const readLogTags = (threadId: ThreadId) =>
  Effect.gen(function* () {
    const store = yield* ThreadStore;
    const records = yield* Stream.runCollect(
      store.read(ThreadRead.make({ threadId, limit: 1_024 })),
    );
    return records.map((envelope) => envelope.record.payload._tag);
  });

describe("NodeDurableRuntime", () => {
  it("preserves independent service construction errors and requirements through runtime and host assembly", () => {
    const contextOnly = NodeDurableRuntime.layer({
      ...runtimeOptions("unused.sqlite"),
      runContext: configuredContext,
    });
    const authorizationOnly = NodeDurableRuntime.layer({
      ...runtimeOptions("unused.sqlite"),
      toolAuthorization: configuredAuthorization,
    });
    const both = NodeDurableHost.layerStack({
      ...runtimeOptions("unused.sqlite"),
      runContext: configuredContext,
      toolAuthorization: configuredAuthorization,
    });
    const contextErrors: Assert<
      Equal<
        Layer.Error<typeof contextOnly>,
        NodeDurableRuntimeInitializationError | ContextSetupError
      >
    > = true;
    const contextNeeds: Assert<Equal<Layer.Services<typeof contextOnly>, ContextConfig>> = true;
    const authorizationErrors: Assert<
      Equal<
        Layer.Error<typeof authorizationOnly>,
        NodeDurableRuntimeInitializationError | AuthorizationSetupError
      >
    > = true;
    const authorizationNeeds: Assert<
      Equal<Layer.Services<typeof authorizationOnly>, AuthorizationConfig>
    > = true;
    const hostErrors: Assert<
      Equal<
        Layer.Error<typeof both>,
        | NodeDurableRuntimeInitializationError
        | DurableWorkerFailure
        | ContextSetupError
        | AuthorizationSetupError
      >
    > = true;
    const hostNeeds: Assert<
      Equal<Layer.Services<typeof both>, ContextConfig | AuthorizationConfig>
    > = true;
    expect([
      contextErrors,
      contextNeeds,
      authorizationErrors,
      authorizationNeeds,
      hostErrors,
      hostNeeds,
    ]).not.toContain(false);
  });

  it.effect("returns service initialization failures without converting them to defects", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        for (const contextFails of [true, false]) {
          const opened = yield* Effect.service(NodeDurableHost).pipe(
            Effect.provide(
              NodeDurableHost.layerStack({
                ...runtimeOptions(filename),
                runContext: configuredContext,
                toolAuthorization: configuredAuthorization,
              }),
            ),
            Effect.provide([
              Layer.succeed(ContextConfig, { fail: contextFails }),
              Layer.succeed(AuthorizationConfig, { fail: !contextFails }),
            ]),
            Effect.exit,
          );
          if (Exit.isSuccess(opened))
            return yield* Effect.die("Expected service initialization to fail");
          expect(Cause.findErrorOption(opened.cause)).toEqual(
            Option.some(contextFails ? new ContextSetupError() : new AuthorizationSetupError()),
          );
        }
      }),
    ),
  );

  it("keeps the assembled Layer contract visible in its types", () => {
    const servicesProof: RuntimeLayerServicesProof = true;
    const errorProof: RuntimeLayerErrorProof = true;
    const requirementsProof: RuntimeLayerRequirementsProof = true;
    const initializationProof: RuntimeInitializationErrorProof = true;
    const hostProof: HostLayerRequirementsProof = true;

    expect(servicesProof).toBe(true);
    expect(errorProof).toBe(true);
    expect(requirementsProof).toBe(true);
    expect(initializationProof).toBe(true);
    expect(hostProof).toBe(true);
  });

  it.effect("refuses out-of-bounds configuration with a typed error", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        const opened = yield* Effect.service(NodeDurableRuntimeConfig).pipe(
          Effect.provide(
            NodeDurableRuntime.layer(runtimeOptions(filename, { workerConcurrency: 0 })),
          ),
          Effect.exit,
        );
        const error = failureOf(opened);
        expect(error).toHaveProperty("_tag", "NodePlatformConfigError");
        const databaseExists = yield* FileSystem.FileSystem.use((fs) => fs.exists(filename)).pipe(
          Effect.provide(NodeFileSystem.layer),
        );
        expect(databaseExists).toBe(false);
      }),
    ),
  );

  for (const configured of [false, true]) {
    it.effect(`RUN-036 Node observer option owns installation (configured=${configured})`, () =>
      withTemporaryDatabase((filename) => {
        const observations: Array<ToolFailureObservation> = [];
        const ambient: Array<ToolFailureObservation> = [];
        const Failed = Tool.make("failed", {
          parameters: Schema.Struct({}),
          success: Schema.String,
          failure: Schema.String,
          failureMode: "return",
        });
        const tools = Toolkit.make(Failed);
        return Effect.gen(function* () {
          const model = yield* makeScriptedModel((n) =>
            n === 0
              ? [
                  {
                    type: "tool-call",
                    id: "node-failure",
                    name: "failed",
                    params: {},
                    providerExecuted: false,
                  },
                  { type: "finish", reason: "tool-calls", usage },
                ]
              : finalParts('{"answer":"fallback"}'),
          );
          const agent = Agent.withModel(
            Agent.make("node-observer", {
              input: Schema.Struct({ question: Schema.String }),
              output: Schema.Struct({ answer: Schema.String }),
              instructions: "Try the Tool, then answer.",
              toolkit: tools,
              policy: plannerDefinition.policy,
            }),
            model,
          );
          const runtime = yield* DurableAgentRuntime;
          const threadId = decodeThreadId("node-observer");
          const receipt = yield* runtime.submit(
            agent,
            { question: "try" },
            submitOptions(threadId, "node-observer"),
          );
          yield* runtime
            .processThread(agent, threadId)
            .pipe(Effect.provide(tools.toLayer({ failed: () => Effect.fail("unavailable") })));
          expect((yield* runtime.awaitSettlement(receipt)).outcome).toBe("completed");
          expect(observations).toEqual(
            configured
              ? [
                  expect.objectContaining({
                    _tag: "ModelToolFailure",
                    kind: "declared-failure",
                    toolName: "failed",
                    toolCallId: "node-failure",
                    tag: "UnknownError",
                  }),
                ]
              : [],
          );
          expect(ambient).toEqual([]);
        }).pipe(
          Effect.provide(
            NodeDurableRuntime.layer(
              runtimeOptions(filename, {
                toolFailureObserver: configured
                  ? {
                      observe: (observation) =>
                        Effect.sync(() => {
                          observations.push(observation);
                        }),
                    }
                  : undefined,
              }),
            ).pipe(
              Layer.provide(
                toolFailureObserverLayer({
                  observe: (observation) =>
                    Effect.sync(() => {
                      ambient.push(observation);
                    }),
                }),
              ),
            ),
          ),
        );
      }),
    );
  }

  it.effect("startup refuses an incompatible v1 storage file without mutating it", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        yield* withSql(
          filename,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;
            yield* sql.unsafe("PRAGMA user_version = 1");
          }),
        );

        const opened = yield* Effect.service(NodeDurableHost).pipe(
          Effect.provide(NodeDurableHost.layerStack(runtimeOptions(filename))),
          Effect.exit,
        );
        const error = failureOf(opened);
        expect(error).toBeInstanceOf(SqliteStorageCompatibilityError);
        if (error instanceof SqliteStorageCompatibilityError) {
          expect(error.actualVersion).toBe(1);
          expect(error.supportedVersion).toBe(CurrentSqliteStorageVersion);
          expect(error.message).toContain("Reset the database file explicitly");
        }

        // Failing closed must not mutate the incompatible file.
        const tables = yield* withSql(
          filename,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;
            return yield* sql<Record<string, unknown>>`
              SELECT name
              FROM sqlite_master
              WHERE type = 'table'
                AND name LIKE 'effect_agent_%'
            `;
          }),
        );
        expect(tables).toEqual([]);
      }),
    ),
  );

  it.effect("captures independent preparation and authorization Layers in each Node host", () =>
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
        const threadId = decodeThreadId("node-run-services");
        for (const incarnation of [1, 2]) {
          const runContext = Layer.effect(
            RunContextPreparation,
            Effect.acquireRelease(
              Effect.sync(() => {
                marks.push(`acquire-context:${incarnation}`);
                return RunContextPreparation.of({
                  hook: {
                    prepare: ({ source }) =>
                      Effect.sync(() => {
                        marks.push(`prepare:${incarnation}`);
                        if (incarnation === 2) expect(JSON.stringify(source)).toContain("booked");
                        return { prompt: source };
                      }),
                  },
                });
              }),
              () =>
                Effect.sync(() => {
                  marks.push(`release-context:${incarnation}`);
                }),
            ),
          );
          const toolAuthorization = Layer.effect(
            RunToolAuthorization,
            Effect.acquireRelease(
              Effect.sync(() =>
                RunToolAuthorization.of({
                  authorize: () =>
                    Effect.sync(() => {
                      marks.push(`authorize:${incarnation}`);
                      return incarnation === 1
                        ? { _tag: "allowed" as const }
                        : { _tag: "denied" as const, reason: "revoked" };
                    }),
                }),
              ),
              () =>
                Effect.sync(() => {
                  marks.push(`release-authorization:${incarnation}`);
                }),
            ),
          );
          yield* withHost(
            runtimeOptions(filename, {
              runContext,
              toolAuthorization,
              runtimeFailpoint: (location) =>
                incarnation === 1 && location === "turn:after-results-append"
                  ? Effect.fail(DurableRuntimeFailpointError.make({ location }))
                  : Effect.void,
            }),
            Effect.gen(function* () {
              const runtime = yield* DurableAgentRuntime;
              if (incarnation === 1) {
                yield* runtime.submit(agent, "book", submitOptions(threadId, "book"));
                const interrupted = yield* runtime.processThread(agent, threadId).pipe(Effect.exit);
                expect(failureOf(interrupted)).toHaveProperty(
                  "_tag",
                  "DurableRuntimeFailpointError",
                );
              } else {
                const settlements = yield* runtime
                  .processThread(agent, threadId)
                  .pipe(Effect.provide(RunToolAuthorization.allowAll));
                expect(settlements[0]).toMatchObject({
                  outcome: "failed",
                  failure: { errorTag: "AgentToolAuthorizationDenied" },
                });
              }
            }).pipe(Effect.provide(handlers)),
          );
          expect(marks).toContain(`release-context:${incarnation}`);
          expect(marks).toContain(`release-authorization:${incarnation}`);
        }
        expect(marks.filter((mark) => /^(authorize|handler)/.test(mark))).toEqual([
          "authorize:1",
          "handler",
          "authorize:2",
        ]);
        for (const incarnation of [1, 2]) {
          expect(marks).toContain(`prepare:${incarnation}`);
          expect(marks.indexOf(`prepare:${incarnation}`)).toBeLessThan(
            marks.indexOf(`authorize:${incarnation}`),
          );
        }
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

  it.effect.each([
    { mode: "failure", kind: "root" },
    { mode: "interruption", kind: "root" },
    { mode: "failure", kind: "joined" },
    { mode: "interruption", kind: "joined" },
  ] as const)(
    "retains canonical input without model calls after $kind projection $mode",
    ({ mode, kind }) =>
      withTemporaryDatabase((filename) =>
        Effect.gen(function* () {
          class ProjectionFailure extends Schema.TaggedError<ProjectionFailure>()(
            "ProjectionFailure",
            {},
          ) {}
          const started = yield* Deferred.make<void>();
          const finalized = yield* Ref.make(false);
          const requests: Array<Prompt.Prompt> = [];
          const input = { question: "public question", hostOnly: "HOST-ONLY-FAILURE-SENTINEL" };
          const model = yield* makeScriptedModel((_call, prompt) => {
            requests.push(prompt);
            return finalParts('"done"');
          });
          const agent = Agent.withModel(
            Agent.make(`node-projection-${kind}-${mode}`, {
              input: Schema.Struct({ question: Schema.String, hostOnly: Schema.String }),
              output: Schema.String,
              instructions: "Answer the public question.",
              inputPrompt: ({ question }) =>
                kind === "joined" && question === input.question
                  ? Effect.succeed(question)
                  : Effect.gen(function* () {
                      yield* Deferred.succeed(started, undefined);
                      return yield* mode === "failure"
                        ? Effect.fail(new ProjectionFailure())
                        : Effect.never;
                    }).pipe(Effect.ensuring(Ref.set(finalized, true))),
              toolkit: Toolkit.empty,
              policy: plannerDefinition.policy,
            }),
            model,
          );
          const threadId = decodeThreadId(`node-projection-${kind}-${mode}`);
          const joinedInput = { ...input, question: "public joined question" };
          yield* withHost(
            runtimeOptions(filename),
            Effect.gen(function* () {
              const runtime = yield* DurableAgentRuntime;
              const receipt = yield* runtime.submit(agent, input, submitOptions(threadId, mode));
              if (kind === "joined") {
                yield* runtime.submit(agent, joinedInput, submitOptions(threadId, "joined"));
              }
              if (mode === "failure") {
                const settlements = yield* runtime.processThread(agent, threadId);
                expect(settlements).toMatchObject([
                  { outcome: "failed", failure: { errorTag: "ProjectionFailure" } },
                ]);
              } else {
                const worker = yield* runtime.processThread(agent, threadId).pipe(Effect.forkChild);
                yield* Deferred.await(started);
                yield* Fiber.interrupt(worker);
                expect(Exit.hasInterrupts(yield* Fiber.await(worker))).toBe(true);
              }
              expect(yield* Ref.get(finalized)).toBe(true);
              const ledger = yield* SubmissionLedger;
              const stored = yield* ledger.lookup(
                SubmissionLookupById.make({ submissionId: receipt.submissionId }),
              );
              expect(Option.isSome(stored)).toBe(true);
              if (Option.isSome(stored)) expect(stored.value.inputPayload).toEqual(input);
              const store = yield* ThreadStore;
              const records = yield* Stream.runCollect(
                store.read(ThreadRead.make({ threadId, limit: 1_024 })),
              );
              expect(
                records.flatMap(({ record }) =>
                  record.payload._tag === "UserInputRecorded" ? [record.payload.input] : [],
                ),
              ).toEqual(kind === "joined" ? [input, joinedInput] : [input]);
              const settled = records.flatMap(({ record }) =>
                record.payload._tag === "SubmissionSettled" ? [record.payload.outcome] : [],
              );
              expect(settled).toEqual(
                mode === "failure" ? (kind === "joined" ? ["failed", "failed"] : ["failed"]) : [],
              );
              expect(
                records.some(({ record }) => record.payload._tag === "ModelResponseRecorded"),
              ).toBe(false);
            }),
          );
          expect(requests).toEqual([]);
        }),
      ),
  );

  it.effect("startup reconciliation settles an orphaned reserved settlement before admission", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        const thread = decodeThreadId("thread-reconcile");

        // Host process 1: the Attempt crashes (typed failpoint) AFTER reserving the exact
        // settlement record but BEFORE appending it canonically (durability §12 step 1→2 gap).
        const receipt = yield* withHost(
          runtimeOptions(filename, {
            runtimeFailpoint: (location) =>
              location === "terminalize:after-reserve"
                ? Effect.fail(DurableRuntimeFailpointError.make({ location }))
                : Effect.void,
          }),
          Effect.gen(function* () {
            const host = yield* NodeDurableHost;
            const runtime = yield* DurableAgentRuntime;
            const model = yield* makeScriptedModel(() => finalParts('{"answer":"ok"}'));
            const agent = Agent.withModel(plannerDefinition, model);

            const receipt = yield* host.submit(
              agent,
              { question: "reconcile?" },
              submitOptions("thread-reconcile", "reconcile-1"),
            );
            const crashed = yield* Effect.exit(runtime.processThread(agent, thread));
            const error = failureOf(crashed);
            expect(error).toHaveProperty("_tag", "DurableRuntimeFailpointError");
            expect(yield* lookupState(receipt.submissionId)).toBe("terminalizing");
            return receipt;
          }),
        );

        // Host process 2: startup recovery appends the EXACT reserved record and finalizes the
        // ledger BEFORE admission opens; the same idempotency key then resumes to the original
        // Receipt and its recorded Settlement.
        yield* withHost(
          runtimeOptions(filename),
          Effect.gen(function* () {
            const host = yield* NodeDurableHost;
            expect(yield* host.admissionOpen).toBe(true);
            const report = host.startupRecovery.find(
              (candidate) => candidate.submissionId === receipt.submissionId,
            );
            expect(report?.decision._tag).toBe("AppendReservedSettlement");
            expect(report?.disposition).toBe("repaired");
            expect(yield* lookupState(receipt.submissionId)).toBe("settled");

            const model = yield* makeScriptedModel(() => finalParts('{"answer":"ok"}'));
            const agent = Agent.withModel(plannerDefinition, model);
            const replayed = yield* host.submit(
              agent,
              { question: "reconcile?" },
              submitOptions("thread-reconcile", "reconcile-1"),
            );
            expect(replayed).toEqual(receipt);

            const settlement = yield* host.awaitSettlement(receipt);
            expect(settlement.outcome).toBe("completed");
            expect(settlement.receiptId).toBe(receipt.receiptId);

            expect(yield* readLogTags(thread)).toEqual([
              "ThreadCreated",
              "UserInputRecorded",
              "RunStarted",
              "ModelResponseRecorded",
              "RunCompleted",
              "SubmissionSettled",
              "RepairAnnotated",
            ]);
          }),
        );
      }),
    ),
  );

  it.effect("shutdown closes admission and releases ownership for the next host", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        const thread = decodeThreadId("thread-shutdown");

        // Host process 1, with an explicit Scope so shutdown ordering is observable.
        const scope = yield* Scope.make();
        const context = yield* Layer.build(
          NodeDurableHost.layerStack(runtimeOptions(filename)),
        ).pipe(Scope.provide(scope));
        const host = Context.get(context, NodeDurableHost);
        const ledger = Context.get(context, SubmissionLedger);

        const model = yield* makeScriptedModel(() => finalParts('{"answer":"ok"}'));
        const agent = Agent.withModel(plannerDefinition, model);
        const receipt = yield* host.submit(
          agent,
          { question: "shutdown?" },
          submitOptions("thread-shutdown", "shutdown-1"),
        );
        expect(yield* host.admissionOpen).toBe(true);

        // Hold an ownership lease (default 30s; the TestClock never advances past it).
        const claimed = yield* ledger.claim(
          ClaimRequest.make({
            threadId: thread,
            producerId: decodeProducerId("producer-platform-node"),
          }),
        );
        expect(Option.isSome(claimed)).toBe(true);

        yield* Scope.close(scope, Exit.void);

        // Shutdown step 1: admission is closed before anything else.
        expect(yield* host.admissionOpen).toBe(false);
        const refused = yield* Effect.exit(
          host.submit(agent, { question: "late" }, submitOptions("thread-shutdown", "late-1")),
        );
        expect(failureOf(refused)).toHaveProperty("_tag", "AdmissionClosed");

        // Host process 2 claims the lane IMMEDIATELY: the drain released the lease instead of
        // leaving the next owner to wait out its expiry.
        yield* withHost(
          runtimeOptions(filename),
          Effect.gen(function* () {
            const nextHost = yield* NodeDurableHost;
            const nextLedger = yield* SubmissionLedger;
            const report = nextHost.startupRecovery.find(
              (candidate) => candidate.submissionId === receipt.submissionId,
            );
            expect(report?.decision._tag).toBe("ApplyInput");
            expect(report?.disposition).toBe("repaired");

            const reclaimed = yield* nextLedger.claim(
              ClaimRequest.make({
                threadId: thread,
                producerId: decodeProducerId("producer-platform-node-2"),
              }),
            );
            expect(Option.isSome(reclaimed)).toBe(true);
            if (Option.isSome(claimed) && Option.isSome(reclaimed)) {
              expect(reclaimed.value.submissionId).toBe(receipt.submissionId);
              expect(reclaimed.value.producerEpoch).toBeGreaterThan(claimed.value.producerEpoch);
            }
          }),
        );
      }),
    ),
  );

  it.effect("wake-scan fallback claims ready work without any notify", () =>
    withTemporaryDatabase((filename) =>
      withHost(
        runtimeOptions(filename, { wakeScanInterval: 1_000 }),
        Effect.gen(function* () {
          const ledger = yield* SubmissionLedger;
          const wake = yield* WakeScheduler;
          const runtime = yield* DurableAgentRuntime;
          const thread = decodeThreadId("thread-wake");

          // Seed accepted work through the ledger alone: no `notify` is ever sent, exactly like
          // an admission from another process that this worker never heard about.
          const input: PersistedJson = { question: "wake?" };
          const inputDigest = yield* digestJson(input).pipe(Effect.provide(NodeCrypto.layer));
          const admitted = yield* ledger.admit(
            AdmissionRequest.make({
              threadId: thread,
              principal: PRINCIPAL,
              idempotencyKey: decodeIdempotencyKey("wake-1"),
              agentId: decodeAgentId("platform-node-planner"),
              agentDigests: DIGESTS,
              deploymentId: decodeDeploymentId("deployment-platform-node"),
              inputPayload: input,
              inputDigest,
            }),
          );
          yield* ledger.markReady(MarkReadyRequest.make({ submissionId: admitted.submissionId }));

          const woken = yield* Effect.forkChild(Stream.runCollect(Stream.take(wake.wakes, 1)));
          yield* TestClock.adjust(Duration.millis(1_000));
          expect(yield* Fiber.join(woken)).toEqual([thread]);

          const model = yield* makeScriptedModel(() => finalParts('{"answer":"woken"}'));
          const agent = Agent.withModel(plannerDefinition, model);
          const settlements = yield* runtime.processThread(agent, thread);
          expect(settlements).toHaveLength(1);
          expect(settlements[0]?.outcome).toBe("completed");
          expect(yield* lookupState(admitted.submissionId)).toBe("settled");
        }),
      ),
    ),
  );

  it.effect("derives every operational cadence from the validated configuration", () =>
    withTemporaryDatabase((filename) =>
      withHost(
        runtimeOptions(filename, {
          settlementPollInterval: 111,
          leaseRenewalInterval: 2_222,
          abortPollInterval: 333,
          workerConcurrency: 3,
        }),
        Effect.gen(function* () {
          const nodeConfig = yield* NodeDurableRuntimeConfig;
          const sessionConfig = yield* DurableRuntimeConfig;
          expect(nodeConfig.workerConcurrency).toBe(3);
          expect(nodeConfig.filename).toBe(filename);
          expect(sessionConfig.deploymentId).toBe("deployment-platform-node");
          expect(sessionConfig.producerId).toBe("producer-platform-node");
          expect(Duration.toMillis(sessionConfig.settlementPollInterval)).toBe(111);
          expect(Duration.toMillis(sessionConfig.leaseRenewalInterval)).toBe(2_222);
          expect(Duration.toMillis(sessionConfig.abortPollInterval)).toBe(333);

          // runWorkers drives exactly `workerConcurrency` copies of the worker effect.
          const started = yield* Ref.make(0);
          const host = yield* NodeDurableHost;
          yield* host.runWorkers(Ref.update(started, (count) => count + 1));
          expect(yield* Ref.get(started)).toBe(3);
        }),
      ),
    ),
  );
});
