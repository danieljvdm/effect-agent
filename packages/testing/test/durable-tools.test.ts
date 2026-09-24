import { MemorySubmissionLedgerLive } from "@effect-agent/storage-memory/memory-submission-ledger";
import { MemoryThreadStoreLive } from "@effect-agent/storage-memory/memory-thread-store";
import { NodeCrypto } from "@effect/platform-node";
import { expect, layer } from "@effect/vitest";
import {
  Cause,
  Context,
  Duration,
  Effect,
  Exit,
  Layer,
  Logger,
  Option,
  Ref,
  Schema,
  SchemaGetter,
  Stream,
} from "effect";
import * as Agent from "effect-agent/agent";
import { AgentToolAuthorizationCheckError } from "effect-agent/agent-error";
import { AgentPolicy } from "effect-agent/agent-policy";
import { compileRegistrations } from "effect-agent/agent-registration";
import {
  DurableAgentRuntime,
  DurableRuntimeConfig,
  type DurableSubmitOptions,
} from "effect-agent/durable-agent-runtime";
import {
  DurableRuntimeFailpointError,
  type DurableRuntimeFailpointLocation,
} from "effect-agent/durable-failpoint";
import { DurableStep, DurableStepError, ToolExecutionClass } from "effect-agent/durable-step";
import * as FailureDiagnostic from "effect-agent/failure-diagnostic";
import type { SubmissionId } from "effect-agent/identifiers";
import { ThreadId, ToolCallId } from "effect-agent/identifiers";
import {
  type CanonicalRecordEnvelope,
  DefinitionDigestInput,
  DefinitionDigests,
  DeploymentId,
  Digest,
  ProducerId,
} from "effect-agent/records";
import {
  promptFromCanonicalRecords,
  runIdForSubmission,
  toolCallPreparedRecordId,
} from "effect-agent/run-journal";
import {
  RunContextPreparation,
  RunContextPreparationPassthrough,
  RunToolAuthorization,
  toolFailureObserverLayer,
  type ToolFailureObservation,
  type RunToolAuthorizationDecision,
  type RunToolAuthorizationRequest,
} from "effect-agent/run-options";
import {
  AbortCommand,
  IdempotencyKey,
  Principal,
  ResolutionCompletedWithResult,
  ResolutionNeverHappened,
  SubmissionLedger,
  SubmissionLookupById,
  UnknownResolutionCommand,
} from "effect-agent/submission-ledger";
import { DurableRuntimeFailpointTestControl } from "effect-agent/testing/durable-failpoint-test-control";
import { ThreadRead, ThreadStore } from "effect-agent/thread-store";
import { DiscoveryTool, RunToolVisibility } from "effect-agent/tool-exposure";
import {
  ReconciliationUncertain,
  ToolReconciler,
  type PreparedToolCallEvidence,
  type ReconciliationDecision,
} from "effect-agent/tool-reconciler";
import { WakeScheduler } from "effect-agent/wake-scheduler";
import { Prompt, LanguageModel, Model, Tool, Toolkit, type Response } from "effect/unstable/ai";

const SHA_A = Schema.decodeSync(Digest)("a".repeat(64));
const PRINCIPAL = Schema.decodeSync(Principal)("principal-durable-tools");
const DIGESTS = DefinitionDigests.make({ agent: SHA_A, model: SHA_A, tools: SHA_A });
const decodeThreadId = Schema.decodeSync(ThreadId);
const decodeIdempotencyKey = Schema.decodeSync(IdempotencyKey);
const decodeToolCallId = Schema.decodeSync(ToolCallId);

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

const toolCall = (id: string, name: string, params: unknown): Response.StreamPartEncoded => ({
  type: "tool-call",
  id,
  name,
  params,
  providerExecuted: false,
});

const toolTurn = (
  ...calls: ReadonlyArray<Response.StreamPartEncoded>
): ReadonlyArray<Response.StreamPartEncoded> => [
  ...calls,
  { type: "finish", reason: "tool-calls", usage },
];

/**
 * Scripted model whose call counter and captured request prompts live OUTSIDE the Model Layer,
 * so they survive Layer rebuilds across Attempts (each Attempt provides the Model afresh).
 */
const makeScriptedModel = (script: (call: number) => ReadonlyArray<Response.StreamPartEncoded>) =>
  Effect.gen(function* () {
    const calls = yield* Ref.make(0);
    const prompts: Array<Prompt.Prompt> = [];

    const model = Model.make(
      "scripted",
      "durable-tools-test",
      Layer.effect(
        LanguageModel.LanguageModel,
        LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: (request) =>
            Stream.unwrap(
              Ref.getAndUpdate(calls, (call) => call + 1).pipe(
                Effect.map((call) => {
                  prompts.push(request.prompt);

                  return Stream.fromIterable(script(call));
                }),
              ),
            ),
        }),
      ),
    );

    return { model, prompts };
  });

const policy = AgentPolicy.make({
  maxTurns: 3,
  maxToolCalls: 4,
  maxDuration: "30 seconds",
  toolConcurrency: 2,
});

/** Unannotated → fail-closed `uncertain`: enters the prepared/settled protocol. */
const Book = Tool.make("book", {
  parameters: Schema.Struct({ ref: Schema.String }),
  success: Schema.Struct({ confirmation: Schema.String }),
});

const bookTools = Toolkit.make(Book);

const bookDefinition = Agent.make("durable-book", {
  input: Schema.Struct({ question: Schema.String }),
  output: Schema.Struct({ answer: Schema.String }),
  instructions: "Book it.",
  toolkit: bookTools,
  policy,
});

/** The declared external idempotency contract: recovery may re-execute without proof. */
const BookIdempotent = Tool.make("book", {
  parameters: Schema.Struct({ ref: Schema.String }),
  success: Schema.Struct({ confirmation: Schema.String }),
}).annotate(ToolExecutionClass, "idempotent");

const bookIdempotentTools = Toolkit.make(BookIdempotent);

/** Durable Tool: declaring `DurableStep` as a dependency is what makes it durable. */
const Itinerary = Tool.make("itinerary", {
  parameters: Schema.Struct({ ref: Schema.String }),
  success: Schema.Struct({ state: Schema.String }),
  failure: DurableStepError,
  dependencies: [DurableStep],
});

const itineraryTools = Toolkit.make(Itinerary);

const itineraryDefinition = Agent.make("durable-itinerary", {
  input: Schema.Struct({ question: Schema.String }),
  output: Schema.Struct({ answer: Schema.String }),
  instructions: "Reserve the itinerary.",
  toolkit: itineraryTools,
  policy,
});

/** Per-ref supplier call counters that survive Tool-Layer rebuilds across Attempts. */
const makeBookDesk = (tools: typeof bookTools | typeof bookIdempotentTools) =>
  Effect.gen(function* () {
    const calls = yield* Ref.make<ReadonlyMap<string, number>>(new Map());

    const toolLayer = tools.toLayer({
      book: ({ ref }) =>
        Ref.update(calls, (current) => new Map(current).set(ref, (current.get(ref) ?? 0) + 1)).pipe(
          Effect.as({ confirmation: `confirmed-${ref}` }),
        ),
    });

    const count = (ref: string) => Ref.get(calls).pipe(Effect.map((m) => m.get(ref) ?? 0));

    return { toolLayer, count };
  });

/** Test control replacing the reconciliation policy per test (default: fail-closed Uncertain). */
class ReconcilerTestControl extends Context.Service<
  ReconcilerTestControl,
  {
    readonly set: (
      decide: (evidence: PreparedToolCallEvidence) => ReconciliationDecision,
    ) => Effect.Effect<void>;
    readonly reset: Effect.Effect<void>;
    readonly consultations: Effect.Effect<number>;
  }
>()("@effect-agent/testing/ReconcilerTestControl") {}

const uncertainDefault = (): ReconciliationDecision =>
  ReconciliationUncertain.make({ reason: "test default: no proof either way" });

const reconcilerTestLayer = Layer.effectContext(
  Effect.gen(function* () {
    const handler =
      yield* Ref.make<(evidence: PreparedToolCallEvidence) => ReconciliationDecision>(
        uncertainDefault,
      );

    const consulted = yield* Ref.make(0);

    return Context.make(
      ToolReconciler,
      ToolReconciler.of({
        reconcile: (evidence) =>
          Ref.update(consulted, (n) => n + 1).pipe(
            Effect.andThen(Ref.get(handler)),
            Effect.map((decide) => decide(evidence)),
          ),
      }),
    ).pipe(
      Context.add(
        ReconcilerTestControl,
        ReconcilerTestControl.of({
          set: (decide) => Ref.set(handler, decide),
          reset: Ref.set(handler, uncertainDefault).pipe(Effect.andThen(Ref.set(consulted, 0))),
          consultations: Ref.get(consulted),
        }),
      ),
    );
  }),
);

/** Independent host action-authorization control, captured at runtime construction. */
class ToolAuthorizationTestControl extends Context.Service<
  ToolAuthorizationTestControl,
  {
    readonly set: (
      decide: (request: RunToolAuthorizationRequest) => RunToolAuthorizationDecision,
    ) => Effect.Effect<void>;
    readonly reset: Effect.Effect<void>;
    readonly requests: Effect.Effect<ReadonlyArray<RunToolAuthorizationRequest>>;
  }
>()("@effect-agent/testing/ToolAuthorizationTestControl") {}

const allowToolExecution = (): RunToolAuthorizationDecision => ({ _tag: "allowed" });

const toolAuthorizationTestLayer = Layer.effectContext(
  Effect.gen(function* () {
    const policy =
      yield* Ref.make<(request: RunToolAuthorizationRequest) => RunToolAuthorizationDecision>(
        allowToolExecution,
      );

    const requests = yield* Ref.make<ReadonlyArray<RunToolAuthorizationRequest>>([]);

    return Context.make(
      RunToolAuthorization,
      RunToolAuthorization.of({
        authorize: (request) =>
          Ref.update(requests, (all) => [...all, request]).pipe(
            Effect.andThen(Ref.get(policy)),
            Effect.map((decide) => decide(request)),
          ),
      }),
    ).pipe(
      Context.add(
        ToolAuthorizationTestControl,
        ToolAuthorizationTestControl.of({
          set: (decide) => Ref.set(policy, decide),
          reset: Ref.set(policy, allowToolExecution).pipe(Effect.andThen(Ref.set(requests, []))),
          requests: Ref.get(requests),
        }),
      ),
    );
  }),
);

const configLayer = DurableRuntimeConfig.layer({
  deploymentId: Schema.decodeSync(DeploymentId)("deployment-durable-tools"),
  producerId: Schema.decodeSync(ProducerId)("producer-durable-tools"),
  settlementPollInterval: Duration.millis(100),
  leaseRenewalInterval: Duration.seconds(5),
  abortPollInterval: Duration.millis(100),
});

const baseLayer = Layer.mergeAll(
  MemorySubmissionLedgerLive,
  MemoryThreadStoreLive,
  WakeScheduler.layerNoop,
  DurableRuntimeFailpointTestControl.layer,
  reconcilerTestLayer,
  toolAuthorizationTestLayer,
  RunContextPreparationPassthrough,
  configLayer,
).pipe(Layer.provideMerge(NodeCrypto.layer));

const testLayer = DurableAgentRuntime.layerWithServices.pipe(Layer.provideMerge(baseLayer));

const readLog = (threadId: string) =>
  Effect.gen(function* () {
    const store = yield* ThreadStore;

    return yield* Stream.runCollect(
      store.read(
        ThreadRead.make({
          threadId: decodeThreadId(threadId),
          limit: 1_024,
        }),
      ),
    );
  });

const logTags = (records: ReadonlyArray<CanonicalRecordEnvelope>): ReadonlyArray<string> =>
  records.map((envelope) => envelope.record.payload._tag);

const lookupState = (submissionId: SubmissionId) =>
  Effect.gen(function* () {
    const ledger = yield* SubmissionLedger;
    const snapshot = yield* ledger.lookup(SubmissionLookupById.make({ submissionId }));

    expect(Option.isSome(snapshot)).toBe(true);
    if (Option.isNone(snapshot)) throw new Error("Expected the Submission to exist");

    return snapshot.value.state;
  });

const armFailpoint = (location: DurableRuntimeFailpointLocation) =>
  Effect.gen(function* () {
    const control = yield* DurableRuntimeFailpointTestControl;

    yield* control.setHandler((hitLocation) =>
      hitLocation === location
        ? Effect.fail(DurableRuntimeFailpointError.make({ location: hitLocation }))
        : Effect.void,
    );
  });

const clearFailpoint = Effect.gen(function* () {
  const control = yield* DurableRuntimeFailpointTestControl;

  yield* control.clear;
});

const resetReconciler = Effect.gen(function* () {
  const control = yield* ReconcilerTestControl;

  yield* control.reset;
});

const failureTag = <A, E>(exit: Exit.Exit<A, E>): string => {
  expect(Exit.isFailure(exit)).toBe(true);
  if (Exit.isSuccess(exit)) throw new Error("Expected the Effect to fail");
  const failure = Cause.findErrorOption(exit.cause);

  expect(Option.isSome(failure)).toBe(true);
  if (Option.isNone(failure)) throw new Error("Expected a typed failure");
  const error: unknown = failure.value;

  return typeof error === "object" && error !== null && "_tag" in error
    ? String(error._tag)
    : "unknown";
};

layer(testLayer)("DUR P5 durable Tools (prepared/settled, reconciliation, unknown)", (it) => {
  {
    const location = "turn:after-response-append" as const;

    it.effect(
      `recovers strict native argument rejection after ${location} without replaying actions`,
      () =>
        Effect.gen(function* () {
          yield* resetReconciler;
          yield* clearFailpoint;
          const runtime = yield* DurableAgentRuntime;
          const authorization = yield* ToolAuthorizationTestControl;

          yield* authorization.reset;

          const Search = Tool.make("strict_search", {
            parameters: Schema.Struct({ query: Schema.NonEmptyString }),
            success: Schema.String,
            failureMode: "return",
          });

          const Action = Tool.make("record_search", {
            parameters: Schema.Struct({}),
            success: Schema.String,
          });

          const tools = Toolkit.make(Search, Action);

          const scripted = yield* makeScriptedModel((call) =>
            call === 0
              ? toolTurn(
                  toolCall("invalid", Search.name, { query: "" }),
                  toolCall("action", Action.name, {}),
                )
              : call === 1
                ? toolTurn(toolCall("corrected", Search.name, { query: "sea" }))
                : finalParts('"done"'),
          );

          const agent = Agent.withModel(
            Agent.make(`strict-arguments-${location}`, {
              input: Schema.String,
              output: Schema.String,
              instructions: "Correct the query, then finish.",
              toolkit: tools,
              policy: { maxTurns: 4, maxToolCalls: 4 },
            }),
            scripted.model,
          );

          const searches: Array<string> = [];
          let actions = 0;

          const handlers = tools.toLayer({
            strict_search: ({ query }) =>
              Effect.sync(() => {
                searches.push(query);

                return query;
              }),
            record_search: () =>
              Effect.sync(() => {
                actions++;

                return "recorded";
              }),
          });

          const thread = `strict-arguments-${location}`;
          const receipt = yield* runtime.submit(agent, "search", submitOptions(thread, thread));

          yield* armFailpoint(location);

          const first = yield* runtime
            .processThread(agent, receipt.threadId)
            .pipe(Effect.provide(handlers), Effect.exit);

          expect(failureTag(first)).toBe("DurableRuntimeFailpointError");
          expect(searches).toEqual([]);
          expect(actions).toBe(0);
          const interrupted = yield* readLog(thread);

          expect(
            interrupted.find(({ record }) => record.payload._tag === "ModelResponseRecorded")
              ?.record.payload,
          ).toMatchObject({
            toolParameterRejections: [
              {
                toolCallId: "invalid",
                parameters: { query: "" },
                error: { _tag: "AiError", reason: { _tag: "ToolParameterValidationError" } },
              },
            ],
          });
          yield* clearFailpoint;
          yield* runtime.runRecovery();

          const completed = yield* runtime
            .processThread(agent, receipt.threadId)
            .pipe(Effect.provide(handlers));

          expect(completed[0]?.outcome).toBe("completed");
          expect(yield* lookupState(receipt.submissionId)).toBe("settled");
          expect(searches).toEqual(["sea"]);
          expect(actions).toBe(1);
          expect((yield* authorization.requests).map(({ call }) => call.toolCallId)).toEqual([
            "action",
            "corrected",
          ]);
          const records = yield* readLog(thread);

          expect(
            records
              .filter(({ record }) => record.payload._tag === "ToolCallPrepared")
              .map(({ record }) =>
                record.payload._tag === "ToolCallPrepared" ? record.payload.toolCallId : "",
              ),
          ).toEqual(["action", "corrected"]);
          const prompt = yield* promptFromCanonicalRecords(records);

          const failures = prompt.content
            .flatMap((message) => (message.role === "tool" ? message.content : []))
            .filter((part) => part.type === "tool-result" && part.id === "invalid");

          expect(failures).toHaveLength(1);
          expect(failures[0]).toMatchObject({
            isFailure: true,
            result: {
              _tag: "AiError",
              reason: { _tag: "ToolParameterValidationError", toolName: "strict_search" },
            },
          });
          expect(scripted.prompts).toHaveLength(3);
          expect(
            scripted.prompts[1]?.content.flatMap((message) =>
              message.role === "tool" ? message.content : [],
            ),
          ).toEqual(
            expect.arrayContaining([expect.objectContaining({ id: "invalid", isFailure: true })]),
          );
        }),
    );
  }

  {
    const location = "turn:after-results-append" as const;

    it.effect(`does not inherit a prior Turn's reused call ID after ${location}`, () =>
      Effect.gen(function* () {
        yield* clearFailpoint;
        const runtime = yield* DurableAgentRuntime;

        const discover = Tool.make("discover", {
          parameters: Schema.Struct({ tool: Schema.String }),
          success: Schema.Struct({ toolNames: Schema.Array(Schema.String) }),
        })
          .annotate(DiscoveryTool, true)
          .annotate(ToolExecutionClass, "readonly");

        const firstAction = Tool.make("first_action", {
          parameters: Schema.Struct({}),
          success: Schema.String,
        }).annotate(ToolExecutionClass, "readonly");

        const lastAction = Tool.make("last_action", {
          parameters: Schema.Struct({}),
          success: Schema.String,
        }).annotate(ToolExecutionClass, "readonly");

        const native = Toolkit.make(discover, firstAction, lastAction);
        const requests: Array<ReadonlyArray<string>> = [];
        let modelCalls = 0;
        let searchCalls = 0;
        let actionCalls = 0;

        const model = Model.make(
          "test",
          "reused-exposure-id",
          Layer.effect(
            LanguageModel.LanguageModel,
            LanguageModel.make({
              generateText: () => Effect.succeed([]),
              streamText: (request) => {
                requests.push(request.tools.map((tool) => tool.name));
                const turn = modelCalls++;

                return Stream.fromIterable(
                  turn === 0
                    ? toolTurn(toolCall("reused", "discover", { tool: "first_action" }))
                    : turn === 1
                      ? toolTurn(toolCall("switch", "discover", { tool: "last_action" }))
                      : turn === 2
                        ? toolTurn(toolCall("reused", "last_action", {}))
                        : finalParts('{"answer":"done"}'),
                );
              },
            }),
          ),
        );

        const agent = Agent.withModel(
          Agent.make("reused-exposure-id", {
            input: Schema.Struct({ question: Schema.String }),
            output: Schema.Struct({ answer: Schema.String }),
            instructions: "Go.",
            toolkit: native,
            toolExposure: {},
            policy: { maxTurns: 5, maxToolCalls: 8 },
          }),
          model,
        );

        const handlers = native.toLayer({
          discover: ({ tool }) =>
            Effect.sync(() => {
              searchCalls++;

              return { toolNames: [tool] };
            }),
          first_action: () => Effect.succeed(""),
          last_action: () =>
            Effect.sync(() => {
              actionCalls++;

              return "acted";
            }),
        });

        const thread = `reused-exposure-id-${location}`;

        yield* runtime.submit(agent, { question: "go" }, submitOptions(thread, thread));
        let commits = 0;
        const control = yield* DurableRuntimeFailpointTestControl;

        yield* control.setHandler((currentLocation) =>
          currentLocation === location && ++commits === 3
            ? Effect.fail(DurableRuntimeFailpointError.make({ location }))
            : Effect.void,
        );

        const interrupted = yield* runtime
          .processThread(agent, decodeThreadId(thread))
          .pipe(Effect.provide(handlers), Effect.exit);

        expect(failureTag(interrupted)).toBe("DurableRuntimeFailpointError");
        yield* clearFailpoint;

        const settled = yield* runtime
          .processThread(agent, decodeThreadId(thread))
          .pipe(Effect.provide(handlers));

        expect(settled[0]?.outcome).toBe("completed");
        expect(searchCalls).toBe(2);
        expect(actionCalls).toBe(1);
        expect(requests).toEqual([
          ["discover"],
          ["discover", "first_action"],
          ["discover", "last_action"],
          ["discover", "last_action"],
        ]);
        const records = yield* readLog(thread);

        const actionResult = records.find(
          (entry) =>
            entry.record.payload._tag === "ToolCallSettled" &&
            entry.record.payload.toolName === "last_action",
        );

        expect(actionResult?.record.payload).not.toHaveProperty("toolSelection");
      }),
    );
  }

  {
    const location = "turn:after-response-append" as const;

    it.effect(
      `restores native exposure after ${location} without rerunning committed discovery`,
      () =>
        Effect.gen(function* () {
          yield* resetReconciler;
          yield* clearFailpoint;
          const runtime = yield* DurableAgentRuntime;

          const discover = Tool.make("discover_tools", {
            parameters: Schema.Struct({}),
            success: Schema.Struct({
              toolNames: Schema.Array(Schema.String),
              padding: Schema.String,
            }),
          })
            .annotate(DiscoveryTool, true)
            .annotate(ToolExecutionClass, "readonly");

          const action = Tool.make("selected_action", {
            parameters: Schema.Struct({}),
            success: Schema.String,
          }).annotate(ToolExecutionClass, "readonly");

          const hidden = Tool.make("unselected_action", {
            parameters: Schema.Struct({}),
            success: Schema.String,
          }).annotate(ToolExecutionClass, "readonly");

          const native = Toolkit.make(discover, action, hidden);
          const requests: Array<ReadonlyArray<string>> = [];
          let searchCalls = 0;
          let actionCalls = 0;
          let modelCalls = 0;

          const model = Model.make(
            "test",
            "durable-exposure",
            Layer.effect(
              LanguageModel.LanguageModel,
              LanguageModel.make({
                generateText: () => Effect.succeed([]),
                streamText: (request) => {
                  requests.push(request.tools.map((tool) => tool.name));
                  const turn = modelCalls++;

                  return Stream.fromIterable(
                    turn === 0
                      ? toolTurn(toolCall("discover-1", "discover_tools", {}))
                      : turn === 1
                        ? toolTurn(toolCall("action-1", "selected_action", {}))
                        : finalParts('{"answer":"done"}'),
                  );
                },
              }),
            ),
          );

          const definition = Agent.make(`durable-exposure-${location}`, {
            input: Schema.Struct({ question: Schema.String }),
            output: Schema.Struct({ answer: Schema.String }),
            instructions: "Discover then act.",
            toolkit: native,
            policy: { maxTurns: 4, maxToolCalls: 5, toolResultBounds: { maxBytes: 256 } },
            toolExposure: { maxTools: 2 },
          });

          const agent = Agent.withModel(definition, model);

          const handlers = native.toLayer({
            discover_tools: () =>
              Effect.sync(() => {
                searchCalls++;

                return {
                  toolNames: [searchCalls === 1 ? "selected_action" : "unselected_action"],
                  padding: "x".repeat(2_000),
                };
              }),
            selected_action: () =>
              Effect.sync(() => {
                actionCalls++;

                return "acted";
              }),
            unselected_action: () => Effect.die("Hidden action executed"),
          });

          const thread = `exposure-${location}`;

          yield* runtime.submit(agent, { question: "go" }, submitOptions(thread, thread));
          yield* armFailpoint(location);

          const first = yield* runtime
            .processThread(agent, decodeThreadId(thread))
            .pipe(Effect.provide(handlers), Effect.exit);

          expect(failureTag(first)).toBe("DurableRuntimeFailpointError");
          expect(searchCalls).toBe(0);
          yield* clearFailpoint;
          yield* runtime.runRecovery();

          const settled = yield* runtime
            .processThread(agent, decodeThreadId(thread))
            .pipe(Effect.provide(handlers));

          expect(settled[0]?.outcome).toBe("completed");
          expect(searchCalls).toBe(1);
          expect(actionCalls).toBe(1);
          expect(requests).toEqual([
            ["discover_tools"],
            ["discover_tools", "selected_action"],
            ["discover_tools", "selected_action"],
          ]);
          const records = yield* readLog(thread);

          const response = records.find(
            (entry) => entry.record.payload._tag === "ModelResponseRecorded",
          );

          expect(response?.record.payload).toMatchObject({
            toolExposure: { exposedToolNames: ["discover_tools"], selection: { toolNames: [] } },
          });

          const result = records.find(
            (entry) =>
              entry.record.payload._tag === "ToolCallSettled" &&
              entry.record.payload.toolCallId === "discover-1",
          );

          expect(result?.record.payload).toMatchObject({
            toolSelection: { toolNames: ["selected_action"] },
            result: { truncatedToolResult: true },
          });
        }),
    );
  }
  it.effect("builds captured Tool services once per Attempt and finalizes before replacement", () =>
    Effect.gen(function* () {
      const lifecycle: Array<string> = [];
      const observed: Array<string> = [];

      const probe = Tool.make("scope_probe", {
        parameters: Schema.Struct({}),
        success: Schema.String,
      });

      const toolkit = Toolkit.make(probe);

      const definition = Agent.make("attempt-scoped-tool-services", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Call the probe twice.",
        toolkit,
        policy,
      });

      const scripted = yield* makeScriptedModel((call) =>
        call % 3 < 2
          ? toolTurn(toolCall(`scope-${call}`, "scope_probe", {}))
          : finalParts('{"answer":"done"}'),
      );

      const bindings = yield* compileRegistrations([
        {
          agent: Agent.withModel(definition, scripted.model),
          definitions: DefinitionDigestInput.make({
            agent: "scope-1",
            model: "scope-1",
            tools: ["scope-1"],
          }),
          attemptLayer: ({ attemptId }) =>
            toolkit.toLayer(
              Effect.gen(function* () {
                yield* Effect.acquireRelease(
                  Effect.sync(() => lifecycle.push(`open:${attemptId}`)),
                  () => Effect.sync(() => lifecycle.push(`close:${attemptId}`)),
                );

                return {
                  scope_probe: () =>
                    Effect.sync(() => {
                      observed.push(attemptId);

                      return "ready";
                    }),
                };
              }),
            ),
        },
      ]);

      const runtime = yield* DurableAgentRuntime.pipe(
        Effect.provide(DurableAgentRuntime.layerWithBindings(bindings)),
      );

      for (let run = 0; run < 2; run++) {
        const threadId = `attempt-scope-${run}`;

        yield* runtime.submit(
          { definition },
          { question: "probe" },
          { ...submitOptions(threadId, threadId), definitions: bindings[0]!.digests },
        );
        yield* runtime.processThreadResolved(decodeThreadId(threadId));
      }
      expect(observed).toHaveLength(4);
      expect(observed[0]).toBe(observed[1]);
      expect(observed[2]).toBe(observed[3]);
      expect(observed[0]).not.toBe(observed[2]);
      expect(lifecycle).toEqual([
        `open:${observed[0]}`,
        `close:${observed[0]}`,
        `open:${observed[2]}`,
        `close:${observed[2]}`,
      ]);
      const interruptedThread = "attempt-scope-replacement";

      yield* runtime.submit(
        { definition },
        { question: "probe" },
        {
          ...submitOptions(interruptedThread, interruptedThread),
          definitions: bindings[0]!.digests,
        },
      );
      yield* armFailpoint("turn:after-results-append");

      const interrupted = yield* runtime
        .processThreadResolved(decodeThreadId(interruptedThread))
        .pipe(Effect.exit);

      expect(Exit.isFailure(interrupted)).toBe(true);
      expect(lifecycle.at(-1)).toBe(`close:${observed[4]}`);
      yield* clearFailpoint;
      yield* runtime.processThreadResolved(decodeThreadId(interruptedThread));
      expect(observed).toHaveLength(6);
      expect(observed[4]).not.toBe(observed[5]);
      expect(lifecycle.slice(-4)).toEqual([
        `open:${observed[4]}`,
        `close:${observed[4]}`,
        `open:${observed[5]}`,
        `close:${observed[5]}`,
      ]);
    }),
  );
  it.effect(
    "RUN-036 captures the host observer for fresh and replacement Attempts and skips settled replay",
    () => {
      const observations: Array<ToolFailureObservation> = [];
      const ambient: Array<ToolFailureObservation> = [];

      const observerLayer = toolFailureObserverLayer({
        observe: (observation) =>
          Effect.sync(() => {
            observations.push(observation);
          }),
      });

      const ambientLayer = toolFailureObserverLayer({
        observe: (observation) =>
          Effect.sync(() => {
            ambient.push(observation);
          }),
      });

      const Failed = Tool.make("failed", {
        parameters: Schema.Struct({ ref: Schema.String }),
        success: Schema.String,
        failure: Schema.Struct({ _tag: Schema.Literal("LookupFailure"), message: Schema.String }),
        failureMode: "return",
      });

      const tools = Toolkit.make(Failed);

      const definition = Agent.make("durable-observed-failure", {
        input: Schema.Struct({ question: Schema.String }),
        output: Schema.Struct({ answer: Schema.String }),
        instructions: "Use the lookup, then answer.",
        toolkit: tools,
        policy,
      });

      const starts: Array<string> = [];

      const handlers = tools.toLayer({
        failed: ({ ref }) =>
          Effect.sync(() => {
            starts.push(ref);
          }).pipe(
            Effect.andThen(
              Effect.fail({ _tag: "LookupFailure" as const, message: "lookup unavailable" }),
            ),
          ),
      });

      return Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;

        const freshModel = yield* makeScriptedModel((n) =>
          n === 0
            ? toolTurn(toolCall("fresh", "failed", { ref: "fresh" }))
            : finalParts('{"answer":"fallback"}'),
        );

        const fresh = Agent.withModel(definition, freshModel.model);

        yield* runtime.submit(
          fresh,
          { question: "lookup" },
          submitOptions("observer-fresh", "fresh"),
        );

        const completed = yield* runtime
          .processThread(fresh, decodeThreadId("observer-fresh"))
          .pipe(Effect.provide(Layer.merge(handlers, ambientLayer)));

        expect(completed[0]?.outcome).toBe("completed");
        expect(
          observations.map((value) =>
            value._tag === "ModelToolFailure" ? value.toolCallId : undefined,
          ),
        ).toEqual(["fresh"]);

        const replacementModel = yield* makeScriptedModel((n) =>
          n === 0
            ? toolTurn(
                toolCall("settled", "failed", { ref: "settled" }),
                toolCall("open", "failed", { ref: "open" }),
              )
            : finalParts('{"answer":"recovered"}'),
        );

        const replacement = Agent.withModel(definition, replacementModel.model);
        const threadId = decodeThreadId("observer-replacement");

        const receipt = yield* runtime.submit(
          replacement,
          { question: "lookup both" },
          submitOptions(threadId, "replacement"),
        );

        yield* armFailpoint("tools:after-prepared-append");

        const interrupted = yield* runtime
          .processThread(replacement, threadId)
          .pipe(Effect.provide(Layer.merge(handlers, ambientLayer)), Effect.exit);

        expect(failureTag(interrupted)).toBe("DurableRuntimeFailpointError");
        yield* clearFailpoint;
        yield* runtime.runRecovery();
        yield* runtime.resolveUnknown(
          UnknownResolutionCommand.make({
            submissionId: receipt.submissionId,
            toolCallId: decodeToolCallId("settled"),
            author: "operator",
            reason: "Recovered external failure",
            resolution: ResolutionCompletedWithResult.make({
              result: { _tag: "LookupFailure", message: "already settled" },
              isFailure: true,
            }),
          }),
        );
        yield* runtime.resolveUnknown(
          UnknownResolutionCommand.make({
            submissionId: receipt.submissionId,
            toolCallId: decodeToolCallId("open"),
            author: "operator",
            reason: "No Handler started",
            resolution: ResolutionNeverHappened.make(),
          }),
        );

        const resumed = yield* runtime
          .processThread(replacement, threadId)
          .pipe(Effect.provide(Layer.merge(handlers, ambientLayer)));

        expect(resumed[0]?.outcome).toBe("completed");
        expect(starts).toEqual(["fresh", "open"]);
        expect(
          observations.map((value) =>
            value._tag === "ModelToolFailure" ? value.toolCallId : undefined,
          ),
        ).toEqual(["fresh", "open"]);
        expect(ambient).toEqual([]);
        expect(
          (yield* readLog(threadId)).filter(
            (envelope) => envelope.record.payload._tag === "ToolCallSettled",
          ),
        ).toHaveLength(2);
      }).pipe(
        Effect.provide(
          DurableAgentRuntime.layerWithServices.pipe(
            Layer.provideMerge(baseLayer),
            Layer.provide(observerLayer),
          ),
          { local: true },
        ),
      );
    },
  );

  {
    const configured = true as const;

    it.effect(`captures host visibility with configured=${configured} over a worker override`, () =>
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const desk = yield* makeBookDesk(bookTools);

        const scripted = yield* makeScriptedModel((call) =>
          call === 0
            ? toolTurn(toolCall("book-1", "book", { ref: "r-visible" }))
            : finalParts('{"answer":"booked"}'),
        );

        const agent = Agent.withModel(bookDefinition, scripted.model);
        const thread = `visibility-captured-${configured}`;
        let workerPolicyCalls = 0;

        yield* runtime.submit(agent, { question: "book" }, submitOptions(thread, "visible"));

        const settlements = yield* runtime.processThread(agent, decodeThreadId(thread)).pipe(
          Effect.provide(desk.toolLayer),
          Effect.provideService(RunToolVisibility, {
            visible: () =>
              Effect.sync(() => {
                workerPolicyCalls++;

                return [];
              }),
          }),
        );

        expect(settlements[0]?.outcome).toBe("completed");
        expect(yield* desk.count("r-visible")).toBe(1);
        expect(workerPolicyCalls).toBe(0);

        const responses = (yield* readLog(thread)).flatMap((entry) =>
          entry.record.payload._tag === "ModelResponseRecorded" ? [entry.record.payload] : [],
        );

        expect(responses).toHaveLength(2);
        expect(
          responses.every((response) => (response.toolExposure !== undefined) === configured),
        ).toBe(true);
      }).pipe(
        Effect.provide(
          testLayer.pipe(
            Layer.provide(
              Layer.succeed(RunToolVisibility, {
                visible: ({ toolNames }) => Effect.succeed(toolNames),
              }),
            ),
          ),
          { local: true },
        ),
      ),
    );
  }

  it.effect(
    "composes context and authorization Layers across later-Turn restart and durable resume",
    () =>
      Effect.gen(function* () {
        yield* resetReconciler;
        const authorization = yield* ToolAuthorizationTestControl;

        yield* authorization.reset;
        const runtime = yield* DurableAgentRuntime;
        const handlerWrites = yield* Ref.make<ReadonlyArray<string>>([]);

        const toolLayer = bookIdempotentTools.toLayer({
          book: ({ ref }) =>
            Ref.update(handlerWrites, (writes) => [...writes, ref]).pipe(
              Effect.as({ confirmation: `confirmed-${ref}` }),
            ),
        });

        const nonIdempotentWake = Schema.String.pipe(
          Schema.decode({
            decode: SchemaGetter.transform((value) => value),
            encode: SchemaGetter.transform((value) => `admitted:${value}`),
          }),
        );

        const definition = Agent.make("durable-book-idempotent-canonical-authority", {
          input: Schema.Struct({ question: Schema.String, wake: nonIdempotentWake }),
          output: Schema.Struct({ answer: Schema.String }),
          instructions: "Book it idempotently.",
          toolkit: bookIdempotentTools,
          policy,
        });

        const scripted = yield* makeScriptedModel((call) => {
          switch (call) {
            case 0:
              return toolTurn(toolCall("book-582-turn-1", "book", { ref: "r-turn-1" }));
            case 1:
              return toolTurn(toolCall("book-582-turn-2", "book", { ref: "r-authorized" }));
            default:
              return finalParts('{"answer":"resumed"}');
          }
        });

        const agent = Agent.withModel(definition, scripted.model);
        const thread = "thread-tool-authorization-resume";

        const receipt = yield* runtime.submit(
          agent,
          { question: "book it", wake: "wake-582" },
          submitOptions(thread, "tool-authorization-resume-1"),
        );

        // Commit Turn 1 and interrupt after its results boundary. No batch remains pending, so the
        // replacement engine really restarts its local Turn counter at 1.
        yield* armFailpoint("turn:after-results-append");

        const turnOneInterrupted = yield* Effect.exit(
          runtime.processThread(agent, decodeThreadId(thread)).pipe(Effect.provide(toolLayer)),
        );

        expect(failureTag(turnOneInterrupted)).toBe("DurableRuntimeFailpointError");
        expect(yield* Ref.get(handlerWrites)).toEqual(["r-turn-1"]);
        expect(yield* authorization.requests).toHaveLength(1);
        yield* clearFailpoint;

        // The replacement declares canonical Turn 2 from engine-local Turn 1, authorizes it, and
        // dies after preparation. Its following Attempt resumes that exact durable batch.
        yield* armFailpoint("tools:after-prepared-append");

        const turnTwoInterrupted = yield* Effect.exit(
          runtime.processThread(agent, decodeThreadId(thread)).pipe(Effect.provide(toolLayer)),
        );

        expect(failureTag(turnTwoInterrupted)).toBe("DurableRuntimeFailpointError");
        expect(yield* Ref.get(handlerWrites)).toEqual(["r-turn-1"]);
        expect(yield* authorization.requests).toHaveLength(2);
        yield* clearFailpoint;

        const runId = runIdForSubmission(receipt.submissionId);

        const targetPreparedId = toolCallPreparedRecordId(
          runId,
          2,
          decodeToolCallId("book-582-turn-2"),
        );

        expect(
          (yield* readLog(thread)).filter(
            (envelope) => envelope.record.recordId === targetPreparedId,
          ),
        ).toHaveLength(1);
        yield* authorization.set(() => ({
          _tag: "denied",
          reason: "the originating task-message wake was superseded before resume",
        }));

        const settlements = yield* runtime
          .processThread(agent, decodeThreadId(thread))
          .pipe(Effect.provide(Layer.merge(toolLayer, RunToolAuthorization.allowAll)));

        expect(settlements).toHaveLength(1);
        expect(settlements[0]).toMatchObject({
          outcome: "failed",
          failure: {
            errorTag: "AgentToolAuthorizationDenied",
            message: "the originating task-message wake was superseded before resume",
          },
        });
        expect(yield* Ref.get(handlerWrites)).toEqual(["r-turn-1"]);

        const requests = yield* authorization.requests;

        expect(requests).toHaveLength(3);
        expect(requests.map((request) => request.turn)).toEqual([1, 2, 2]);
        const freshTurnTwo = requests[1];
        const resumedTurnTwo = requests[2];

        expect(freshTurnTwo).toBeDefined();
        expect(resumedTurnTwo).toEqual(freshTurnTwo);
        expect(freshTurnTwo).toMatchObject({
          threadId: thread,
          turn: 2,
          input: { question: "book it", wake: "admitted:wake-582" },
          call: {
            toolCallId: "book-582-turn-2",
            toolName: "book",
            parameters: { ref: "r-authorized" },
            executionClass: "idempotent",
          },
        });
        expect(freshTurnTwo?.runId).toBeDefined();
        expect(freshTurnTwo?.turnId).toBeDefined();
        expect(scripted.prompts).toHaveLength(2);
        expect(
          scripted.prompts.every((prompt) =>
            JSON.stringify(prompt).includes("host-prepared-context"),
          ),
        ).toBe(true);

        const records = yield* readLog(thread);

        expect(
          records.filter((envelope) => envelope.record.recordId === targetPreparedId),
        ).toHaveLength(1);
        expect(
          records.filter((envelope) => envelope.record.payload._tag === "SubmissionSettled"),
        ).toHaveLength(1);

        const replay = yield* runtime
          .processThread(agent, decodeThreadId(thread))
          .pipe(Effect.provide(toolLayer));

        expect(replay).toEqual([]);
        expect(yield* Ref.get(handlerWrites)).toEqual(["r-turn-1"]);
        expect(yield* authorization.requests).toHaveLength(3);
        yield* authorization.reset;
      }).pipe(
        Effect.provide(
          Layer.fresh(DurableAgentRuntime.layerWithServices).pipe(
            Layer.provide(
              Layer.succeed(RunContextPreparation, {
                hook: {
                  prepare: ({ source }) =>
                    Effect.succeed({
                      prompt: Prompt.fromMessages([
                        ...source.content,
                        Prompt.systemMessage({ content: "host-prepared-context" }),
                      ]),
                    }),
                },
              }),
            ),
            Layer.provideMerge(baseLayer),
          ),
          { local: true },
        ),
      ),
  );

  it.effect(
    "retains an authorization check failure in the canonical settlement and reports its original Cause once",
    () =>
      Effect.gen(function* () {
        const admissionRuntime = yield* DurableAgentRuntime;
        const desk = yield* makeBookDesk(bookTools);

        const leaf = Object.assign(new Error("Database is locked"), { code: "SQLITE_BUSY_LEAF" });
        let wrapped: unknown = leaf;

        // Execution, RPC, Worker admission, delivery and driver wrappers must not hide the leaf.
        for (let index = 0; index < 30; index++) {
          wrapped = Object.assign(new Error(`Call dependency layer ${index}`), {
            _tag: `CallDependencyLayer${index}`,
            cause: wrapped,
          });
          if (index % 10 === 9) wrapped = Cause.fail(wrapped);
        }

        const dependency = Object.assign(new Error("Delivery lookup failed"), {
          _tag: "DeliveryStorageError",
          reason: "busy",
          code: "SQLITE_BUSY",
          callId: "phone-call-1",
          cause: wrapped,
          payload: { private: "private-authority-payload" },
        });

        const rpcError = Schema.Struct({
          _tag: Schema.Literal("AuthorityRpcError"),
          reason: Schema.Literal("unavailable"),
          cause: FailureDiagnostic.Cause,
        });

        const rpcExit = Schema.toCodecJson(
          Schema.Exit(Schema.Void, rpcError, FailureDiagnostic.Value),
        );

        const encoded = yield* Schema.encodeEffect(rpcExit)(
          Exit.fail({
            _tag: "AuthorityRpcError",
            reason: "unavailable",
            cause: Cause.fail(dependency),
          }),
        );

        const received = yield* Schema.decodeUnknownEffect(rpcExit)(
          JSON.parse(JSON.stringify(encoded)),
        );

        if (Exit.isSuccess(received)) throw new Error("Expected the RPC's original failure");
        const original = received.cause;

        const check = AgentToolAuthorizationCheckError.make({
          toolCallId: decodeToolCallId("book-check-failed"),
          toolName: "book",
          check: "pending-deliveries",
          message: "Could not verify authority",
          cause: original,
        });

        const logs: Array<Cause.Cause<unknown>> = [];

        const logger = Logger.make<unknown, void>(({ message, cause }) => {
          if (Array.isArray(message) && message.includes("Agent run failed")) logs.push(cause);
        });

        const scripted = yield* makeScriptedModel(() =>
          toolTurn(toolCall("book-check-failed", "book", { ref: "r-check" })),
        );

        const agent = Agent.withModel(bookDefinition, scripted.model);
        // Regression: https://github.com/danieljvdm/effect-agent/commit/d9249632b9f7ab70f6aca23ac0630f99c3c4d31d
        // Admitted Thread identity is authoritative; a bounded diagnostic copy must not prevent settlement.
        const threadId = `thread-check-failed-${"x".repeat(1_024)}`;

        const receipt = yield* admissionRuntime.submit(
          agent,
          { question: "book it" },
          submitOptions(threadId, "check-failed"),
        );

        expect(receipt.threadId).toBe(threadId);

        const runtime = yield* DurableAgentRuntime.pipe(
          Effect.provide(
            Layer.fresh(DurableAgentRuntime.layerWithServices).pipe(
              Layer.provide(
                Layer.succeed(RunToolAuthorization, { authorize: () => Effect.fail(check) }),
              ),
            ),
          ),
        );

        const settlements = yield* runtime
          .processThread(agent, receipt.threadId)
          .pipe(Effect.provide([desk.toolLayer, Logger.layer([logger])]));

        expect(yield* desk.count("r-check")).toBe(0);
        expect(settlements).toHaveLength(1);
        expect(settlements[0]).toMatchObject({
          outcome: "failed",
          receiptId: receipt.receiptId,
          submissionId: receipt.submissionId,
          failure: {
            errorTag: "AgentToolAuthorizationCheckError",
            message: "Could not verify authority",
            context: {
              threadId: expect.stringMatching(/^thread-check-failed-.*\[truncated\]$/),
              submissionId: receipt.submissionId,
            },
          },
        });
        expect(logs).toHaveLength(1);
        expect(Cause.findErrorOption(logs[0]).pipe(Option.getOrThrow)).toBe(check);
        expect(check.cause).toBe(original);
        const settled = yield* runtime.awaitSettlement(receipt);

        expect(settled).toEqual(settlements[0]);
        const records = yield* readLog(receipt.threadId);

        expect(records.every((record) => record.threadId === threadId)).toBe(true);
        expect(records.some(({ record }) => record.payload._tag === "ToolCallPrepared")).toBe(
          false,
        );

        const canonical = records.find(({ record }) => record.payload._tag === "SubmissionSettled")
          ?.record.payload;

        expect(canonical).toMatchObject({ result: settlements[0]?.failure });
        const encodedFailure = JSON.stringify(canonical);

        expect(encodedFailure).toContain("SQLITE_BUSY_LEAF");
        expect(encodedFailure).toContain("Database is locked");
        expect(encodedFailure).not.toContain('"reason":"limit"');
        expect(encodedFailure).not.toContain("private-authority-payload");
        expect(
          yield* runtime
            .processThread(agent, receipt.threadId)
            .pipe(Effect.provide(desk.toolLayer)),
        ).toEqual([]);
        expect(logs).toHaveLength(1);
        expect(Schema.is(FailureDiagnostic.Failure)(settlements[0]?.failure)).toBe(true);
        expect(settlements[0]?.failure?.context?.threadId).toHaveLength(1_024);
        expect(Schema.decodeExit(FailureDiagnostic.Context)({ threadId })._tag).toBe("Failure");
      }),
  );

  it.effect("settles aborted when authorization observes cancellation before the watcher", () =>
    Effect.gen(function* () {
      const admissionRuntime = yield* DurableAgentRuntime;
      const ledger = yield* SubmissionLedger;
      const desk = yield* makeBookDesk(bookTools);

      const scripted = yield* makeScriptedModel(() =>
        toolTurn(toolCall("book-cancelled", "book", { ref: "r-cancelled" })),
      );

      const agent = Agent.withModel(bookDefinition, scripted.model);
      const thread = "thread-tool-authorization-cancelled";

      const receipt = yield* admissionRuntime.submit(
        agent,
        { question: "book it" },
        submitOptions(thread, "tool-authorization-cancelled-1"),
      );

      const command = AbortCommand.make({
        submissionId: receipt.submissionId,
        author: "operator",
        reason: "stop the booking",
      });

      const runtime = yield* DurableAgentRuntime.pipe(
        Effect.provide(
          Layer.fresh(DurableAgentRuntime.layerWithServices).pipe(
            Layer.provide(
              Layer.succeed(RunToolAuthorization, {
                // TestClock stays frozen: persist intent and deny before the watcher can tick.
                authorize: () =>
                  ledger
                    .requestAbort(command)
                    .pipe(
                      Effect.orDie,
                      Effect.as({ _tag: "denied", reason: "the current work was cancelled" }),
                    ),
              }),
            ),
          ),
        ),
      );

      const settlements = yield* runtime
        .processThread(agent, receipt.threadId)
        .pipe(Effect.provide(desk.toolLayer));

      expect(settlements).toHaveLength(1);
      expect(yield* desk.count("r-cancelled")).toBe(0);
      expect(settlements[0]).toMatchObject({
        submissionId: receipt.submissionId,
        receiptId: receipt.receiptId,
        outcome: "aborted",
      });
      const records = yield* readLog(thread);

      expect(logTags(records)).toEqual([
        "ThreadCreated",
        "UserInputRecorded",
        "RunStarted",
        "ModelResponseRecorded",
        "AbortRequested",
        "SubmissionSettled",
      ]);
      expect(
        records.find(({ record }) => record.payload._tag === "AbortRequested")?.record.payload,
      ).toMatchObject(command);
      expect(yield* runtime.awaitSettlement(receipt)).toEqual(settlements[0]);

      yield* runtime.recoverSubmission(receipt.submissionId);
      expect(
        yield* runtime.processThread(agent, receipt.threadId).pipe(Effect.provide(desk.toolLayer)),
      ).toEqual([]);
      expect(yield* runtime.awaitSettlement(receipt)).toEqual(settlements[0]);
      expect(yield* readLog(thread)).toEqual(records);
      expect(yield* desk.count("r-cancelled")).toBe(0);
      expect(scripted.prompts).toHaveLength(1);
    }),
  );

  it.effect(
    "resolveUnknown applies recovered results without execution and resumes exactly the open calls",
    () =>
      Effect.gen(function* () {
        yield* resetReconciler;
        const runtime = yield* DurableAgentRuntime;
        const desk = yield* makeBookDesk(bookTools);

        const scripted = yield* makeScriptedModel((call) =>
          call === 0
            ? toolTurn(
                toolCall("book-1", "book", { ref: "r-a" }),
                toolCall("book-2", "book", { ref: "r-b" }),
              )
            : finalParts('{"answer":"resolved"}'),
        );

        const agent = Agent.withModel(bookDefinition, scripted.model);
        const thread = "thread-resolve-two";

        const receipt = yield* runtime.submit(
          agent,
          { question: "book both" },
          submitOptions(thread, "resolve-two-1"),
        );

        yield* armFailpoint("tools:after-prepared-append");

        const killed = yield* Effect.exit(
          runtime.processThread(agent, decodeThreadId(thread)).pipe(Effect.provide(desk.toolLayer)),
        );

        expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
        yield* clearFailpoint;
        yield* runtime.runRecovery();
        expect(yield* lookupState(receipt.submissionId)).toBe("unknown");

        // book-1 completed externally (recovered supplier truth); book-2 provably never started.
        yield* runtime.resolveUnknown(
          UnknownResolutionCommand.make({
            submissionId: receipt.submissionId,
            toolCallId: decodeToolCallId("book-1"),
            author: "operator",
            reason: "the supplier store shows the booking",
            resolution: ResolutionCompletedWithResult.make({
              result: { confirmation: "external-r-a" },
              isFailure: false,
            }),
          }),
        );
        yield* runtime.resolveUnknown(
          UnknownResolutionCommand.make({
            submissionId: receipt.submissionId,
            toolCallId: decodeToolCallId("book-2"),
            author: "operator",
            reason: "the supplier store shows no attempt",
            resolution: ResolutionNeverHappened.make(),
          }),
        );
        expect(yield* lookupState(receipt.submissionId)).toBe("input-applied");

        const settlements = yield* runtime
          .processThread(agent, decodeThreadId(thread))
          .pipe(Effect.provide(desk.toolLayer));

        expect(settlements[0]?.outcome).toBe("completed");
        // Only the open call executed; the resolved result was injected without execution.
        expect(yield* desk.count("r-a")).toBe(0);
        expect(yield* desk.count("r-b")).toBe(1);

        const runId = runIdForSubmission(receipt.submissionId);
        const records = yield* readLog(thread);

        const settledA = records.find(
          (envelope) => envelope.record.recordId === `tool-settled:${runId}:1:book-1`,
        )?.record.payload;

        if (settledA?._tag === "ToolCallSettled") {
          expect(settledA.result).toEqual({ confirmation: "external-r-a" });
          expect(settledA.isFailure).toBe(false);
        }
        expect(
          records.filter((envelope) => envelope.record.payload._tag === "ToolCallSettled"),
        ).toHaveLength(2);

        // The audit tags stay prompt-transparent: the journal replays one contiguous tool
        // message for the Turn regardless of late per-call settlements, while retaining the
        // original user request and omitting the prior system instruction.
        const prompt = yield* promptFromCanonicalRecords(records);

        expect(prompt.content.map((message) => message.role)).toEqual([
          "user",
          "assistant",
          "tool",
          "assistant",
        ]);
        expect(JSON.stringify(prompt.content[0])).toContain("book both");
        const toolMessage = prompt.content.find((message) => message.role === "tool");

        expect(
          toolMessage?.content.filter((part) => part.type === "tool-result").map((part) => part.id),
        ).toEqual(["book-1", "book-2"]);
      }),
  );

  it.effect(
    "resolveUnknown is idempotent across the intent failpoint and conflicts on divergence",
    () =>
      Effect.gen(function* () {
        yield* resetReconciler;
        const runtime = yield* DurableAgentRuntime;
        const desk = yield* makeBookDesk(bookTools);

        const scripted = yield* makeScriptedModel((call) =>
          call === 0
            ? toolTurn(toolCall("book-1", "book", { ref: "r-idemres" }))
            : finalParts('{"answer":"resolved"}'),
        );

        const agent = Agent.withModel(bookDefinition, scripted.model);
        const thread = "thread-resolve-idempotent";

        const receipt = yield* runtime.submit(
          agent,
          { question: "book it" },
          submitOptions(thread, "resolve-idem-1"),
        );

        yield* armFailpoint("tools:after-prepared-append");
        yield* Effect.exit(
          runtime.processThread(agent, decodeThreadId(thread)).pipe(Effect.provide(desk.toolLayer)),
        );
        yield* clearFailpoint;
        yield* runtime.runRecovery();

        const command = UnknownResolutionCommand.make({
          submissionId: receipt.submissionId,
          toolCallId: decodeToolCallId("book-1"),
          author: "operator",
          reason: "the call never started",
          resolution: ResolutionNeverHappened.make(),
        });

        // Kill immediately after the durable intent write: the intent survives, the caller replays.
        yield* armFailpoint("resolve:after-intent");
        const killed = yield* Effect.exit(runtime.resolveUnknown(command));

        expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
        yield* clearFailpoint;

        const replayed = yield* runtime.resolveUnknown(command);

        expect(replayed.resolution._tag).toBe("NeverHappened");

        // A divergent re-resolution conflicts typed (DUR-017).
        const divergent = yield* Effect.exit(
          runtime.resolveUnknown(
            UnknownResolutionCommand.make({
              submissionId: receipt.submissionId,
              toolCallId: decodeToolCallId("book-1"),
              author: "operator",
              reason: "changed my mind",
              resolution: ResolutionCompletedWithResult.make({
                result: { confirmation: "no" },
                isFailure: false,
              }),
            }),
          ),
        );

        expect(failureTag(divergent)).toBe("UnknownResolutionConflict");

        const settlements = yield* runtime
          .processThread(agent, decodeThreadId(thread))
          .pipe(Effect.provide(desk.toolLayer));

        expect(settlements[0]?.outcome).toBe("completed");
        expect(yield* desk.count("r-idemres")).toBe(1);
      }),
  );

  it.effect(
    "records distinct Steps when legal Tool Call IDs and Step names contain separators",
    () =>
      Effect.gen(function* () {
        yield* resetReconciler;
        const runtime = yield* DurableAgentRuntime;
        const executed = yield* Ref.make<ReadonlyArray<string>>([]);

        const toolLayer = itineraryTools.toLayer({
          itinerary: ({ ref }) =>
            Effect.gen(function* () {
              const step = yield* DurableStep;

              const state = yield* step.do(
                ref,
                Schema.String,
                Ref.update(executed, (names) => [...names, ref]).pipe(Effect.as(ref)),
              );

              return { state };
            }),
        });

        const definition = Agent.make("durable-step-identities", {
          input: itineraryDefinition.input,
          output: itineraryDefinition.output,
          instructions: itineraryDefinition.instructions,
          toolkit: itineraryTools,
          policy: { ...policy, toolConcurrency: 1 },
        });

        const scripted = yield* makeScriptedModel((call) =>
          call === 0
            ? toolTurn(
                toolCall("a:b", "itinerary", { ref: "c" }),
                toolCall("a", "itinerary", { ref: "b:c" }),
              )
            : finalParts('{"answer":"reserved"}'),
        );

        const agent = Agent.withModel(definition, scripted.model);
        const thread = "thread-durable-step-identities";

        yield* runtime.submit(
          agent,
          { question: "reserve both" },
          submitOptions(thread, "step-identities-1"),
        );

        const settlements = yield* runtime
          .processThread(agent, decodeThreadId(thread))
          .pipe(Effect.provide(toolLayer));

        expect(settlements[0]?.outcome).toBe("completed");
        expect(yield* Ref.get(executed)).toEqual(["c", "b:c"]);

        const records = yield* readLog(thread);

        const steps = records.filter(
          (envelope) => envelope.record.payload._tag === "ToolStepSettled",
        );

        expect(steps.map((envelope) => envelope.record.payload)).toMatchObject([
          { toolCallId: "a:b", stepName: "c", output: "c" },
          { toolCallId: "a", stepName: "b:c", output: "b:c" },
        ]);
        expect(new Set(steps.map((envelope) => envelope.record.recordId)).size).toBe(2);
        expect(new Set(steps.map((envelope) => envelope.batchId)).size).toBe(2);
        expect(
          records.flatMap((envelope) =>
            envelope.record.payload._tag === "ToolCallSettled"
              ? [envelope.record.payload.result]
              : [],
          ),
        ).toEqual([{ state: "c" }, { state: "b:c" }]);
      }),
  );
});
