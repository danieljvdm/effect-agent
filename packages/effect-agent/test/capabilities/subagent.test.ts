import { expect, layer } from "@effect/vitest";
import { Context, Deferred, Effect, Fiber, Layer, Ref, Schema, Stream } from "effect";
import * as Agent from "effect-agent/agent";
import { AgentPolicy } from "effect-agent/agent-policy";
import * as AgentRuntime from "effect-agent/agent-runtime";
import {
  type AgentSpawner,
  type SubagentDurability,
  type SubagentDurabilityError,
  type ToolCallWaiting,
  type RuntimeBinding,
} from "effect-agent/agent-runtime";
import { IdGenerator } from "effect-agent/id-generator";
import { ThreadId, RunId, TurnId } from "effect-agent/identifiers";
import { type RunEventSink } from "effect-agent/run-event-sink";
import { RunContextPreparationPassthrough } from "effect-agent/run-options";
import * as Subagent from "effect-agent/subagent";
import {
  type SubagentExecutionFailure,
  type SubagentPrestartDenied,
  type SubagentProjectionFailure,
  type SubagentChildRunFailure,
  SubagentPolicy,
} from "effect-agent/subagent";
import {
  type SubagentBudgetExhausted,
  SubagentReservations,
  SubagentReservationsMemoryLive,
  type SubagentReservationView,
} from "effect-agent/subagent-reservations";
import { ThreadHistory } from "effect-agent/thread-history";
import {
  type AiError,
  LanguageModel,
  Model,
  type Response,
  Tool,
  Toolkit,
} from "effect/unstable/ai";

type Equal<Left, Right> =
  (<Value>() => Value extends Left ? 1 : 2) extends <Value>() => Value extends Right ? 1 : 2
    ? (<Value>() => Value extends Right ? 1 : 2) extends <Value>() => Value extends Left ? 1 : 2
      ? true
      : false
    : false;
type Assert<Value extends true> = Value;

const usage = {
  inputTokens: {},
  outputTokens: {},
};

const decodeThreadId = Schema.decodeSync(ThreadId);
const decodeRunId = Schema.decodeSync(RunId);
const decodeTurnId = Schema.decodeSync(TurnId);

/** Deterministic, distinct identities so preallocated child IDs are observable. */
const identifiers = Layer.effect(IdGenerator)(
  Effect.gen(function* () {
    const counter = yield* Ref.make(0);

    const next = <A>(decode: (value: string) => A, prefix: string) =>
      Ref.updateAndGet(counter, (value) => value + 1).pipe(
        Effect.map((value) => decode(`${prefix}-${value}`)),
      );

    return {
      nextThreadId: next(decodeThreadId, "thread"),
      nextRunId: next(decodeRunId, "run"),
      nextTurnId: next(decodeTurnId, "turn"),
    };
  }),
);

const TestServices = Layer.mergeAll(
  identifiers,
  SubagentReservationsMemoryLive,
  ThreadHistory.layer,
  RunContextPreparationPassthrough,
);

const finalParts = (text: string): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: text },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage },
];

/** One-turn scripted child model that answers immediately and can capture its prompt. */
const answeringModel = (name: string, answerText: string, promptRef?: Ref.Ref<unknown>) =>
  Model.make(
    "scripted",
    name,
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: (options) =>
          promptRef === undefined
            ? Stream.fromIterable(finalParts(answerText))
            : Stream.unwrap(
                Ref.set(promptRef, options.prompt).pipe(
                  Effect.as(Stream.fromIterable(finalParts(answerText))),
                ),
              ),
      }),
    ),
  );

/** Scripted parent model: turn one declares the given Tool Calls, turn two answers. */
const delegatingModel = (
  name: string,
  toolName: string,
  calls: ReadonlyArray<{ readonly id: string; readonly params: unknown }>,
  answerText: string,
) =>
  Model.make(
    "scripted",
    name,
    Layer.effect(
      LanguageModel.LanguageModel,
      Effect.gen(function* () {
        const turn = yield* Ref.make(0);

        return yield* LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: () =>
            Stream.unwrap(
              Ref.getAndUpdate(turn, (value) => value + 1).pipe(
                Effect.map((value) =>
                  Stream.fromIterable<Response.StreamPartEncoded>(
                    value === 0
                      ? [
                          ...calls.map((call): Response.StreamPartEncoded => ({
                            type: "tool-call",
                            id: call.id,
                            name: toolName,
                            params: call.params,
                            providerExecuted: false,
                          })),
                          { type: "finish", reason: "tool-calls", usage },
                        ]
                      : finalParts(answerText),
                  ),
                ),
              ),
            ),
        });
      }),
    ),
  );

const ChildInput = Schema.Struct({ question: Schema.String });
const ChildOutput = Schema.Struct({ answer: Schema.String });

const childPolicy = AgentPolicy.make({
  maxTurns: 2,
  maxToolCalls: 1,
  maxDuration: "30 seconds",
  toolConcurrency: 1,
});

const childDefinition = Agent.make("research-child", {
  input: ChildInput,
  output: ChildOutput,
  instructions: "Answer as JSON.",
  toolkit: Toolkit.empty,
  policy: childPolicy,
});

class ResearchDelegationFailed extends Schema.TaggedError<ResearchDelegationFailed>()(
  "ResearchDelegationFailed",
  {
    childErrorTag: Schema.String,
  },
) {}

const ResearchParams = Schema.Struct({ topic: Schema.String });
const ResearchFindings = Schema.Struct({ summary: Schema.String });

const researchPolicy = SubagentPolicy.make({
  maxChildren: 2,
  maxConcurrency: 2,
  maxTurns: 4,
  maxToolCalls: 4,
  maxDuration: "10 seconds",
});

const researchDelegation = Subagent.define("delegate_research", {
  description: "Research one bounded question and return findings.",
  target: childDefinition,
  parameters: ResearchParams,
  success: ResearchFindings,
  failure: ResearchDelegationFailed,
  prepareInput: ({ topic }) => Effect.succeed({ question: `research:${topic}` }),
  projectResult: (output, _context, parameters) =>
    Effect.succeed({ summary: `finding:${parameters.topic}:${output.answer}` }),
  policy: researchPolicy,
});

const parentPolicy = AgentPolicy.make({
  maxTurns: 2,
  maxToolCalls: 2,
  maxDuration: "30 seconds",
  toolConcurrency: 2,
});

const coordinatorDefinition = Agent.make("coordinator", {
  input: Schema.Struct({ mission: Schema.String }),
  output: Schema.Struct({ report: Schema.String }),
  instructions: "Delegate, then answer as JSON.",
  toolkit: Toolkit.make(researchDelegation.tool),
  policy: parentPolicy,
});

const mapChildFailure = (failure: { readonly _tag: string }) =>
  ResearchDelegationFailed.make({ childErrorTag: failure._tag });

const researchLayer = <Provider, ModelProvides, ModelRequires>(
  childBinding: RuntimeBinding<
    typeof ChildInput,
    typeof ChildOutput,
    string,
    {},
    Provider,
    ModelProvides,
    ModelRequires
  >,
) => Subagent.layer(researchDelegation, childBinding, { mapChildFailure });

const dimensionKeys = [
  "turns",
  "toolCalls",
  "durationMillis",
  "inputTokens",
  "outputTokens",
  "costMicrousd",
  "resultBytes",
] as const;

/** Spec §7: at `released`, allocated = covered + released and observed = covered + overrun. */
const expectSettledOnce = (view: SubagentReservationView | undefined): void => {
  expect(view).toBeDefined();
  if (view === undefined) {
    return;
  }
  expect(view.status).toBe("released");
  for (const key of dimensionKeys) {
    expect(view.allocated[key]).toBe(view.coveredConsumed[key] + view.released[key]);
    expect(view.observedConsumed[key] ?? 0).toBe(view.coveredConsumed[key] + view.overrun[key]);
  }
};

layer(TestServices)("Subagent.layer S1 attached delegation", (it) => {
  it.effect("settles the reservation when parent interruption reaches the child", () =>
    Effect.gen(function* () {
      const modelStarted = yield* Deferred.make<void>();
      const modelReleased = yield* Deferred.make<void>();

      const blockingModel = Model.make(
        "scripted",
        "blocking-child",
        Layer.effect(
          LanguageModel.LanguageModel,
          Effect.acquireRelease(
            LanguageModel.make({
              generateText: () => Effect.succeed([]),
              streamText: () =>
                Stream.unwrap(
                  Deferred.succeed(modelStarted, undefined).pipe(Effect.andThen(Effect.never)),
                ),
            }),
            () => Deferred.succeed(modelReleased, undefined),
          ),
        ),
      );

      const childBinding = Agent.withModel(childDefinition, blockingModel);

      const parent = Agent.withModel(
        coordinatorDefinition,
        delegatingModel(
          "parent-interrupted",
          "delegate_research",
          [{ id: "call-1", params: { topic: "blocked" } }],
          '{"report":"unreached"}',
        ),
      );

      const runId = decodeRunId("parent-run-interrupted");

      const fiber = yield* AgentRuntime.run(parent, { mission: "m" }, { runId }).pipe(
        Effect.provide(researchLayer(childBinding)),
        Effect.scoped,
        Effect.exit,
        Effect.forkChild,
      );

      yield* Deferred.await(modelStarted);
      yield* Fiber.interrupt(fiber);
      // Interruption reached the child Run and its model Layer finalizer.
      yield* Deferred.await(modelReleased);

      const reservations = yield* SubagentReservations;
      const snapshot = yield* reservations.parentSnapshot(runId);
      const view = snapshot.reservations[0];

      expectSettledOnce(view);
      // No wall-clock time elapsed before interruption, so refund the full duration.
      expect(view?.observedConsumed.durationMillis).toBe(0);
      expect(view?.released.durationMillis).toBe(10_000);
    }),
  );
});

// ---------------------------------------------------------------------------
// Compile-time proofs
// ---------------------------------------------------------------------------

class ChildModelConfig extends Context.Service<ChildModelConfig, { readonly modelName: string }>()(
  "@effect-agent/capabilities/test/ChildModelConfig",
) {}

class ChildCatalog extends Context.Service<
  ChildCatalog,
  { readonly search: Effect.Effect<ReadonlyArray<string>> }
>()("@effect-agent/capabilities/test/ChildCatalog") {}

class PrepareDirectory extends Context.Service<PrepareDirectory, { readonly prefix: string }>()(
  "@effect-agent/capabilities/test/PrepareDirectory",
) {}

class ProjectStamper extends Context.Service<ProjectStamper, { readonly stamp: string }>()(
  "@effect-agent/capabilities/test/ProjectStamper",
) {}

class ChildInputRenderer extends Context.Service<
  ChildInputRenderer,
  { readonly render: (question: string) => Effect.Effect<string, ChildInputFailure> }
>()("@effect-agent/capabilities/test/ChildInputRenderer") {}

class ChildInputFailure extends Schema.TaggedError<ChildInputFailure>()("ChildInputFailure", {}) {}

class SearchFailure extends Schema.TaggedError<SearchFailure>()("SearchFailure", {
  message: Schema.String,
}) {}

const SearchDocs = Tool.make("search_docs", {
  parameters: Schema.Struct({ query: Schema.String }),
  success: Schema.Array(Schema.String),
  failure: SearchFailure,
  dependencies: [ChildCatalog],
});

const typedChildTools = Toolkit.make(SearchDocs);

const typedChildDefinition = Agent.make("typed-child", {
  input: ChildInput,
  output: ChildOutput,
  instructions: "Search, then answer as JSON.",
  inputPrompt: ({ question }) =>
    Effect.flatMap(ChildInputRenderer, ({ render }) => render(question)),
  toolkit: typedChildTools,
  policy: childPolicy,
});

const typedModel = Model.make(
  "scripted",
  "typed-model",
  Layer.effect(
    LanguageModel.LanguageModel,
    Effect.gen(function* () {
      yield* ChildModelConfig;

      return yield* LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: () => Stream.empty,
      });
    }),
  ),
);

const typedDelegation = Subagent.make("typed", {
  description: "Typed delegation for compile proofs.",
  target: typedChildDefinition,
  parameters: ResearchParams,
  success: ResearchFindings,
  failure: ResearchDelegationFailed,
  prepareInput: ({ topic }) =>
    Effect.gen(function* () {
      const directory = yield* PrepareDirectory;

      return { question: `${directory.prefix}:${topic}` };
    }),
  projectResult: (output) =>
    Effect.gen(function* () {
      const stamper = yield* ProjectStamper;

      return { summary: `${stamper.stamp}:${output.answer}` };
    }),
  policy: researchPolicy,
});

const typedLayer = Subagent.layer(typedDelegation, typedModel, {
  mapChildFailure: (failure) => ResearchDelegationFailed.make({ childErrorTag: failure._tag }),
});

const typedParentDefinition = Agent.make("typed-parent", {
  input: Schema.Struct({ mission: Schema.String }),
  output: Schema.Struct({ report: Schema.String }),
  instructions: "Delegate, then answer as JSON.",
  toolkit: Toolkit.make(typedDelegation.tool),
  policy: parentPolicy,
});

const typedParent = Agent.withModel(
  typedParentDefinition,
  answeringModel("typed-parent-model", '{"report":"typed"}'),
);

const typedProgram = AgentRuntime.run(typedParent, { mission: "m" }).pipe(
  Effect.provide(typedLayer),
);

type LayerContext<L> = L extends Layer.Layer<infer _ROut, infer _E, infer RIn> ? RIn : never;
type TypedLayerRequirements = LayerContext<typeof typedLayer>;
type TypedProgramServices = Effect.Services<typeof typedProgram>;
type TypedHandlerError = Tool.HandlerError<typeof typedDelegation.tool>;
type TypedHandlerServices = Tool.HandlerServices<typeof typedDelegation.tool>;
// Per-call handler failures are exactly the declared Tool failure union plus
// Effect AI's own error (spec §4.2); nothing else can leak through `E`. The
// S2 members travel typed: the engine-owned waiting signal, the coordinator
// seam failure, and the bounded durable execution failure (D5).
type HandlerErrorProof = Assert<
  Equal<
    TypedHandlerError,
    | ResearchDelegationFailed
    | SubagentPrestartDenied
    | SubagentBudgetExhausted
    | SubagentProjectionFailure
    | SubagentExecutionFailure
    | ToolCallWaiting
    | SubagentDurabilityError
    | AiError.AiError
  >
>;
type HandlerRequirementsProof = Assert<
  Equal<TypedHandlerServices, AgentSpawner | RunEventSink | SubagentDurability>
>;
// Layer construction carries the child Model, child Tool handler, projection,
// and reservation requirements visibly (spec §4 compile-proof list).
type LayerModelProof = Assert<
  Equal<Extract<TypedLayerRequirements, ChildModelConfig>, ChildModelConfig>
>;
type LayerInputPromptProof = Assert<
  Equal<Extract<TypedLayerRequirements, ChildInputRenderer>, ChildInputRenderer>
>;
type LayerChildToolServiceProof = Assert<
  Equal<Extract<TypedLayerRequirements, ChildCatalog>, ChildCatalog>
>;
type LayerChildHandlersProof = Assert<
  Equal<
    Extract<TypedLayerRequirements, Tool.HandlersFor<Toolkit.Tools<typeof typedChildTools>>>,
    Tool.HandlersFor<Toolkit.Tools<typeof typedChildTools>>
  >
>;
type LayerPrepareProof = Assert<
  Equal<Extract<TypedLayerRequirements, PrepareDirectory>, PrepareDirectory>
>;
type LayerProjectProof = Assert<
  Equal<Extract<TypedLayerRequirements, ProjectStamper>, ProjectStamper>
>;
// The provided parent program still needs the child requirements but never
// the engine-provided services.
type ProgramModelProof = Assert<
  Equal<Extract<TypedProgramServices, ChildModelConfig>, ChildModelConfig>
>;

const unboundDefinitionRejected = () =>
  Subagent.layer(
    typedDelegation,
    // @ts-expect-error an unbound child Definition cannot be executed
    typedChildDefinition,
    {
      mapChildFailure: (failure) => ResearchDelegationFailed.make({ childErrorTag: failure._tag }),
    },
  );

// The expected child-failure domain covers the child's Tool failures and the
// interpreter's own failures; a mapping over any narrower domain is not
// assignable, so a non-total mapping is a compile error (SUB-028).
type TypedChildFailure = SubagentChildRunFailure<
  typeof ChildInput,
  typeof ChildOutput,
  string,
  Toolkit.Tools<typeof typedChildTools>,
  "scripted",
  never,
  never,
  never,
  never,
  typeof typedChildDefinition.inputPrompt
>;
type MappingDomainInputPromptFailureProof = Assert<
  Equal<Extract<TypedChildFailure, ChildInputFailure>, ChildInputFailure>
>;
type MappingDomainToolFailureProof = Assert<
  Equal<Extract<TypedChildFailure, SearchFailure>, SearchFailure>
>;
type PartialMappingRejectedProof = Assert<
  Equal<
    ((failure: SearchFailure) => ResearchDelegationFailed) extends (
      failure: TypedChildFailure,
    ) => ResearchDelegationFailed
      ? true
      : false,
    false
  >
>;

// ---------------------------------------------------------------------------
// SUB-033 containment: `failureMode: "return"` turns expected delegation
// failures into model-visible result data; only the engine signals
// (`ToolCallWaiting`, `SubagentDurabilityError`) stay in the error channel.
// ---------------------------------------------------------------------------

const containedDelegation = Subagent.make("delegate_contained", {
  description: "Research one bounded question; failures are contained result data.",
  target: childDefinition,
  parameters: ResearchParams,
  success: ResearchFindings,
  failure: ResearchDelegationFailed,
  failureMode: "return",
  prepareInput: ({ topic }) => Effect.succeed({ question: `research:${topic}` }),
  projectResult: (output) => Effect.succeed({ summary: `finding:${output.answer}` }),
  policy: researchPolicy,
});

export const verifyKeepsPerCallAndConstructionRequirementsDistinct = () => {
  const handlerErrorProof: HandlerErrorProof = true;
  const handlerRequirementsProof: HandlerRequirementsProof = true;
  const layerModelProof: LayerModelProof = true;
  const layerInputPromptProof: LayerInputPromptProof = true;
  const mappingDomainInputPromptFailureProof: MappingDomainInputPromptFailureProof = true;
  const layerChildToolServiceProof: LayerChildToolServiceProof = true;
  const layerChildHandlersProof: LayerChildHandlersProof = true;
  const layerPrepareProof: LayerPrepareProof = true;
  const layerProjectProof: LayerProjectProof = true;
  const programModelProof: ProgramModelProof = true;
  const mappingDomainToolFailureProof: MappingDomainToolFailureProof = true;
  const partialMappingRejectedProof: PartialMappingRejectedProof = true;

  void [
    handlerErrorProof,
    handlerRequirementsProof,
    layerModelProof,
    layerInputPromptProof,
    mappingDomainInputPromptFailureProof,
    layerChildToolServiceProof,
    layerChildHandlersProof,
    layerPrepareProof,
    layerProjectProof,
    programModelProof,
    mappingDomainToolFailureProof,
    partialMappingRejectedProof,
  ];
  void unboundDefinitionRejected;
};

export const verifySUB033KeepsTheContainedToolChannelsTypedModeTypeProofs = () => {
  type ContainedTool = typeof containedDelegation.tool;
  type ErrorTool = typeof researchDelegation.tool;

  // Under "return" only the engine signals (plus Effect AI's own permitted
  // error) remain raisable.
  type ContainedHandlerErrorProof = Assert<
    Equal<
      Tool.HandlerError<ContainedTool>,
      AiError.AiError | ToolCallWaiting | SubagentDurabilityError
    >
  >;
  // The declared failure family is model-visible RESULT data.
  type ContainedSuccessProof = Assert<
    Equal<
      Tool.Success<ContainedTool>,
      | { readonly summary: string }
      | ResearchDelegationFailed
      | SubagentPrestartDenied
      | SubagentBudgetExhausted
      | SubagentProjectionFailure
      | SubagentExecutionFailure
    >
  >;
  // The default mode keeps today's full error-channel union.
  type ErrorModeUnchangedProof = Assert<
    Equal<
      Tool.HandlerError<ErrorTool>,
      | AiError.AiError
      | ResearchDelegationFailed
      | SubagentPrestartDenied
      | SubagentBudgetExhausted
      | SubagentProjectionFailure
      | SubagentExecutionFailure
      | ToolCallWaiting
      | SubagentDurabilityError
    >
  >;

  const containedHandlerErrorProof: ContainedHandlerErrorProof = true;
  const containedSuccessProof: ContainedSuccessProof = true;
  const errorModeUnchangedProof: ErrorModeUnchangedProof = true;

  void [containedHandlerErrorProof, containedSuccessProof, errorModeUnchangedProof];
};
