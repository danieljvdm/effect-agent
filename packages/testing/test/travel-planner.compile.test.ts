import * as Agent from "@effect-agent/core/Agent";
import {
  type AgentApprovalDenied,
  type AgentApprovalPending,
  type AgentInputError,
  type AgentOutputError,
  type AgentPolicyError,
  type ContextBudgetError,
  type ContextOverflowError,
  type ModelProtocolError,
  type AgentToolAuthorizationDenied,
} from "@effect-agent/core/AgentError";
import { AgentPolicy } from "@effect-agent/core/AgentPolicy";
import { IdGenerator } from "@effect-agent/core/IdGenerator";
import { type MemoryRecallError } from "@effect-agent/core/MemoryReference";
import { type RunEvent } from "@effect-agent/core/RunEvent";
import * as AgentRuntime from "@effect-agent/engine/AgentRuntime";
import {
  type AgentChildPending,
  type AgentRuntimeFailure,
  type AgentRuntimeRequirements,
  type AgentResult,
  type DetachedRun,
} from "@effect-agent/engine/AgentRuntime";
import { type CompactionError } from "@effect-agent/engine/ContextCompactor";
import { RunContextPreparationPassthrough, type RunOptions } from "@effect-agent/engine/RunOptions";
import { ThreadHistory, type ThreadHistoryError } from "@effect-agent/engine/ThreadHistory";
import { ScriptedModel } from "@effect-agent/testing/ScriptedModel";
import {
  type ActivityCatalog,
  type ActivityUnavailable,
  type FlightCatalog,
  type FlightUnavailable,
  type GuidanceFailure,
  type LodgingCatalog,
  type LodgingUnavailable,
  type TravelGuidance,
  type TravelPlannerToolkit,
} from "@effect-agent/testing/TravelPlanner";
import { phase1Trip, TravelPlanner } from "@effect-agent/testing/TravelPlanner";
import { type DurableWorkerRequirements } from "@effect-agent/thread/DurableAgentRuntime";
import { Context, Effect, Layer, Schema, SchemaGetter, Scope, type Stream } from "effect";
import { type AiError, Model, Tool, Toolkit } from "effect/unstable/ai";
import { describe, expect, it } from "vite-plus/test";

type Equal<Left, Right> =
  (<Value>() => Value extends Left ? 1 : 2) extends <Value>() => Value extends Right ? 1 : 2
    ? (<Value>() => Value extends Right ? 1 : 2) extends <Value>() => Value extends Left ? 1 : 2
      ? true
      : false
    : false;
type Assert<Value extends true> = Value;

const model = Model.make("scripted", "travel-planner-type-proof", ScriptedModel.layer([]));
const agent = Agent.withModel(TravelPlanner, model);
const program = AgentRuntime.run(TravelPlanner, phase1Trip).pipe(Effect.provide(model));
const events = AgentRuntime.stream(agent, phase1Trip);
const started = AgentRuntime.start(agent, phase1Trip);

class CompletionResultDecoder extends Context.Service<
  CompletionResultDecoder,
  { readonly validate: (value: string) => string }
>()("@effect-agent/testing/CompletionResultDecoder") {}

const ContextualCompletionResult = Schema.String.pipe(
  Schema.decode({
    decode: SchemaGetter.transformOrFail((value) =>
      Effect.map(CompletionResultDecoder, ({ validate }) => validate(value)),
    ),
    encode: SchemaGetter.transform((value) => value),
  }),
);

const Complete = Tool.make("complete", {
  parameters: Schema.Struct({ answer: Schema.String }),
  success: ContextualCompletionResult,
});

const completionToolkit = Toolkit.make(Complete);

const completionDefinition = Agent.make("completion-requirements-proof", {
  input: Schema.Struct({ question: Schema.String }),
  output: Schema.Struct({ answer: Schema.String }),
  instructions: "Complete through the Tool.",
  toolkit: completionToolkit,
  policy: AgentPolicy.make({
    maxTurns: 1,
    maxToolCalls: 1,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  }),
  completion: {
    tool: "complete",
    project: ({ result }) => ({ answer: result }),
  },
});

const completionAgent = Agent.withModel(completionDefinition, model);

type ExpectedRequirements =
  | FlightCatalog
  | LodgingCatalog
  | ActivityCatalog
  | TravelGuidance
  | Tool.HandlersFor<Toolkit.Tools<typeof TravelPlannerToolkit>>
  | IdGenerator
  | ThreadHistory;
type ExpectedFailure =
  | FlightUnavailable
  | LodgingUnavailable
  | ActivityUnavailable
  | GuidanceFailure
  | AiError.AiError
  | AgentInputError
  | AgentOutputError
  | AgentPolicyError
  | ContextBudgetError
  | ContextOverflowError
  | CompactionError
  | MemoryRecallError
  | ThreadHistoryError
  | ModelProtocolError
  | AgentApprovalDenied
  | AgentToolAuthorizationDenied
  | AgentApprovalPending
  | AgentChildPending;

type RequirementsProof = Assert<Equal<Effect.Services<typeof program>, ExpectedRequirements>>;
type FailureProof = Assert<Equal<Effect.Error<typeof program>, ExpectedFailure>>;
type PublicRequirementsProof = Assert<
  Equal<AgentRuntimeRequirements<typeof agent>, ExpectedRequirements>
>;
type PublicFailureProof = Assert<Equal<AgentRuntimeFailure<typeof agent>, ExpectedFailure>>;
type CompletionRuntimeRequirementsProof = Assert<
  CompletionResultDecoder extends AgentRuntimeRequirements<typeof completionAgent> ? true : false
>;
type CompletionDurableRequirementsProof = Assert<
  CompletionResultDecoder extends DurableWorkerRequirements<typeof completionAgent> ? true : false
>;

describe("TEST-009 P1 Travel Planner public-contract inference", () => {
  it("removes only runtime-owned Scope and preserves caller-contributed services", () => {
    class CallerService extends Context.Service<CallerService, { readonly text: string }>()(
      "@effect-agent/testing/ScopedCallerService",
    ) {}
    class HookFailure extends Schema.TaggedError<HookFailure>()("ScopedHookFailure", {}) {}

    const scopedText = Effect.gen(function* () {
      yield* Scope.Scope;

      return (yield* CallerService).text;
    });

    const config = {
      input: Schema.String,
      output: Schema.String,
      instructions: "Answer.",
      toolkit: Toolkit.empty,
      policy: AgentPolicy.make({
        maxTurns: 1,
        maxToolCalls: 1,
        maxDuration: "30 seconds",
        toolConcurrency: 1,
      }),
    };

    const plain = Agent.withModel(Agent.make("scope-free", config), model);

    const selfContained = AgentRuntime.run(plain, "question").pipe(
      Effect.provide(
        Layer.mergeAll(
          IdGenerator.layer,
          ThreadHistory.layerTransient,
          RunContextPreparationPassthrough,
        ),
      ),
    );

    const instructionAgent = Agent.withModel(
      Agent.make("scoped-instructions", {
        ...config,
        instructions: (_input: string) => scopedText,
      }),
      model,
    );

    const instructionRun = AgentRuntime.run(instructionAgent, "question");

    const projectionAgent = Agent.withModel(
      Agent.make("scoped-input-projection", { ...config, inputPrompt: () => scopedText }),
      model,
    );

    const projectionRun = AgentRuntime.run(projectionAgent, "question");

    const options: RunOptions<HookFailure, CallerService | Scope.Scope> = {
      onHistory: () => scopedText.pipe(Effect.andThen(Effect.fail(new HookFailure()))),
    };

    const hookRun = AgentRuntime.run(plain, "question", options);

    // run re-decodes this output after the inner stream has closed.
    const scopedOutput = Schema.String.pipe(
      Schema.decode({
        decode: SchemaGetter.transformOrFail((value) => scopedText.pipe(Effect.as(value))),
        encode: SchemaGetter.transform((value) => value),
      }),
    );

    const outputAgent = Agent.withModel(
      Agent.make("scoped-terminal-decoder", { ...config, output: scopedOutput }),
      model,
    );

    const outputRun = AgentRuntime.run(outputAgent, "question");

    type ScopedRequirements = IdGenerator | ThreadHistory | CallerService | Scope.Scope;
    type BaseFailure = Exclude<
      ExpectedFailure,
      FlightUnavailable | LodgingUnavailable | ActivityUnavailable | GuidanceFailure
    >;

    const proofs: {
      selfContained: Assert<Equal<Effect.Services<typeof selfContained>, never>>;
      instructionsNoExtraServices: Assert<
        Equal<Exclude<Effect.Services<typeof instructionRun>, ScopedRequirements>, never>
      >;
      instructionsNoMissingServices: Assert<
        Equal<Exclude<ScopedRequirements, Effect.Services<typeof instructionRun>>, never>
      >;
      projection: Assert<Equal<Effect.Services<typeof projectionRun>, ScopedRequirements>>;
      hook: Assert<Equal<Effect.Services<typeof hookRun>, ScopedRequirements>>;
      output: Assert<Equal<Effect.Services<typeof outputRun>, ScopedRequirements>>;
      hookFailure: Assert<Equal<Effect.Error<typeof hookRun>, BaseFailure | HookFailure>>;
      outputFailure: Assert<Equal<Effect.Error<typeof outputRun>, BaseFailure>>;
      outputSuccess: Assert<Equal<Effect.Success<typeof outputRun>, AgentResult<string>>>;
    } = {
      selfContained: true,
      instructionsNoExtraServices: true,
      instructionsNoMissingServices: true,
      projection: true,
      hook: true,
      output: true,
      hookFailure: true,
      outputFailure: true,
      outputSuccess: true,
    };

    expect(Object.values(proofs).every((proof) => proof)).toBe(true);
  });

  it("preserves Tool and instruction failures and requirements in Run E/R", () => {
    const requirementsProof: RequirementsProof = true;
    const failureProof: FailureProof = true;
    const publicRequirementsProof: PublicRequirementsProof = true;
    const publicFailureProof: PublicFailureProof = true;
    const completionRuntimeRequirementsProof: CompletionRuntimeRequirementsProof = true;
    const completionDurableRequirementsProof: CompletionDurableRequirementsProof = true;

    const entrypointProofs: {
      runSuccess: Assert<
        Equal<Effect.Success<typeof program>, AgentResult<Agent.Output<typeof agent>>>
      >;
      streamSuccess: Assert<Equal<Stream.Success<typeof events>, RunEvent>>;
      streamRequirements: Assert<Equal<Stream.Services<typeof events>, ExpectedRequirements>>;
      streamFailure: Assert<Equal<Stream.Error<typeof events>, ExpectedFailure>>;
      startRequirements: Assert<
        Equal<Effect.Services<typeof started>, ExpectedRequirements | Scope.Scope>
      >;
      startFailure: Assert<Equal<Effect.Error<typeof started>, never>>;
      startSuccess: Assert<
        Equal<
          Effect.Success<typeof started>,
          DetachedRun<Agent.Output<typeof agent>, ExpectedFailure>
        >
      >;
      awaitSuccess: Assert<
        Equal<
          Effect.Success<Effect.Success<typeof started>["await"]>,
          AgentResult<Agent.Output<typeof agent>>
        >
      >;
      awaitFailure: Assert<
        Equal<Effect.Error<Effect.Success<typeof started>["await"]>, ExpectedFailure>
      >;
      awaitRequirements: Assert<
        Equal<Effect.Services<Effect.Success<typeof started>["await"]>, never>
      >;
    } = {
      runSuccess: true,
      streamSuccess: true,
      streamRequirements: true,
      streamFailure: true,
      startRequirements: true,
      startFailure: true,
      startSuccess: true,
      awaitSuccess: true,
      awaitFailure: true,
      awaitRequirements: true,
    };

    expect(Object.values(entrypointProofs).every((proof) => proof)).toBe(true);

    expect({
      requirementsProof,
      failureProof,
      publicRequirementsProof,
      publicFailureProof,
      completionRuntimeRequirementsProof,
      completionDurableRequirementsProof,
    }).toEqual({
      requirementsProof: true,
      failureProof: true,
      publicRequirementsProof: true,
      publicFailureProof: true,
      completionRuntimeRequirementsProof: true,
      completionDurableRequirementsProof: true,
    });
  });
});
