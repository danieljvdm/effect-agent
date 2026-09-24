import { ScriptedModel } from "@effect-agent/testing/scripted-model";
import { Context, Effect, Layer, Schema, SchemaGetter, Scope } from "effect";
import * as Agent from "effect-agent/agent";
import { AgentPolicy } from "effect-agent/agent-policy";
import * as AgentRuntime from "effect-agent/agent-runtime";
import { type DurableWorkerRequirements } from "effect-agent/durable-agent-runtime";
import { RunContextPreparationPassthrough, type RunOptions } from "effect-agent/run-options";
import { ThreadHistory } from "effect-agent/thread-history";
import { Model, Tool, Toolkit } from "effect/unstable/ai";

type Equal<Left, Right> =
  (<Value>() => Value extends Left ? 1 : 2) extends <Value>() => Value extends Right ? 1 : 2
    ? (<Value>() => Value extends Right ? 1 : 2) extends <Value>() => Value extends Left ? 1 : 2
      ? true
      : false
    : false;
type Assert<Value extends true> = Value;

const model = Model.make("scripted", "travel-planner-type-proof", ScriptedModel.layer([]));

class CompletionResultDecoder extends Context.Service<
  CompletionResultDecoder,
  { readonly validate: (value: string) => string }
>()("@effect-agent/testing/CompletionResultDecoder") {}

const ContextualCompletionResult = Schema.String.pipe(
  Schema.decode({
    decode: SchemaGetter.transformEffect((value) =>
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

type CompletionDurableRequirementsProof = Assert<
  CompletionResultDecoder extends DurableWorkerRequirements<typeof completionAgent> ? true : false
>;

const verifyCallerScopeRequirements = () => {
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
    Effect.provide(Layer.mergeAll(ThreadHistory.layer, RunContextPreparationPassthrough)),
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
      decode: SchemaGetter.transformEffect((value) => scopedText.pipe(Effect.as(value))),
      encode: SchemaGetter.transform((value) => value),
    }),
  );

  const outputAgent = Agent.withModel(
    Agent.make("scoped-terminal-decoder", { ...config, output: scopedOutput }),
    model,
  );

  const outputRun = AgentRuntime.run(outputAgent, "question");

  type ScopedRequirements = ThreadHistory | CallerService | Scope.Scope;

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
  } = {
    selfContained: true,
    instructionsNoExtraServices: true,
    instructionsNoMissingServices: true,
    projection: true,
    hook: true,
    output: true,
  };

  void proofs;
};

void verifyCallerScopeRequirements;

const verifyDurableCompletionRequirements = () => {
  const completionDurableRequirementsProof: CompletionDurableRequirementsProof = true;

  void completionDurableRequirementsProof;
};

void verifyDurableCompletionRequirements;
