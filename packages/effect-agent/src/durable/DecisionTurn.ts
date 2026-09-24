import {
  Context as EffectContext,
  type Crypto,
  Effect,
  Layer,
  Option,
  Schema,
  type Scope,
} from "effect";
import { type Decision, DecisionModel, Model, Tool } from "effect/unstable/ai";

import type * as Agent from "../core/Agent.ts";
import { DecisionTurnError } from "../core/AgentError.ts";
import {
  DecisionTurnEvidence,
  DecisionTurnModel,
  DecisionTurnResult,
} from "../core/DecisionTurn.ts";
import { utf8ByteLength } from "../core/internal/utf8.ts";
import type {
  DecisionTurnRequest,
  PreparedDecisionTurn,
} from "../engine/internal/decision-turn.ts";
import { digestJson, DigestError } from "./Digest.ts";
import { type Digest, PersistedJson } from "./Records.ts";

export { DecisionTurnError } from "../core/AgentError.ts";
export { DecisionTurnEvidence, DecisionTurnModel } from "../core/DecisionTurn.ts";

/** Host-only decoded input and native Run identity. Not a grant or model-selected destination. */
export type Context<Input> = Omit<DecisionTurnRequest, "input"> & { readonly input: Input };

/** Opt-in durable first-eligible ordinary-Turn inference. All concrete callback and model requirements remain visible. */
export interface Definition<Error, Requirements> {
  readonly parent: Agent.AnyDefinition;
  readonly tool: Tool.Any;
  readonly contract: Effect.Effect<Digest, DigestError, Crypto.Crypto>;
  readonly prepare: (
    request: DecisionTurnRequest,
  ) => Effect.Effect<
    Option.Option<PreparedDecisionTurn<Error, Requirements>>,
    Error | DecisionTurnError,
    Requirements
  >;
}

export type Error<T> = T extends Definition<infer E, infer _R> ? E : never;
export type Requirements<T> = T extends Definition<infer _E, infer R> ? R : never;

/**
 * Select one existing Tool through a native classification and pure host projection.
 * Preparation may return None until an ordinary Turn is eligible. It then evaluates at most
 * one committed Decision per Run: a projected Tool or an abstention both consume that slot,
 * including after checkpoint recovery. None from project commits a metered Decision Turn and
 * continues with the registered LanguageModel. Preparation is read-only; no callback grants
 * Tool authority or executes actions. Native visibility, approvals and authorization still apply.
 * Input is the originating Run input; use the assembled Prompt for current steering and Tool results.
 * A failure before the canonical decision append may require inference again on recovery.
 */
export const make = <
  Parent extends Agent.AnyDefinition,
  State extends Schema.Top,
  Label extends string,
  SelectedTool extends Tool.Any,
  E,
  R,
  Provider extends string,
  ModelRequirements,
>(
  parent: Parent,
  options: {
    readonly version: PersistedJson;
    readonly decision: Decision.Definition<State, { readonly route: Decision.Classify<Label> }>;
    readonly model: Model.Model<Provider, DecisionModel.DecisionModel, ModelRequirements>;
    readonly tool: SelectedTool;
    readonly prepare: (
      context: Context<Parent["input"]["Type"]>,
    ) => Effect.Effect<Option.Option<State["Type"]>, E, R>;
    readonly project: (
      state: State["Type"],
      answers: Decision.Answers<{ readonly route: Decision.Classify<Label> }>,
      context: Context<Parent["input"]["Type"]>,
    ) => Option.Option<{
      readonly text: string;
      readonly parameters: SelectedTool["parametersSchema"]["Type"];
    }>;
  },
) => {
  const inputSchema: Parent["input"] = parent.input;
  const parametersSchema: SelectedTool["parametersSchema"] = options.tool.parametersSchema;

  const contract = Effect.try({
    try: () =>
      Schema.decodeUnknownSync(PersistedJson)({
        version: options.version,
        scheduling: "first-eligible-ordinary-turn-once-per-run",
        input: Tool.getJsonSchemaFromSchema(options.decision.input),
        decisions: options.decision.decisions,
        tool: options.tool.name,
        parameters: Tool.getJsonSchemaFromSchema(options.tool.parametersSchema),
        textMaxBytes: 4_096,
      }),
    catch: (cause) =>
      DigestError.make({ message: "Decision Turn contract is not serializable", cause }),
  }).pipe(Effect.flatMap(digestJson));

  const prepare = Effect.fnUntraced(function* (request: DecisionTurnRequest) {
    const input = yield* Schema.decodeUnknownEffect(inputSchema)(request.input).pipe(
      Effect.mapError((cause) => DecisionTurnError.make({ stage: "prepare", cause })),
    );

    const context = { ...request, input };
    const state = yield* options.prepare(context);

    if (Option.isNone(state)) return Option.none();
    const selected = state.value;

    const encoded = yield* Schema.encodeEffect(Schema.toCodecJson(options.decision.input))(
      selected,
    ).pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(PersistedJson)),
      Effect.mapError((cause) => DecisionTurnError.make({ stage: "prepare", cause })),
    );

    const contractDigest = yield* contract.pipe(
      Effect.mapError((cause) => DecisionTurnError.make({ stage: "prepare", cause })),
    );

    const stateDigest = yield* digestJson(encoded).pipe(
      Effect.mapError((cause) => DecisionTurnError.make({ stage: "prepare", cause })),
    );

    const modelServices = yield* Layer.build(options.model);

    const model = yield* Schema.decodeEffect(DecisionTurnModel)({
      provider: EffectContext.get(modelServices, Model.ProviderName),
      model: EffectContext.get(modelServices, Model.ModelName),
      purpose: "decision",
    }).pipe(Effect.mapError((cause) => DecisionTurnError.make({ stage: "prepare", cause })));

    const evaluate = Effect.gen(function* () {
      const response = yield* DecisionModel.decide(options.decision, { input: selected });

      const result = yield* Schema.decodeUnknownEffect(DecisionTurnResult)({
        ...model,
        contractDigest,
        stateDigest,
        toolName: options.tool.name,
        answer: response.answers.route,
        rawUsage: {
          ...(response.usage.inputTokens === undefined
            ? {}
            : { inputTokens: response.usage.inputTokens }),
          ...(response.usage.outputTokens === undefined
            ? {}
            : { outputTokens: response.usage.outputTokens }),
        },
      });

      // The interpreter retains actual usage before this pure projection/encoding can fail.
      const project = Effect.gen(function* () {
        const projected = yield* Effect.try({
          try: () => options.project(selected, response.answers, context),
          catch: (cause) => DecisionTurnError.make({ stage: "project", model, cause }),
        });

        const call = yield* Option.match(projected, {
          onNone: () =>
            Effect.succeed(
              Option.none<{ readonly text: string; readonly parameters: Schema.Json }>(),
            ),
          onSome: (value) =>
            Effect.gen(function* () {
              const text = yield* Schema.decodeEffect(
                Schema.NonEmptyString.check(
                  Schema.makeFilter((value) => utf8ByteLength(value) <= 4_096),
                ),
              )(value.text);

              const parameters = yield* Schema.encodeEffect(parametersSchema)(
                value.parameters,
              ).pipe(Effect.flatMap(Schema.decodeUnknownEffect(PersistedJson)));

              return Option.some({ text, parameters });
            }),
        });

        return {
          evidence: DecisionTurnEvidence.make({
            ...result,
            projection: Option.isSome(call) ? "tool" : "continue",
          }),
          call,
        };
      }).pipe(
        Effect.catchTag("SchemaError", (cause) =>
          Effect.fail(DecisionTurnError.make({ stage: "project", model, cause })),
        ),
      );

      return { result, project };
    }).pipe(
      Effect.provide(modelServices),
      Effect.mapError((cause) => DecisionTurnError.make({ stage: "decide", model, cause })),
    );

    return Option.some({ toolName: options.tool.name, model, evaluate });
  });

  return { parent, tool: options.tool, contract, prepare } satisfies Definition<
    E,
    | R
    | ModelRequirements
    | Crypto.Crypto
    | Scope.Scope
    | Parent["input"]["DecodingServices"]
    | State["EncodingServices"]
    | SelectedTool["parametersSchema"]["EncodingServices"]
  >;
};
