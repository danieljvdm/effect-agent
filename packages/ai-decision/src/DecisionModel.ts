import { Context, Effect, Schema, type SchemaAST, type Scope } from "effect";
import { AiError } from "effect/unstable/ai";

import * as DecisionSchema from "./DecisionSchema.ts";
import type * as DecisionSet from "./DecisionSet.ts";
import { responseFor } from "./internal/schema.ts";

/** Typed semantic evaluations; the caller owns thresholds, transitions, and side effects. */
export interface Service {
  readonly evaluate: {
    <const Q extends DecisionSchema.Questions>(
      request: DecisionSchema.EvaluateRequest<Q>,
    ): Effect.Effect<DecisionSchema.EvaluateResponse<Q>, AiError.AiError>;
    /** Encode typed input as state, preserving the schema's encoding requirements. */
    <Input extends Schema.Top, const Q extends DecisionSchema.Questions>(
      set: DecisionSet.DecisionSet<Input, Q>,
      input: NoInfer<Input["Type"]>,
    ): Effect.Effect<
      DecisionSchema.EvaluateResponse<Q>,
      AiError.AiError,
      Input["EncodingServices"]
    >;
  };
}

/** A decision provider, independent of the agent's generative LanguageModel. */
export class DecisionModel extends Context.Service<DecisionModel, Service>()(
  "@effect-agent/ai-decision/DecisionModel",
) {}

/**
 * Construct a provider with request-derived response validation. Capture dependencies once;
 * resources acquired by evaluate close per call. Invalid data fails with native AiError.
 * Defects and interruption propagate. There are no implicit retries, deadlines, confidence
 * thresholds, or transitions. Usage is returned to the caller, not charged to an agent Run.
 */
export const make = Effect.fnUntraced(function* <R>(options: {
  /**
   * Provider-owned rounding check replacing only the Choice probability-sum check.
   * Defaults to a sum of 1 within 1e-6. Exact keys, ranges, winning choices, and
   * all Score checks remain enforced. This trusted construction option never
   * comes from response metadata and must not normalize the supplied values.
   */
  readonly choiceProbabilitySum?: SchemaAST.Check<Readonly<Record<string, number>>>;
  readonly evaluate: (
    request: DecisionSchema.EvaluateRequest,
  ) => Effect.Effect<unknown, AiError.AiError, R>;
}): Effect.fn.Return<Service, never, Exclude<R, Scope.Scope>> {
  const services = yield* Effect.context<Exclude<R, Scope.Scope>>();

  const evaluateRequest = Effect.fn("DecisionModel.evaluate")(function* <
    const Q extends DecisionSchema.Questions,
  >(
    request: DecisionSchema.EvaluateRequest<Q>,
  ): Effect.fn.Return<DecisionSchema.EvaluateResponse<Q>, AiError.AiError> {
    // Encode and decode once to snapshot caller-owned state and criteria before provider I/O.
    const encoded = yield* Schema.encodeEffect(
      Schema.fromJsonString(DecisionSchema.EvaluateRequest),
    )(request).pipe(
      Effect.mapError(
        () =>
          new AiError.AiError({
            module: "DecisionModel",
            method: "evaluate",
            reason: new AiError.InvalidRequestError({
              description: "Invalid decision evaluation request",
            }),
          }),
      ),
    );

    const snapshot = yield* Schema.decodeEffect(
      Schema.fromJsonString(DecisionSchema.EvaluateRequest),
    )(encoded).pipe(
      Effect.mapError(
        () =>
          new AiError.AiError({
            module: "DecisionModel",
            method: "evaluate",
            reason: new AiError.InvalidRequestError({
              description: "Invalid decision evaluation request",
            }),
          }),
      ),
    );

    const schema = responseFor(request.questions, options.choiceProbabilitySum);

    const result = yield* Effect.scoped(options.evaluate(snapshot)).pipe(
      Effect.provideContext(services),
    );

    return yield* Schema.decodeUnknownEffect(schema)(result, { onExcessProperty: "error" }).pipe(
      Effect.mapError(
        () =>
          new AiError.AiError({
            module: "DecisionModel",
            method: "evaluate",
            reason: new AiError.InvalidOutputError({
              description: "Decision response disagrees with the submitted questions",
            }),
          }),
      ),
    );
  });

  const evaluateSet = Effect.fnUntraced(function* <
    Input extends Schema.Top,
    const Q extends DecisionSchema.Questions,
  >(set: DecisionSet.DecisionSet<Input, Q>, input: Input["Type"]) {
    const state = yield* Schema.encodeEffect(set.input)(input).pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(DecisionSchema.Content)),
      Effect.mapError(
        () =>
          new AiError.AiError({
            module: "DecisionModel",
            method: "evaluate",
            reason: new AiError.InvalidRequestError({
              description: "Decision set input must encode to valid decision state",
            }),
          }),
      ),
    );

    return yield* evaluateRequest({ state, questions: set.questions });
  });

  function evaluate<const Q extends DecisionSchema.Questions>(
    request: DecisionSchema.EvaluateRequest<Q>,
  ): Effect.Effect<DecisionSchema.EvaluateResponse<Q>, AiError.AiError>;
  function evaluate<Input extends Schema.Top, const Q extends DecisionSchema.Questions>(
    set: DecisionSet.DecisionSet<Input, Q>,
    input: NoInfer<Input["Type"]>,
  ): Effect.Effect<DecisionSchema.EvaluateResponse<Q>, AiError.AiError, Input["EncodingServices"]>;
  function evaluate<Input extends Schema.Top, const Q extends DecisionSchema.Questions>(
    ...args:
      | [DecisionSchema.EvaluateRequest<Q>]
      | [DecisionSet.DecisionSet<Input, Q>, Input["Type"]]
  ): Effect.Effect<DecisionSchema.EvaluateResponse<Q>, AiError.AiError, Input["EncodingServices"]> {
    return args.length === 1 ? evaluateRequest(args[0]) : evaluateSet(args[0], args[1]);
  }

  return DecisionModel.of({ evaluate });
});
