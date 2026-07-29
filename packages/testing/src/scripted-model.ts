import { Context, Effect, Layer, Ref, Schema, Stream } from "effect";
import { AiError, LanguageModel, type Response } from "effect/unstable/ai";

const generatePartTypes = new Set([
  "text",
  "reasoning",
  "reasoning-delta",
  "reasoning-end",
  "tool-call",
  "tool-result",
  "tool-approval-request",
  "file",
  "source",
  "response-metadata",
  "finish",
]);

const streamPartTypes = new Set([
  ...generatePartTypes,
  "text-start",
  "text-delta",
  "text-end",
  "reasoning-start",
  "tool-params-start",
  "tool-params-delta",
  "tool-params-end",
  "error",
]);

const hasPartType = (
  input: unknown,
  types: ReadonlySet<string>,
): input is { readonly type: string } =>
  typeof input === "object" &&
  input !== null &&
  "type" in input &&
  typeof input.type === "string" &&
  types.has(input.type);

/**
 * Schema for encoded, non-streaming Effect AI response parts.
 *
 * Tool-specific fields are decoded by `LanguageModel.make` against the toolkit
 * on the normalized request. This declaration validates the protocol
 * discriminator at script construction boundaries without inventing a second
 * response-part union.
 */
export const ScriptedGeneratePart = Schema.declare(
  (input): input is Response.PartEncoded => hasPartType(input, generatePartTypes),
  { identifier: "ScriptedGeneratePart" },
);
export type ScriptedGeneratePart = typeof ScriptedGeneratePart.Type;

/**
 * Schema for encoded Effect AI streaming response parts.
 */
export const ScriptedStreamPart = Schema.declare(
  (input): input is Response.StreamPartEncoded => hasPartType(input, streamPartTypes),
  { identifier: "ScriptedStreamPart" },
);
export type ScriptedStreamPart = typeof ScriptedStreamPart.Type;

/** Controls whether a scripted stream completes, fails, or waits for interruption. */
export const ScriptedStreamTermination = Schema.Union([
  Schema.TaggedStruct("Complete", {}),
  Schema.TaggedStruct("Fail", {
    description: Schema.String,
  }),
  Schema.TaggedStruct("Hang", {}),
]);
export type ScriptedStreamTermination = typeof ScriptedStreamTermination.Type;

/** One non-streaming invocation and the encoded response parts it returns. */
export const ScriptedGenerateTurn = Schema.TaggedStruct("Generate", {
  parts: Schema.Array(ScriptedGeneratePart),
});
export type ScriptedGenerateTurn = typeof ScriptedGenerateTurn.Type;

/** One streaming invocation with its encoded parts and terminal behavior. */
export const ScriptedStreamTurn = Schema.TaggedStruct("Stream", {
  parts: Schema.Array(ScriptedStreamPart),
  termination: ScriptedStreamTermination,
});
export type ScriptedStreamTurn = typeof ScriptedStreamTurn.Type;

/**
 * Serializable grammar for one finite scripted provider invocation.
 */
export const ScriptedTurn = Schema.Union([ScriptedGenerateTurn, ScriptedStreamTurn]);
export type ScriptedTurn = typeof ScriptedTurn.Type;

export type ScriptedRequestKind = "generate" | "stream";

/**
 * A request after Effect AI has normalized its prompt, tools, response format,
 * tool choice, span, and incremental-response fields.
 */
export interface ScriptedRequest {
  readonly kind: ScriptedRequestKind;
  readonly options: LanguageModel.ProviderOptions;
}

/** Optional request assertions and stream lifecycle effects for one scripted turn. */
export interface ScriptedTurnHooks {
  /** Runs against the normalized provider request before producing the response. */
  readonly assertRequest?: (
    request: LanguageModel.ProviderOptions,
  ) => Effect.Effect<void, AiError.AiError> | void;
  /** Runs immediately before the scripted stream begins emitting parts. */
  readonly onStreamStart?: Effect.Effect<void> | undefined;
  /** Runs when the scripted stream completes, fails, or is interrupted. */
  readonly onStreamFinalize?: Effect.Effect<void> | undefined;
}

/**
 * Runtime hooks are deliberately separate from the serializable turn grammar.
 */
export type ScriptedTurnInput = ScriptedTurn & ScriptedTurnHooks;

interface ScriptState {
  readonly remaining: ReadonlyArray<ScriptedTurnInput>;
  readonly requests: ReadonlyArray<ScriptedRequest>;
}

const scriptedError = (method: string, description: string): AiError.AiError =>
  new AiError.AiError({
    module: "@effect-agent/testing/ScriptedModel",
    method,
    reason: new AiError.UnknownError({ description }),
  });

const runAssertion = (
  assertion: ScriptedTurnHooks["assertRequest"],
  request: LanguageModel.ProviderOptions,
): Effect.Effect<void, AiError.AiError> => {
  if (assertion === undefined) {
    return Effect.void;
  }
  return Effect.suspend(() => {
    const result = assertion(request);
    return Effect.isEffect(result) ? result : Effect.void;
  });
};

const takeTurn = (
  state: Ref.Ref<ScriptState>,
  kind: ScriptedRequestKind,
  options: LanguageModel.ProviderOptions,
): Effect.Effect<ScriptedTurnInput, AiError.AiError> =>
  Ref.modify(state, (current) => {
    const turn = current.remaining[0];
    if (turn === undefined) {
      return [
        undefined,
        {
          ...current,
          requests: [...current.requests, { kind, options }],
        },
      ] as const;
    }
    return [
      turn,
      {
        remaining: current.remaining.slice(1),
        requests: [...current.requests, { kind, options }],
      },
    ] as const;
  }).pipe(
    Effect.flatMap((turn) =>
      turn === undefined
        ? Effect.fail(scriptedError(kind, `Script exhausted before the ${kind} request`))
        : Effect.succeed(turn),
    ),
  );

const requireGenerateTurn = (
  turn: ScriptedTurnInput,
): Effect.Effect<ScriptedGenerateTurn & ScriptedTurnHooks, AiError.AiError> =>
  turn._tag === "Generate"
    ? Effect.succeed(turn)
    : Effect.fail(scriptedError("generate", `Expected a Generate turn but found ${turn._tag}`));

const requireStreamTurn = (
  turn: ScriptedTurnInput,
): Effect.Effect<ScriptedStreamTurn & ScriptedTurnHooks, AiError.AiError> =>
  turn._tag === "Stream"
    ? Effect.succeed(turn)
    : Effect.fail(scriptedError("stream", `Expected a Stream turn but found ${turn._tag}`));

const streamForTurn = (
  turn: ScriptedStreamTurn & ScriptedTurnHooks,
): Stream.Stream<Response.StreamPartEncoded, AiError.AiError> => {
  let stream: Stream.Stream<Response.StreamPartEncoded, AiError.AiError> = Stream.fromIterable(
    turn.parts,
  );
  switch (turn.termination._tag) {
    case "Complete": {
      break;
    }
    case "Fail": {
      stream = stream.pipe(
        Stream.concat(Stream.fail(scriptedError("stream", turn.termination.description))),
      );
      break;
    }
    case "Hang": {
      stream = stream.pipe(Stream.concat(Stream.never));
      break;
    }
  }
  if (turn.onStreamStart !== undefined) {
    stream = Stream.fromEffectDrain(turn.onStreamStart).pipe(Stream.concat(stream));
  }
  if (turn.onStreamFinalize !== undefined) {
    stream = stream.pipe(Stream.ensuring(turn.onStreamFinalize));
  }
  return stream;
};

/** Inspection service for a deterministic LanguageModel backed by finite scripted turns. */
export class ScriptedModel extends Context.Service<
  ScriptedModel,
  {
    /** Normalized provider requests captured in invocation order. */
    readonly requests: Effect.Effect<ReadonlyArray<ScriptedRequest>>;
    /** Number of scripted turns not yet consumed. */
    readonly remaining: Effect.Effect<number>;
    /** Fails with `AiError` when any scripted turns remain. */
    readonly assertExhausted: Effect.Effect<void, AiError.AiError>;
  }
>()("@effect-agent/testing/ScriptedModel") {
  /**
   * Provides the native Effect AI `LanguageModel` and this inspection service.
   * Supplying the extra inspection service does not add it to model-call
   * requirements. Each model invocation consumes one turn before assertion and
   * turn-kind validation.
   */
  static layer(
    turns: ReadonlyArray<ScriptedTurnInput>,
  ): Layer.Layer<LanguageModel.LanguageModel | ScriptedModel, never, never> {
    return Layer.effectContext(
      Effect.gen(function* () {
        const state = yield* Ref.make<ScriptState>({
          remaining: [...turns],
          requests: [],
        });

        const languageModel = yield* LanguageModel.make({
          generateText: (options) =>
            Effect.gen(function* () {
              const turn = yield* takeTurn(state, "generate", options);
              yield* runAssertion(turn.assertRequest, options);
              const generateTurn = yield* requireGenerateTurn(turn);
              return [...generateTurn.parts];
            }),
          streamText: (options) =>
            Stream.unwrap(
              Effect.gen(function* () {
                const turn = yield* takeTurn(state, "stream", options);
                yield* runAssertion(turn.assertRequest, options);
                const streamTurn = yield* requireStreamTurn(turn);
                return streamForTurn(streamTurn);
              }),
            ),
        });

        const inspection = ScriptedModel.of({
          requests: Ref.get(state).pipe(Effect.map((current) => current.requests)),
          remaining: Ref.get(state).pipe(Effect.map((current) => current.remaining.length)),
          assertExhausted: Ref.get(state).pipe(
            Effect.flatMap((current) =>
              current.remaining.length === 0
                ? Effect.void
                : Effect.fail(
                    scriptedError(
                      "assertExhausted",
                      `${current.remaining.length} scripted turn(s) remain`,
                    ),
                  ),
            ),
          ),
        });

        return Context.make(LanguageModel.LanguageModel, languageModel).pipe(
          Context.add(ScriptedModel, inspection),
        );
      }),
    );
  }
}
