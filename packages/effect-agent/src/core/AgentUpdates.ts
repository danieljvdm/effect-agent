import { Context, Effect, Option, Schema, Stream } from "effect";

import { AgentId, ThreadId, RunId } from "./Identifiers.ts";
import { IdempotencyKey } from "./Receipt.ts";

/** Canonical schema-encoded intermediate output, scoped to one logical Run. */
export class Update extends Schema.Class<Update>("@effect-agent/core/AgentUpdates/Update")({
  schemaVersion: Schema.Literal(1),
  agentId: AgentId,
  threadId: ThreadId,
  runId: RunId,
  updateId: IdempotencyKey,
  sequence: Schema.Int.check(Schema.isGreaterThan(0)),
  value: Schema.Json,
}) {}

/** Fail-closed update acceptance and decoding errors; payloads are never included. */
export class UpdateError extends Schema.TaggedError<UpdateError>()("AgentUpdateError", {
  reason: Schema.Literals([
    "validation",
    "unavailable",
    "conflict",
    "capacity",
    "storage",
    "identity",
    "denied",
  ]),
}) {}

/** Engine-owned invocation port. The Agent ID and exact update Schema must match the executing definition. */
export class Emitter extends Context.Service<
  Emitter,
  {
    readonly emit: (request: {
      readonly target: { readonly id: AgentId; readonly updates?: Schema.Top | undefined };
      readonly updateId: IdempotencyKey;
      readonly value: Schema.Json;
    }) => Effect.Effect<Update, UpdateError>;
  }
>()("@effect-agent/core/AgentUpdates/Emitter") {}

/** Structural schema boundary accepted by definitions and their model bindings. */
type Source<S extends Schema.Top> = {
  readonly id: AgentId;
  readonly updates?: S | undefined;
};

type BoundSource<S extends Schema.Top> = Source<S> | { readonly definition: Source<S> };

/** Emit a decoded update under an explicit stable key. Retries cannot replace accepted values. */
export const emit = Effect.fn("AgentUpdates.emit")(function* <S extends Schema.Top>(
  agent: BoundSource<S>,
  value: NoInfer<S["Type"]>,
  options: { readonly idempotencyKey: IdempotencyKey },
): Effect.fn.Return<Update, UpdateError, Emitter | S["EncodingServices"]> {
  const target = "definition" in agent ? agent.definition : agent;

  if (target.updates === undefined) return yield* new UpdateError({ reason: "unavailable" });

  const updateId = yield* Schema.decodeEffect(IdempotencyKey)(options.idempotencyKey).pipe(
    Effect.mapError(() => new UpdateError({ reason: "validation" })),
  );

  const encoded = yield* Schema.encodeEffect(target.updates)(value).pipe(
    Effect.flatMap(Schema.decodeUnknownEffect(Schema.Json)),
    Effect.mapError(() => new UpdateError({ reason: "validation" })),
  );

  const emitter = yield* Effect.serviceOption(Emitter);

  if (Option.isNone(emitter)) return yield* new UpdateError({ reason: "unavailable" });

  return yield* emitter.value.emit({ target, updateId, value: encoded });
});

/** Decode canonical update data through its declaring Agent Schema. */
export const decode = Effect.fn("AgentUpdates.decode")(function* <S extends Schema.Top>(
  agent: BoundSource<S>,
  update: Update,
): Effect.fn.Return<S["Type"], UpdateError, S["DecodingServices"]> {
  const definition = "definition" in agent ? agent.definition : agent;

  if (definition.updates === undefined) return yield* new UpdateError({ reason: "unavailable" });

  const decoded = yield* Schema.decodeEffect(Update)(update).pipe(
    Effect.mapError(() => new UpdateError({ reason: "validation" })),
  );

  if (decoded.agentId !== definition.id) return yield* new UpdateError({ reason: "identity" });

  return yield* Schema.decodeEffect(definition.updates)(decoded.value).pipe(
    Effect.mapError(() => new UpdateError({ reason: "validation" })),
  );
});

/** Decode an encoded update stream without hiding its failures or requirements. */
export const observe = <S extends Schema.Top, E, R>(
  agent: BoundSource<S>,
  updates: Stream.Stream<Update, E, R>,
) => updates.pipe(Stream.mapEffect((update) => decode(agent, update)));
