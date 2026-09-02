import type { AgentId } from "@effect-agent/core";
import { Context, Effect, Schema } from "effect";

import { definitionDigestsEqual } from "./agent-registration.ts";
import { type DefinitionDigests, PersistedJson } from "./records.ts";
import {
  type AcceptedEvent,
  type EventSourceVersion,
  type SubscriptionConfiguration,
  type SubscriptionRecord,
  SubscriptionError,
  SubscriptionSourceError,
} from "./subscription.ts";

/** Host-retained preparation code for one source and exact destination Agent definition. */
export interface SubscriptionInputBinding {
  readonly source: EventSourceVersion;
  readonly agentId: AgentId;
  readonly definitions: DefinitionDigests;
  readonly context: (value: unknown) => Effect.Effect<PersistedJson, SubscriptionSourceError>;
  readonly prepare: (
    event: AcceptedEvent,
    subscription: SubscriptionRecord,
  ) => Effect.Effect<PersistedJson, SubscriptionSourceError>;
}

export class SubscriptionInputBindings extends Context.Service<
  SubscriptionInputBindings,
  { readonly bindings: ReadonlyArray<SubscriptionInputBinding> }
>()("@effect-agent/thread/SubscriptionInputBindings") {}

/** Missing or ambiguous code stays pending. Never substitute a newer destination definition. */
export const resolveSubscriptionInput = (
  bindings: ReadonlyArray<SubscriptionInputBinding>,
  configuration: Pick<SubscriptionConfiguration, "source" | "agentId" | "definitions">,
): Effect.Effect<SubscriptionInputBinding, SubscriptionError> => {
  const found = bindings.filter(
    (binding) =>
      binding.source.name === configuration.source.name &&
      binding.source.version === configuration.source.version &&
      binding.agentId === configuration.agentId &&
      definitionDigestsEqual(binding.definitions, configuration.definitions),
  );

  const binding = found[0];

  return found.length === 1 && binding !== undefined
    ? Effect.succeed(binding)
    : Effect.fail(SubscriptionError.make({ reason: "unsupported-binding", code: "input-binding" }));
};

/**
 * Capture explicit dependencies at host assembly. Pass the destination definition's input Schema.
 * Include the context Schema and mapper in its retained definition version; changing either
 * requires new definition digests. Preparation may repeat until its envelope is committed.
 */
export const makeSubscriptionInputBinding = Effect.fn("Thread.makeSubscriptionInputBinding")(
  function* <
    Event extends Schema.Top,
    Parameters extends Schema.Top,
    Continuation extends Schema.Top,
    Input extends Schema.Top,
    R,
  >(options: {
    readonly source: EventSourceVersion;
    readonly agentId: AgentId;
    readonly definitions: DefinitionDigests;
    readonly event: Event;
    readonly parameters: Parameters;
    readonly context: Continuation;
    readonly input: Input;
    readonly prepare: (
      event: Event["Type"],
      parameters: Parameters["Type"],
      context: Continuation["Type"],
    ) => Effect.Effect<Input["Type"], SubscriptionSourceError, R>;
  }): Effect.fn.Return<
    SubscriptionInputBinding,
    never,
    | R
    | Event["DecodingServices"]
    | Parameters["DecodingServices"]
    | Continuation["DecodingServices"]
    | Continuation["EncodingServices"]
    | Input["EncodingServices"]
  > {
    const services = yield* Effect.context<
      | R
      | Event["DecodingServices"]
      | Parameters["DecodingServices"]
      | Continuation["DecodingServices"]
      | Continuation["EncodingServices"]
      | Input["EncodingServices"]
    >();

    const invalid = () =>
      SubscriptionSourceError.make({ code: "input-binding-schema", retryable: false });

    const encode = <S extends Schema.Top>(schema: S, value: S["Type"]) =>
      Schema.encodeEffect(schema)(value).pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(PersistedJson)),
        Effect.mapError(invalid),
      );

    return {
      source: options.source,
      agentId: options.agentId,
      definitions: options.definitions,
      context: (value) =>
        Schema.decodeUnknownEffect(PersistedJson)(value).pipe(
          Effect.flatMap(Schema.decodeUnknownEffect(options.context)),
          Effect.mapError(invalid),
          Effect.flatMap((decoded) => encode(options.context, decoded)),
          Effect.provideContext(services),
        ),
      prepare: (event, subscription) =>
        Effect.gen(function* () {
          const e = yield* Schema.decodeUnknownEffect(options.event)(event.payload).pipe(
            Effect.mapError(invalid),
          );

          const p = yield* Schema.decodeUnknownEffect(options.parameters)(
            subscription.configuration.parameters,
          ).pipe(Effect.mapError(invalid));

          const c = yield* Schema.decodeUnknownEffect(options.context)(
            subscription.configuration.context,
          ).pipe(Effect.mapError(invalid));

          return yield* encode(options.input, yield* options.prepare(e, p, c));
        }).pipe(Effect.provideContext(services)),
    };
  },
);
