import { Context, Effect, Schema } from "effect";

import { PersistedJson } from "./records.ts";
import {
  type AcceptedEvent,
  type EventSourceVersion,
  type SubscriptionRecord,
  SubscriptionSourceError,
} from "./subscription.ts";

export interface NormalizedEvent {
  readonly eventId: string;
  readonly matchingKey: string;
  readonly payload: PersistedJson;
}

/** Runtime-only versioned behavior. Never persist this object or silently replace its version. */
export interface EventSource {
  readonly source: EventSourceVersion;
  readonly continuity: string;
  readonly parameters: (
    value: unknown,
  ) => Effect.Effect<
    { readonly parameters: PersistedJson; readonly matchingKey: string },
    SubscriptionSourceError
  >;
  readonly normalize: (value: unknown) => Effect.Effect<NormalizedEvent, SubscriptionSourceError>;
  readonly matches: (
    event: AcceptedEvent,
    subscription: SubscriptionRecord,
  ) => Effect.Effect<boolean, SubscriptionSourceError>;
  readonly reconcile?: (
    subscription: SubscriptionRecord,
  ) => Effect.Effect<NormalizedEvent | null, SubscriptionSourceError>;
}

export class EventSources extends Context.Service<
  EventSources,
  {
    readonly sources: ReadonlyArray<EventSource>;
  }
>()("@effect-agent/session/EventSources") {}

const invalid = () => SubscriptionSourceError.make({ code: "source-schema", retryable: false });

/**
 * Capture the source's explicit Effect dependencies at host assembly. Matching and key functions
 * must be pure and bounded under this exact semantic version.
 * The returned source accepts Schema-encoded values at intake and management boundaries.
 */
export const makeEventSource = Effect.fn("Session.makeEventSource")(function* <
  Event extends Schema.Top,
  Parameters extends Schema.Top,
  R = never,
>(options: {
  readonly source: EventSourceVersion;
  readonly continuity: string;
  readonly event: Event;
  readonly parameters: Parameters;
  readonly identity: (event: Event["Type"]) => string;
  readonly eventKey: (event: Event["Type"]) => string;
  readonly parameterKey: (parameters: Parameters["Type"]) => string;
  readonly matches: (event: Event["Type"], parameters: Parameters["Type"]) => boolean;
  readonly reconcile?: (
    parameters: Parameters["Type"],
  ) => Effect.Effect<Event["Type"] | null, SubscriptionSourceError, R>;
}): Effect.fn.Return<
  EventSource,
  never,
  | R
  | Event["DecodingServices"]
  | Event["EncodingServices"]
  | Parameters["DecodingServices"]
  | Parameters["EncodingServices"]
> {
  const services = yield* Effect.context<
    | R
    | Event["DecodingServices"]
    | Event["EncodingServices"]
    | Parameters["DecodingServices"]
    | Parameters["EncodingServices"]
  >();
  const encode = <S extends Schema.Top>(schema: S, value: S["Type"]) =>
    Schema.encodeEffect(schema)(value).pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(PersistedJson)),
      Effect.mapError(invalid),
    );
  const normalized = Effect.fn("EventSource.normalized")(function* (event: Event["Type"]) {
    return {
      eventId: options.identity(event),
      matchingKey: options.eventKey(event),
      payload: yield* encode(options.event, event),
    };
  });
  const normalize: EventSource["normalize"] = (value) =>
    Schema.decodeUnknownEffect(PersistedJson)(value).pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(options.event)),
      Effect.mapError(invalid),
      Effect.flatMap(normalized),
      Effect.provideContext(services),
    );
  const parameters: EventSource["parameters"] = (value) =>
    Effect.gen(function* () {
      yield* Schema.decodeUnknownEffect(PersistedJson)(value).pipe(Effect.mapError(invalid));
      const decoded = yield* Schema.decodeUnknownEffect(options.parameters)(value).pipe(
        Effect.mapError(invalid),
      );
      return {
        parameters: yield* encode(options.parameters, decoded),
        matchingKey: options.parameterKey(decoded),
      };
    }).pipe(Effect.provideContext(services));
  const matches: EventSource["matches"] = (event, subscription) =>
    Effect.gen(function* () {
      const e = yield* Schema.decodeUnknownEffect(options.event)(event.payload).pipe(
        Effect.mapError(invalid),
      );
      const p = yield* Schema.decodeUnknownEffect(options.parameters)(
        subscription.configuration.parameters,
      ).pipe(Effect.mapError(invalid));
      return options.matches(e, p);
    }).pipe(Effect.provideContext(services));
  const reconcile = options.reconcile;
  return {
    source: options.source,
    continuity: options.continuity,
    parameters,
    normalize,
    matches,
    ...(reconcile === undefined
      ? {}
      : {
          reconcile: (subscription: SubscriptionRecord) =>
            Effect.gen(function* () {
              const p = yield* Schema.decodeUnknownEffect(options.parameters)(
                subscription.configuration.parameters,
              ).pipe(Effect.mapError(invalid));
              const event = yield* reconcile(p);
              return event === null ? null : yield* normalized(event);
            }).pipe(Effect.provideContext(services)),
        }),
  };
});
