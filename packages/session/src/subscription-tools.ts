import type { AgentId, ConversationId } from "@effect-agent/core";
import { DurableStep, ToolExecutionClass } from "@effect-agent/engine";
import type { Layer } from "effect";
import { Crypto, Effect, Schema } from "effect";
import { Tool, Toolkit } from "effect/unstable/ai";

import type { Principal } from "./ledger.ts";
import type { DefinitionDigests, PersistedJson } from "./records.ts";
import type { ScheduleDestination } from "./schedule.ts";
import {
  type EventSourceVersion,
  SubscriptionError,
  SubscriptionFailpointError,
  SubscriptionName,
  SubscriptionSnapshot,
  SubscriptionSourceError,
  type SubscriptionScope,
} from "./subscription.ts";
import { Subscriptions } from "./subscriptions.ts";

export const SubscriptionToolRegistration = Schema.Struct({
  subscriptionId: SubscriptionName,
  source: Schema.Struct({ name: SubscriptionName, version: SubscriptionName }),
  mode: Schema.Literals(["once", "continuous"]),
  state: Schema.Literals(["active", "consumed", "cancelled", "expired"]),
  createdAtMillis: Schema.Int,
  expiresAtMillis: Schema.Int,
  recovery: SubscriptionSnapshot.fields.recovery,
});
export type SubscriptionToolRegistration = typeof SubscriptionToolRegistration.Type;

export const SubscribeToEventParameters = Schema.Struct({
  source: Schema.Struct({ name: SubscriptionName, version: SubscriptionName }),
  parameters: Schema.Json,
  mode: Schema.Literals(["once", "continuous"]),
  expiresAtMillis: Schema.Int.annotate({
    description:
      "Absolute Unix epoch milliseconds; it must remain unchanged if this call is retried",
  }),
  context: Schema.Json,
});

export const ListEventSubscriptionsParameters = Schema.Struct({
  after: Schema.optionalKey(Schema.Natural),
  limit: Schema.optionalKey(
    Schema.Int.check(Schema.isGreaterThan(0), Schema.isLessThanOrEqualTo(100)),
  ),
});

export const CancelEventSubscriptionParameters = Schema.Struct({
  subscriptionId: SubscriptionName,
});

export const SubscriptionToolSourceCatalogEntry = Schema.Struct({
  source: Schema.Struct({ name: SubscriptionName, version: SubscriptionName }),
  description: Schema.NonEmptyString.check(Schema.isMaxLength(2_048)),
  parametersJsonSchema: Schema.Json,
  contextJsonSchema: Schema.Json,
});
export type SubscriptionToolSourceCatalogEntry = typeof SubscriptionToolSourceCatalogEntry.Type;

const SubscriptionToolFailure = Schema.Union([
  SubscriptionError,
  SubscriptionSourceError,
  SubscriptionFailpointError,
]);

export const ListEventSources = Tool.make("list_event_sources", {
  description:
    "List the exact event source versions this host permits, with their parameter and continuation-context JSON Schemas. Inspect this catalog before subscribing.",
  parameters: Schema.Struct({}),
  success: Schema.Array(SubscriptionToolSourceCatalogEntry),
  failure: SubscriptionError,
  failureMode: "return",
})
  .annotate(Tool.Readonly, true)
  .annotate(ToolExecutionClass, "readonly");

export const SubscribeToEvent = Tool.make("subscribe_to_event", {
  description:
    "Register a durable event subscription from the host-permitted source catalog. The destination, Agent, authority, and delivery principal are fixed by the host.",
  parameters: SubscribeToEventParameters,
  success: SubscriptionToolRegistration,
  failure: SubscriptionToolFailure,
  failureMode: "return",
  dependencies: [DurableStep],
})
  .annotate(Tool.Readonly, false)
  .annotate(ToolExecutionClass, "idempotent");

export const ListEventSubscriptions = Tool.make("list_event_subscriptions", {
  description: "List redacted durable event subscriptions in the current host-bound owner scope.",
  parameters: ListEventSubscriptionsParameters,
  success: Schema.Struct({
    items: Schema.Array(SubscriptionToolRegistration),
    next: Schema.NullOr(Schema.Natural),
  }),
  failure: SubscriptionError,
  failureMode: "return",
})
  .annotate(Tool.Readonly, true)
  .annotate(ToolExecutionClass, "readonly");

export const CancelEventSubscription = Tool.make("cancel_event_subscription", {
  description: "Cancel one durable event subscription in the current host-bound owner scope.",
  parameters: CancelEventSubscriptionParameters,
  success: SubscriptionToolRegistration,
  failure: Schema.Union([SubscriptionError, SubscriptionFailpointError]),
  failureMode: "return",
  dependencies: [DurableStep],
})
  .annotate(Tool.Readonly, false)
  .annotate(ToolExecutionClass, "idempotent");

export const SubscriptionTools = Toolkit.make(
  ListEventSources,
  SubscribeToEvent,
  ListEventSubscriptions,
  CancelEventSubscription,
);

export interface PermittedSubscriptionToolSource {
  readonly source: EventSourceVersion;
  /** The source's input Schema must be the input Schema of this exact host Agent identity. */
  readonly agentId: AgentId;
  readonly description: string;
  readonly parameters: Schema.Top;
  readonly context: Schema.Top;
}

export interface SubscriptionToolsOptions {
  readonly scope: SubscriptionScope;
  readonly currentConversationId: ConversationId;
  readonly deliveryPrincipal: Principal;
  readonly agentId: AgentId;
  readonly definitions: DefinitionDigests;
  readonly permittedSources: ReadonlyArray<PermittedSubscriptionToolSource>;
  /** Defaults to the current Conversation; a host may instead bind deterministic fresh delivery. */
  readonly destination?: ScheduleDestination | undefined;
}

const sameSource = (left: EventSourceVersion, right: EventSourceVersion): boolean =>
  left.name === right.name && left.version === right.version;

const registration = (
  snapshot: typeof SubscriptionSnapshot.Type,
): SubscriptionToolRegistration => ({
  subscriptionId: snapshot.key.subscriptionId,
  source: snapshot.source,
  mode: snapshot.mode,
  state: snapshot.state,
  createdAtMillis: snapshot.createdAtMillis,
  expiresAtMillis: snapshot.expiresAtMillis,
  recovery: snapshot.recovery,
});

const toolFailure = (reason: SubscriptionError["reason"], code: string): SubscriptionError =>
  SubscriptionError.make({ reason, code });

/**
 * Bind management authority before exposing Tools to a model. The model can select only an exact
 * source/version and source-defined parameters; it cannot choose another tenant, owner,
 * Conversation, Agent, definition digest, principal, destination policy, or credential.
 */
export const subscriptionToolsLayer = (
  options: SubscriptionToolsOptions,
): Layer.Layer<
  Tool.HandlersFor<Toolkit.Tools<typeof SubscriptionTools>>,
  SubscriptionError,
  Subscriptions | Crypto.Crypto
> => {
  const destination: ScheduleDestination = options.destination ?? {
    _tag: "ExistingConversation",
    conversationId: options.currentConversationId,
  };
  const build = Effect.gen(function* () {
    const duplicate = options.permittedSources.find(
      (candidate, index, all) =>
        all.findIndex((item) => sameSource(item.source, candidate.source)) !== index,
    );
    if (duplicate !== undefined) {
      return yield* toolFailure("validation", "duplicate-source-catalog-entry");
    }
    const mismatched = options.permittedSources.find(
      (candidate) => candidate.agentId !== options.agentId,
    );
    if (mismatched !== undefined) {
      return yield* toolFailure("validation", "source-agent-mismatch");
    }
    const catalog = yield* Effect.forEach(options.permittedSources, (candidate) =>
      Effect.gen(function* () {
        const description = yield* Schema.decodeUnknownEffect(
          SubscriptionToolSourceCatalogEntry.fields.description,
        )(candidate.description).pipe(
          Effect.mapError(() => toolFailure("validation", "source-catalog-description")),
        );
        return SubscriptionToolSourceCatalogEntry.make({
          source: candidate.source,
          description,
          parametersJsonSchema: yield* Effect.try({
            try: () => Tool.getJsonSchemaFromSchema(candidate.parameters),
            catch: () => toolFailure("validation", "source-parameters-schema"),
          }),
          contextJsonSchema: yield* Effect.try({
            try: () => Tool.getJsonSchemaFromSchema(candidate.context),
            catch: () => toolFailure("validation", "source-context-schema"),
          }),
        });
      }),
    );
    const subscriptions = yield* Subscriptions;
    const crypto = yield* Crypto.Crypto;
    const permitted = (source: EventSourceVersion) =>
      options.permittedSources.some((candidate) => sameSource(candidate.source, source));
    const subscribe = Effect.fn("SubscriptionTools.subscribe")(function* (parameters: {
      readonly source: EventSourceVersion;
      readonly parameters: PersistedJson;
      readonly mode: "once" | "continuous";
      readonly expiresAtMillis: number;
      readonly context: PersistedJson;
    }) {
      if (!permitted(parameters.source)) {
        return yield* toolFailure("unauthorized", "source-catalog");
      }
      const step = yield* DurableStep;
      // The first committed Step creates one identity per Tool Call. Re-entry observes the same
      // value, while a separate identical call receives a distinct identity.
      const subscriptionId = yield* step
        .do(
          "subscription-identity",
          SubscriptionName,
          crypto.randomUUIDv7.pipe(
            Effect.map((id) => `tool:${id}`),
            Effect.mapError(() => toolFailure("storage", "creation-identity")),
          ),
        )
        .pipe(
          Effect.catchTag("DurableStepError", (error) =>
            Effect.fail(toolFailure("storage", `durable-step-${error.reason}`)),
          ),
        );
      return yield* step
        .do(
          "register-subscription",
          SubscriptionToolRegistration,
          subscriptions
            .subscribe(options.scope, {
              subscriptionId,
              source: parameters.source,
              parameters: parameters.parameters,
              context: parameters.context,
              mode: parameters.mode,
              expiresAtMillis: parameters.expiresAtMillis,
              destination,
              deliveryPrincipal: options.deliveryPrincipal,
              agentId: options.agentId,
              definitions: options.definitions,
            })
            .pipe(Effect.map(registration)),
        )
        .pipe(
          Effect.catchTag("DurableStepError", (error) =>
            Effect.fail(toolFailure("storage", `durable-step-${error.reason}`)),
          ),
        );
    });
    const list = Effect.fn("SubscriptionTools.list")(function* (parameters: {
      readonly after?: number | undefined;
      readonly limit?: number | undefined;
    }) {
      const page = yield* subscriptions.listSubscriptions(
        options.scope,
        parameters.after,
        parameters.limit,
      );
      return { items: page.items.map(registration), next: page.next };
    });
    const cancel = Effect.fn("SubscriptionTools.cancel")(function* (parameters: {
      readonly subscriptionId: string;
    }) {
      const step = yield* DurableStep;
      return yield* step
        .do(
          "cancel-subscription",
          SubscriptionToolRegistration,
          subscriptions
            .cancelSubscription(options.scope, {
              partition: options.scope.partition,
              ownerId: options.scope.ownerId,
              subscriptionId: parameters.subscriptionId,
            })
            .pipe(Effect.map(registration)),
        )
        .pipe(
          Effect.catchTag("DurableStepError", (error) =>
            Effect.fail(toolFailure("storage", `durable-step-${error.reason}`)),
          ),
        );
    });
    return SubscriptionTools.of({
      list_event_sources: () => Effect.succeed(catalog),
      subscribe_to_event: subscribe,
      list_event_subscriptions: list,
      cancel_event_subscription: cancel,
    });
  });
  return SubscriptionTools.toLayer(build);
};
