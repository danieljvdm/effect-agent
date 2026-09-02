import { AgentId, ThreadId } from "@effect-agent/core";
import {
  EventSources,
  Principal,
  SourcePartition,
  SubscriptionAuthorizer,
  SubscriptionFailpoint,
  makeEventSource,
  makeSubscriptionInputBinding,
  SubscriptionInputBindings,
} from "@effect-agent/thread";
import { Effect, Layer, Schema } from "effect";
import { DurableObjectState } from "effect-cf";

import { TEST_DIGESTS } from "./fixtures.ts";

export const subscriptionPartition = SourcePartition.make({
  tenantId: "cf-subscription-tenant",
  address: "application:events",
});

export const subscriptionPrincipal = Schema.decodeSync(Principal)("cf-subscription-principal");
export const subscriptionAgentId = Schema.decodeSync(AgentId)("cf-planner");
export { TEST_DIGESTS as subscriptionDefinitions } from "./fixtures.ts";

export const SubscriptionTestSourceVersion = {
  name: "test-application-event",
  version: "1",
} as const;

const armed = new Map<string, Array<string>>();

export const armSubscriptionEviction = (partitionName: string, point: string): void => {
  const queue = armed.get(partitionName) ?? [];

  queue.push(point);
  armed.set(partitionName, queue);
};

export const subscriptionEvictionsRemaining = (partitionName: string): number =>
  armed.get(partitionName)?.length ?? 0;

export const subscriptionFailpointLayer = Layer.effect(
  SubscriptionFailpoint,
  Effect.map(DurableObjectState.DurableObjectState, (state) => ({
    hit: (point: string) =>
      Effect.suspend(() => {
        const name = state.raw.id.name;

        if (name === undefined) return Effect.void;
        const queue = armed.get(name);

        if (queue === undefined || queue[0] !== point) return Effect.void;
        queue.shift();
        if (queue.length === 0) armed.delete(name);

        return Effect.sync((): never => {
          state.raw.abort(`armed subscription eviction at ${point}`);
          throw new Error(`Durable Object abort returned at ${point}`);
        });
      }),
  })),
);

export const subscriptionAuthorizerLayer = Layer.succeed(SubscriptionAuthorizer)({
  manage: () => Effect.void,
  intake: () => Effect.void,
  reconcile: () => Effect.void,
  prepare: () => Effect.succeed({ policyId: "cf-subscription-policy", decisionId: "allow" }),
});

export const subscriptionSourcesLayer = Layer.merge(
  Layer.effect(
    EventSources,
    makeEventSource({
      source: SubscriptionTestSourceVersion,
      continuity: "Trusted application events begin at durable framework intake.",
      event: Schema.Struct({
        eventId: Schema.String,
        topic: Schema.String,
        message: Schema.String,
      }),
      parameters: Schema.Struct({ topic: Schema.String }),
      identity: (event) => event.eventId,
      eventKey: (event) => event.topic,
      parameterKey: (parameters) => parameters.topic,
      matches: (event, parameters) => event.topic === parameters.topic,
    }).pipe(Effect.map((source) => ({ sources: [source] }))),
  ),
  Layer.effect(
    SubscriptionInputBindings,
    makeSubscriptionInputBinding({
      source: SubscriptionTestSourceVersion,
      agentId: subscriptionAgentId,
      definitions: TEST_DIGESTS,
      event: Schema.Struct({
        eventId: Schema.String,
        topic: Schema.String,
        message: Schema.String,
      }),
      parameters: Schema.Struct({ topic: Schema.String }),
      context: Schema.Struct({ instruction: Schema.String }),
      input: Schema.Struct({ question: Schema.String, ref: Schema.String }),
      prepare: (event, _parameters, context) =>
        Effect.succeed({ question: context.instruction, ref: event.message }),
    }).pipe(Effect.map((binding) => ({ bindings: [binding] }))),
  ),
);

export const subscriptionThreadId = (suffix: string) =>
  Schema.decodeSync(ThreadId)(`cf-subscription-${suffix}`);
