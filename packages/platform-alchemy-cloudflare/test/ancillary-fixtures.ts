import { threadNamespaceLayer } from "@effect-agent/platform-cloudflare/cloudflare-host-bindings";
import {
  MemoryOwnerAuthorizer,
  MemoryOwnerIdentity,
  MemoryRpcError,
} from "@effect-agent/storage-cloudflare/memory-protocol";
import { BrowserCrypto } from "@effect/platform-browser";
import { DurableObject as AlchemyDurableObject } from "alchemy/Cloudflare/Workers/DurableObject";
import { WorkerEnvironment } from "alchemy/Cloudflare/Workers/WorkerRuntime";
import { Effect, Layer, Schema } from "effect";
import { digestDefinitions } from "effect-agent/digest";
import { EventSources, makeEventSource } from "effect-agent/event-source";
import * as MemoryNamespace from "effect-agent/memory-namespace";
import { MemoryAccess } from "effect-agent/memory-revalidation";
import { MemoryScope, MemoryWrite } from "effect-agent/memory-store";
import { ScheduleAuthorizationError, ScheduleAuthorizer } from "effect-agent/schedule";
import { Principal } from "effect-agent/submission-ledger";
import { SubscriptionAuthorizer, SubscriptionError } from "effect-agent/subscription";
import {
  makeSubscriptionInputBinding,
  SubscriptionInputBindings,
} from "effect-agent/subscription-input";

import * as MemoryObject from "../src/MemoryObject.ts";
import * as Scheduling from "../src/Scheduling.ts";
import * as Subscriptions from "../src/Subscriptions.ts";
import { definitions, planner } from "./fixtures.ts";

export const principal = Principal.make("ancillary-owner");
export const deniedPrincipal = Principal.make("not-the-owner");

export const definitionDigests = digestDefinitions(definitions).pipe(
  Effect.provide(BrowserCrypto.layer),
);

export const MemoryProjects = MemoryNamespace.define({
  name: "alchemy/projects",
  version: 1,
  identity: Schema.String,
});

const memoryScope = MemoryScope.make("private");

export const memoryAccess = (project: string) =>
  MemoryAccess.make({ namespace: MemoryProjects.make(project), scope: memoryScope });

export const memoryPut = (project: string) =>
  MemoryWrite.make({
    _tag: "Put",
    key: { namespace: MemoryProjects.make(project), id: "note" },
    operationId: "put-note",
    expectedRevision: null,
    locator: "memory://note",
    scopes: [memoryScope],
    content: {
      text: "The memory survives its RPC invocation.",
      attributions: [
        {
          originId: "input",
          speaker: "Dan",
          observers: ["caller"],
          locator: "thread://input",
          activityAt: 1,
          interpretation: "statement",
        },
      ],
      metadata: {},
      recordedAt: 2,
    },
  });

const memoryAuthorizer = Layer.effect(MemoryOwnerAuthorizer)(
  Effect.gen(function* () {
    const { namespace } = yield* MemoryOwnerIdentity;

    yield* MemoryProjects.restore(namespace.address);

    return {
      authorize: (request) =>
        request.principal === principal && request.access.scope === memoryScope
          ? Effect.void
          : MemoryRpcError.make({ reason: "denied" }),
    };
  }),
);

export class Memories extends AlchemyDurableObject<Memories, MemoryObject.Rpc>()("MEMORIES") {}
export const MemoriesLive = Memories.make(MemoryObject.make(memoryAuthorizer));

const threadsLayer = Layer.unwrap(
  Effect.map(WorkerEnvironment, (env) => threadNamespaceLayer(env, "THREADS")),
);

const scheduleAuthorizer = Layer.effect(ScheduleAuthorizer)(
  Effect.gen(function* () {
    const { owner } = yield* Scheduling.ScheduleOwnerIdentity;

    return {
      manage: (request) =>
        request.scope.principal === principal &&
        request.scope.owner.tenantId === owner.tenantId &&
        request.scope.owner.ownerId === owner.ownerId
          ? Effect.void
          : ScheduleAuthorizationError.make({ code: "denied" }),
      prepare: (request) =>
        request.configuration.deliveryPrincipal === principal
          ? Effect.succeed({ policyId: "alchemy-schedule-policy", decisionId: "allow" })
          : ScheduleAuthorizationError.make({ code: "denied" }),
    };
  }),
);

export class Schedules extends AlchemyDurableObject<Schedules, Scheduling.Rpc>()("SCHEDULES") {}

export const SchedulesLive = Schedules.make(
  Scheduling.make(Layer.merge(scheduleAuthorizer, threadsLayer)),
);

export const sourceVersion = { name: "alchemy-event", version: "1" } as const;

const Event = Schema.Struct({
  eventId: Schema.String,
  topic: Schema.String,
  message: Schema.String,
});

const Parameters = Schema.Struct({ topic: Schema.String });

const subscriptionAuthorizer = Layer.effect(SubscriptionAuthorizer)(
  Effect.gen(function* () {
    const { partition } = yield* Subscriptions.SubscriptionPartitionIdentity;

    const authorize = (candidate: Principal, tenantId: string, address: string) =>
      candidate === principal && tenantId === partition.tenantId && address === partition.address
        ? Effect.void
        : SubscriptionError.make({ reason: "unauthorized", code: "denied" });

    return {
      manage: (_operation, scope) =>
        authorize(scope.principal, scope.partition.tenantId, scope.partition.address),
      intake: (candidate, _source, caller) =>
        authorize(caller, candidate.tenantId, candidate.address),
      reconcile: (subscription) =>
        authorize(
          subscription.configuration.deliveryPrincipal,
          subscription.key.partition.tenantId,
          subscription.key.partition.address,
        ),
      prepare: (subscription) =>
        authorize(
          subscription.configuration.deliveryPrincipal,
          subscription.key.partition.tenantId,
          subscription.key.partition.address,
        ).pipe(Effect.as({ policyId: "alchemy-subscription-policy", decisionId: "allow" })),
    };
  }),
);

const eventSources = Layer.effect(EventSources)(
  makeEventSource({
    source: sourceVersion,
    continuity: "Trusted application events begin at durable framework intake.",
    event: Event,
    parameters: Parameters,
    identity: (event) => event.eventId,
    eventKey: (event) => event.topic,
    parameterKey: (parameters) => parameters.topic,
    matches: (event, parameters) => event.topic === parameters.topic,
  }).pipe(Effect.map((source) => ({ sources: [source] }))),
);

const inputBindings = Layer.effect(SubscriptionInputBindings)(
  Effect.gen(function* () {
    const hashes = yield* definitionDigests;

    const binding = yield* makeSubscriptionInputBinding({
      source: sourceVersion,
      agentId: planner.id,
      definitions: hashes,
      event: Event,
      parameters: Parameters,
      context: Schema.Struct({ instruction: Schema.String }),
      input: Schema.Struct({ question: Schema.String }),
      prepare: (event, _parameters, context) =>
        Effect.succeed({ question: `${context.instruction}: ${event.message}` }),
    });

    return { bindings: [binding] };
  }),
);

export class SubscriptionPartitions extends AlchemyDurableObject<
  SubscriptionPartitions,
  Subscriptions.Rpc
>()("SUBSCRIPTIONS") {}

export const SubscriptionPartitionsLive = SubscriptionPartitions.make(
  Subscriptions.make(
    Layer.mergeAll(subscriptionAuthorizer, eventSources, inputBindings, threadsLayer),
  ),
);
