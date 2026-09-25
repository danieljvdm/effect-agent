import {
  ThreadObjectNamespace,
  ThreadObjectIdentity,
  type ThreadObjectRpc,
} from "@effect-agent/platform-cloudflare/cloudflare-bindings";
import {
  MemoryObject,
  CloudflareMemoryClient,
} from "@effect-agent/platform-cloudflare/cloudflare-memory";
import {
  makeScheduleOwnerObjectClass,
  ScheduleOwnerIdentity,
} from "@effect-agent/platform-cloudflare/cloudflare-scheduling";
import { makeSubscriptionPartitionObjectClass } from "@effect-agent/platform-cloudflare/cloudflare-subscriptions";
import * as ThreadObject from "@effect-agent/platform-cloudflare/thread-object";
import { Clock, Effect, Layer, Schema } from "effect";
import { DurableAgentRuntime } from "effect-agent/durable-agent-runtime";
import { RecalledMemory } from "effect-agent/memory";
import { MemoryLookup } from "effect-agent/memory-reference";
import { MemoryWrite, MemoryDocument } from "effect-agent/memory-store";
import { ScheduleAuthorizer, ScheduleFailpoint } from "effect-agent/schedule";
import { DurableObject, DurableObjectState, RpcTracing, WorkerEnvironment } from "effect-cf";

import { ThreadMaintenance } from "../src/Alarm.ts";
import { layerFromBindings } from "../src/internal/layers.ts";
import {
  backgroundWorkerBindings,
  backgroundWorkerAuthority,
  backgroundWakeDropPrefixes,
  customRuntimeThreads,
} from "./background-worker-fixture.ts";
import {
  THREADS_BINDING,
  DEPLOYMENT_ID,
  PRODUCER_PREFIX,
  fixtureReconcilerLayer,
  maintenanceRaceFailpoint,
  maintenanceClocks,
  unavailableBindingThreads,
  upgradedBookBindingThreads,
  upgradedBookBinding,
  makeContextCompactorLayer,
  makeContextAuthorizationLayer,
  testRuntimeLayer,
  makeTestBindings,
  runtimeEvictionFailpoint,
  notifyScheduleAlarmCompleted,
  scheduleAuthorizer,
  scheduleFailpoint,
  storageEvictionFailpoint,
} from "./fixtures.ts";
import {
  memoryAuthorizer,
  memoryFailpoints,
  memoryAccess,
  memoryPrincipal,
  memoryRecallLimits,
  MemoryProjects,
} from "./memory-fixtures.ts";
import {
  droppedMessageWakes,
  messageDeliveryFaultLayer,
  testMessageRecovery,
} from "./message-delivery-fixture.ts";
import { observabilityProbeLayer, telemetryProbe } from "./observability-fixture.ts";
import { hostMaintenanceLayer, projectionLayer } from "./projection-fixture.ts";
import { publicationLayer, lifecyclePublicationTestLayer } from "./publication-fixture.ts";
import { recoveryTestLayer } from "./recovery-fixture.ts";
import { makeSubagentTestBindings, transportFaultReason } from "./subagent-fixtures.ts";
import {
  subscriptionAlarmExtensionLayer,
  subscriptionAuthorizerLayer,
  subscriptionFailpointLayer,
  subscriptionSourcesLayer,
} from "./subscription-fixtures.ts";

export class TestMemoryObject extends MemoryObject.make(memoryAuthorizer, {
  failpoints: memoryFailpoints,
}) {}

/**
 * The WP3 test Worker entry: the REAL `ThreadObject.make` output under three
 * bindings. Cadences are compressed for test speed; the armed-failpoint factories map hits
 * to `ctx.abort()` (arm-once, isolate-shared with the test files); the fixture Bindings are
 * captured per incarnation during Layer construction. The classes hold no test state of
 * their own — everything observable lives in Durable Object storage or the fixtures module.
 */

const baseOptions: ThreadObject.Options = {
  namespaceBinding: THREADS_BINDING,
  deploymentId: DEPLOYMENT_ID,
  producerPrefix: PRODUCER_PREFIX,
  // A dead incarnation's lease must lapse quickly so alarm passes reclaim its lane.
  ownershipLeaseDuration: 250,
  leaseRenewalInterval: 50,
  wakeScanInterval: 100,
  settlementPollInterval: 25,
  abortPollInterval: 25,
  alarmBackoffBase: 10,
  projectionDispatchTimeoutMillis: 1_000,
  alarmBackoffCap: 100,
  observationPollInterval: 10,
  toolReconciler: fixtureReconcilerLayer,
  storageFailpoint: storageEvictionFailpoint,
  runtimeFailpoint: runtimeEvictionFailpoint,
  maintenanceFailpoint: maintenanceRaceFailpoint,
};

const scheduleHostLayer = Layer.mergeAll(
  Layer.effect(
    ScheduleAuthorizer,
    Effect.map(ScheduleOwnerIdentity, ({ owner }) => scheduleAuthorizer(owner)),
  ),
  Layer.effect(
    ScheduleFailpoint,
    Effect.map(DurableObjectState.DurableObjectState, (state) => scheduleFailpoint(state.raw)),
  ),
  Layer.effect(
    ThreadObjectNamespace,
    Effect.map(WorkerEnvironment, (env) =>
      ThreadObjectNamespace.of({
        get: (threadId) => env.THREADS.get(env.THREADS.idFromName(threadId)),
      }),
    ),
  ),
);

/** Real Schedule Owner object routed to the test Thread namespace. */
export class TestScheduleOwnerObject extends makeScheduleOwnerObjectClass(scheduleHostLayer, {
  maxSchedulesPerOwner: 100,
  minIntervalMillis: 60_000,
  maxInputBytes: 65_536,
  dueBatchSize: 16,
  admissionConcurrency: 4,
  retryBaseMillis: 10,
  retryMaxMillis: 100,
  admissionTimeoutMillis: 5_000,
  recoveryPollMillis: 100,
}) {
  override async alarm(alarmInfo?: AlarmInvocationInfo): Promise<void> {
    await super.alarm(alarmInfo);
    notifyScheduleAlarmCompleted(this.ctx);
  }
}

const subscriptionHostLayer = Layer.mergeAll(
  subscriptionAlarmExtensionLayer,
  subscriptionAuthorizerLayer,
  subscriptionSourcesLayer,
  subscriptionFailpointLayer,
  Layer.effect(
    ThreadObjectNamespace,
    Effect.map(WorkerEnvironment, (env) =>
      ThreadObjectNamespace.of({
        get: (threadId) => env.THREADS.get(env.THREADS.idFromName(threadId)),
      }),
    ),
  ),
);

/** Real source-addressed Subscription Partition object routed to Thread Objects. */
export class TestSubscriptionPartitionObject extends makeSubscriptionPartitionObjectClass(
  subscriptionHostLayer,
  {
    maxRegistrations: 100,
    maxRegistrationsPerOwner: 100,
    maxEvents: 100,
    maxDeliveries: 100,
    maxDeliveriesPerOwner: 100,
    maxPayloadBytes: 65_536,
    maxContextBytes: 16_384,
    maxLifetimeMillis: 86_400_000,
    batchSize: 1,
    concurrency: 1,
    retryMillis: 10,
    operationTimeoutMillis: 5_000,
  },
) {}

/** The eviction/alarm/chaos suites' Thread Object. */
const progressWaiterCounts = new WeakMap<DurableObjectState, number>();

interface ProgressWaiterCountLatch {
  readonly expected: number;
  readonly resolve: () => void;
}

const progressWaiterCountLatches = new WeakMap<
  DurableObjectState,
  Array<ProgressWaiterCountLatch>
>();

const progressIncarnations = new WeakMap<DurableObjectState, number>();
let nextProgressIncarnation = 0;

const setProgressWaiterCount = (ctx: DurableObjectState, count: number): void => {
  progressWaiterCounts.set(ctx, count);
  const latches = progressWaiterCountLatches.get(ctx);

  if (latches === undefined) return;
  const pending: Array<ProgressWaiterCountLatch> = [];

  for (const latch of latches) {
    if (latch.expected === count) {
      latch.resolve();
    } else {
      pending.push(latch);
    }
  }
  if (pending.length === 0) {
    progressWaiterCountLatches.delete(ctx);
  } else {
    progressWaiterCountLatches.set(ctx, pending);
  }
};

const awaitProgressWaiterCount = (ctx: DurableObjectState, expected: number): Promise<void> => {
  if ((progressWaiterCounts.get(ctx) ?? 0) === expected) return Promise.resolve();

  return new Promise((resolve) => {
    const latches = progressWaiterCountLatches.get(ctx) ?? [];

    latches.push({ expected, resolve });
    progressWaiterCountLatches.set(ctx, latches);
  });
};

const progressIncarnation = (ctx: DurableObjectState): number => {
  const existing = progressIncarnations.get(ctx);

  if (existing !== undefined) return existing;
  const created = ++nextProgressIncarnation;

  progressIncarnations.set(ctx, created);

  return created;
};

// Initialization arms maintenance alarms before event-only layers are available.
const maintenanceClockLayer = Layer.effect(
  Clock.Clock,
  Effect.gen(function* () {
    const identity = yield* ThreadObjectIdentity;

    return maintenanceClocks.get(identity.threadId) ?? (yield* Clock.Clock);
  }),
);

export class PublicationThreadObject extends ThreadObject.make(
  Layer.unwrap(
    Effect.map(makeTestBindings, (bindings) =>
      Layer.fresh(ThreadMaintenance.layer).pipe(
        Layer.provideMerge(DurableAgentRuntime.layerWithBindings(bindings)),
      ),
    ),
  ).pipe(
    Layer.provideMerge(
      Layer.unwrap(
        Effect.map(ThreadObjectIdentity, ({ threadId }) =>
          ThreadObject.layer([], {
            publication: publicationLayer,
            ...(threadId.startsWith("lifecycle-publication-")
              ? { lifecyclePublication: lifecyclePublicationTestLayer }
              : {}),
          }),
        ),
      ),
    ),
    Layer.provideMerge(maintenanceClockLayer),
  ),
  { ...baseOptions, namespaceBinding: "PUBLICATIONS" },
) {}

export class ProjectionThreadObject extends ThreadObject.make(
  Layer.unwrap(
    Effect.map(makeTestBindings, (bindings) =>
      Layer.fresh(ThreadMaintenance.layer).pipe(
        Layer.provideMerge(DurableAgentRuntime.layerWithBindings(bindings)),
        Layer.provide(hostMaintenanceLayer),
      ),
    ),
  ).pipe(
    Layer.provideMerge(
      ThreadObject.layer([], { projection: projectionLayer, publication: publicationLayer }),
    ),
    Layer.provideMerge(maintenanceClockLayer),
  ),
  { ...baseOptions, namespaceBinding: "PROJECTIONS" },
) {}

export class TestThreadObject extends ThreadObject.make(
  Layer.unwrap(
    Effect.map(Effect.all([makeTestBindings, backgroundWorkerBindings]), ([existing, workers]) =>
      Layer.unwrap(
        Effect.map(ThreadObjectIdentity, ({ threadId }) =>
          threadId.startsWith("recovery-alarm-") || threadId.startsWith("recovery-retirement-")
            ? recoveryTestLayer(
                existing,
                threadId.startsWith("recovery-retirement-") ? hostMaintenanceLayer : undefined,
              ).pipe(Layer.provideMerge(layerFromBindings([])))
            : unavailableBindingThreads.has(threadId)
              ? layerFromBindings([])
              : upgradedBookBindingThreads.has(threadId)
                ? Layer.unwrap(
                    Effect.map(upgradedBookBinding, (replacement) =>
                      layerFromBindings([
                        ...existing.filter((binding) => binding.agentId !== replacement.agentId),
                        replacement,
                        ...workers,
                      ]),
                    ),
                  )
                : threadId.startsWith("background-cf-custom-") || customRuntimeThreads.has(threadId)
                  ? Layer.fresh(ThreadMaintenance.layer).pipe(
                      Layer.provideMerge(
                        DurableAgentRuntime.layerWithBindings([...existing, ...workers]),
                      ),
                      Layer.provideMerge(layerFromBindings([])),
                    )
                  : threadId.startsWith("messages-")
                    ? Layer.fresh(ThreadMaintenance.layer).pipe(
                        Layer.provide(testMessageRecovery),
                        Layer.provideMerge(layerFromBindings([...existing, ...workers])),
                      )
                    : layerFromBindings([...existing, ...workers]),
        ),
      ),
    ),
  ).pipe(
    Layer.provide(backgroundWorkerAuthority),
    Layer.provide(messageDeliveryFaultLayer),
    Layer.provideMerge(maintenanceClockLayer),
  ),
  {
    ...baseOptions,
    // Scripted providers have no spend; explicit cost-bound scout Runs still require pricing.
    estimateCostMicrousd: () => Effect.succeed({ costMicrousd: 0 }),
  },
) {
  override wake(): Promise<void> {
    const name = this.ctx.id.name ?? "";

    return droppedMessageWakes.has(name) ||
      [...backgroundWakeDropPrefixes].some((prefix) => name.startsWith(prefix))
      ? Promise.resolve()
      : super.wake();
  }

  memoryChange(project: string, encoded: unknown) {
    return this[DurableObject.RunSymbol](
      Effect.gen(function* () {
        const env = yield* WorkerEnvironment;

        const client = yield* CloudflareMemoryClient.fromBinding(env.MEMORIES, {
          access: memoryAccess(project),
          principal: memoryPrincipal,
        });

        const write = yield* Schema.decodeUnknownEffect(MemoryWrite.Wire)(encoded);

        const document = yield* client.change({
          ...write,
          key: {
            ...write.key,
            namespace: yield* MemoryProjects.restore(write.key.namespace.address),
          },
        });

        return yield* Schema.encodeEffect(MemoryDocument.Wire)(document);
      }),
    );
  }

  memoryRecall(project: string, encoded: unknown) {
    return this[DurableObject.RunSymbol](
      Effect.gen(function* () {
        const env = yield* WorkerEnvironment;

        const client = yield* CloudflareMemoryClient.fromBinding(env.MEMORIES, {
          access: memoryAccess(project),
          principal: memoryPrincipal,
        });

        const lookup = yield* Schema.decodeUnknownEffect(MemoryLookup)(encoded);

        return yield* client
          .recall(lookup, memoryRecallLimits)
          .pipe(Effect.flatMap(Schema.encodeEffect(RecalledMemory)));
      }),
    );
  }
  override async awaitProgressEncoded(encoded: unknown): Promise<unknown> {
    progressIncarnation(this.ctx);
    setProgressWaiterCount(this.ctx, (progressWaiterCounts.get(this.ctx) ?? 0) + 1);
    try {
      return await super.awaitProgressEncoded(encoded);
    } finally {
      setProgressWaiterCount(this.ctx, Math.max(0, (progressWaiterCounts.get(this.ctx) ?? 1) - 1));
    }
  }

  progressWaiterCount(): number {
    return progressWaiterCounts.get(this.ctx) ?? 0;
  }

  awaitProgressWaiterCount(expected: number): Promise<void> {
    return awaitProgressWaiterCount(this.ctx, expected);
  }

  async awaitProgressWaiterCountAfter(
    previousIncarnation: number,
    expected: number,
  ): Promise<number | null> {
    const incarnation = progressIncarnation(this.ctx);

    if (incarnation === previousIncarnation) return null;
    await awaitProgressWaiterCount(this.ctx, expected);

    return incarnation;
  }

  progressIncarnation(): number {
    return progressIncarnation(this.ctx);
  }
}

/** A scoped compactor Layer captured once per Object incarnation. */
export class ContextCompactorThreadObject extends ThreadObject.make(
  testRuntimeLayer.pipe(
    Layer.provide(
      Layer.unwrap(
        Effect.map(ThreadObjectIdentity, ({ threadId }) => makeContextCompactorLayer(threadId)),
      ),
    ),
    Layer.provide(
      Layer.unwrap(
        Effect.map(ThreadObjectIdentity, ({ threadId }) => makeContextAuthorizationLayer(threadId)),
      ),
    ),
  ),
  {
    ...baseOptions,
    namespaceBinding: "CONTEXT_COMPACTOR",
  },
) {}

/** Native RPC invocation and parent-span observations. */
const TelemetryThreadObjectBase = ThreadObject.make(testRuntimeLayer, {
  ...baseOptions,
  namespaceBinding: "TELEMETRY",
  wakeScanInterval: 60_000,
  rpcTracing: true,
  eventLayer: observabilityProbeLayer,
});

type TelemetryServices = Effect.Services<
  Parameters<InstanceType<typeof TelemetryThreadObjectBase>[typeof DurableObject.RunSymbol]>[0]
>;

export class TelemetryThreadObject extends TelemetryThreadObjectBase {
  override [DurableObject.RunSymbol]<A, E>(
    effect: Effect.Effect<A, E, TelemetryServices>,
    options: DurableObject.RunOptions = {},
  ): Promise<A> {
    const event = options.event;

    if (event === undefined) return super[DurableObject.RunSymbol](effect, options);
    const threadId = this.ctx.id.name ?? this.ctx.id.toString();

    const observed = Effect.gen(function* () {
      telemetryProbe(threadId).invocations.push(options);

      return yield* effect;
    });

    return super[DurableObject.RunSymbol](
      options.rpc === undefined
        ? Effect.withSpan(observed, `TELEMETRY/${event}`, { kind: "server", root: true })
        : RpcTracing.withRpcServerSpan(observed, options.rpc),
      options,
    );
  }
}

/**
 * The WP4 cross-Object subagent matrix's Thread Object: parent and child Threads
 * of one delegation are DIFFERENT Objects of this namespace by the identity rule. The
 * namespace wrapper is the DO-unreachable lever — an armed transport fault makes the
 * caller-side stub throw BEFORE owner-side execution, so the routed caller observes a
 * `PortTransportError` (and `AdmissionIndeterminate` on `resolveAdmission`, SUB-031). Wake
 * hints fail at the same seam and remain droppable. Unarmed, every stub is a passthrough.
 */
const SubagentThreadObjectBase = ThreadObject.make(
  Layer.unwrap(Effect.map(makeSubagentTestBindings, layerFromBindings)),
  {
    ...baseOptions,
    namespaceBinding: "SUBAGENTS",
  },
);

const faultableStub = <RpcService extends ThreadObjectRpc>(
  stub: DurableObjectStub<RpcService>,
  name: string | undefined,
): DurableObjectStub<RpcService> =>
  new Proxy(stub, {
    get(target, property, receiver) {
      if (property === "portCall") {
        return (...args: Parameters<ThreadObjectRpc["portCall"]>): Promise<unknown> => {
          const reason = transportFaultReason(name);

          if (reason !== undefined) throw new Error(reason);

          return target.portCall(...args);
        };
      }
      if (property === "wake") {
        return (): Promise<void> => {
          const reason = transportFaultReason(name);

          if (reason !== undefined) throw new Error(reason);

          return target.wake();
        };
      }
      const value: unknown = Reflect.get(target, property, receiver);

      return typeof value === "function" ? value.bind(target) : value;
    },
  });

const faultableNamespace = <RpcService extends ThreadObjectRpc>(
  namespace: DurableObjectNamespace<RpcService>,
): DurableObjectNamespace<RpcService> =>
  new Proxy(namespace, {
    get(target, property, receiver) {
      if (property === "get") {
        return (
          id: DurableObjectId,
          options?: DurableObjectNamespaceGetDurableObjectOptions,
        ): DurableObjectStub<RpcService> => faultableStub(target.get(id, options), id.name);
      }
      if (property === "getByName") {
        return (
          name: string,
          options?: DurableObjectNamespaceGetDurableObjectOptions,
        ): DurableObjectStub<RpcService> => faultableStub(target.getByName(name, options), name);
      }
      const value: unknown = Reflect.get(target, property, receiver);

      return typeof value === "function" ? value.bind(target) : value;
    },
  });

const faultableEnvironment = (env: Cloudflare.Env): Cloudflare.Env =>
  new Proxy(env, {
    get(target, property, receiver) {
      if (property === "SUBAGENTS") return faultableNamespace(target.SUBAGENTS);

      return Reflect.get(target, property, receiver);
    },
  });

export class SubagentThreadObject extends SubagentThreadObjectBase {
  constructor(ctx: DurableObjectState, env: Cloudflare.Env) {
    super(ctx, faultableEnvironment(env));
  }
}

export default {
  fetch(): Response {
    return new Response("effect-agent platform-cloudflare test worker");
  },
};
