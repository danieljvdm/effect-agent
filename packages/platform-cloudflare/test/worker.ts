import * as Agent from "@effect-agent/core/Agent";
import { RecalledMemory } from "@effect-agent/core/Memory";
import { MemoryLookup } from "@effect-agent/core/MemoryReference";
import { MemoryWrite, MemoryDocument } from "@effect-agent/core/MemoryStore";
import {
  ThreadObjectNamespace,
  ThreadObjectIdentity,
  type ThreadObjectRpc,
} from "@effect-agent/platform-cloudflare/CloudflareBindings";
import {
  MemoryObject,
  CloudflareMemoryClient,
} from "@effect-agent/platform-cloudflare/CloudflareMemory";
import {
  makeScheduleOwnerObjectClass,
  ScheduleOwnerIdentity,
} from "@effect-agent/platform-cloudflare/CloudflareScheduling";
import { makeSubscriptionPartitionObjectClass } from "@effect-agent/platform-cloudflare/CloudflareSubscriptions";
import * as ThreadObject from "@effect-agent/platform-cloudflare/ThreadObject";
import { OperationDenied } from "@effect-agent/thread/OperationAuthorizer";
import { ScheduleAuthorizer, ScheduleFailpoint } from "@effect-agent/thread/Schedule";
import { Context, Crypto, Effect, Layer, Schema } from "effect";
import { DurableObject, DurableObjectState, RpcTracing, WorkerEnvironment } from "effect-cf";
import { OtlpExporter } from "effect/unstable/observability";

import { layerFromBindings } from "../src/internal/layers.ts";
import {
  THREADS_BINDING,
  DEPLOYMENT_ID,
  PRODUCER_PREFIX,
  fixtureReconcilerLayer,
  maintenanceRaceFailpoint,
  makeContextCompactorRunContextLayer,
  makeContextAuthorizationLayer,
  plannerDefinition,
  plannerModel,
  registrationDefinitions,
  testRuntimeLayer,
  runtimeEvictionFailpoint,
  notifyScheduleAlarmCompleted,
  scheduleAuthorizer,
  scheduleFailpoint,
  storageEvictionFailpoint,
} from "./fixtures.ts";
import {
  memoryAuthorizer,
  memoryFailpoints,
  memoryCalls,
  memoryAccess,
  memoryPrincipal,
  memoryRecallLimits,
  MemoryProjects,
} from "./memory-fixtures.ts";
import {
  failNextFlush,
  flushCount,
  observabilityProbeLayer,
  telemetryProbe,
} from "./observability-fixture.ts";
import { makeSubagentTestBindings, transportFaultReason } from "./subagent-fixtures.ts";
import {
  subscriptionAuthorizerLayer,
  subscriptionFailpointLayer,
  subscriptionSourcesLayer,
} from "./subscription-fixtures.ts";

export class TestMemoryObject extends MemoryObject.make(memoryAuthorizer, {
  failpoints: memoryFailpoints,
}) {
  override memory(encoded: string): Promise<string> {
    const name = this.ctx.id.name ?? "";

    memoryCalls.set(name, (memoryCalls.get(name) ?? 0) + 1);

    return super.memory(encoded);
  }
}

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
    Effect.map(WorkerEnvironment, (env) => ({ namespace: env.THREADS })),
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
  subscriptionAuthorizerLayer,
  subscriptionSourcesLayer,
  subscriptionFailpointLayer,
  Layer.effect(
    ThreadObjectNamespace,
    Effect.map(WorkerEnvironment, (env) => ({ namespace: env.THREADS })),
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

interface BindingSourceProbe {
  readonly evaluationCount: number;
  readonly incarnation: number;
  readonly threadId: string;
  readonly producerId: string;
  readonly rawEnvHasNamespace: boolean;
}

let nextBindingSourceIncarnation = 0;
const bindingSourceProbes = new WeakMap<DurableObjectState, BindingSourceProbe>();

class RegistrationResource extends Context.Service<
  RegistrationResource,
  {
    readonly check: Effect.Effect<void, string>;
  }
>()("@effect-agent/platform-cloudflare/test/RegistrationResource") {}

const registrationResourceLayer = Layer.effect(
  RegistrationResource,
  Effect.gen(function* () {
    const { raw: ctx } = yield* DurableObjectState.DurableObjectState;
    const env = yield* WorkerEnvironment;
    const { threadId, producerId } = yield* ThreadObjectIdentity;

    yield* Crypto.Crypto;
    const previous = bindingSourceProbes.get(ctx);

    bindingSourceProbes.set(ctx, {
      evaluationCount: (previous?.evaluationCount ?? 0) + 1,
      incarnation: previous?.incarnation ?? ++nextBindingSourceIncarnation,
      threadId,
      producerId,
      rawEnvHasNamespace: env.DYNAMIC_BINDINGS !== undefined,
    });

    const resource = yield* Effect.acquireRelease(
      Effect.sync(() => ({ open: true })),
      (resource) =>
        Effect.sync(() => {
          resource.open = false;
        }),
    );

    return {
      check: Effect.suspend(() =>
        resource.open ? Effect.void : Effect.fail("registration resource closed"),
      ),
    };
  }),
);

const dynamicRuntime = ThreadObject.layer([
  {
    agent: Agent.withModel(
      Agent.make(plannerDefinition.id, {
        input: plannerDefinition.input,
        output: plannerDefinition.output,
        toolkit: plannerDefinition.toolkit,
        policy: plannerDefinition.policy,
        instructions: (input: Agent.Input<typeof plannerDefinition>) =>
          Effect.gen(function* () {
            const resource = yield* RegistrationResource;

            yield* resource.check;

            return plannerDefinition.instructions(input);
          }),
      }),
      plannerModel,
    ),
    definitions: registrationDefinitions,
  },
]).pipe(Layer.provideMerge(registrationResourceLayer));

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

export class TestThreadObject extends ThreadObject.make(testRuntimeLayer, baseOptions) {
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

/** Tight queue-depth and input-size quotas for the admission-limits gate rows. */
export class LimitedThreadObject extends ThreadObject.make(testRuntimeLayer, {
  ...baseOptions,
  namespaceBinding: "LIMITED",
  maxQueueDepthPerLane: 2,
  maxInputBytes: 512,
}) {}

/** A database-size ceiling below any real database: every admission must refuse typed. */
export class TinyDatabaseThreadObject extends ThreadObject.make(testRuntimeLayer, {
  ...baseOptions,
  namespaceBinding: "TINYDB",
  maxDatabaseBytes: 1,
}) {}

/** Fail-closed authorization fixture for host-protocol error-tag fidelity. */
export class DeniedThreadObject extends ThreadObject.make(testRuntimeLayer, {
  ...baseOptions,
  namespaceBinding: "DENIED",
  operationAuthorizer: {
    authorize: (request) =>
      Effect.fail(
        OperationDenied.make({
          operation: request.operation,
          reason: "denied by the #94 Cloudflare fixture",
          ...(request.threadId === undefined ? {} : { threadId: request.threadId }),
          ...(request.submissionId === undefined ? {} : { submissionId: request.submissionId }),
        }),
      ),
  },
}) {}

/** Registration acquisition through yielded effect-cf and platform services. */
export class DynamicBindingsThreadObject extends ThreadObject.make(dynamicRuntime, {
  ...baseOptions,
  namespaceBinding: "DYNAMIC_BINDINGS",
  eventLayer: Layer.effectDiscard(
    Effect.flatMap(RegistrationResource, (resource) => resource.check),
  ),
}) {
  async bindingSourceProbe(): Promise<BindingSourceProbe & { readonly stateMatches: boolean }> {
    const probe = bindingSourceProbes.get(this.ctx);

    if (probe === undefined) throw new Error("Binding source was not evaluated");

    return { ...probe, stateMatches: bindingSourceProbes.has(this.ctx) };
  }
}

/** Issue #49: a scoped run-context Layer captured once per Object incarnation. */
export class ContextCompactorThreadObject extends ThreadObject.make(
  testRuntimeLayer.pipe(
    Layer.provide(
      Layer.unwrap(
        Effect.map(ThreadObjectIdentity, ({ threadId }) =>
          makeContextCompactorRunContextLayer(threadId),
        ),
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

/** Minimal integration proof that effect-cf owns native RPC event scopes and OTLP flushing. */
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
      // Also proves that the factory's public hook retains the event Layer's service type.
      yield* OtlpExporter.Flusher;
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

  failNextFlush(): void {
    failNextFlush(this.ctx.id.name ?? this.ctx.id.toString());
  }

  async flushCount(): Promise<number> {
    return flushCount(this.ctx.id.name ?? this.ctx.id.toString());
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
        return (encoded: unknown): Promise<unknown> => {
          const reason = transportFaultReason(name);

          if (reason !== undefined) throw new Error(reason);

          return target.portCall(encoded);
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
