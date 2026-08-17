import {
  OperationDenied,
  operationAuthorizerLayer,
  possessionChildAdmissionAuthorizerLayer,
  possessionOperationAuthorizerLayer,
} from "@effect-agent/session";
import { Effect, Layer, Schema } from "effect";
import { WorkerEnvironment } from "effect-cf";

import {
  makeConversationObjectClass,
  type ConversationObjectOptions,
  type ConversationObjectRpc,
} from "../src/index.ts";
import {
  CONVERSATIONS_BINDING,
  DEPLOYMENT_ID,
  PRODUCER_PREFIX,
  fixtureReconcilerLayer,
  maintenanceRaceFailpoint,
  makeTestBindings,
  runtimeEvictionFailpoint,
  storageEvictionFailpoint,
} from "./fixtures.ts";
import { failNextFlush, flushCount, observabilityProbeLayer } from "./observability-fixture.ts";
import { makeSubagentTestBindings, transportFaultReason } from "./subagent-fixtures.ts";

/**
 * The WP3 test Worker entry: the REAL `makeConversationObjectClass` output under three
 * bindings. Cadences are compressed for test speed; the armed-failpoint factories map hits
 * to `ctx.abort()` (arm-once, isolate-shared with the test files); the fixture Bindings are
 * captured per incarnation during Layer construction. The classes hold no test state of
 * their own — everything observable lives in Durable Object storage or the fixtures module.
 */

const baseOptions: ConversationObjectOptions = {
  namespaceBinding: CONVERSATIONS_BINDING,
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
  bindings: makeTestBindings,
  toolReconciler: fixtureReconcilerLayer,
  storageFailpoint: storageEvictionFailpoint,
  runtimeFailpoint: runtimeEvictionFailpoint,
  maintenanceFailpoint: maintenanceRaceFailpoint,
};

const possessionAuthorizationLayer = Layer.merge(
  possessionOperationAuthorizerLayer,
  possessionChildAdmissionAuthorizerLayer,
);

class AuthorizationFixtureInitializationError extends Schema.TaggedError<AuthorizationFixtureInitializationError>()(
  "AuthorizationFixtureInitializationError",
  { message: Schema.String },
) {}

/** Proves policy acquisition can use effect-cf's Worker environment and fail typed. */
const environmentAuthorizationLayer = Layer.unwrap(
  Effect.gen(function* () {
    const env = yield* WorkerEnvironment;
    if (!("TELEMETRY" in env)) {
      return yield* AuthorizationFixtureInitializationError.make({
        message: "The TELEMETRY binding is required by the authorization fixture",
      });
    }
    return possessionAuthorizationLayer;
  }),
);

const deniedAuthorizationLayer = Layer.merge(
  operationAuthorizerLayer({
    authorize: (request) =>
      Effect.fail(
        OperationDenied.make({
          operation: request.operation,
          principal: request.principal,
          reason: "denied by the platform RPC policy fixture",
          ...(request.conversationId === undefined
            ? {}
            : { conversationId: request.conversationId }),
          ...(request.submissionId === undefined ? {} : { submissionId: request.submissionId }),
        }),
      ),
  }),
  possessionChildAdmissionAuthorizerLayer,
);

const changingAuthorizationCalls = new Map<string, number>();
const changingAuthorizationLayer = Layer.merge(
  operationAuthorizerLayer({
    authorize: (request) =>
      Effect.suspend(() => {
        const key = `${request.operation}:${request.submissionId ?? request.conversationId ?? "none"}`;
        const calls = (changingAuthorizationCalls.get(key) ?? 0) + 1;
        changingAuthorizationCalls.set(key, calls);
        return calls === 1
          ? Effect.void
          : Effect.fail(
              OperationDenied.make({
                operation: request.operation,
                principal: request.principal,
                reason: "changing policy denies every authorization after the first",
                ...(request.conversationId === undefined
                  ? {}
                  : { conversationId: request.conversationId }),
                ...(request.submissionId === undefined
                  ? {}
                  : { submissionId: request.submissionId }),
              }),
            );
      }),
  }),
  possessionChildAdmissionAuthorizerLayer,
);

interface BindingSourceProbe {
  readonly evaluationCount: number;
  readonly incarnation: number;
  readonly conversationId: string;
  readonly producerId: string;
  readonly rawEnvHasNamespace: boolean;
}

let nextBindingSourceIncarnation = 0;
const bindingSourceProbes = new WeakMap<DurableObjectState, BindingSourceProbe>();

const dynamicBindings: NonNullable<ConversationObjectOptions["bindings"]> = ({
  ctx,
  env,
  conversationId,
  producerId,
}) =>
  Effect.sync(() => {
    const previous = bindingSourceProbes.get(ctx);
    const probe = {
      evaluationCount: (previous?.evaluationCount ?? 0) + 1,
      incarnation: previous?.incarnation ?? ++nextBindingSourceIncarnation,
      conversationId,
      producerId,
      rawEnvHasNamespace: typeof env === "object" && env !== null && "DYNAMIC_BINDINGS" in env,
    };
    bindingSourceProbes.set(ctx, probe);
    return [];
  });

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

/** The eviction/alarm/chaos and progress-wait suites' Conversation Object. */
export class TestConversationObject extends makeConversationObjectClass(
  baseOptions,
  possessionAuthorizationLayer,
) {
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

/** Fail-closed policy proving denials survive the real Worker↔DO RPC boundary. */
export class DeniedConversationObject extends makeConversationObjectClass(
  { ...baseOptions, namespaceBinding: "DENIED" },
  deniedAuthorizationLayer,
) {}

/** Allow-once policy proving one protected mutation performs exactly one authorization. */
export class ChangingAuthorizationConversationObject extends makeConversationObjectClass(
  { ...baseOptions, namespaceBinding: "CHANGING_AUTH" },
  changingAuthorizationLayer,
) {}

/** RPC fixture proving journal failures retain their concrete Schema error on the client. */
export class RunJournalFailureConversationObject extends makeConversationObjectClass(
  { ...baseOptions, namespaceBinding: "RUN_JOURNAL_FAILURE" },
  possessionAuthorizationLayer,
) {
  override async resolveApprovalEncoded(_encoded: unknown): Promise<unknown> {
    // RPC transports encoded protocol data. Returning a Schema class here would be rejected by
    // structured clone before the client codec gets a chance to reconstruct the error class.
    return {
      _tag: "HostFailed",
      failure: { _tag: "RunJournalError", message: "fixture Run journal projection failure" },
    };
  }
}

/** Tight queue-depth and input-size quotas for the admission-limits gate rows. */
export class LimitedConversationObject extends makeConversationObjectClass(
  {
    ...baseOptions,
    namespaceBinding: "LIMITED",
    maxQueueDepthPerLane: 2,
    maxInputBytes: 512,
  },
  possessionAuthorizationLayer,
) {}

/** A database-size ceiling below any real database: every admission must refuse typed. */
export class TinyDatabaseConversationObject extends makeConversationObjectClass(
  { ...baseOptions, namespaceBinding: "TINYDB", maxDatabaseBytes: 1 },
  possessionAuthorizationLayer,
) {}

/** Callback-form Binding capture probe. */
export class DynamicBindingsConversationObject extends makeConversationObjectClass(
  { ...baseOptions, namespaceBinding: "DYNAMIC_BINDINGS", bindings: dynamicBindings },
  possessionAuthorizationLayer,
) {
  async bindingSourceProbe(): Promise<BindingSourceProbe & { readonly stateMatches: boolean }> {
    const probe = bindingSourceProbes.get(this.ctx);
    if (probe === undefined) throw new Error("Binding source was not evaluated");
    return { ...probe, stateMatches: bindingSourceProbes.has(this.ctx) };
  }
}

/** Legacy array-form Binding capture probe. */
export class ArrayBindingsConversationObject extends makeConversationObjectClass(
  { ...baseOptions, namespaceBinding: "ARRAY_BINDINGS", bindings: [] },
  possessionAuthorizationLayer,
) {
  async bindingSourceKind(): Promise<string> {
    return "array";
  }
}

/** Legacy Effect-form Binding capture probe. */
export class EffectBindingsConversationObject extends makeConversationObjectClass(
  { ...baseOptions, namespaceBinding: "EFFECT_BINDINGS", bindings: Effect.succeed([]) },
  possessionAuthorizationLayer,
) {
  async bindingSourceKind(): Promise<string> {
    return "effect";
  }
}

/** Minimal integration proof that effect-cf owns native RPC event scopes and OTLP flushing. */
export class TelemetryConversationObject extends makeConversationObjectClass(
  {
    ...baseOptions,
    namespaceBinding: "TELEMETRY",
    wakeScanInterval: 60_000,
  },
  environmentAuthorizationLayer,
  observabilityProbeLayer,
) {
  failNextFlush(): void {
    failNextFlush(this.ctx.id.name ?? this.ctx.id.toString());
  }

  async flushCount(): Promise<number> {
    return flushCount(this.ctx.id.name ?? this.ctx.id.toString());
  }
}

/**
 * The WP4 cross-Object subagent matrix's Conversation Object: parent and child Conversations
 * of one delegation are DIFFERENT Objects of this namespace by the identity rule. The
 * namespace wrapper is the DO-unreachable lever — an armed transport fault makes the
 * caller-side stub throw BEFORE owner-side execution, so the routed caller observes a
 * `PortTransportError` (and `AdmissionIndeterminate` on `resolveAdmission`, SUB-031). Wake
 * hints fail at the same seam and remain droppable. Unarmed, every stub is a passthrough.
 */
const SubagentConversationObjectBase = makeConversationObjectClass(
  {
    ...baseOptions,
    namespaceBinding: "SUBAGENTS",
    bindings: makeSubagentTestBindings,
  },
  possessionAuthorizationLayer,
);

const faultableStub = <RpcService extends ConversationObjectRpc>(
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

const faultableNamespace = <RpcService extends ConversationObjectRpc>(
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

export class SubagentConversationObject extends SubagentConversationObjectBase {
  constructor(ctx: DurableObjectState, env: Cloudflare.Env) {
    super(ctx, faultableEnvironment(env));
  }
}

export default {
  fetch(): Response {
    return new Response("effect-agent platform-cloudflare test worker");
  },
};
