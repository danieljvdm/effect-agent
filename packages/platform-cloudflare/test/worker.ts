import { OperationDenied } from "@effect-agent/session";
import { Effect } from "effect";

import { makeConversationObjectClass, type ConversationObjectOptions } from "../src/index.ts";
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

/** The eviction/alarm/chaos suites' Conversation Object. */
const progressWaiterCounts = new WeakMap<DurableObjectState, number>();
const progressIncarnations = new WeakMap<DurableObjectState, number>();
let nextProgressIncarnation = 0;

const progressIncarnation = (ctx: DurableObjectState): number => {
  const existing = progressIncarnations.get(ctx);
  if (existing !== undefined) return existing;
  const created = ++nextProgressIncarnation;
  progressIncarnations.set(ctx, created);
  return created;
};

export class TestConversationObject extends makeConversationObjectClass(baseOptions) {
  override async awaitProgressEncoded(encoded: unknown): Promise<unknown> {
    progressIncarnation(this.ctx);
    progressWaiterCounts.set(this.ctx, (progressWaiterCounts.get(this.ctx) ?? 0) + 1);
    try {
      return await super.awaitProgressEncoded(encoded);
    } finally {
      progressWaiterCounts.set(
        this.ctx,
        Math.max(0, (progressWaiterCounts.get(this.ctx) ?? 1) - 1),
      );
    }
  }

  progressWaiterCount(): number {
    return progressWaiterCounts.get(this.ctx) ?? 0;
  }

  progressIncarnation(): number {
    return progressIncarnation(this.ctx);
  }
}

/** Tight queue-depth and input-size quotas for the admission-limits gate rows. */
export class LimitedConversationObject extends makeConversationObjectClass({
  ...baseOptions,
  namespaceBinding: "LIMITED",
  maxQueueDepthPerLane: 2,
  maxInputBytes: 512,
}) {}

/** A database-size ceiling below any real database: every admission must refuse typed. */
export class TinyDatabaseConversationObject extends makeConversationObjectClass({
  ...baseOptions,
  namespaceBinding: "TINYDB",
  maxDatabaseBytes: 1,
}) {}

/** Fail-closed authorization fixture for host-protocol error-tag fidelity. */
export class DeniedConversationObject extends makeConversationObjectClass({
  ...baseOptions,
  namespaceBinding: "DENIED",
  operationAuthorizer: {
    authorize: (request) =>
      Effect.fail(
        OperationDenied.make({
          operation: request.operation,
          reason: "denied by the #94 Cloudflare fixture",
          ...(request.conversationId === undefined
            ? {}
            : { conversationId: request.conversationId }),
          ...(request.submissionId === undefined ? {} : { submissionId: request.submissionId }),
        }),
      ),
  },
}) {}

/** Callback-form Binding capture probe. */
export class DynamicBindingsConversationObject extends makeConversationObjectClass({
  ...baseOptions,
  namespaceBinding: "DYNAMIC_BINDINGS",
  bindings: dynamicBindings,
}) {
  async bindingSourceProbe(): Promise<BindingSourceProbe & { readonly stateMatches: boolean }> {
    const probe = bindingSourceProbes.get(this.ctx);
    if (probe === undefined) throw new Error("Binding source was not evaluated");
    return { ...probe, stateMatches: bindingSourceProbes.has(this.ctx) };
  }
}

/** Legacy array-form Binding capture probe. */
export class ArrayBindingsConversationObject extends makeConversationObjectClass({
  ...baseOptions,
  namespaceBinding: "ARRAY_BINDINGS",
  bindings: [],
}) {
  async bindingSourceKind(): Promise<string> {
    return "array";
  }
}

/** Legacy Effect-form Binding capture probe. */
export class EffectBindingsConversationObject extends makeConversationObjectClass({
  ...baseOptions,
  namespaceBinding: "EFFECT_BINDINGS",
  bindings: Effect.succeed([]),
}) {
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
 * `portCall` override is the DO-unreachable lever — an armed transport fault makes the
 * incoming cross-Object RPC reject exactly like an unreachable Object's stub would, BEFORE
 * any owner-side execution, so the routed caller observes a `PortTransportError` (and
 * `AdmissionIndeterminate` on `resolveAdmission`, SUB-031). Unarmed, it is a passthrough.
 */
export class SubagentConversationObject extends makeConversationObjectClass({
  ...baseOptions,
  namespaceBinding: "SUBAGENTS",
  bindings: makeSubagentTestBindings,
}) {
  override async portCall(encoded: unknown): Promise<unknown> {
    const reason = transportFaultReason(this.ctx.id.name);
    if (reason !== undefined) throw new Error(reason);
    return super.portCall(encoded);
  }

  /** An unreachable Object drops wake hints too — droppable by contract, so senders swallow. */
  override async wake(): Promise<void> {
    const reason = transportFaultReason(this.ctx.id.name);
    if (reason !== undefined) throw new Error(reason);
    return super.wake();
  }
}

export default {
  fetch(): Response {
    return new Response("effect-agent platform-cloudflare test worker");
  },
};
