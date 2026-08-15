import { Context, Effect, Layer } from "effect";

import { makeConversationObjectClass, type ConversationObjectOptions } from "../src/index.ts";
import {
  CONVERSATIONS_BINDING,
  DEPLOYMENT_ID,
  PRODUCER_PREFIX,
  fixtureReconcilerLayer,
  makeTestBindings,
  runtimeEvictionFailpoint,
  storageEvictionFailpoint,
} from "./fixtures.ts";
import { makeSubagentTestBindings, transportFaultReason } from "./subagent-fixtures.ts";
import {
  failingTelemetryAcquisitionRouterLayer,
  telemetryProbeRouterLayer,
  type TelemetryLayerAcquisitionError,
} from "./telemetry-fixtures.ts";

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

class TelemetryHostOutput extends Context.Service<TelemetryHostOutput, string>()(
  "@effect-agent/platform-cloudflare/test/TelemetryHostOutput",
) {}

// Compile-time public-API proof: a host observability Layer may provide additional runtime
// services alongside the required flush capability. The Workerd span assertions below prove the
// merged Tracer Reference is also installed in the same ManagedRuntime.
const telemetryHostLayer = Layer.merge(
  telemetryProbeRouterLayer,
  Layer.succeed(TelemetryHostOutput, "host-observability-output"),
);

/** The eviction/alarm/chaos suites' Conversation Object. */
export class TestConversationObject extends makeConversationObjectClass(baseOptions) {}

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

/** Host-telemetry probe; timeout behavior is virtual-time tested below the Workerd boundary. */
export class TelemetryConversationObject extends makeConversationObjectClass(
  {
    ...baseOptions,
    namespaceBinding: "TELEMETRY",
    // Public waitUntil tests control exporter completion explicitly. Virtual-time unit coverage
    // pins the configured cooperative budget without racing the Workerd wall clock.
    telemetryFlushTimeout: 60_000,
    wakeScanInterval: 60_000,
  },
  telemetryHostLayer,
) {}

/** Compile/runtime probe: host telemetry may require the DO context and fail acquisition typed. */
export class TelemetryAcquisitionConversationObject extends makeConversationObjectClass<TelemetryLayerAcquisitionError>(
  {
    ...baseOptions,
    namespaceBinding: "TELEMETRY_ACQUISITION",
  },
  failingTelemetryAcquisitionRouterLayer,
) {}

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
