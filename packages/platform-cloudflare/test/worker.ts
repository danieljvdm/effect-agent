import {
  OperationDenied,
  operationAuthorizerLayer,
  possessionChildAdmissionAuthorizerLayer,
  possessionOperationAuthorizerLayer,
} from "@effect-agent/session";
import { Effect, Layer } from "effect";

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
};

const possessionAuthorizationLayer = Layer.merge(
  possessionOperationAuthorizerLayer,
  possessionChildAdmissionAuthorizerLayer,
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

/** The eviction/alarm/chaos suites' Conversation Object. */
export class TestConversationObject extends makeConversationObjectClass(
  baseOptions,
  possessionAuthorizationLayer,
) {}

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
  possessionAuthorizationLayer,
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
export class SubagentConversationObject extends makeConversationObjectClass(
  { ...baseOptions, namespaceBinding: "SUBAGENTS", bindings: makeSubagentTestBindings },
  possessionAuthorizationLayer,
) {
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
