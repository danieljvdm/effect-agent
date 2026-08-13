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
