import {
  makePhase6TravelPlannerBindings,
  phase6SupplierReconcilerLayer,
  phase6TravelPlannerDeploymentId,
  phase6TravelPlannerProducerPrefix,
} from "@effect-agent/testing";

import { makeConversationObjectClass, type ConversationObjectOptions } from "../src/index.ts";
import { runtimeEvictionFailpoint, storageEvictionFailpoint } from "./fixtures.ts";

/**
 * The WP5 Travel Planner test Worker entry (plan §6): the REAL
 * `makeConversationObjectClass` output serving the SAME cumulative Travel Planner fixtures as
 * the DN suites — the P4 planner, the P5 booking agent over the shared supplier desk with the
 * REAL supplier reconciliation policy, and the S2 coordinator/researcher delegation pair —
 * under the shared armed-failpoint eviction levers. It runs as its own vitest project (own
 * workerd instance) so the Travel Planner registrations never touch the WP3/WP4 eviction
 * worker. Everything observable lives in Durable Object storage or the platform-neutral
 * phase-6 fixture module (supplier desk, guide counter, gates) — never in Object fields.
 */

const baseOptions: ConversationObjectOptions = {
  namespaceBinding: "CONVERSATIONS",
  deploymentId: phase6TravelPlannerDeploymentId,
  producerPrefix: phase6TravelPlannerProducerPrefix,
  // Compressed cadences (worker.ts conventions): a dead incarnation's lease must lapse
  // quickly so alarm passes reclaim its lane. The lease is deliberately LONGER than the
  // eviction worker's: the Travel Planner rows drive TWO lanes (parent and child Objects)
  // with accelerated alarm deliveries, and a live Attempt's renewal cadence must survive
  // that extra load — a lease that lapses mid-handler would honestly (but spuriously) route
  // the open uncertain-class call into the Unknown Outcome protocol.
  ownershipLeaseDuration: 1_000,
  leaseRenewalInterval: 100,
  wakeScanInterval: 100,
  settlementPollInterval: 25,
  abortPollInterval: 25,
  alarmBackoffBase: 10,
  alarmBackoffCap: 100,
  observationPollInterval: 10,
  bindings: makePhase6TravelPlannerBindings,
  toolReconciler: phase6SupplierReconcilerLayer,
  storageFailpoint: storageEvictionFailpoint,
  runtimeFailpoint: runtimeEvictionFailpoint,
};

/** The Travel Planner DC suite's Conversation Object. */
export class TravelPlannerConversationObject extends makeConversationObjectClass(baseOptions) {}

/** Tight quotas for the Travel Planner admission-limits rows (refusal before any row). */
export class TravelPlannerLimitedObject extends makeConversationObjectClass({
  ...baseOptions,
  namespaceBinding: "LIMITED",
  maxQueueDepthPerLane: 2,
  maxInputBytes: 512,
}) {}

export default {
  fetch(): Response {
    return new Response("effect-agent travel planner DC test worker");
  },
};
