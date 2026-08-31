import { describe, expect, it } from "@effect/vitest";
import { Schema } from "effect";

import {
  AcceptedEvent,
  EventAcknowledgement,
  PreparedInput,
  SubscriptionDelivery,
  SubscriptionDeliverySnapshot,
  SubscriptionRecord,
  SubscriptionSnapshot,
} from "../src/index.ts";

// Encoded fixtures pin the durable wire format independently of constructors and driver output.
const partition = { tenantId: "fixture-tenant", address: "repository:42" };
const source = { name: "workflow-completed", version: "1" };
const key = { partition, ownerId: "reviewer", subscriptionId: "watch-101" };
const digest = "a".repeat(64);
const registration = {
  schemaVersion: 1,
  key,
  creationFingerprint: digest,
  createdBy: "principal",
  createdAtMillis: 10,
  ordinal: 1,
  configuration: {
    source,
    matchingKey: "run:101:attempt:1",
    parameters: { runId: 101, attempt: 1 },
    context: { instruction: "continue reviewing" },
    mode: "once",
    expiresAtMillis: 1_000,
    destination: { _tag: "FreshThread" },
    deliveryPrincipal: "principal",
    agentId: "reviewer",
    definitions: { agent: digest, model: digest, tools: digest },
  },
  state: "active",
  recovery: { attempts: 1, nextAttemptAtMillis: 100, lastFailure: "github-rate-limited" },
};
const accepted = {
  schemaVersion: 1,
  partition,
  source,
  eventId: "run:101:attempt:1:completed",
  matchingKey: registration.configuration.matchingKey,
  payload: { conclusion: "failure" },
  payloadDigest: digest,
  acceptedAtMillis: 50,
  cutoff: 2,
  cursor: 1,
  routingComplete: true,
  routingFailure: null,
  nextAttemptAtMillis: 50,
};
const envelope = {
  schemaVersion: 1,
  threadId: `subscription:${digest}`,
  deliveryPrincipal: "principal",
  agentId: "reviewer",
  definitions: registration.configuration.definitions,
  input: { instruction: "continue reviewing", conclusion: "failure" },
  inputDigest: digest,
  admissionKey: `subscription:${digest}`,
  authorization: { policyId: "repository-policy", decisionId: "allowed" },
};
const selected = {
  schemaVersion: 1,
  key: { subscription: key, eventId: accepted.eventId },
  deliveryId: digest,
  subscriptionFingerprint: digest,
  eventDigest: digest,
  source,
  threadId: envelope.threadId,
  admissionKey: envelope.admissionKey,
  selectedAtMillis: 50,
  state: "selected",
  envelope: null,
  envelopeDigest: null,
  retry: { attempts: 0, nextAttemptAtMillis: 50, lastAttemptAtMillis: null, lastFailure: null },
  receipt: null,
  refusal: null,
};
const prepared = { ...selected, state: "prepared", envelope, envelopeDigest: digest };
const receipt = {
  receiptId: "receipt",
  submissionId: "submission",
  threadId: envelope.threadId,
  queueSequence: 1,
};

const fixture = <A, I>(schema: Schema.Codec<A, I>, encoded: unknown): void => {
  const value = Schema.decodeUnknownSync(schema)(encoded);
  expect(Schema.encodeSync(schema)(value)).toEqual(encoded);
};

describe("Subscription wire fixtures", () => {
  it("preserves persisted registration, event, selection, preparation, and terminal evidence", () => {
    expect.assertions(11);
    fixture(SubscriptionRecord, registration);
    fixture(SubscriptionRecord, { ...registration, state: "consumed", recovery: null });
    fixture(SubscriptionRecord, { ...registration, state: "cancelled", recovery: null });
    fixture(SubscriptionRecord, {
      ...registration,
      recovery: { attempts: 2, nextAttemptAtMillis: null, lastFailure: "github-forbidden" },
    });
    fixture(AcceptedEvent, accepted);
    fixture(PreparedInput, envelope);
    fixture(SubscriptionDelivery, selected);
    fixture(SubscriptionDelivery, prepared);
    fixture(SubscriptionDelivery, { ...prepared, state: "delivered", receipt });
    fixture(SubscriptionDelivery, {
      ...selected,
      state: "refused",
      refusal: { phase: "preparation", code: "expired" },
    });
    fixture(SubscriptionDelivery, {
      ...prepared,
      state: "refused",
      refusal: { phase: "admission", code: "policy" },
    });
  });

  it("keeps acknowledgement and redacted management fixtures distinct from durable records", () => {
    expect.assertions(3);
    fixture(EventAcknowledgement, {
      partition,
      eventId: accepted.eventId,
      acceptedAtMillis: accepted.acceptedAtMillis,
    });
    fixture(SubscriptionSnapshot, {
      key,
      source,
      mode: "once",
      state: "active",
      createdAtMillis: 10,
      expiresAtMillis: 1_000,
      recovery: registration.recovery,
    });
    fixture(SubscriptionDeliverySnapshot, {
      key: selected.key,
      state: "delivered",
      retry: selected.retry,
      receipt,
      refusal: null,
    });
  });

  it("rejects unsupported versions and contradictory lifecycle evidence", () => {
    expect(Schema.is(SubscriptionRecord)({ ...registration, schemaVersion: 2 })).toBe(false);
    expect(Schema.is(AcceptedEvent)({ ...accepted, schemaVersion: 2 })).toBe(false);
    expect(Schema.is(PreparedInput)({ ...envelope, schemaVersion: 2 })).toBe(false);
    expect(Schema.is(SubscriptionDelivery)({ ...selected, schemaVersion: 2 })).toBe(false);
    expect(Schema.is(AcceptedEvent)({ ...accepted, cursor: accepted.cutoff + 1 })).toBe(false);
    expect(Schema.is(SubscriptionDelivery)({ ...selected, state: "prepared" })).toBe(false);
    expect(Schema.is(SubscriptionDelivery)({ ...prepared, state: "delivered" })).toBe(false);
    expect(
      Schema.is(SubscriptionDelivery)({
        ...prepared,
        state: "refused",
        refusal: { phase: "preparation", code: "expired" },
      }),
    ).toBe(false);
    expect(
      Schema.is(SubscriptionRecord)({
        ...registration,
        state: "consumed",
        recovery: null,
        configuration: { ...registration.configuration, mode: "continuous" },
      }),
    ).toBe(false);
    expect(
      Schema.is(SubscriptionRecord)({
        ...registration,
        recovery: { attempts: 1, nextAttemptAtMillis: null, lastFailure: null },
      }),
    ).toBe(false);
  });
});
