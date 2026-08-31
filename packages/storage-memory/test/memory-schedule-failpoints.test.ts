import { AgentId, ThreadId, ReceiptId, SubmissionId } from "@effect-agent/core";
import {
  DefinitionDigests,
  Digest,
  IdempotencyKey,
  Principal,
  QueueSequence,
  Receipt,
  type ScheduleChange,
  ScheduleFailpoint,
  ScheduleFailpointError,
  ScheduleId,
  type ScheduleKey,
  type ScheduleRecord,
  ScheduleStore,
} from "@effect-agent/thread";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Layer, Schema } from "effect";

import { MemoryScheduleStoreLive } from "../src/index.ts";

const principal = Schema.decodeSync(Principal)("memory-schedule-failpoint-principal");
const agentId = Schema.decodeSync(AgentId)("memory-schedule-failpoint-agent");
const threadId = Schema.decodeSync(ThreadId)("memory-schedule-failpoint-thread");
const digest = Schema.decodeSync(Digest)("d".repeat(64));
const occurrenceId = Schema.decodeSync(Digest)("e".repeat(64));
const definitions = DefinitionDigests.make({ agent: digest, model: digest, tools: digest });
const receipt = Receipt.make({
  receiptId: Schema.decodeSync(ReceiptId)("memory-schedule-failpoint-receipt"),
  submissionId: Schema.decodeSync(SubmissionId)("memory-schedule-failpoint-submission"),
  threadId,
  queueSequence: Schema.decodeSync(QueueSequence)(1),
});

const key = (name: string): ScheduleKey => ({
  owner: { tenantId: "memory-schedule-failpoint", ownerId: name },
  scheduleId: Schema.decodeSync(ScheduleId)(name),
});

const record = (name: string, pending: boolean): ScheduleRecord => {
  const recordKey = key(name);
  const base: ScheduleRecord = {
    schemaVersion: 1,
    ...recordKey,
    creationFingerprint: Schema.decodeSync(Digest)("a".repeat(64)),
    createdBy: principal,
    createdAtMillis: 1,
    updatedAtMillis: 1,
    configurationRevision: 1,
    version: 1,
    configuration: {
      timing: { _tag: "At", atMillis: 100 },
      destination: { _tag: "ExistingThread", threadId },
      deliveryPrincipal: principal,
      agentId,
      definitions,
      input: { work: name },
      inputDigest: digest,
    },
    state: "active",
    nextAtMillis: pending ? null : 100,
    pending: null,
    lastReceipt: null,
    lastRefusal: null,
    lastSkippedRange: null,
  };
  if (!pending) return base;
  return {
    ...base,
    pending: {
      envelope: {
        schemaVersion: 1,
        ...recordKey,
        configurationRevision: 1,
        intendedAtMillis: 100,
        preparedAtMillis: 110,
        occurrenceId,
        threadId,
        deliveryPrincipal: principal,
        agentId,
        definitions,
        input: base.configuration.input,
        inputDigest: digest,
        admissionKey: Schema.decodeSync(IdempotencyKey)(`schedule-failpoint:${name}`),
        authorization: { policyId: "policy", decisionId: "decision" },
      },
      retry: {
        attempts: 0,
        nextAttemptAtMillis: 110,
        lastAttemptAtMillis: null,
        lastFailure: null,
      },
    },
  };
};

const pendingEnvelope = (name: string) => {
  const envelope = record(name, true).pending?.envelope;
  if (envelope === undefined) throw new Error("pending fixture is missing its envelope");
  return envelope;
};

interface MutationCase {
  readonly tag: ScheduleChange["_tag"];
  readonly initial: ScheduleRecord;
  readonly change: ScheduleChange;
}

const mutationCases: ReadonlyArray<MutationCase> = [
  {
    tag: "Update",
    initial: record("update", false),
    change: {
      _tag: "Update",
      expectedRevision: 1,
      configuration: {
        ...record("update", false).configuration,
        timing: { _tag: "At", atMillis: 200 },
      },
      nextAtMillis: 200,
      nowMillis: 120,
    },
  },
  {
    tag: "Control",
    initial: record("control", false),
    change: {
      _tag: "Control",
      expectedRevision: 1,
      expectedVersion: 1,
      action: "pause",
      nextAtMillis: 100,
      nowMillis: 120,
      skippedRange: null,
    },
  },
  {
    tag: "Prepare",
    initial: record("prepare", false),
    change: {
      _tag: "Prepare",
      expectedRevision: 1,
      expectedCursor: 100,
      envelope: pendingEnvelope("prepare"),
      nextAtMillis: null,
      skippedRange: null,
      nowMillis: 110,
    },
  },
  {
    tag: "DenyPreparation",
    initial: record("deny-preparation", false),
    change: {
      _tag: "DenyPreparation",
      expectedRevision: 1,
      expectedCursor: 100,
      refusal: {
        atMillis: 110,
        intendedAtMillis: 100,
        occurrenceId,
        phase: "preparation",
        code: "denied",
      },
      nowMillis: 110,
    },
  },
  {
    tag: "Complete",
    initial: record("complete", true),
    change: { _tag: "Complete", occurrenceId, receipt, nowMillis: 120 },
  },
  {
    tag: "Retry",
    initial: record("retry", true),
    change: {
      _tag: "Retry",
      occurrenceId,
      retry: {
        attempts: 1,
        nextAttemptAtMillis: 1_120,
        lastAttemptAtMillis: 120,
        lastFailure: "storage",
      },
      nowMillis: 120,
    },
  },
  {
    tag: "Refuse",
    initial: record("refuse", true),
    change: {
      _tag: "Refuse",
      occurrenceId,
      refusal: {
        atMillis: 120,
        intendedAtMillis: 100,
        occurrenceId,
        phase: "admission",
        code: "refused",
      },
      nowMillis: 120,
    },
  },
];

const layer = (selected: string) =>
  MemoryScheduleStoreLive.pipe(
    Layer.provide(
      Layer.succeed(ScheduleFailpoint, {
        hit: (point) =>
          point === selected ? Effect.fail(ScheduleFailpointError.make({ point })) : Effect.void,
      }),
    ),
  );

describe("MemoryScheduleStore mutation failpoints", () => {
  for (const phase of ["before", "after"] as const) {
    it.effect(`keeps insert ${phase} failure semantics atomic`, () => {
      const point = `schedule:insert:${phase}`;
      const initial = record(`insert-${phase}`, false);
      return Effect.gen(function* () {
        const store = yield* ScheduleStore;
        const failure = yield* store.insert(initial, 10).pipe(Effect.flip);
        expect(failure).toMatchObject({ _tag: "ScheduleFailpointError", point });
        const stored = yield* store.get(key(`insert-${phase}`));
        expect(stored === null).toBe(phase === "before");
      }).pipe(Effect.provide(layer(point)));
    });

    for (const mutation of mutationCases) {
      it.effect(`keeps ${mutation.tag} ${phase} failure semantics atomic`, () => {
        const point = `schedule:${mutation.tag.toLowerCase()}:${phase}`;
        return Effect.gen(function* () {
          const store = yield* ScheduleStore;
          yield* store.insert(mutation.initial, 10);
          const failure = yield* store
            .change(
              { owner: mutation.initial.owner, scheduleId: mutation.initial.scheduleId },
              mutation.change,
            )
            .pipe(Effect.flip);
          expect(failure).toMatchObject({ _tag: "ScheduleFailpointError", point });
          const stored = yield* store.get({
            owner: mutation.initial.owner,
            scheduleId: mutation.initial.scheduleId,
          });
          expect(stored?.version).toBe(phase === "before" ? 1 : 2);
        }).pipe(Effect.provide(layer(point)));
      });
    }
  }

  it.effect("rejects malformed keys, page requests, and changes at the adapter boundary", () =>
    Effect.gen(function* () {
      const store = yield* ScheduleStore;
      const initial = record("boundary", false);
      yield* store.insert(initial, 10);
      const invalidKey = yield* store
        .get({ owner: { ...initial.owner, tenantId: "" }, scheduleId: initial.scheduleId })
        .pipe(Effect.flip);
      expect(invalidKey).toMatchObject({ _tag: "ScheduleStorageError", reason: "corrupt" });
      const invalidPage = yield* store.list({ owner: initial.owner, limit: 101 }).pipe(Effect.flip);
      expect(invalidPage).toMatchObject({ _tag: "ScheduleStorageError", reason: "corrupt" });
      const invalidChange = yield* store
        .change(
          { owner: initial.owner, scheduleId: initial.scheduleId },
          {
            _tag: "Update",
            expectedRevision: 0,
            configuration: initial.configuration,
            nextAtMillis: 100,
            nowMillis: 1,
          },
        )
        .pipe(Effect.flip);
      expect(invalidChange).toMatchObject({ _tag: "ScheduleStorageError", reason: "corrupt" });
      expect((yield* store.get(key("boundary")))?.version).toBe(1);
    }).pipe(Effect.provide(MemoryScheduleStoreLive)),
  );
});
