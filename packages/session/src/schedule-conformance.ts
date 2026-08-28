import { AgentId, ConversationId, ReceiptId, SubmissionId } from "@effect-agent/core";
import { Effect, Result, Schema } from "effect";

import { Receipt } from "./durable-runtime.ts";
import { IdempotencyKey, Principal, QueueSequence } from "./ledger.ts";
import { DefinitionDigests, Digest, type PersistedJson } from "./records.ts";
import {
  ScheduleCapacityError,
  ScheduleId,
  type ScheduleKey,
  type ScheduleRecord,
  ScheduleStore,
  type ScheduleStoreFailure,
} from "./schedule.ts";

export class ScheduleStoreConformanceViolation extends Schema.TaggedError<ScheduleStoreConformanceViolation>()(
  "ScheduleStoreConformanceViolation",
  { caseName: Schema.String, message: Schema.String },
) {}

export type ScheduleStoreConformanceFailure =
  | ScheduleStoreFailure
  | ScheduleStoreConformanceViolation;

export interface ScheduleStoreConformanceCase {
  readonly name: string;
  readonly run: Effect.Effect<void, ScheduleStoreConformanceFailure, ScheduleStore>;
}

const id = Schema.decodeSync(ScheduleId);
const principal = Schema.decodeSync(Principal)("schedule-conformance-principal");
const agentId = Schema.decodeSync(AgentId)("schedule-conformance-agent");
const conversationId = Schema.decodeSync(ConversationId)("schedule-conformance-conversation");
const digest = Schema.decodeSync(Digest)("d".repeat(64));
const definitions = DefinitionDigests.make({ agent: digest, model: digest, tools: digest });

const key = (name: string, ownerId = "owner"): ScheduleKey => ({
  owner: { tenantId: "schedule-conformance-tenant", ownerId },
  scheduleId: id(name),
});

const record = (
  name: string,
  input: PersistedJson = { value: name },
  ownerId = "owner",
): ScheduleRecord => ({
  schemaVersion: 1,
  ...key(name, ownerId),
  creationFingerprint: Schema.decodeSync(Digest)("a".repeat(64)),
  createdBy: principal,
  createdAtMillis: 1,
  updatedAtMillis: 1,
  configurationRevision: 1,
  version: 1,
  configuration: {
    timing: { _tag: "At", atMillis: 100 },
    destination: { _tag: "ExistingConversation", conversationId },
    deliveryPrincipal: principal,
    agentId,
    definitions,
    input,
    inputDigest: digest,
  },
  state: "active",
  nextAtMillis: 100,
  pending: null,
  lastReceipt: null,
  lastRefusal: null,
  lastSkippedRange: null,
});

const pendingRecord = (name: string): ScheduleRecord => {
  const base = record(name);
  const occurrenceId = Schema.decodeSync(Digest)("e".repeat(64));
  return {
    ...base,
    nextAtMillis: null,
    pending: {
      envelope: {
        schemaVersion: 1,
        ...key(name),
        configurationRevision: 1,
        intendedAtMillis: 100,
        preparedAtMillis: 110,
        occurrenceId,
        conversationId,
        deliveryPrincipal: principal,
        agentId,
        definitions,
        input: base.configuration.input,
        inputDigest: digest,
        admissionKey: Schema.decodeSync(IdempotencyKey)(`schedule:${name}`),
        authorization: { policyId: "policy", decisionId: "decision" },
      },
      retry: {
        attempts: 0,
        nextAttemptAtMillis: 120,
        lastAttemptAtMillis: null,
        lastFailure: null,
      },
    },
  };
};

const receipt = Receipt.make({
  receiptId: Schema.decodeSync(ReceiptId)("schedule-conformance-receipt"),
  submissionId: Schema.decodeSync(SubmissionId)("schedule-conformance-submission"),
  conversationId,
  queueSequence: Schema.decodeSync(QueueSequence)(1),
});

const conformanceCase = (
  name: string,
  run: (
    ensure: (
      condition: boolean,
      message: string,
    ) => Effect.Effect<void, ScheduleStoreConformanceViolation>,
  ) => Effect.Effect<void, ScheduleStoreConformanceFailure, ScheduleStore>,
): ScheduleStoreConformanceCase => ({
  name,
  run: run((condition, message) =>
    condition
      ? Effect.void
      : Effect.fail(ScheduleStoreConformanceViolation.make({ caseName: name, message })),
  ).pipe(Effect.withSpan(`ScheduleStoreConformance.${name}`)),
});

const creationReplay = conformanceCase(
  "returns the current record when creation replays after an edit",
  (ensure) =>
    Effect.gen(function* () {
      const store = yield* ScheduleStore;
      const original = record("creation-replay");
      yield* store.insert(original, 10);
      const edited = yield* store.change(key("creation-replay"), {
        _tag: "Update",
        expectedRevision: 1,
        configuration: {
          ...original.configuration,
          input: { value: "edited" },
        },
        nextAtMillis: 200,
        nowMillis: 10,
      });
      const replayed = yield* store.insert(original, 10);
      yield* ensure(replayed.version === edited.version, "creation replay returned stale state");
      yield* ensure(
        replayed.configurationRevision === 2,
        "creation replay lost the edited configuration revision",
      );
      yield* ensure(
        JSON.stringify(replayed.configuration.input) === JSON.stringify({ value: "edited" }),
        "creation replay returned the original input",
      );
    }),
);

const staleCompletion = conformanceCase(
  "makes stale completions and retry regressions full no-ops",
  (ensure) =>
    Effect.gen(function* () {
      const store = yield* ScheduleStore;
      const original = pendingRecord("stale-completion");
      yield* store.insert(original, 10);
      const stale = yield* store.change(key("stale-completion"), {
        _tag: "Complete",
        occurrenceId: Schema.decodeSync(Digest)("f".repeat(64)),
        receipt,
        nowMillis: 130,
      });
      yield* ensure(stale.version === original.version, "stale completion changed the version");
      yield* ensure(stale.pending !== null, "stale completion cleared pending work");
      yield* ensure(stale.lastReceipt === null, "stale completion changed bounded status");

      const advanced = yield* store.change(key("stale-completion"), {
        _tag: "Retry",
        occurrenceId: original.pending?.envelope.occurrenceId ?? digest,
        retry: {
          attempts: 2,
          nextAttemptAtMillis: 500,
          lastAttemptAtMillis: 200,
          lastFailure: "transport",
        },
        nowMillis: 200,
      });
      const regressed = yield* store.change(key("stale-completion"), {
        _tag: "Retry",
        occurrenceId: original.pending?.envelope.occurrenceId ?? digest,
        retry: {
          attempts: 1,
          nextAttemptAtMillis: 300,
          lastAttemptAtMillis: 150,
          lastFailure: "timeout",
        },
        nowMillis: 210,
      });
      yield* ensure(regressed.version === advanced.version, "retry regression changed the version");
      yield* ensure(
        regressed.pending?.retry.nextAttemptAtMillis === 500,
        "retry regression moved the deadline backward",
      );
    }),
);

const isolationAndCloning = conformanceCase(
  "isolates owner pages, prioritizes pending deadlines, and clones writes",
  (ensure) =>
    Effect.gen(function* () {
      const store = yield* ScheduleStore;
      const mutableInput = { value: "before" };
      yield* store.insert(record("z-page", mutableInput), 10);
      mutableInput.value = "after";
      yield* store.insert(record("a-page"), 10);
      yield* store.insert(pendingRecord("pending-first"), 10);
      yield* store.insert(record("foreign", { value: "foreign" }, "other-owner"), 10);

      const page = yield* store.list({ owner: key("ignored").owner, limit: 2 });
      yield* ensure(page.items.length === 2, "owner page returned the wrong item count");
      yield* ensure(page.items[0]?.scheduleId === id("a-page"), "owner page order is unstable");
      yield* ensure(page.next === id("pending-first"), "owner page cursor is not its last item");
      yield* ensure(
        page.items.every((item) => item.owner.ownerId === "owner"),
        "owner page leaked a foreign owner",
      );

      const stored = yield* store.get(key("z-page"));
      yield* ensure(
        JSON.stringify(stored?.configuration.input) === JSON.stringify({ value: "before" }),
        "caller mutation changed the authoritative stored input",
      );
      const due = yield* store.due(150, 10, key("ignored").owner);
      yield* ensure(
        due[0]?.scheduleId === id("a-page") || due[0]?.scheduleId === id("z-page"),
        "due ordering did not use the earliest deadline",
      );
      yield* ensure(
        due.some((item) => item.scheduleId === id("pending-first")),
        "due query omitted pending recovery",
      );
    }),
);

const ownerCapacity = conformanceCase(
  "enforces owner capacity without affecting other owners",
  (ensure) =>
    Effect.gen(function* () {
      const store = yield* ScheduleStore;
      yield* store.insert(record("capacity-one", { value: 1 }, "capacity-owner"), 1);
      const attempted = yield* store
        .insert(record("capacity-two", { value: 2 }, "capacity-owner"), 1)
        .pipe(Effect.result);
      yield* ensure(
        Result.isFailure(attempted) && attempted.failure instanceof ScheduleCapacityError,
        "owner capacity did not fail with ScheduleCapacityError",
      );
      const other = yield* store.insert(
        record("capacity-other", { value: 3 }, "capacity-other"),
        1,
      );
      yield* ensure(other.scheduleId === id("capacity-other"), "capacity leaked across owners");
    }),
);

const transitionFencing = conformanceCase(
  "fences stale preparation and control while preserving frozen pending work",
  (ensure) =>
    Effect.gen(function* () {
      const store = yield* ScheduleStore;

      const prepareOriginal = record("stale-prepare");
      yield* store.insert(prepareOriginal, 10);
      const edited = yield* store.change(key("stale-prepare"), {
        _tag: "Update",
        expectedRevision: 1,
        configuration: {
          ...prepareOriginal.configuration,
          timing: { _tag: "At", atMillis: 200 },
          input: { value: "edited-before-prepare" },
        },
        nextAtMillis: 200,
        nowMillis: 10,
      });
      const staleEnvelope = pendingRecord("stale-prepare").pending?.envelope;
      if (staleEnvelope === undefined) {
        return yield* ScheduleStoreConformanceViolation.make({
          caseName: "fences stale preparation and control while preserving frozen pending work",
          message: "fixture did not contain a pending envelope",
        });
      }
      const stalePrepare = yield* store
        .change(key("stale-prepare"), {
          _tag: "Prepare",
          expectedRevision: 1,
          expectedCursor: 100,
          envelope: staleEnvelope,
          nextAtMillis: null,
          skippedRange: null,
          nowMillis: 110,
        })
        .pipe(Effect.result);
      yield* ensure(
        Result.isFailure(stalePrepare) && stalePrepare.failure._tag === "ScheduleConflict",
        "stale preparation did not conflict",
      );
      yield* ensure(
        JSON.stringify(yield* store.get(key("stale-prepare"))) === JSON.stringify(edited),
        "stale preparation changed the edited record",
      );

      const frozen = pendingRecord("pending-update");
      yield* store.insert(frozen, 10);
      const frozenEnvelope = JSON.stringify(frozen.pending?.envelope);
      const updatedPending = yield* store.change(key("pending-update"), {
        _tag: "Update",
        expectedRevision: 1,
        configuration: {
          ...frozen.configuration,
          timing: { _tag: "At", atMillis: 300 },
          input: { value: "new-configuration" },
        },
        nextAtMillis: 300,
        nowMillis: 20,
      });
      yield* ensure(
        JSON.stringify(updatedPending.pending?.envelope) === frozenEnvelope,
        "configuration update changed the frozen pending envelope",
      );

      const completedOriginal = pendingRecord("stale-control");
      yield* store.insert(completedOriginal, 10);
      const occurrenceId = completedOriginal.pending?.envelope.occurrenceId;
      if (occurrenceId === undefined) {
        return yield* ScheduleStoreConformanceViolation.make({
          caseName: "fences stale preparation and control while preserving frozen pending work",
          message: "fixture did not contain an occurrence identifier",
        });
      }
      const completed = yield* store.change(key("stale-control"), {
        _tag: "Complete",
        occurrenceId,
        receipt,
        nowMillis: 130,
      });
      const staleControl = yield* store
        .change(key("stale-control"), {
          _tag: "Control",
          expectedRevision: 1,
          expectedVersion: 1,
          action: "resume",
          nextAtMillis: 100,
          nowMillis: 140,
          skippedRange: null,
        })
        .pipe(Effect.result);
      yield* ensure(
        Result.isFailure(staleControl) && staleControl.failure._tag === "ScheduleConflict",
        "stale control did not conflict",
      );
      const afterControl = yield* store.get(key("stale-control"));
      yield* ensure(
        afterControl?.nextAtMillis === null,
        "stale control revived a completed one-shot",
      );
      yield* ensure(
        afterControl?.version === completed.version && afterControl.lastReceipt !== null,
        "stale control changed completed state",
      );
    }),
);

/** Adapter-neutral contract cases. Each case uses disjoint owner and Schedule identities. */
export const scheduleStoreConformanceCases: ReadonlyArray<ScheduleStoreConformanceCase> = [
  creationReplay,
  staleCompletion,
  isolationAndCloning,
  ownerCapacity,
  transitionFencing,
];
