import { Update } from "@effect-agent/core/AgentUpdates";
import {
  AgentId,
  ReceiptId,
  SettlementId,
  SubmissionId,
  ThreadId,
  RunId,
  DelegationId,
} from "@effect-agent/core/Identifiers";
import { WorkerUpdate } from "@effect-agent/core/Worker";
import { DateTime, Effect, Equal, Result, Schema } from "effect";

import { digestJson } from "./Digest.ts";
import { Receipt } from "./DurableAgentRuntime.ts";
import {
  MessageDeliveryStore,
  messageDeliveryCapacity,
  prepareMessageDelivery,
} from "./MessageDelivery.ts";
import { DefinitionDigests, Digest } from "./Records.ts";
import { IdempotencyKey, Principal, QueueSequence, Settlement } from "./SubmissionLedger.ts";

export class MessageDeliveryConformanceViolation extends Schema.TaggedError<MessageDeliveryConformanceViolation>()(
  "MessageDeliveryConformanceViolation",
  { message: Schema.String },
) {}

const verify = (condition: boolean, message: string) =>
  condition ? Effect.void : Effect.fail(MessageDeliveryConformanceViolation.make({ message }));

/** One valid frozen envelope shared by real storage and restart tests. */
export const makeMessageDeliveryFixture = Effect.fn("MessageDeliveryConformance.fixture")(
  function* (messageId = "message", owner = "sender", text = "hello") {
    const digest = Schema.decodeSync(Digest)("a".repeat(64));
    const input = { text };

    return yield* prepareMessageDelivery({
      key: {
        ownerThreadId: Schema.decodeSync(ThreadId)(owner),
        messageId: Schema.decodeSync(IdempotencyKey)(messageId),
      },
      createdAtMillis: 0,
      deadlineAtMillis: 1_000,
      policy: {
        maxAutomaticAttempts: 3,
        attemptTimeoutMillis: 100,
        retryBaseMillis: 10,
        retryMaxMillis: 20,
        settlementPollMillis: 5,
      },
      envelope: {
        schemaVersion: 1,
        threadId: Schema.decodeSync(ThreadId)("receiver"),
        deliveryPrincipal: Schema.decodeSync(Principal)("principal"),
        agentId: Schema.decodeSync(AgentId)("agent"),
        definitions: DefinitionDigests.make({ agent: digest, model: digest, tools: digest }),
        input,
        inputDigest: yield* digestJson(input),
        admissionKey: Schema.decodeSync(IdempotencyKey)(`admit:${owner}:${messageId}`),
        authorization: { policyId: "policy", decisionId: "decision" },
      },
    });
  },
);

export const messageDeliveryFixtureReceipt = Receipt.make({
  threadId: Schema.decodeSync(ThreadId)("receiver"),
  receiptId: Schema.decodeSync(ReceiptId)("receipt"),
  submissionId: Schema.decodeSync(SubmissionId)("submission"),
  queueSequence: Schema.decodeSync(QueueSequence)(0),
});

export const makeWorkerUpdateDeliveryFixture = Effect.fn(
  "MessageDeliveryConformance.updateFixture",
)(function* (id: string) {
  const base = yield* makeMessageDeliveryFixture(id);

  return yield* prepareMessageDelivery({
    key: base.key,
    createdAtMillis: base.createdAtMillis,
    deadlineAtMillis: base.deadlineAtMillis,
    policy: base.policy,
    envelope: {
      ...base.envelope,
      messageAdmission: WorkerUpdate.make({
        _tag: "WorkerUpdate",
        schemaVersion: 1,
        worker: {
          schemaVersion: 1,
          delegationId: Schema.decodeSync(DelegationId)("worker"),
          targetAgentId: Schema.decodeSync(AgentId)("child"),
          threadId: base.key.ownerThreadId,
        },
        update: Update.make({
          schemaVersion: 1,
          agentId: Schema.decodeSync(AgentId)("child"),
          threadId: base.key.ownerThreadId,
          runId: Schema.decodeSync(RunId)("run"),
          updateId: base.key.messageId,
          sequence: 1,
          value: { finding: id },
        }),
      }),
    },
  });
});

/** Every case starts with an empty store; all clocks and expected values are explicit. */
export const messageDeliveryStoreConformanceCases = [
  {
    name: "reserves ordinary delivery capacity when worker updates saturate their separate bound",
    run: Effect.gen(function* () {
      const store = yield* MessageDeliveryStore;
      const capacity = messageDeliveryCapacity(store.limits, true);

      for (let index = 0; index < capacity.pending; index++)
        yield* store.insert(yield* makeWorkerUpdateDeliveryFixture(`update-${index}`));

      const refused = yield* store
        .insert(yield* makeWorkerUpdateDeliveryFixture("overflow"))
        .pipe(Effect.result);

      yield* verify(
        Result.isFailure(refused) &&
          refused.failure._tag === "MessageDeliveryError" &&
          refused.failure.reason === "capacity",
        "Updates must stop at their independent pending bound",
      );
      const terminal = yield* makeMessageDeliveryFixture("terminal");

      yield* verify(
        (yield* store.insert(terminal)).status === "pending",
        "Update pressure must preserve ordinary terminal capacity",
      );
      yield* verify(
        (yield* store.get({
          ...terminal.key,
          messageId: Schema.decodeSync(IdempotencyKey)("overflow"),
        })) === null,
        "Rejected update must not mutate delivery storage",
      );
    }),
  },
  {
    name: "retains predecessor identity and defers ordering waits without spending attempts",
    run: Effect.gen(function* () {
      const store = yield* MessageDeliveryStore;

      const initial = {
        ...(yield* makeMessageDeliveryFixture("later")),
        predecessor: Schema.decodeSync(IdempotencyKey)("earlier"),
      };

      yield* store.insert(initial);

      const deferred = yield* store.change(initial.key, {
        _tag: "Defer",
        expectedVersion: 1,
        nowMillis: 0,
        untilMillis: 100,
      });

      yield* verify(
        deferred.retry.attempts === 0 &&
          deferred.retry.automaticAttempts === 0 &&
          deferred.retry.nextAttemptAtMillis === 100,
        "Ordering waits must remain bounded without exhausting transport retries",
      );
      yield* verify(
        (yield* store.due(99, 10)).length === 0 && (yield* store.due(100, 10)).length === 1,
        "Deferral must advance the persisted wake deadline",
      );

      const changed = yield* store
        .insert({ ...initial, predecessor: Schema.decodeSync(IdempotencyKey)("different") })
        .pipe(Effect.result);

      yield* verify(
        Result.isFailure(changed) &&
          changed.failure._tag === "MessageDeliveryError" &&
          changed.failure.reason === "conflict",
        "A retry cannot replace its ordering predecessor",
      );
    }),
  },
  {
    name: "retains pending, accepted and processed facts with stable duplicate admission",
    run: Effect.gen(function* () {
      const store = yield* MessageDeliveryStore;
      const initial = yield* makeMessageDeliveryFixture();

      yield* store.insert(initial);
      yield* verify(
        Equal.equals(yield* store.due(0, 10), [initial.key]),
        "Pending work must be independently discoverable",
      );

      const claim = yield* store.change(initial.key, {
        _tag: "Claim",
        nowMillis: 0,
        expectedVersion: 1,
      });

      const accepted = yield* store.change(initial.key, {
        _tag: "Accept",
        nowMillis: 1,
        expectedVersion: claim.version,
        receipt: messageDeliveryFixtureReceipt,
      });

      yield* verify(
        accepted.status === "accepted" && accepted.settlement === null,
        "Acceptance must not claim processing",
      );

      const secondClaim = yield* store.change(initial.key, {
        _tag: "Claim",
        nowMillis: 6,
        expectedVersion: accepted.version,
      });

      const settlement = Settlement.make({
        receiptId: messageDeliveryFixtureReceipt.receiptId,
        submissionId: messageDeliveryFixtureReceipt.submissionId,
        settlementId: Schema.decodeSync(SettlementId)("settlement"),
        outcome: "completed",
        settledAt: DateTime.makeUnsafe(7),
      });

      const processed = yield* store.change(initial.key, {
        _tag: "Process",
        nowMillis: 7,
        expectedVersion: secondClaim.version,
        settlement,
      });

      yield* verify(
        processed.status === "processed",
        "Canonical settlement must finish processing",
      );
      yield* verify(
        Equal.equals(yield* store.insert(initial), processed),
        "Replay must preserve terminal evidence",
      );
      yield* verify(
        (yield* store.nextDeadline()) === null,
        "Processed work must release its wake deadline",
      );
    }),
  },
  {
    name: "rejects conflicting identity and scopes cursors and discovery to the source owner",
    run: Effect.gen(function* () {
      const store = yield* MessageDeliveryStore;
      const a = yield* makeMessageDeliveryFixture("a");
      const b = yield* makeMessageDeliveryFixture("b");
      const other = yield* makeMessageDeliveryFixture("a", "other");

      yield* store.insert(a);
      yield* store.insert(b);
      yield* store.insert(other);

      const conflict = yield* store
        .insert(yield* makeMessageDeliveryFixture("a", "sender", "changed"))
        .pipe(Effect.result);

      yield* verify(
        Result.isFailure(conflict) &&
          conflict.failure._tag === "MessageDeliveryError" &&
          conflict.failure.reason === "conflict",
        "Conflicting replay must be refused",
      );
      yield* verify(
        Equal.equals(yield* store.get(a.key), a),
        "Refusal must preserve original input",
      );
      const page = yield* store.list({ ownerThreadId: a.key.ownerThreadId, limit: 1 });

      yield* verify(
        page.items.length === 1 && page.items[0]?.key.messageId === "a" && page.next === "a",
        "First page must remain owner scoped",
      );

      const next = yield* store.list({
        ownerThreadId: a.key.ownerThreadId,
        limit: 1,
        after: a.key.messageId,
      });

      yield* verify(
        next.items.length === 1 && next.items[0]?.key.messageId === "b" && next.next === null,
        "Cursor must be stable",
      );
      yield* verify(
        Equal.equals(yield* store.due(0, 10, other.key.ownerThreadId), [other.key]),
        "Owner-scoped scans must not expose another source",
      );
    }),
  },
  {
    name: "fences stale completion and preserves parked work for explicit recovery",
    run: Effect.gen(function* () {
      const store = yield* MessageDeliveryStore;
      const initial = yield* makeMessageDeliveryFixture();

      yield* store.insert(initial);

      const old = yield* store.change(initial.key, {
        _tag: "Claim",
        nowMillis: 0,
        expectedVersion: 1,
      });

      const current = yield* store.change(initial.key, {
        _tag: "Claim",
        nowMillis: 101,
        expectedVersion: old.version,
      });

      const stale = yield* store
        .change(initial.key, {
          _tag: "Accept",
          nowMillis: 102,
          expectedVersion: old.version,
          receipt: messageDeliveryFixtureReceipt,
        })
        .pipe(Effect.result);

      yield* verify(
        Result.isFailure(stale) &&
          stale.failure._tag === "MessageDeliveryError" &&
          stale.failure.reason === "conflict",
        "Old claims cannot commit",
      );

      const parked = yield* store.change(initial.key, {
        _tag: "Park",
        nowMillis: 102,
        expectedVersion: current.version,
        reason: "exhausted",
      });

      yield* verify(
        (yield* store.nextDeadline()) === null && parked.status === "parked",
        "Parked work must remain inspectable without busy polling",
      );

      const recovered = yield* store.change(initial.key, {
        _tag: "Recover",
        nowMillis: 103,
        expectedVersion: parked.version,
        deadlineAtMillis: 2_000,
      });

      yield* verify(
        recovered.retry.generation === 1 && Equal.equals(recovered.envelope, initial.envelope),
        "Explicit retry must preserve the frozen envelope",
      );
      yield* verify((yield* store.nextDeadline()) === 103, "Recovered work must be discoverable");
    }),
  },
] as const;
