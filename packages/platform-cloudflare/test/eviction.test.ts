import { CloudflareThreadClient } from "@effect-agent/platform-cloudflare/CloudflareThreadClient";
import { type DoStorageFailpointLocation } from "@effect-agent/storage-cloudflare/DoStorageError";
import { layer as doThreadStoreLayer } from "@effect-agent/storage-cloudflare/DoThreadStore";
import { StoreExportCall, encodePortRequest } from "@effect-agent/storage-cloudflare/PortProtocol";
import { type Receipt } from "@effect-agent/thread/DurableAgentRuntime";
import { type DurableRuntimeFailpointLocation } from "@effect-agent/thread/DurableFailpoint";
import {
  AbortCommand,
  ApprovalDecisionCommand,
  ResolutionNeverHappened,
  UnknownResolutionCommand,
} from "@effect-agent/thread/SubmissionLedger";
import {
  ThreadCheckpoint,
  ThreadExportRequest,
  ThreadTailRequest,
  ThreadStore,
  SaveCheckpointRequest,
} from "@effect-agent/thread/ThreadStore";
import { runInDurableObject } from "cloudflare:test";
import { DateTime, Effect } from "effect";
import { describe, expect, it } from "vite-plus/test";

import {
  BOOK_TOOL_CALL_ID,
  TEST_DIGESTS,
  approvalDefinition,
  armRuntimeEviction,
  armStorageEviction,
  armedEvictionsRemaining,
  bookDefinition,
  decodeThreadId,
  itineraryDefinition,
  joinDefinition,
  plannerDefinition,
  releaseGate,
  searchDefinition,
  storageEvictionFailpoint,
  submitOptions,
  supplierCountsFor,
} from "./fixtures.ts";
import {
  allSettled,
  anyInState,
  assertConvergence,
  drainAlarmsUntil,
  laneRows,
  readCanonical,
  runClient,
  runClientExit,
  scheduledAlarm,
  stubFor,
  type SupplierExpectation,
} from "./harness.ts";

/**
 * The DC eviction matrix (plan §3, §4 WP3; exit gates 1–2): every crash-matrix row keeps the
 * SAME failpoint-location name as the Node process-kill harness, but the kill lever is the
 * platform's real failure mode — an armed failpoint calls `ctx.abort()`, destroying the
 * incarnation mid-flight — and convergence is proven by ALARM DELIVERY ALONE
 * (`drainAlarmsUntil` fires only the persisted alarm; no client entry point ever drives
 * recovery). Armed failpoints are consumed exactly once, so the doomed incarnation dies and
 * the fresh one converges, exactly like a production eviction.
 *
 * The `subagent:*` coordinator locations and the `ledger:child-*` storage locations are
 * deliberately NOT here: in DC a parent and its child Threads always live in different
 * Durable Objects, so those rows belong to WP4's cross-Object subagent matrix
 * (`subagents-cross-do.test.ts`), which re-runs them with parent and child evictions.
 */

let laneCounter = 0;

const lane = (location: string): string =>
  `cf-ev-${location.replaceAll(":", "-")}-${laneCounter++}`;

type Fixture = "planner" | "search" | "book" | "approval" | "itinerary" | "join";

const definitionFor = (fixture: Fixture) => {
  switch (fixture) {
    case "planner":
      return plannerDefinition;
    case "search":
      return searchDefinition;
    case "book":
      return bookDefinition;
    case "approval":
      return approvalDefinition;
    case "itinerary":
      return itineraryDefinition;
    case "join":
      return joinDefinition;
  }
};

const submitFixture = (fixture: Fixture, thread: string, key: string) =>
  runClient(
    Effect.gen(function* () {
      const client = yield* CloudflareThreadClient;

      return yield* client.submit(
        { definition: definitionFor(fixture) },
        { question: "converge across eviction", ref: thread },
        submitOptions(thread, key),
      );
    }),
  );

const submitFixtureExit = (fixture: Fixture, thread: string, key: string) =>
  runClientExit(
    Effect.gen(function* () {
      const client = yield* CloudflareThreadClient;

      return yield* client.submit(
        { definition: definitionFor(fixture) },
        { question: "converge across eviction", ref: thread },
        submitOptions(thread, key),
      );
    }),
  );

type Arm =
  | { readonly kind: "storage"; readonly location: DoStorageFailpointLocation }
  | { readonly kind: "runtime"; readonly location: DurableRuntimeFailpointLocation };

const arm = (thread: string, target: Arm): void => {
  if (target.kind === "storage") armStorageEviction(thread, target.location);
  else armRuntimeEviction(thread, target.location);
};

const armConsumed = (thread: string): boolean => armedEvictionsRemaining(thread) === 0;

const modelResponseCount = async (thread: string): Promise<number> => {
  const records = await readCanonical(thread);

  return records.filter((envelope) => envelope.record.payload._tag === "ModelResponseRecorded")
    .length;
};

const settlementOutcome = async (thread: string): Promise<string | undefined> => {
  const records = await readCanonical(thread);

  const settled = records.find((envelope) => envelope.record.payload._tag === "SubmissionSettled")
    ?.record.payload;

  return settled !== undefined && "outcome" in settled && typeof settled.outcome === "string"
    ? settled.outcome
    : undefined;
};

// ---------------------------------------------------------------------------
// Driver A — submit-path rows: the armed location is crossed INSIDE the client's
// `submitEncoded` call, which dies with the incarnation. The alarm pre-armed BEFORE the
// first durable mutation then converges every committed admission with no further request.
// ---------------------------------------------------------------------------

const submitPathRow = async (
  target: Arm,
  options: { readonly nothingDurable?: boolean },
): Promise<void> => {
  const thread = lane(target.location);
  const key = `${thread}-key`;

  arm(thread, target);

  const first = await submitFixtureExit("planner", thread, key);

  expect(first.ok, "the armed eviction must kill the submit call").toBe(false);
  expect(armConsumed(thread), "the armed location must actually have fired").toBe(true);

  if (options.nothingDurable === true) {
    // Nothing was admitted, so nothing is owed: alarms (if any) settle to an empty lane.
    await drainAlarmsUntil(thread, async () => {
      const rows = await laneRows(thread);

      return rows.length === 0 && (await scheduledAlarm(thread)) === null;
    });
    // The client legitimately retries the refused call; the lane then converges normally.
    const receipt = await submitFixture("planner", thread, key);

    await drainAlarmsUntil(thread, allSettled(thread));
    const rows = await laneRows(thread);

    expect(rows.map((row) => row.submission_id)).toEqual([receipt.submissionId]);
    await assertConvergence(thread);

    return;
  }

  // The admission is durable even though the caller never saw a Receipt: the persisted
  // alarm ALONE converges the accepted work (exit gate 1) …
  await drainAlarmsUntil(thread, allSettled(thread));
  await assertConvergence(thread);
  // … and the same idempotency key resumes with the ORIGINAL durable identity.
  const rows = await laneRows(thread);
  const replayed = await submitFixture("planner", thread, key);

  expect(replayed.submissionId).toBe(rows[0]?.submission_id);
};

// ---------------------------------------------------------------------------
// Driver B — pass rows: the armed location is only crossed by the maintenance pass, so it
// is armed BEFORE the submit and the doomed pass aborts mid-recovery/drain. Convergence is
// alarm-only to the row's honest terminal ("settled", or the blocked "unknown"/"suspended"
// states that only the authorized resolution paths may release).
// ---------------------------------------------------------------------------

const passRow = async (
  fixture: Fixture,
  target: Arm,
  expectation: "settled" | "unknown" | "suspended",
  options?: { readonly supplier?: SupplierExpectation },
): Promise<{ readonly thread: string; readonly receipt: Receipt }> => {
  const thread = lane(target.location);
  const key = `${thread}-key`;

  arm(thread, target);
  const receipt = await submitFixture(fixture, thread, key);

  const predicate =
    expectation === "settled" ? allSettled(thread) : anyInState(thread, expectation);

  await drainAlarmsUntil(thread, predicate);
  expect(armConsumed(thread), "the armed location must actually have fired").toBe(true);

  if (expectation === "settled") {
    await assertConvergence(thread, { supplier: options?.supplier });
  }

  return { thread, receipt };
};

const approveBooking = (thread: string, receipt: Receipt, decision: "approved" | "denied") =>
  runClient(
    Effect.gen(function* () {
      const client = yield* CloudflareThreadClient;

      return yield* client.resolveApproval(
        decodeThreadId(thread),
        ApprovalDecisionCommand.make({
          submissionId: receipt.submissionId,
          toolCallId: BOOK_TOOL_CALL_ID,
          decision,
          resolver: "cf-eviction-approver",
          reason: "eviction matrix decision",
        }),
      );
    }),
  );

const resolveNeverHappened = (thread: string, receipt: Receipt) =>
  runClient(
    Effect.gen(function* () {
      const client = yield* CloudflareThreadClient;

      return yield* client.resolveUnknown(
        decodeThreadId(thread),
        UnknownResolutionCommand.make({
          submissionId: receipt.submissionId,
          toolCallId: BOOK_TOOL_CALL_ID,
          author: "operator",
          reason: "eviction matrix resolution",
          resolution: ResolutionNeverHappened.make(),
        }),
      );
    }),
  );

// ---------------------------------------------------------------------------
// Submit-path rows
// ---------------------------------------------------------------------------

describe("DC eviction matrix — submit path (abort mid-submitEncoded, alarm-only convergence)", () => {
  it(
    "eviction at ledger:admit:before leaves nothing durable; the retried key admits fresh",
    () =>
      submitPathRow({ kind: "storage", location: "ledger:admit:before" }, { nothingDurable: true }),
    30_000,
  );

  it(
    "eviction at submit:after-admit: recovery completes materialization; the same key resumes",
    () => submitPathRow({ kind: "runtime", location: "submit:after-admit" }, {}),
    30_000,
  );

  it(
    "eviction at ledger:admit:after: the accepted admission settles by alarm alone; the same key returns the original Receipt",
    () => submitPathRow({ kind: "storage", location: "ledger:admit:after" }, {}),
    30_000,
  );

  it(
    "eviction at materialize:before: the admitted Submission completes materialization by alarm alone",
    () => submitPathRow({ kind: "storage", location: "materialize:before" }, {}),
    30_000,
  );

  it(
    "eviction at materialize:after: the materialized lane completes readiness by alarm alone",
    () => submitPathRow({ kind: "storage", location: "materialize:after" }, {}),
    30_000,
  );

  it(
    "eviction at append:before: the thread-created append is retried idempotently",
    () => submitPathRow({ kind: "storage", location: "append:before" }, {}),
    30_000,
  );

  it(
    "eviction at append:after-batch-insert: the partial batch rolls back with the incarnation",
    () => submitPathRow({ kind: "storage", location: "append:after-batch-insert" }, {}),
    30_000,
  );

  it(
    "eviction at append:after-record-insert: the partial records roll back with the incarnation",
    () => submitPathRow({ kind: "storage", location: "append:after-record-insert" }, {}),
    30_000,
  );

  it(
    "eviction at append:after-tail-update: the committed batch replays idempotently",
    () => submitPathRow({ kind: "storage", location: "append:after-tail-update" }, {}),
    30_000,
  );

  it(
    "eviction at append:after: the committed append is never double-applied",
    () => submitPathRow({ kind: "storage", location: "append:after" }, {}),
    30_000,
  );

  it(
    "eviction at submit:after-materialize: readiness completes by alarm alone",
    () => submitPathRow({ kind: "runtime", location: "submit:after-materialize" }, {}),
    30_000,
  );

  it(
    "eviction at ledger:mark-ready:before: the accepted-but-not-ready Submission completes by alarm alone",
    () => submitPathRow({ kind: "storage", location: "ledger:mark-ready:before" }, {}),
    30_000,
  );

  it(
    "eviction at ledger:mark-ready:after: the same key returns the original Receipt",
    () => submitPathRow({ kind: "storage", location: "ledger:mark-ready:after" }, {}),
    30_000,
  );
});

// ---------------------------------------------------------------------------
// Pass rows — planner (claim/input/turn/terminalize) and their storage twins
// ---------------------------------------------------------------------------

describe("DC eviction matrix — maintenance pass (abort mid-pass, alarm-only convergence)", () => {
  it("eviction at claim:after-claim: recovery re-applies the input exactly once", async () => {
    await passRow("planner", { kind: "runtime", location: "claim:after-claim" }, "settled");
  }, 30_000);

  it("eviction at ledger:claim:before: the unclaimed lane is claimed by a later pass", async () => {
    await passRow("planner", { kind: "storage", location: "ledger:claim:before" }, "settled");
  }, 30_000);

  it("eviction at ledger:claim:after: the orphaned lease expires and a fresh incarnation reclaims at a higher epoch", async () => {
    await passRow("planner", { kind: "storage", location: "ledger:claim:after" }, "settled");
  }, 30_000);

  it("eviction at input:after-canonical-append: the marker is repaired and FIFO holds for the queued Submission", async () => {
    const thread = lane("input:after-canonical-append");

    arm(thread, { kind: "runtime", location: "input:after-canonical-append" });
    await submitFixture("planner", thread, `${thread}-k1`);
    await submitFixture("planner", thread, `${thread}-k2`);
    await drainAlarmsUntil(thread, allSettled(thread));
    expect(armConsumed(thread)).toBe(true);
    expect(await laneRows(thread)).toHaveLength(2);
    await assertConvergence(thread);
  }, 30_000);

  it("eviction at ledger:mark-input-applied:before: the committed input reattaches through prompt coverage", async () => {
    await passRow(
      "planner",
      { kind: "storage", location: "ledger:mark-input-applied:before" },
      "settled",
    );
  }, 30_000);

  it("eviction at ledger:mark-input-applied:after: the durable marker is never re-applied", async () => {
    await passRow(
      "planner",
      { kind: "storage", location: "ledger:mark-input-applied:after" },
      "settled",
    );
  }, 30_000);

  it("eviction at turn:after-canonical-append: a new Attempt resumes from the committed Turn boundary", async () => {
    await passRow(
      "planner",
      { kind: "runtime", location: "turn:after-canonical-append" },
      "settled",
    );
  }, 30_000);

  it("eviction at turn:after-response-append: the declared batch resumes without model re-invocation", async () => {
    const { thread } = await passRow(
      "search",
      { kind: "runtime", location: "turn:after-response-append" },
      "settled",
    );

    // Exactly two model responses ever became canonical: the declared batch resumed from
    // canonical history, and only the post-batch request re-invoked the model.
    expect(await modelResponseCount(thread)).toBe(2);
  }, 30_000);

  it("eviction at turn:after-results-append: committed results are never re-executed", async () => {
    const { thread } = await passRow(
      "search",
      { kind: "runtime", location: "turn:after-results-append" },
      "settled",
    );

    expect(await modelResponseCount(thread)).toBe(2);
  }, 30_000);

  it("eviction at ledger:reserve-settlement:before: the outcome is recomputed and settles exactly once", async () => {
    await passRow(
      "planner",
      { kind: "storage", location: "ledger:reserve-settlement:before" },
      "settled",
    );
  }, 30_000);

  it("eviction at terminalize:after-reserve: recovery appends the EXACT reserved record", async () => {
    const { thread } = await passRow(
      "planner",
      { kind: "runtime", location: "terminalize:after-reserve" },
      "settled",
    );

    expect(await settlementOutcome(thread)).toBe("completed");
  }, 30_000);

  it("eviction at ledger:reserve-settlement:after: the durable reservation finalizes from history", async () => {
    await passRow(
      "planner",
      { kind: "storage", location: "ledger:reserve-settlement:after" },
      "settled",
    );
  }, 30_000);

  it("eviction at terminalize:after-canonical-append: the ledger is finalized from history, the record never rewritten", async () => {
    await passRow(
      "planner",
      { kind: "runtime", location: "terminalize:after-canonical-append" },
      "settled",
    );
  }, 30_000);

  it("eviction at ledger:finalize-settlement:before: the terminalizing lane finalizes by alarm alone", async () => {
    await passRow(
      "planner",
      { kind: "storage", location: "ledger:finalize-settlement:before" },
      "settled",
    );
  }, 30_000);

  it("eviction at ledger:finalize-settlement:after: the settled lane replays the recorded Settlement", async () => {
    await passRow(
      "planner",
      { kind: "storage", location: "ledger:finalize-settlement:after" },
      "settled",
    );
  }, 30_000);
});

// ---------------------------------------------------------------------------
// Pass rows — uncertainty (book), approval suspension, durable steps
// ---------------------------------------------------------------------------

describe("DC eviction matrix — uncertainty, approval, and durable steps", () => {
  it("eviction at tools:after-prepared-append under the default reconciler: Unknown blocks the lane until resolveUnknown", async () => {
    const { thread, receipt } = await passRow(
      "book",
      { kind: "runtime", location: "tools:after-prepared-append" },
      "unknown",
    );

    // The unresolved ordinary call is never auto-replayed: alarm passes grant nothing.
    expect(supplierCountsFor(thread)).toEqual({});
    await resolveNeverHappened(thread, receipt);
    await drainAlarmsUntil(thread, allSettled(thread));
    await assertConvergence(thread, {
      supplier: { ref: thread, counts: { book: 1 } },
    });
  }, 30_000);

  it("eviction at ledger:mark-unknown:before: the open call is re-reconciled and blocks honestly", async () => {
    // The Unknown mark is only crossed by the RECOVERY of a stranded prepared call, so the
    // whole chain arms up front: tools abort first, then the mark eviction.
    const thread = lane("ledger:mark-unknown:before");

    armRuntimeEviction(thread, "tools:after-prepared-append");
    armStorageEviction(thread, "ledger:mark-unknown:before");
    const receipt = await submitFixture("book", thread, `${thread}-key`);

    await drainAlarmsUntil(thread, anyInState(thread, "unknown"));
    expect(armConsumed(thread)).toBe(true);
    await resolveNeverHappened(thread, receipt);
    await drainAlarmsUntil(thread, allSettled(thread));
    await assertConvergence(thread, {
      supplier: { ref: thread, counts: { book: 1 } },
    });
  }, 30_000);

  it("eviction at ledger:mark-unknown:after: the durable Unknown mark is never re-applied", async () => {
    const thread = lane("ledger:mark-unknown:after");

    armRuntimeEviction(thread, "tools:after-prepared-append");
    armStorageEviction(thread, "ledger:mark-unknown:after");
    const receipt = await submitFixture("book", thread, `${thread}-key`);

    await drainAlarmsUntil(thread, anyInState(thread, "unknown"));
    expect(armConsumed(thread)).toBe(true);
    await resolveNeverHappened(thread, receipt);
    await drainAlarmsUntil(thread, allSettled(thread));
    await assertConvergence(thread, {
      supplier: { ref: thread, counts: { book: 1 } },
    });
  }, 30_000);

  it("eviction at ledger:release:before: the blocked lane's claim lapses by lease expiry", async () => {
    // Chain: first strand the prepared call (tools abort), then evict the RECOVERY pass at
    // the release that follows its Unknown mark.
    const thread = lane("ledger:release:before");

    armRuntimeEviction(thread, "tools:after-prepared-append");
    armStorageEviction(thread, "ledger:release:before");
    const receipt = await submitFixture("book", thread, `${thread}-key`);

    await drainAlarmsUntil(thread, anyInState(thread, "unknown"));
    expect(armConsumed(thread)).toBe(true);
    await resolveNeverHappened(thread, receipt);
    await drainAlarmsUntil(thread, allSettled(thread));
    await assertConvergence(thread, {
      supplier: { ref: thread, counts: { book: 1 } },
    });
  }, 30_000);

  it("eviction at ledger:release:after: the released claim is not double-released", async () => {
    const thread = lane("ledger:release:after");

    armRuntimeEviction(thread, "tools:after-prepared-append");
    armStorageEviction(thread, "ledger:release:after");
    const receipt = await submitFixture("book", thread, `${thread}-key`);

    await drainAlarmsUntil(thread, anyInState(thread, "unknown"));
    expect(armConsumed(thread)).toBe(true);
    await resolveNeverHappened(thread, receipt);
    await drainAlarmsUntil(thread, allSettled(thread));
    await assertConvergence(thread, {
      supplier: { ref: thread, counts: { book: 1 } },
    });
  }, 30_000);

  it("eviction at approval:after-request-append: recovery repairs the suspension from history and nothing executes", async () => {
    const { thread, receipt } = await passRow(
      "approval",
      { kind: "runtime", location: "approval:after-request-append" },
      "suspended",
    );

    expect(supplierCountsFor(thread)).toEqual({});
    await approveBooking(thread, receipt, "approved");
    await drainAlarmsUntil(thread, allSettled(thread));
    await assertConvergence(thread, {
      supplier: { ref: thread, counts: { book: 1 } },
    });
  }, 30_000);

  it("eviction at approval:after-suspend: resolveApproval(approved) resumes the declared batch", async () => {
    const { thread, receipt } = await passRow(
      "approval",
      { kind: "runtime", location: "approval:after-suspend" },
      "suspended",
    );

    await approveBooking(thread, receipt, "approved");
    await drainAlarmsUntil(thread, allSettled(thread));
    await assertConvergence(thread, {
      supplier: { ref: thread, counts: { book: 1 } },
    });
  }, 30_000);

  it("eviction at approval:after-suspend: a denial settles failed with the canonical decision", async () => {
    const { thread, receipt } = await passRow(
      "approval",
      { kind: "runtime", location: "approval:after-suspend" },
      "suspended",
    );

    await approveBooking(thread, receipt, "denied");
    await drainAlarmsUntil(thread, allSettled(thread));
    expect(await settlementOutcome(thread)).toBe("failed");
    // The denied handler never executed: no supplier call ever happened.
    expect(supplierCountsFor(thread)).toEqual({});
  }, 30_000);

  it("eviction at ledger:suspend:before: the suspension is repaired from the canonical request", async () => {
    const { thread, receipt } = await passRow(
      "approval",
      { kind: "storage", location: "ledger:suspend:before" },
      "suspended",
    );

    await approveBooking(thread, receipt, "approved");
    await drainAlarmsUntil(thread, allSettled(thread));
    await assertConvergence(thread, {
      supplier: { ref: thread, counts: { book: 1 } },
    });
  }, 30_000);

  it("eviction at ledger:suspend:after: the durable suspension holds until the recorded decision", async () => {
    const { thread, receipt } = await passRow(
      "approval",
      { kind: "storage", location: "ledger:suspend:after" },
      "suspended",
    );

    await approveBooking(thread, receipt, "approved");
    await drainAlarmsUntil(thread, allSettled(thread));
    await assertConvergence(thread, {
      supplier: { ref: thread, counts: { book: 1 } },
    });
  }, 30_000);

  it("eviction at step:after-step-append: step 1 replays from its record while step 2 executes once", async () => {
    const { thread } = await passRow(
      "itinerary",
      { kind: "runtime", location: "step:after-step-append" },
      "settled",
    );

    // The handler is honestly re-entered; each Step's external effect happened exactly once
    // because step 1 replayed from its exactly-once record (durability §10).
    expect(supplierCountsFor(thread)).toEqual({
      "itinerary-enter": 2,
      "reserve-flight": 1,
      "reserve-lodging": 1,
    });
    await assertConvergence(thread, {
      supplier: {
        ref: thread,
        counts: { "itinerary-enter": 2, "reserve-flight": 1, "reserve-lodging": 1 },
      },
    });
  }, 30_000);
});

// ---------------------------------------------------------------------------
// Client-mutation rows — abort/resolve entry points dying mid-eviction
// ---------------------------------------------------------------------------

describe("DC eviction matrix — client mutation entry points", () => {
  it("eviction at abort:after-intent: ready work settles aborted without an Attempt", async () => {
    const thread = lane("abort:after-intent");
    const receipt = await submitFixture("planner", thread, `${thread}-key`);

    arm(thread, { kind: "runtime", location: "abort:after-intent" });

    const aborted = await runClientExit(
      Effect.gen(function* () {
        const client = yield* CloudflareThreadClient;

        return yield* client.abort(
          decodeThreadId(thread),
          AbortCommand.make({
            submissionId: receipt.submissionId,
            author: "cf-eviction-operator",
            reason: "eviction abort row",
          }),
        );
      }),
    );

    expect(aborted.ok).toBe(false);
    expect(armConsumed(thread)).toBe(true);
    await drainAlarmsUntil(thread, allSettled(thread));
    expect(await settlementOutcome(thread)).toBe("aborted");
  }, 30_000);

  it("eviction at ledger:request-abort:after: the durable intent settles the lane aborted", async () => {
    const thread = lane("ledger:request-abort:after");
    const receipt = await submitFixture("planner", thread, `${thread}-key`);

    arm(thread, { kind: "storage", location: "ledger:request-abort:after" });

    const aborted = await runClientExit(
      Effect.gen(function* () {
        const client = yield* CloudflareThreadClient;

        return yield* client.abort(
          decodeThreadId(thread),
          AbortCommand.make({
            submissionId: receipt.submissionId,
            author: "cf-eviction-operator",
            reason: "eviction abort row",
          }),
        );
      }),
    );

    expect(aborted.ok).toBe(false);
    expect(armConsumed(thread)).toBe(true);
    await drainAlarmsUntil(thread, allSettled(thread));
    expect(await settlementOutcome(thread)).toBe("aborted");
  }, 30_000);

  it("an aborted non-head ready submission settles without waiting for the head", async () => {
    // P7 §7(c): settlement order of never-run work is not execution order. The head suspends
    // on a durable approval; a second Submission queues behind it; aborting the second
    // settles it by alarm alone while the head is STILL suspended (DUR-004 bounds execution
    // order; DUR-012 permits settling inactive accepted work without an Attempt).
    const thread = lane("abort-queued-non-head");
    const head = await submitFixture("approval", thread, `${thread}-head`);

    await drainAlarmsUntil(thread, anyInState(thread, "suspended"));

    const queued = await submitFixture("planner", thread, `${thread}-queued`);

    await runClient(
      Effect.gen(function* () {
        const client = yield* CloudflareThreadClient;

        return yield* client.abort(
          decodeThreadId(thread),
          AbortCommand.make({
            submissionId: queued.submissionId,
            author: "cf-eviction-operator",
            reason: "cancelled while queued behind a suspended head",
          }),
        );
      }),
    );
    await drainAlarmsUntil(thread, async () => {
      const rows = await laneRows(thread);

      return rows.some(
        (row) => row.submission_id === queued.submissionId && row.state === "settled",
      );
    });

    // The aborted non-head settled while the head is still durably suspended.
    const rows = await laneRows(thread);

    expect(
      rows.find((row) => row.submission_id === head.submissionId)?.state,
      "the suspended head must remain unsettled",
    ).toBe("suspended");
    const records = await readCanonical(thread);

    const queuedSettlement = records.find(
      (envelope) =>
        envelope.record.payload._tag === "SubmissionSettled" &&
        envelope.record.payload.submissionId === queued.submissionId,
    )?.record.payload;

    expect(
      queuedSettlement !== undefined &&
        "outcome" in queuedSettlement &&
        queuedSettlement.outcome === "aborted",
    ).toBe(true);
    // Never claimed: no canonical input exists for the aborted row.
    expect(
      records.some(
        (envelope) =>
          envelope.record.payload._tag === "UserInputRecorded" &&
          envelope.record.payload.submissionId === queued.submissionId,
      ),
    ).toBe(false);

    // The head then resumes through its own authorized decision and completes normally.
    await approveBooking(thread, head, "approved");
    await drainAlarmsUntil(thread, allSettled(thread));
    await assertConvergence(thread, {
      supplier: { ref: thread, counts: { book: 1 } },
    });
  }, 30_000);

  it("eviction at ledger:request-abort:before: no intent exists, so the accepted work completes", async () => {
    const thread = lane("ledger:request-abort:before");
    const receipt = await submitFixture("planner", thread, `${thread}-key`);

    arm(thread, { kind: "storage", location: "ledger:request-abort:before" });

    const aborted = await runClientExit(
      Effect.gen(function* () {
        const client = yield* CloudflareThreadClient;

        return yield* client.abort(
          decodeThreadId(thread),
          AbortCommand.make({
            submissionId: receipt.submissionId,
            author: "cf-eviction-operator",
            reason: "eviction abort row",
          }),
        );
      }),
    );

    expect(aborted.ok).toBe(false);
    expect(armConsumed(thread)).toBe(true);
    await drainAlarmsUntil(thread, allSettled(thread));
    expect(await settlementOutcome(thread)).toBe("completed");
    await assertConvergence(thread);
  }, 30_000);

  it("eviction at ledger:approval-decision:before: the undecided suspension holds; the re-issued decision resumes", async () => {
    const { thread, receipt } = await passRow(
      "approval",
      { kind: "runtime", location: "approval:after-suspend" },
      "suspended",
    );

    // The suspension row above consumed its arm; now evict the DECISION write itself.
    arm(thread, { kind: "storage", location: "ledger:approval-decision:before" });

    const decided = await runClientExit(
      Effect.gen(function* () {
        const client = yield* CloudflareThreadClient;

        return yield* client.resolveApproval(
          decodeThreadId(thread),
          ApprovalDecisionCommand.make({
            submissionId: receipt.submissionId,
            toolCallId: BOOK_TOOL_CALL_ID,
            decision: "approved",
            resolver: "cf-eviction-approver",
            reason: "eviction decision row",
          }),
        );
      }),
    );

    expect(decided.ok).toBe(false);
    expect(armConsumed(thread)).toBe(true);
    // No decision committed: alarm passes keep the suspension honestly durable.
    await drainAlarmsUntil(thread, anyInState(thread, "suspended"));
    await approveBooking(thread, receipt, "approved");
    await drainAlarmsUntil(thread, allSettled(thread));
    await assertConvergence(thread, {
      supplier: { ref: thread, counts: { book: 1 } },
    });
  }, 30_000);

  it("eviction at ledger:approval-decision:after: the durable decision resumes the lane by alarm alone", async () => {
    const { thread, receipt } = await passRow(
      "approval",
      { kind: "runtime", location: "approval:after-suspend" },
      "suspended",
    );

    arm(thread, { kind: "storage", location: "ledger:approval-decision:after" });

    const decided = await runClientExit(
      Effect.gen(function* () {
        const client = yield* CloudflareThreadClient;

        return yield* client.resolveApproval(
          decodeThreadId(thread),
          ApprovalDecisionCommand.make({
            submissionId: receipt.submissionId,
            toolCallId: BOOK_TOOL_CALL_ID,
            decision: "approved",
            resolver: "cf-eviction-approver",
            reason: "eviction decision row",
          }),
        );
      }),
    );

    expect(decided.ok).toBe(false);
    expect(armConsumed(thread)).toBe(true);
    // The decision committed before the abort: alarms alone resume and settle the lane.
    await drainAlarmsUntil(thread, allSettled(thread));
    await assertConvergence(thread, {
      supplier: { ref: thread, counts: { book: 1 } },
    });
  }, 30_000);

  it("eviction at ledger:unknown-resolution:before: the lane stays blocked; the re-issued resolution releases it", async () => {
    const { thread, receipt } = await passRow(
      "book",
      { kind: "runtime", location: "tools:after-prepared-append" },
      "unknown",
    );

    arm(thread, { kind: "storage", location: "ledger:unknown-resolution:before" });

    const resolved = await runClientExit(
      Effect.gen(function* () {
        const client = yield* CloudflareThreadClient;

        return yield* client.resolveUnknown(
          decodeThreadId(thread),
          UnknownResolutionCommand.make({
            submissionId: receipt.submissionId,
            toolCallId: BOOK_TOOL_CALL_ID,
            author: "operator",
            reason: "eviction resolution row",
            resolution: ResolutionNeverHappened.make(),
          }),
        );
      }),
    );

    expect(resolved.ok).toBe(false);
    expect(armConsumed(thread)).toBe(true);
    await drainAlarmsUntil(thread, anyInState(thread, "unknown"));
    await resolveNeverHappened(thread, receipt);
    await drainAlarmsUntil(thread, allSettled(thread));
    await assertConvergence(thread, {
      supplier: { ref: thread, counts: { book: 1 } },
    });
  }, 30_000);

  it("eviction at ledger:unknown-resolution:after: the durable covering resolution wakes the lane by alarm alone", async () => {
    const { thread, receipt } = await passRow(
      "book",
      { kind: "runtime", location: "tools:after-prepared-append" },
      "unknown",
    );

    arm(thread, { kind: "storage", location: "ledger:unknown-resolution:after" });

    const resolved = await runClientExit(
      Effect.gen(function* () {
        const client = yield* CloudflareThreadClient;

        return yield* client.resolveUnknown(
          decodeThreadId(thread),
          UnknownResolutionCommand.make({
            submissionId: receipt.submissionId,
            toolCallId: BOOK_TOOL_CALL_ID,
            author: "operator",
            reason: "eviction resolution row",
            resolution: ResolutionNeverHappened.make(),
          }),
        );
      }),
    );

    expect(resolved.ok).toBe(false);
    expect(armConsumed(thread)).toBe(true);
    await drainAlarmsUntil(thread, allSettled(thread));
    await assertConvergence(thread, {
      supplier: { ref: thread, counts: { book: 1 } },
    });
  }, 30_000);

  it("eviction at resolve:after-intent: the durable resolution intent survives and converges", async () => {
    const { thread, receipt } = await passRow(
      "book",
      { kind: "runtime", location: "tools:after-prepared-append" },
      "unknown",
    );

    arm(thread, { kind: "runtime", location: "resolve:after-intent" });

    const resolved = await runClientExit(
      Effect.gen(function* () {
        const client = yield* CloudflareThreadClient;

        return yield* client.resolveUnknown(
          decodeThreadId(thread),
          UnknownResolutionCommand.make({
            submissionId: receipt.submissionId,
            toolCallId: BOOK_TOOL_CALL_ID,
            author: "operator",
            reason: "eviction resolution row",
            resolution: ResolutionNeverHappened.make(),
          }),
        );
      }),
    );

    expect(resolved.ok).toBe(false);
    expect(armConsumed(thread)).toBe(true);
    await drainAlarmsUntil(thread, allSettled(thread));
    await assertConvergence(thread, {
      supplier: { ref: thread, counts: { book: 1 } },
    });
  }, 30_000);
});

// ---------------------------------------------------------------------------
// Join and renewal rows — an ACTIVE host (gate-hung model) claiming queued input
// ---------------------------------------------------------------------------

describe("DC eviction matrix — joined input and lease renewal", () => {
  const joinRow = async (target: Arm): Promise<string> => {
    const thread = lane(target.location);
    // S1's model hangs on the gate, so the pass stays active while S2 queues.
    const receipt1 = await submitFixture("join", thread, `${thread}-k1`);

    void receipt1;

    const passPromise = drainAlarmsUntil(thread, allSettled(thread), {
      rounds: 1_200,
    });

    await submitFixture("join", thread, `${thread}-k2`);
    arm(thread, target);
    releaseGate(thread);
    await passPromise;
    expect(armConsumed(thread)).toBe(true);
    expect(await laneRows(thread)).toHaveLength(2);
    await assertConvergence(thread);

    return thread;
  };

  it("eviction at join:after-claim: RevertJoining returns the queued Submission and it joins exactly once", async () => {
    await joinRow({ kind: "runtime", location: "join:after-claim" });
  }, 60_000);

  it("eviction at join:after-canonical-append: RepairJoinMarker reattaches the input without duplication", async () => {
    await joinRow({ kind: "runtime", location: "join:after-canonical-append" });
  }, 60_000);

  it("eviction at ledger:claim-joining:before: the queued Submission stays ready and joins later", async () => {
    await joinRow({ kind: "storage", location: "ledger:claim-joining:before" });
  }, 60_000);

  it("eviction at ledger:claim-joining:after: the durable joining transition is reverted exactly once", async () => {
    await joinRow({ kind: "storage", location: "ledger:claim-joining:after" });
  }, 60_000);

  it("eviction at ledger:mark-joined:before: the joining Submission reverts and rejoins", async () => {
    await joinRow({ kind: "storage", location: "ledger:mark-joined:before" });
  }, 60_000);

  it("eviction at ledger:mark-joined:after: the joined marker is never re-applied", async () => {
    await joinRow({ kind: "storage", location: "ledger:mark-joined:after" });
  }, 60_000);

  it("eviction at ledger:revert-joining:before and after: the revert repair is idempotent across evictions", async () => {
    // Chain: strand a joining Submission (claim-joining:after), then evict the recovery
    // pass INSIDE its RevertJoining repair — first before the revert commits, then after.
    const thread = lane("ledger:revert-joining");

    await submitFixture("join", thread, `${thread}-k1`);

    const passPromise = drainAlarmsUntil(thread, allSettled(thread), {
      rounds: 1_200,
    });

    await submitFixture("join", thread, `${thread}-k2`);
    // The whole chain up front: the recovery pass after each abort runs within milliseconds
    // (due alarms auto-fire), so post-abort arming would always lose the race.
    armStorageEviction(
      thread,
      "ledger:claim-joining:after",
      "ledger:revert-joining:before",
      "ledger:revert-joining:after",
    );
    releaseGate(thread);
    await passPromise;
    expect(armConsumed(thread)).toBe(true);
    expect(await laneRows(thread)).toHaveLength(2);
    await assertConvergence(thread);
  }, 60_000);

  it("eviction at ledger:renew:before: the unrenewed lease lapses and the next incarnation reclaims", async () => {
    const thread = lane("ledger:renew:before");

    arm(thread, { kind: "storage", location: "ledger:renew:before" });
    await submitFixture("join", thread, `${thread}-key`);

    // The hung model keeps the Attempt alive until the renewal cadence crosses the armed
    // location; the abort then kills the incarnation mid-lease.
    const passPromise = drainAlarmsUntil(thread, allSettled(thread), {
      rounds: 1_200,
    });

    await new Promise((resolve) => setTimeout(resolve, 300));
    releaseGate(thread);
    await passPromise;
    expect(armConsumed(thread)).toBe(true);
    await assertConvergence(thread);
  }, 60_000);

  it("eviction at ledger:renew:after: the extended lease still lapses with its incarnation", async () => {
    const thread = lane("ledger:renew:after");

    arm(thread, { kind: "storage", location: "ledger:renew:after" });
    await submitFixture("join", thread, `${thread}-key`);

    const passPromise = drainAlarmsUntil(thread, allSettled(thread), {
      rounds: 1_200,
    });

    await new Promise((resolve) => setTimeout(resolve, 300));
    releaseGate(thread);
    await passPromise;
    expect(armConsumed(thread)).toBe(true);
    await assertConvergence(thread);
  }, 60_000);
});

// ---------------------------------------------------------------------------
// Disposable-derivative rows — checkpoints and export (eviction is harmless by design)
// ---------------------------------------------------------------------------

describe("DC eviction matrix — checkpoints and export", () => {
  const settledLane = async (): Promise<string> => {
    const thread = lane("derivative");

    await submitFixture("planner", thread, `${thread}-key`);
    await drainAlarmsUntil(thread, allSettled(thread));

    return thread;
  };

  it("eviction at export:after-thread-read: the read-only export dies without a trace", async () => {
    const thread = await settledLane();
    const before = await readCanonical(thread);

    armStorageEviction(thread, "export:after-thread-read");

    const request = await Effect.runPromise(
      encodePortRequest(
        StoreExportCall.make({
          request: ThreadExportRequest.make({
            threadId: decodeThreadId(thread),
          }),
        }),
      ),
    );

    const failure = await runInDurableObject(stubFor(thread), (instance) =>
      instance.portCall(request),
    ).then(
      () => undefined,
      (error: unknown) => error,
    );

    expect(failure).toBeDefined();
    expect(armConsumed(thread)).toBe(true);
    const after = await readCanonical(thread);

    expect(after).toEqual(before);
    await assertConvergence(thread);
  }, 30_000);

  const checkpointRow = async (location: DoStorageFailpointLocation): Promise<void> => {
    const thread = await settledLane();
    const before = await readCanonical(thread);

    armStorageEviction(thread, location);

    const attempted = await runInDurableObject(stubFor(thread), async (_instance, state) => {
      // Drive the checkpoint through the adapter with THIS incarnation's abort lever;
      // checkpoints are disposable derivatives, so an eviction here must never matter.
      const program = Effect.gen(function* () {
        const store = yield* ThreadStore;

        const tail = yield* store.inspectTail(
          ThreadTailRequest.make({
            threadId: decodeThreadId(thread),
          }),
        );

        const checkpoint = ThreadCheckpoint.make({
          schemaVersion: 1,
          threadId: decodeThreadId(thread),
          throughSequence: tail.tailSequence,
          tailDigest: tail.tailDigest,
          engineVersion: "cf-eviction-harness",
          agentDefinitionDigest: TEST_DIGESTS.agent,
          modelDigest: TEST_DIGESTS.model,
          toolDigest: TEST_DIGESTS.tools,
          state: {},
          createdAt: DateTime.toUtc(DateTime.makeUnsafe(Date.now())),
        });

        yield* store.checkpoints!.save(SaveCheckpointRequest.make({ checkpoint }));
      }).pipe(
        Effect.provide(
          doThreadStoreLayer({
            storage: state.storage,
            failpoint: storageEvictionFailpoint(state),
          }),
        ),
      );

      return Effect.runPromise(program).then(
        () => "completed" as const,
        () => "aborted" as const,
      );
    }).catch(() => "aborted" as const);

    expect(attempted).toBe("aborted");
    expect(armConsumed(thread)).toBe(true);
    const after = await readCanonical(thread);

    expect(after).toEqual(before);
    await assertConvergence(thread);
  };

  it(
    "eviction at save-checkpoint:before: no checkpoint exists and nothing canonical moved",
    () => checkpointRow("save-checkpoint:before"),
    30_000,
  );

  it(
    "eviction at save-checkpoint:after: the durable checkpoint stays a disposable derivative",
    () => checkpointRow("save-checkpoint:after"),
    30_000,
  );
});
