import { CloudflareThreadClient } from "@effect-agent/platform-cloudflare/cloudflare-thread-client";
import { type DoStorageFailpointLocation } from "@effect-agent/storage-cloudflare/do-storage-error";
import { Effect } from "effect";
import { type Receipt } from "effect-agent/durable-agent-runtime";
import { type DurableRuntimeFailpointLocation } from "effect-agent/durable-failpoint";
import {
  ApprovalDecisionCommand,
  ResolutionNeverHappened,
  UnknownResolutionCommand,
} from "effect-agent/submission-ledger";
import { describe, expect, it } from "vite-plus/test";

import {
  BOOK_TOOL_CALL_ID,
  approvalDefinition,
  armRuntimeEviction,
  armStorageEviction,
  armedEvictionsRemaining,
  bookDefinition,
  decodeThreadId,
  plannerDefinition,
  submitOptions,
} from "./fixtures.ts";
import {
  allSettled,
  anyInState,
  assertConvergence,
  drainAlarmsUntil,
  laneRows,
  runClient,
  runClientExit,
} from "./harness.ts";

/** Real Object retirement at partial append and committed client acknowledgement boundaries. */

let laneCounter = 0;

const lane = (location: string): string =>
  `cf-ev-${location.replaceAll(":", "-")}-${laneCounter++}`;

type Fixture = "planner" | "book" | "approval";

const definitionFor = (fixture: Fixture) => {
  switch (fixture) {
    case "planner":
      return plannerDefinition;
    case "book":
      return bookDefinition;
    case "approval":
      return approvalDefinition;
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

// ---------------------------------------------------------------------------
// Driver A — submit-path rows: the armed location is crossed INSIDE the client's
// `submitEncoded` call, which dies with the incarnation. The alarm pre-armed BEFORE the
// first durable mutation then converges every committed admission with no further request.
// ---------------------------------------------------------------------------

const submitPathRow = async (target: Arm): Promise<void> => {
  const thread = lane(target.location);
  const key = `${thread}-key`;

  arm(thread, target);

  const first = await submitFixtureExit("planner", thread, key);

  expect(first.ok, "the armed eviction must kill the submit call").toBe(false);
  expect(armConsumed(thread), "the armed location must actually have fired").toBe(true);

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
  expectation: "unknown" | "suspended",
): Promise<{ readonly thread: string; readonly receipt: Receipt }> => {
  const thread = lane(target.location);
  const key = `${thread}-key`;

  arm(thread, target);
  const receipt = await submitFixture(fixture, thread, key);

  const predicate = anyInState(thread, expectation);

  await drainAlarmsUntil(thread, predicate);
  expect(armConsumed(thread), "the armed location must actually have fired").toBe(true);

  return { thread, receipt };
};

// ---------------------------------------------------------------------------
// Submit-path rows
// ---------------------------------------------------------------------------

describe("Durable Object retirement — submit path (abort mid-submitEncoded, alarm-only convergence)", () => {
  it(
    "eviction at ledger:admit:after: the accepted admission settles by alarm alone; the same key returns the original Receipt",
    () => submitPathRow({ kind: "storage", location: "ledger:admit:after" }),
    30_000,
  );

  it(
    "eviction at append:after-record-insert: the partial records roll back with the incarnation",
    () => submitPathRow({ kind: "storage", location: "append:after-record-insert" }),
    30_000,
  );
});

// ---------------------------------------------------------------------------
// Client-mutation rows — abort/resolve entry points dying mid-eviction
// ---------------------------------------------------------------------------

describe("Durable Object retirement — committed client acknowledgements", () => {
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
});
