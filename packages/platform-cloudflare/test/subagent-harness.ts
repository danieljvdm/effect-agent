import { ToolCallId } from "@effect-agent/core/Identifiers";
import { type Receipt } from "@effect-agent/thread/DurableAgentRuntime";
import { type CanonicalRecordEnvelope, type SubagentStarted } from "@effect-agent/thread/Records";
import {
  childThreadIdFor,
  runIdForSubmission,
  toolCallSettledRecordId,
} from "@effect-agent/thread/RunJournal";
import { SqliteClient } from "@effect/sql-sqlite-do";
import { runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { Effect, Schema } from "effect";
import * as SqlClientService from "effect/unstable/sql/SqlClient";
import { expect } from "vite-plus/test";

import { assertConvergence, laneRows, readCanonical, stubFor } from "./harness.ts";
import {
  PROJECTED_SUMMARY,
  childModelInvocations,
  delegateCallIdFor,
} from "./subagent-fixtures.ts";

/**
 * WP4 cross-Object helpers over the WP3 eviction harness: two-lane alarm drains (a
 * delegation's accepted work spans the parent's Object AND the child's Object, and either
 * lane's persisted alarm may hold the next protocol step), raw probes of the parent-owned
 * reservation and child-settlement marker tables, and the one-established-child assertion
 * bundle every matrix row ends on (the DC twin of the Node crash suite's
 * `assertOneEstablishedChild`).
 */

/** The WP4 namespace: every lane of the delegation matrix lives in SUBAGENTS Objects. */
export const SUBAGENTS = "SUBAGENTS" as const;

const decodeToolCallId = Schema.decodeSync(ToolCallId);

/** The parent's one delegation Tool Call identity on the `ref` lane. */
export const delegateCallOf = (ref: string): ToolCallId => decodeToolCallId(delegateCallIdFor(ref));

/**
 * The derived child Thread of one coordinator Submission — a DIFFERENT Durable Object
 * of the same namespace by the identity rule (`idFromName`), which is the whole point of the
 * WP4 matrix.
 */
export const childThreadOf = (receipt: Receipt, ref: string): string =>
  childThreadIdFor(receipt.submissionId, delegateCallOf(ref));

const sleep = (millis: number) => new Promise((resolve) => setTimeout(resolve, millis));

/**
 * Alarm-only convergence across BOTH Objects of one delegation: each round fires whatever
 * persisted alarm each listed Thread holds (at-least-once; an armed eviction or a typed
 * pass failure rejects the delivery while the pre-armed slot survives for the next round).
 * NO client entry point is ever called — the exit gate's "recovers without an incoming
 * request", now with the accepted work spread over two Durable Objects.
 */
export const drainDelegationUntil = async (
  threads: ReadonlyArray<string>,
  predicate: () => Promise<boolean>,
  options?: { readonly rounds?: number },
): Promise<void> => {
  const rounds = options?.rounds ?? 600;
  let lastDeliveryError: unknown;

  for (let round = 0; round < rounds; round++) {
    if (await predicate()) return;
    for (const thread of threads) {
      try {
        await runDurableObjectAlarm(stubFor(thread, SUBAGENTS));
      } catch (error) {
        // The pass aborted its Object (armed eviction) or failed typed (workerd would retry);
        // either way the pre-armed alarm survives in storage for the next round. The last
        // rejection is kept for the timeout diagnostics only.
        lastDeliveryError = error;
      }
    }
    await sleep(10);
  }
  const lanes: Record<string, unknown> = {};

  for (const thread of threads) {
    const rows = await laneRows(thread, SUBAGENTS);

    const tags = await readCanonical(thread, SUBAGENTS).then(
      (records) => records.map((envelope) => envelope.record.payload._tag),
      (error) => [`<read failed: ${String(error)}>`],
    );

    lanes[thread] = { rows, tags };
  }
  throw new Error(
    `Delegation drain did not converge; lanes: ${JSON.stringify(lanes)}; ` +
      `last alarm delivery rejection: ${String(lastDeliveryError)}`,
  );
};

/** Predicate: every listed lane exists and every Submission on it is settled. */
export const allLanesSettled =
  (...threads: ReadonlyArray<string>) =>
  async (): Promise<boolean> => {
    for (const thread of threads) {
      const rows = await laneRows(thread, SUBAGENTS);

      if (rows.length === 0 || rows.some((row) => row.state !== "settled")) return false;
    }

    return true;
  };

/** Poll module/storage state that changes without any alarm to fire (e.g. a hanging child). */
export const waitFor = async (
  probe: () => Promise<boolean> | boolean,
  what: string,
  rounds = 600,
): Promise<void> => {
  for (let round = 0; round < rounds; round++) {
    if (await probe()) return;
    await sleep(10);
  }
  throw new Error(`Timed out waiting for ${what}`);
};

/**
 * A probe issued right after `ctx.abort()` can land on the dying incarnation and reject with
 * the abort reason; retry with fresh stubs, as any real caller would (the WP3 pattern).
 */
const withAbortedInstanceRetry = async <A>(probe: () => Promise<A>): Promise<A> => {
  let lastError: unknown;

  for (let attempt = 0; attempt < 20; attempt++) {
    try {
      return await probe();
    } catch (error) {
      lastError = error;
      await sleep(10);
    }
  }
  throw lastError;
};

const sqlProbe = <Row extends object>(
  thread: string,
  query: (sql: SqlClientService.SqlClient) => Effect.Effect<ReadonlyArray<Row>, unknown>,
): Promise<ReadonlyArray<Row>> =>
  withAbortedInstanceRetry(() =>
    runInDurableObject(stubFor(thread, SUBAGENTS), (_instance, state) =>
      Effect.runPromise(
        Effect.gen(function* () {
          const sql = yield* SqlClientService.SqlClient;

          return yield* query(sql).pipe(Effect.orDie);
        }).pipe(Effect.provide(SqliteClient.layer({ storage: state.storage }))),
      ),
    ),
  );

/** Child budget reservation statuses straight from the PARENT Object's own storage. */
export const reservationStatuses = (parent: string): Promise<ReadonlyArray<string>> =>
  sqlProbe(
    parent,
    (sql) =>
      sql<{ status: string }>`
        SELECT status FROM effect_agent_child_reservations ORDER BY reservation_id
      `,
  ).then((rows) => rows.map((row) => row.status));

export interface SettlementMarkerRow {
  readonly parent_submission_id: string;
  readonly child_submission_id: string;
}

/**
 * The durable cross-store notification markers `recordChildSettled` wrote into the PARENT
 * Object (the `effect_agent_child_settlements` table of plan §1.2) — the artifact that makes
 * the child→parent wake a durable fact instead of an in-memory promise.
 */
export const settlementMarkers = (parent: string): Promise<ReadonlyArray<SettlementMarkerRow>> =>
  sqlProbe(
    parent,
    (sql) => sql<SettlementMarkerRow>`
      SELECT parent_submission_id, child_submission_id
      FROM effect_agent_child_settlements
      ORDER BY child_submission_id
    `,
  );

/** Every canonical envelope of `thread` whose payload carries this `_tag`. */
export const payloadsOf = (
  records: ReadonlyArray<CanonicalRecordEnvelope>,
  tag: string,
): ReadonlyArray<CanonicalRecordEnvelope> =>
  records.filter((envelope) => envelope.record.payload._tag === tag);

/** The one canonical `SubagentStarted` payload of the parent lane. */
export const startedPayloadOf = async (parent: string): Promise<SubagentStarted> => {
  const records = await readCanonical(parent, SUBAGENTS);
  const payload = payloadsOf(records, "SubagentStarted")[0]?.record.payload;

  if (payload?._tag !== "SubagentStarted") {
    throw new Error(`Expected one SubagentStarted record on ${parent}`);
  }

  return payload;
};

/** The parent's terminal outcome, from its canonical settlement record. */
export const parentOutcomeOf = async (parent: string): Promise<string | undefined> => {
  const records = await readCanonical(parent, SUBAGENTS);
  const payload = payloadsOf(records, "SubmissionSettled")[0]?.record.payload;

  return payload !== undefined && "outcome" in payload && typeof payload.outcome === "string"
    ? payload.outcome
    : undefined;
};

export interface DelegationExpectation {
  /** The coordinator Submission's Receipt. */
  readonly receipt: Receipt;
  /** The Thread-unique ref (also the parent Thread name). */
  readonly ref: string;
  /** Expected parent settlement outcome. */
  readonly outcome: "completed" | "aborted";
  /** Expected EXACT researcher model invocation count across every incarnation. */
  readonly childModelCalls: number;
  /** Expected delegation Tool result payload. */
  readonly delegationResult: { readonly isFailure: boolean; readonly result?: unknown };
}

/**
 * The convergence claims common to every WP4 row (the Node crash suite's
 * `assertOneEstablishedChild` + `assertDelegationSettled`, re-proven across two Objects):
 * exactly one SubagentRequested/Started/Joined record; one child Thread with one
 * lineage record in ITS OWN Object; the recorded start link naming the one admitted child
 * (Receipt included); the delegation Tool Call settled exactly once with the expected
 * projection; the parent-owned reservation released; the durable child-settlement marker
 * present; the researcher model invoked EXACTLY the claimed number of times; and both lanes
 * passing the generic convergence audit (unique record identities, FIFO, one settlement).
 */
export const assertDelegationConverged = async (
  expectation: DelegationExpectation,
): Promise<SubagentStarted> => {
  const parent = expectation.ref;
  const child = childThreadOf(expectation.receipt, expectation.ref);

  const parentRecords = await readCanonical(parent, SUBAGENTS);

  expect(payloadsOf(parentRecords, "SubagentRequested")).toHaveLength(1);
  expect(payloadsOf(parentRecords, "SubagentStarted")).toHaveLength(1);
  expect(payloadsOf(parentRecords, "SubagentJoined")).toHaveLength(1);

  const childRecords = await readCanonical(child, SUBAGENTS);

  expect(payloadsOf(childRecords, "ThreadCreated")).toHaveLength(1);
  expect(payloadsOf(childRecords, "SubagentLineageRecorded")).toHaveLength(1);

  const started = await startedPayloadOf(parent);

  expect(started.childThreadId).toBe(child);

  // The child lane in ITS OWN Object: exactly the one admitted Submission, settled, and its
  // canonical settlement carrying the Receipt the start link recorded (one child Receipt).
  const childLane = await laneRows(child, SUBAGENTS);

  expect(childLane.map((row) => row.submission_id)).toEqual([started.childSubmissionId]);
  expect(childLane[0]?.state).toBe("settled");
  const childSettled = payloadsOf(childRecords, "SubmissionSettled")[0]?.record.payload;

  if (childSettled?._tag !== "SubmissionSettled") {
    throw new Error(`Expected one SubmissionSettled record on ${child}`);
  }
  expect(childSettled.submissionId).toBe(started.childSubmissionId);
  expect(childSettled.receiptId).toBe(started.childReceiptId);

  // The delegation call settled exactly once, atomically joined (SUB-019).
  const runId = runIdForSubmission(expectation.receipt.submissionId);
  const settledRecordId = toolCallSettledRecordId(runId, 1, delegateCallOf(expectation.ref));

  const delegationSettled = parentRecords.filter(
    (envelope) => envelope.record.recordId === settledRecordId,
  );

  expect(delegationSettled).toHaveLength(1);
  const delegationPayload = delegationSettled[0]?.record.payload;

  expect(delegationPayload?._tag).toBe("ToolCallSettled");
  if (delegationPayload?._tag === "ToolCallSettled") {
    expect(delegationPayload.isFailure).toBe(expectation.delegationResult.isFailure);
    if (expectation.delegationResult.result !== undefined) {
      expect(delegationPayload.result).toEqual(expectation.delegationResult.result);
    }
  }

  expect(await parentOutcomeOf(parent)).toBe(expectation.outcome);
  expect(await reservationStatuses(parent)).toEqual(["released"]);
  const markers = await settlementMarkers(parent);

  expect(markers).toEqual([
    {
      parent_submission_id: expectation.receipt.submissionId,
      child_submission_id: started.childSubmissionId,
    },
  ]);
  // NOTE (issue #12): this exact count is a cadence-conditioned expectation,
  // not a durability invariant. Model invocation is honestly at-least-once:
  // an alarm redelivered while the child's turn is in flight (workerd input
  // gates open during non-storage awaits) may legally re-drive the head, with
  // producer-epoch fencing keeping canonical records exactly-once — observed
  // once on a loaded CI runner at 3x drain rounds, where every canonical
  // exactly-once assertion above still held. Under the default drain cadence
  // the count stays exact and still catches unbounded re-execution.
  expect(childModelInvocations(expectation.ref)).toBe(expectation.childModelCalls);

  await assertConvergence(parent, { namespace: SUBAGENTS });
  await assertConvergence(child, { namespace: SUBAGENTS });

  return started;
};

/** The completed-delegation expectation every non-abort row converges on. */
export const completedDelegation = (
  receipt: Receipt,
  ref: string,
  childModelCalls = 1,
): DelegationExpectation => ({
  receipt,
  ref,
  outcome: "completed",
  childModelCalls,
  delegationResult: { isFailure: false, result: { summary: PROJECTED_SUMMARY } },
});
