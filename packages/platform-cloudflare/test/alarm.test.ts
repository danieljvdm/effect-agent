import {
  ApprovalDecisionCommand,
  ResolutionNeverHappened,
  UnknownResolutionCommand,
} from "@effect-agent/session";
import { runDurableObjectAlarm } from "cloudflare:test";
import { Effect } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { CloudflareConversationClient } from "../src/index.ts";
import {
  BOOK_TOOL_CALL_ID,
  approvalDefinition,
  armRuntimeEviction,
  armedEvictionsRemaining,
  armedRuntimeFailures,
  bookDefinition,
  decodeConversationId,
  plannerDefinition,
  submitOptions,
} from "./fixtures.ts";
import {
  allSettled,
  anyInState,
  assertConvergence,
  drainAlarmsUntil,
  laneRows,
  readCanonical,
  runClient,
  scheduledAlarm,
  stubFor,
} from "./harness.ts";

/**
 * Alarm semantics (plan §3, D-P6-2; exit gate 2): the single multiplexed alarm's maintenance
 * pass is idempotent under at-least-once delivery (double-fired alarms change nothing), a
 * typed failure inside the pass REJECTS the delivery so workerd redelivers while the pass's
 * own pre-arm keeps the slot committed, and the alarm invariant — committed nonterminal work
 * implies a committed alarm — holds from admission to settlement.
 */

let laneCounter = 0;
const lane = (label: string): string => `cf-alarm-${label}-${laneCounter++}`;

const submitTo = (
  definition: typeof plannerDefinition | typeof approvalDefinition | typeof bookDefinition,
  conversation: string,
) =>
  runClient(
    Effect.gen(function* () {
      const client = yield* CloudflareConversationClient;
      return yield* client.submit(
        { definition },
        { question: "alarm semantics", ref: conversation },
        submitOptions(conversation, `${conversation}-key`),
      );
    }),
  );

const canonicalFingerprint = async (conversation: string): Promise<string> => {
  const records = await readCanonical(conversation);
  return JSON.stringify(
    records.map((envelope) => ({
      recordId: envelope.record.recordId,
      sequence: envelope.sequence,
      tag: envelope.record.payload._tag,
    })),
  );
};

describe("DC alarm semantics", () => {
  it("double-fired alarms are idempotent on a ready lane: one settlement, no duplicate records", async () => {
    const conversation = lane("ready-double");
    await submitTo(plannerDefinition, conversation);
    // At-least-once delivery: fire the SAME obligation twice back to back.
    await runDurableObjectAlarm(stubFor(conversation)).catch(() => undefined);
    await runDurableObjectAlarm(stubFor(conversation)).catch(() => undefined);
    await drainAlarmsUntil(conversation, allSettled(conversation));
    const settledFingerprint = await canonicalFingerprint(conversation);
    // Extra deliveries on the settled lane are no-ops (the pass cleared the slot; a forced
    // redelivery would still find nothing to do).
    await runDurableObjectAlarm(stubFor(conversation)).catch(() => undefined);
    expect(await canonicalFingerprint(conversation)).toBe(settledFingerprint);
    await assertConvergence(conversation);
  }, 30_000);

  it("double-fired alarms are idempotent on a durably suspended lane", async () => {
    const conversation = lane("suspended-double");
    const receipt = await submitTo(approvalDefinition, conversation);
    await drainAlarmsUntil(conversation, anyInState(conversation, "suspended"));
    const suspendedFingerprint = await canonicalFingerprint(conversation);
    await runDurableObjectAlarm(stubFor(conversation)).catch(() => undefined);
    await runDurableObjectAlarm(stubFor(conversation)).catch(() => undefined);
    // The suspension is durable state, not alarm-driven state: re-delivery changes nothing.
    expect(await canonicalFingerprint(conversation)).toBe(suspendedFingerprint);
    expect((await laneRows(conversation))[0]?.state).toBe("suspended");
    // Only the authorized decision path releases it.
    await runClient(
      Effect.gen(function* () {
        const client = yield* CloudflareConversationClient;
        return yield* client.resolveApproval(
          decodeConversationId(conversation),
          ApprovalDecisionCommand.make({
            submissionId: receipt.submissionId,
            toolCallId: BOOK_TOOL_CALL_ID,
            decision: "approved",
            resolver: "cf-alarm-approver",
            reason: "double-fire idempotency row",
          }),
        );
      }),
    );
    await drainAlarmsUntil(conversation, allSettled(conversation));
    await assertConvergence(conversation);
  }, 30_000);

  it("double-fired alarms are idempotent on a lane blocked by an Unknown Outcome", async () => {
    const conversation = lane("unknown-double");
    armRuntimeEviction(conversation, "tools:after-prepared-append");
    const receipt = await submitTo(bookDefinition, conversation);
    await drainAlarmsUntil(conversation, anyInState(conversation, "unknown"));
    const blockedFingerprint = await canonicalFingerprint(conversation);
    await runDurableObjectAlarm(stubFor(conversation)).catch(() => undefined);
    await runDurableObjectAlarm(stubFor(conversation)).catch(() => undefined);
    // DUR-009: the unresolved ordinary call is never auto-replayed by redelivered alarms.
    expect(await canonicalFingerprint(conversation)).toBe(blockedFingerprint);
    expect((await laneRows(conversation))[0]?.state).toBe("unknown");
    await runClient(
      Effect.gen(function* () {
        const client = yield* CloudflareConversationClient;
        return yield* client.resolveUnknown(
          decodeConversationId(conversation),
          UnknownResolutionCommand.make({
            submissionId: receipt.submissionId,
            toolCallId: BOOK_TOOL_CALL_ID,
            author: "operator",
            reason: "double-fire idempotency row",
            resolution: ResolutionNeverHappened.make(),
          }),
        );
      }),
    );
    await drainAlarmsUntil(conversation, allSettled(conversation));
    await assertConvergence(conversation);
  }, 30_000);

  it("a typed failure inside the pass rejects the delivery and redelivery converges the lane", async () => {
    const conversation = lane("throw-retry");
    // Armed BEFORE the submit: the FIRST delivery (the pool auto-fires due alarms in the
    // background) fails typed — the alarm handler rejects, which is exactly what makes
    // workerd redeliver under at-least-once semantics. Convergence despite the failed
    // delivery, with no client request, is the retry evidence.
    armedRuntimeFailures.set(conversation, "claim:after-claim");
    await submitTo(plannerDefinition, conversation);
    await drainAlarmsUntil(conversation, () =>
      Promise.resolve(!armedRuntimeFailures.has(conversation)),
    );
    await drainAlarmsUntil(conversation, allSettled(conversation));
    await assertConvergence(conversation);
  }, 30_000);

  it("the alarm invariant holds while work is nonterminal and clears once everything settles", async () => {
    const conversation = lane("invariant");
    const receipt = await submitTo(approvalDefinition, conversation);
    // A durably suspended lane is STABLE nonterminal state: passes must keep the slot armed
    // (the slot is only transiently empty while a delivered pass is mid-execution).
    await drainAlarmsUntil(conversation, anyInState(conversation, "suspended"));
    let observedArmed = false;
    for (let round = 0; round < 100 && !observedArmed; round++) {
      observedArmed = (await scheduledAlarm(conversation)) !== null;
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    expect(observedArmed, "a suspended lane must keep a committed alarm").toBe(true);
    await runClient(
      Effect.gen(function* () {
        const client = yield* CloudflareConversationClient;
        return yield* client.resolveApproval(
          decodeConversationId(conversation),
          ApprovalDecisionCommand.make({
            submissionId: receipt.submissionId,
            toolCallId: BOOK_TOOL_CALL_ID,
            decision: "approved",
            resolver: "cf-alarm-approver",
            reason: "invariant row",
          }),
        );
      }),
    );
    await drainAlarmsUntil(conversation, allSettled(conversation));
    // All settled ⇒ the final pass cleared the slot.
    await drainAlarmsUntil(conversation, async () => (await scheduledAlarm(conversation)) === null);
    expect(await scheduledAlarm(conversation)).toBeNull();
  }, 30_000);

  it("the alarm invariant survives an eviction mid-pass: the persisted alarm outlives the incarnation", async () => {
    const conversation = lane("invariant-evict");
    armRuntimeEviction(conversation, "claim:after-claim");
    await submitTo(plannerDefinition, conversation);
    // Wait for the doomed pass (auto-fired or drain-fired) to pre-arm, claim, and die.
    await drainAlarmsUntil(conversation, () =>
      Promise.resolve(armedEvictionsRemaining(conversation) === 0),
    );
    // The alarm that outlives the dead incarnation (its deadline was committed BEFORE the
    // abort) is what converges the lane with no incoming request.
    let observedArmed = false;
    for (let round = 0; round < 100 && !observedArmed; round++) {
      observedArmed = (await scheduledAlarm(conversation)) !== null;
      if (
        !observedArmed &&
        (await laneRows(conversation)).every((row) => row.state === "settled")
      ) {
        // Converged before we sampled the slot: the persisted alarm did its work already.
        observedArmed = true;
      }
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    expect(observedArmed).toBe(true);
    await drainAlarmsUntil(conversation, allSettled(conversation));
    await assertConvergence(conversation);
  }, 30_000);
});
