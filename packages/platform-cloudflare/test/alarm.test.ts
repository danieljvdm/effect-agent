import {
  ApprovalDecisionCommand,
  ResolutionNeverHappened,
  UnknownResolutionCommand,
} from "@effect-agent/session";
import { runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { Effect, Schema } from "effect";
import { describe, expect, it, vi } from "vite-plus/test";

import { CloudflareConversationClient } from "../src/index.ts";
import {
  BOOK_TOOL_CALL_ID,
  approvalDefinition,
  armRuntimeEviction,
  armedEvictionsRemaining,
  armedRuntimeFailures,
  bookDefinition,
  decodeConversationId,
  armMaintenancePause,
  awaitMaintenancePause,
  plannerDefinition,
  releaseMaintenancePause,
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
 * dirty generation keeps the slot committed, stable external waits quiesce, and autonomous work
 * retains bounded rearming through settlement.
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

const MaintenanceGenerationProbe = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  dirty: Schema.BigIntFromString,
  processed: Schema.BigIntFromString,
});

const maintenanceGeneration = (conversation: string) =>
  runInDurableObject(stubFor(conversation), async (_instance, state) =>
    Schema.decodeUnknownSync(MaintenanceGenerationProbe)(
      await state.storage.get<unknown>("effect-agent:conversation-maintenance:v1"),
    ),
  );

describe("DC alarm semantics", () => {
  it("issue #93: a stable approval wait quiesces and a forced caught-up alarm performs no SQL work", async () => {
    const conversation = lane("issue-93-quiescent-approval");
    const receipt = await submitTo(approvalDefinition, conversation);
    await drainAlarmsUntil(conversation, anyInState(conversation, "suspended"));
    await drainAlarmsUntil(conversation, async () => (await scheduledAlarm(conversation)) === null);

    const suspendedFingerprint = await canonicalFingerprint(conversation);
    await runInDurableObject(stubFor(conversation), async (instance, state) => {
      const sql = vi.spyOn(state.storage.sql, "exec").mockImplementation(() => {
        throw new Error("a caught-up maintenance pass must not touch SQLite");
      });
      try {
        await expect(instance.alarm()).resolves.toBeUndefined();
        expect(sql).not.toHaveBeenCalled();
      } finally {
        sql.mockRestore();
      }
    });
    expect(await canonicalFingerprint(conversation)).toBe(suspendedFingerprint);
    expect(await scheduledAlarm(conversation)).toBeNull();

    await runClient(
      Effect.gen(function* () {
        const client = yield* CloudflareConversationClient;
        return yield* client.resolveApproval(
          decodeConversationId(conversation),
          ApprovalDecisionCommand.make({
            submissionId: receipt.submissionId,
            toolCallId: BOOK_TOOL_CALL_ID,
            decision: "approved",
            resolver: "cf-issue-93-approver",
            reason: "durable input must wake a quiescent lane exactly once",
          }),
        );
      }),
    );
    await drainAlarmsUntil(conversation, allSettled(conversation));
    await assertConvergence(conversation);
  }, 30_000);

  it("issue #93: a mutation racing stable-wait cancellation remains dirty and resumes exactly once", async () => {
    const conversation = lane("issue-93-cancel-race");
    armMaintenancePause(conversation, "maintenance:finish:before");
    const receipt = await submitTo(approvalDefinition, conversation);

    await awaitMaintenancePause(conversation, "maintenance:finish:before");
    expect((await laneRows(conversation))[0]?.state).toBe("suspended");

    // The alarm pass has observed the stable wait but has not acknowledged/cancelled yet.
    // This public resolving mutation advances a NEW durable generation before its intent.
    await runClient(
      Effect.gen(function* () {
        const client = yield* CloudflareConversationClient;
        return yield* client.resolveApproval(
          decodeConversationId(conversation),
          ApprovalDecisionCommand.make({
            submissionId: receipt.submissionId,
            toolCallId: BOOK_TOOL_CALL_ID,
            decision: "approved",
            resolver: "cf-issue-93-race-approver",
            reason: "race the pass's stable-wait acknowledgement",
          }),
        );
      }),
    );
    expect(await scheduledAlarm(conversation)).not.toBeNull();

    releaseMaintenancePause(conversation);
    await drainAlarmsUntil(conversation, allSettled(conversation));
    await assertConvergence(conversation);
  }, 30_000);

  it("issue #93: a pass cannot acknowledge a pre-armed generation while its RPC mutation is in flight", async () => {
    const conversation = lane("issue-93-in-flight-mutation");
    const receipt = await submitTo(approvalDefinition, conversation);
    await drainAlarmsUntil(conversation, anyInState(conversation, "suspended"));
    await drainAlarmsUntil(conversation, async () => (await scheduledAlarm(conversation)) === null);

    armMaintenancePause(
      conversation,
      "maintenance:mutation:armed",
      "maintenance:begin:after",
      "maintenance:mutation:finished",
      "maintenance:finish:before",
    );
    const resolution = runClient(
      Effect.gen(function* () {
        const client = yield* CloudflareConversationClient;
        return yield* client.resolveApproval(
          decodeConversationId(conversation),
          ApprovalDecisionCommand.make({
            submissionId: receipt.submissionId,
            toolCallId: BOOK_TOOL_CALL_ID,
            decision: "approved",
            resolver: "cf-issue-93-in-flight-approver",
            reason: "hold the RPC after its pre-arm and before its durable decision",
          }),
        );
      }),
    );
    await awaitMaintenancePause(conversation, "maintenance:mutation:armed");

    // Start a forced pass while the RPC is still between pre-arm and body. It snapshots both the
    // new generation and the active-mutation count, then pauses before recovery.
    const forcedPass = runDurableObjectAlarm(stubFor(conversation)).catch(() => undefined);
    await awaitMaintenancePause(conversation, "maintenance:begin:after");

    // Let the mutation body finish BEFORE the pass observes durable state, but hold the RPC at
    // its body-complete boundary. The pass may see the approval decision, but must conservatively
    // retain this overlapped generation rather than acknowledge a body that was not visible at
    // its snapshot boundary.
    releaseMaintenancePause(conversation, "maintenance:mutation:armed");
    await awaitMaintenancePause(conversation, "maintenance:mutation:finished");
    releaseMaintenancePause(conversation, "maintenance:mutation:finished");
    releaseMaintenancePause(conversation, "maintenance:begin:after");
    await awaitMaintenancePause(conversation, "maintenance:finish:before");
    const generation = await maintenanceGeneration(conversation);
    expect(generation.dirty > generation.processed).toBe(true);
    releaseMaintenancePause(conversation, "maintenance:finish:before");
    await resolution;
    await forcedPass;
    await drainAlarmsUntil(conversation, allSettled(conversation));
    await assertConvergence(conversation);
  }, 30_000);

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
    await drainAlarmsUntil(conversation, async () => (await scheduledAlarm(conversation)) === null);
    const blockedFingerprint = await canonicalFingerprint(conversation);
    await runDurableObjectAlarm(stubFor(conversation)).catch(() => undefined);
    await runDurableObjectAlarm(stubFor(conversation)).catch(() => undefined);
    // DUR-009: the unresolved ordinary call is never auto-replayed by redelivered alarms.
    expect(await canonicalFingerprint(conversation)).toBe(blockedFingerprint);
    expect((await laneRows(conversation))[0]?.state).toBe("unknown");
    expect(
      await scheduledAlarm(conversation),
      "AwaitUnknownResolution must quiesce (#93)",
    ).toBeNull();
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

  it("the alarm invariant quiesces an external wait and its resolving mutation restores liveness", async () => {
    const conversation = lane("invariant");
    const receipt = await submitTo(approvalDefinition, conversation);
    // A durably suspended lane is a stable externally-driven wait: elapsed time is not work.
    await drainAlarmsUntil(conversation, anyInState(conversation, "suspended"));
    let observedCleared = false;
    for (let round = 0; round < 100 && !observedCleared; round++) {
      observedCleared = (await scheduledAlarm(conversation)) === null;
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    expect(observedCleared, "a suspended lane must quiesce its autonomous alarm").toBe(true);
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
