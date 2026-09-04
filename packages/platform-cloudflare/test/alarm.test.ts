import { CloudflareThreadClient } from "@effect-agent/platform-cloudflare/CloudflareThreadClient";
import {
  AbortCommand,
  ApprovalDecisionCommand,
  ResolutionNeverHappened,
  UnknownResolutionCommand,
  RecoverySnapshotRequest,
  SubmissionLedger,
} from "@effect-agent/thread/SubmissionLedger";
import { runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { Cause, Clock, Deferred, Effect, Exit, Fiber, Schema } from "effect";
import { DurableObject } from "effect-cf";
import { TestClock } from "effect/testing";
import { describe, expect, it, vi } from "vite-plus/test";

import {
  BOOK_TOOL_CALL_ID,
  approvalDefinition,
  armRuntimeEviction,
  armStorageEviction,
  armedEvictionsRemaining,
  armedRuntimeFailures,
  bookDefinition,
  decodeThreadId,
  armMaintenancePause,
  awaitMaintenancePause,
  plannerDefinition,
  lostBookReplies,
  supplierCountsFor,
  releaseMaintenancePause,
  submitOptions,
  alarmAttemptHolds,
  maintenanceClocks,
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
  thread: string,
  key = `${thread}-key`,
) =>
  runClient(
    Effect.gen(function* () {
      const client = yield* CloudflareThreadClient;

      return yield* client.submit(
        { definition },
        { question: "alarm semantics", ref: thread },
        submitOptions(thread, key),
      );
    }),
  );

const canonicalFingerprint = async (thread: string): Promise<string> => {
  const records = await readCanonical(thread);

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

const maintenanceGeneration = (thread: string) =>
  runInDurableObject(stubFor(thread), async (_instance, state) =>
    Schema.decodeUnknownSync(MaintenanceGenerationProbe)(
      await state.storage.get<unknown>("effect-agent:thread-maintenance:v1"),
    ),
  );

describe("DC alarm semantics", () => {
  // Regression: https://github.com/danieljvdm/effect-agent/commit/e6407479ae233527685928bead040dbfe5153a22
  it("returns after one head Attempt and leaves later FIFO work armed for another event", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const thread = lane("one-head");

        // Native Promise continuations keep each waiter in its own Durable Object event.
        const makeHold = () => {
          let resolveEntered!: () => void;
          let resolveReleased!: () => void;
          let resolveFinished!: () => void;
          let didEnter = false;

          const entered = new Promise<void>((resolve) => {
            resolveEntered = resolve;
          });

          const released = new Promise<void>((resolve) => {
            resolveReleased = resolve;
          });

          const finished = new Promise<void>((resolve) => {
            resolveFinished = resolve;
          });

          return {
            entered,
            release: () => resolveReleased(),
            finish: () => (didEnter ? finished : Promise.resolve()),
            hold: {
              entered: Effect.sync(() => {
                didEnter = true;
                resolveEntered();
              }),
              release: Effect.promise(() => released),
              finished: Effect.sync(() => resolveFinished()),
            },
          };
        };

        const head = makeHold();
        const follower = makeHold();
        let passFinished = Promise.resolve();

        // Future virtual time keeps native automatic alarms from racing explicit deliveries.
        yield* TestClock.setTime(Date.now() + 86_400_000);
        maintenanceClocks.set(thread, yield* Clock.Clock);
        alarmAttemptHolds.set(thread, {
          location: "terminalize:after-canonical-append",
          ...head.hold,
        });
        yield* Effect.addFinalizer(() =>
          Effect.promise(async () => {
            alarmAttemptHolds.delete(thread);
            head.release();
            follower.release();
            await passFinished;
            await head.finish();
            await follower.finish();
            maintenanceClocks.delete(thread);
          }),
        );
        const first = yield* Effect.promise(() => submitTo(plannerDefinition, thread));

        const pass = yield* Effect.promise(() => {
          const promise = runInDurableObject(stubFor(thread), (instance) =>
            Promise.resolve(instance.alarm()),
          );

          // Observe native completion even if the waiting child fiber is interrupted.
          passFinished = promise.then(
            () => undefined,
            () => undefined,
          );

          return promise;
        }).pipe(Effect.forkChild);

        yield* Effect.promise(() => head.entered);
        // The head has completed its model and join seams. This follower needs its own Attempt.
        // Native alarms may redeliver immediately. Hold the follower so only a separate
        // event can own it; the first pass must return without waiting for this resource.
        alarmAttemptHolds.set(thread, {
          location: "claim:after-claim",
          ...follower.hold,
        });

        const second = yield* Effect.promise(() =>
          submitTo(plannerDefinition, thread, `${thread}-next`),
        );

        head.release();
        yield* Fiber.join(pass);
        const rows = yield* Effect.promise(() => laneRows(thread));

        expect(rows[0]).toMatchObject({ submission_id: first.submissionId, state: "settled" });
        expect(rows[1]?.submission_id).toBe(second.submissionId);
        expect(rows[1]?.state).not.toBe("settled");
        const generation = yield* Effect.promise(() => maintenanceGeneration(thread));

        expect(generation.dirty > generation.processed).toBe(true);
        expect(yield* Effect.promise(() => scheduledAlarm(thread))).not.toBeNull();
        follower.release();
        yield* Effect.promise(() =>
          runInDurableObject(stubFor(thread), (instance) => Promise.resolve(instance.alarm())),
        );
        expect((yield* Effect.promise(() => laneRows(thread))).map((row) => row.state)).toEqual([
          "settled",
          "settled",
        ]);
        yield* Effect.promise(() => assertConvergence(thread));
      }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
    ));

  it("interrupts an overlong alarm, closes scoped work and preserves its dirty retry obligation", () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const thread = lane("event-deadline");
        const entered = yield* Deferred.make<void>();
        const finished = yield* Deferred.make<void>();

        yield* TestClock.setTime(Date.now() + 86_400_000);
        maintenanceClocks.set(thread, yield* Clock.Clock);
        alarmAttemptHolds.set(thread, {
          location: "claim:after-claim",
          entered: Deferred.succeed(entered, undefined).pipe(Effect.asVoid),
          finished: Deferred.succeed(finished, undefined).pipe(Effect.asVoid),
        });
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            maintenanceClocks.delete(thread);
            alarmAttemptHolds.delete(thread);
          }),
        );

        const initializedAlarm = yield* Effect.promise(() =>
          runInDurableObject(stubFor(thread), async (instance, state) => {
            await instance[DurableObject.RunSymbol](Effect.void);

            return state.storage.getAlarm();
          }),
        );

        expect(initializedAlarm).toBeGreaterThanOrEqual(yield* Clock.currentTimeMillis);
        const receipt = yield* Effect.promise(() => submitTo(plannerDefinition, thread));

        const pass = yield* Effect.tryPromise({
          try: () =>
            runInDurableObject(stubFor(thread), (instance) => Promise.resolve(instance.alarm())),
          catch: (cause) => String(cause),
        }).pipe(Effect.exit, Effect.forkChild);

        yield* Deferred.await(entered);
        yield* TestClock.adjust("14 minutes");
        const exit = yield* Fiber.join(pass);

        expect(Exit.isFailure(exit) ? Cause.pretty(exit.cause) : "success").toContain(
          "14 minute deadline",
        );
        expect(yield* Deferred.isDone(finished)).toBe(true);
        const generation = yield* Effect.promise(() => maintenanceGeneration(thread));

        expect(generation.dirty > generation.processed).toBe(true);
        expect(yield* Effect.promise(() => scheduledAlarm(thread))).not.toBeNull();

        const snapshot = yield* Effect.promise(() =>
          runInDurableObject(stubFor(thread), (instance) =>
            instance[DurableObject.RunSymbol](
              Effect.gen(function* () {
                const ledger = yield* SubmissionLedger;

                return yield* ledger.loadRecoverySnapshot(
                  RecoverySnapshotRequest.make({ submissionId: receipt.submissionId }),
                );
              }),
            ),
          ),
        );

        expect(snapshot.ownership).toBeUndefined();
        const before = yield* Effect.promise(() => readCanonical(thread));

        expect(before.some(({ record }) => record.payload._tag === "SubmissionSettled")).toBe(
          false,
        );
        // The same live incarnation can claim again; no lease wait or policy failure is needed.
        yield* Effect.promise(() =>
          runInDurableObject(stubFor(thread), (instance) => Promise.resolve(instance.alarm())),
        );
        yield* Effect.promise(() => assertConvergence(thread));
      }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
    ));

  it("issue #93: a stable approval wait quiesces and a forced caught-up alarm performs no SQL work", async () => {
    const thread = lane("issue-93-quiescent-approval");
    const receipt = await submitTo(approvalDefinition, thread);

    await drainAlarmsUntil(thread, anyInState(thread, "suspended"));
    await submitTo(plannerDefinition, thread, `${thread}-follower`);
    await drainAlarmsUntil(thread, async () => (await scheduledAlarm(thread)) === null);

    const suspendedFingerprint = await canonicalFingerprint(thread);

    await runInDurableObject(stubFor(thread), async (instance, state) => {
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
    expect(await canonicalFingerprint(thread)).toBe(suspendedFingerprint);
    expect(await scheduledAlarm(thread)).toBeNull();

    await runClient(
      Effect.gen(function* () {
        const client = yield* CloudflareThreadClient;

        return yield* client.resolveApproval(
          decodeThreadId(thread),
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
    await drainAlarmsUntil(thread, allSettled(thread));
    await assertConvergence(thread);
  }, 30_000);

  it("issue #93: a mutation racing stable-wait cancellation remains dirty and resumes exactly once", async () => {
    const thread = lane("issue-93-cancel-race");

    armMaintenancePause(thread, "maintenance:finish:before");
    const receipt = await submitTo(approvalDefinition, thread);

    await awaitMaintenancePause(thread, "maintenance:finish:before");
    expect((await laneRows(thread))[0]?.state).toBe("suspended");

    // The alarm pass has observed the stable wait but has not acknowledged/cancelled yet.
    // This public resolving mutation advances a NEW durable generation before its intent.
    await runClient(
      Effect.gen(function* () {
        const client = yield* CloudflareThreadClient;

        return yield* client.resolveApproval(
          decodeThreadId(thread),
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
    expect(await scheduledAlarm(thread)).not.toBeNull();

    releaseMaintenancePause(thread);
    await drainAlarmsUntil(thread, allSettled(thread));
    await assertConvergence(thread);
  }, 30_000);

  it("issue #93: a pass cannot acknowledge a pre-armed generation while its RPC mutation is in flight", async () => {
    const thread = lane("issue-93-in-flight-mutation");
    const receipt = await submitTo(approvalDefinition, thread);

    await drainAlarmsUntil(thread, anyInState(thread, "suspended"));
    await drainAlarmsUntil(thread, async () => (await scheduledAlarm(thread)) === null);

    armMaintenancePause(
      thread,
      "maintenance:mutation:armed",
      "maintenance:begin:after",
      "maintenance:mutation:finished",
      "maintenance:finish:before",
      "maintenance:finish:after",
    );

    const resolution = runClient(
      Effect.gen(function* () {
        const client = yield* CloudflareThreadClient;

        return yield* client.resolveApproval(
          decodeThreadId(thread),
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

    await awaitMaintenancePause(thread, "maintenance:mutation:armed");

    // Start a forced pass while the RPC is still between pre-arm and body. It snapshots both the
    // new generation and the active-mutation count, then pauses before recovery.
    const forcedPass = runDurableObjectAlarm(stubFor(thread)).catch(() => undefined);

    await awaitMaintenancePause(thread, "maintenance:begin:after");

    // Let the mutation body finish BEFORE the pass observes durable state, but hold the RPC at
    // its body-complete boundary. The pass may see the approval decision, but must conservatively
    // retain this overlapped generation rather than acknowledge a body that was not visible at
    // its snapshot boundary.
    releaseMaintenancePause(thread, "maintenance:mutation:armed");
    await awaitMaintenancePause(thread, "maintenance:mutation:finished");
    releaseMaintenancePause(thread, "maintenance:mutation:finished");
    releaseMaintenancePause(thread, "maintenance:begin:after");
    await awaitMaintenancePause(thread, "maintenance:finish:before");
    releaseMaintenancePause(thread, "maintenance:finish:before");
    await awaitMaintenancePause(thread, "maintenance:finish:after");
    const generation = await maintenanceGeneration(thread);

    expect(generation.dirty > generation.processed).toBe(true);
    expect(await scheduledAlarm(thread)).not.toBeNull();
    releaseMaintenancePause(thread, "maintenance:finish:after");
    await resolution;
    await forcedPass;
    await drainAlarmsUntil(thread, allSettled(thread));
    await assertConvergence(thread);
  }, 30_000);

  it("double-fired alarms are idempotent on a ready lane: one settlement, no duplicate records", async () => {
    const thread = lane("ready-double");

    await submitTo(plannerDefinition, thread);
    // At-least-once delivery: fire the SAME obligation twice back to back.
    await runDurableObjectAlarm(stubFor(thread)).catch(() => undefined);
    await runDurableObjectAlarm(stubFor(thread)).catch(() => undefined);
    await drainAlarmsUntil(thread, allSettled(thread));
    const settledFingerprint = await canonicalFingerprint(thread);

    // Extra deliveries on the settled lane are no-ops (the pass cleared the slot; a forced
    // redelivery would still find nothing to do).
    await runDurableObjectAlarm(stubFor(thread)).catch(() => undefined);
    expect(await canonicalFingerprint(thread)).toBe(settledFingerprint);
    await assertConvergence(thread);
  }, 30_000);

  it("double-fired alarms are idempotent on a durably suspended lane", async () => {
    const thread = lane("suspended-double");
    const receipt = await submitTo(approvalDefinition, thread);

    await drainAlarmsUntil(thread, anyInState(thread, "suspended"));
    const suspendedFingerprint = await canonicalFingerprint(thread);

    await runDurableObjectAlarm(stubFor(thread)).catch(() => undefined);
    await runDurableObjectAlarm(stubFor(thread)).catch(() => undefined);
    // The suspension is durable state, not alarm-driven state: re-delivery changes nothing.
    expect(await canonicalFingerprint(thread)).toBe(suspendedFingerprint);
    expect((await laneRows(thread))[0]?.state).toBe("suspended");
    // Only the authorized decision path releases it.
    await runClient(
      Effect.gen(function* () {
        const client = yield* CloudflareThreadClient;

        return yield* client.resolveApproval(
          decodeThreadId(thread),
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
    await drainAlarmsUntil(thread, allSettled(thread));
    await assertConvergence(thread);
  }, 30_000);

  it("double-fired alarms are idempotent on a lane blocked by an Unknown Outcome", async () => {
    const thread = lane("unknown-double");

    armRuntimeEviction(thread, "tools:after-prepared-append");
    const receipt = await submitTo(bookDefinition, thread);

    await drainAlarmsUntil(thread, anyInState(thread, "unknown"));
    await submitTo(plannerDefinition, thread, `${thread}-follower`);
    await drainAlarmsUntil(thread, async () => (await scheduledAlarm(thread)) === null);
    const blockedFingerprint = await canonicalFingerprint(thread);

    await runDurableObjectAlarm(stubFor(thread)).catch(() => undefined);
    await runDurableObjectAlarm(stubFor(thread)).catch(() => undefined);
    // DUR-009: the unresolved ordinary call is never auto-replayed by redelivered alarms.
    expect(await canonicalFingerprint(thread)).toBe(blockedFingerprint);
    expect((await laneRows(thread))[0]?.state).toBe("unknown");
    expect(await scheduledAlarm(thread), "AwaitUnknownResolution must quiesce (#93)").toBeNull();
    await runClient(
      Effect.gen(function* () {
        const client = yield* CloudflareThreadClient;

        return yield* client.resolveUnknown(
          decodeThreadId(thread),
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
    await drainAlarmsUntil(thread, allSettled(thread));
    await assertConvergence(thread);
  }, 30_000);

  it.each([
    undefined,
    "abort:after-intent",
    "terminalize:after-reserve",
    "terminalize:after-canonical-append",
  ] as const)(
    "authorized abort releases an unknown head after lost external reply (eviction=%s)",
    async (eviction) => {
      const thread = lane(`unknown-abort-${eviction ?? "none"}`);

      lostBookReplies.add(thread);
      // Real eviction after recovery has persisted uncertainty. No fake clock races automatic
      // alarms: both the reply loss and eviction are armed before admission.
      armStorageEviction(thread, "ledger:mark-unknown:after");
      const receipt = await submitTo(bookDefinition, thread);

      await drainAlarmsUntil(thread, anyInState(thread, "unknown"));
      const follower = await submitTo(plannerDefinition, thread, `${thread}-follower`);

      expect(supplierCountsFor(thread)).toEqual({ book: 1 });

      const unknownBefore = (await readCanonical(thread)).filter(
        ({ record }) => record.payload._tag === "ToolCallUnknown",
      );

      expect(unknownBefore).toHaveLength(1);
      if (eviction !== undefined) armRuntimeEviction(thread, eviction);

      const command = AbortCommand.make({
        submissionId: receipt.submissionId,
        author: "authorized-operator",
        reason: "stop this submission; the external outcome is still uncertain",
      });

      const accepted = await runClientExit(
        Effect.gen(function* () {
          const client = yield* CloudflareThreadClient;

          return yield* client.abort(decodeThreadId(thread), command);
        }),
      );

      expect(accepted.ok).toBe(eviction !== "abort:after-intent");
      await drainAlarmsUntil(thread, allSettled(thread));
      await assertConvergence(thread, {
        supplier: { ref: thread, counts: { book: 1 } },
      });
      expect(armedEvictionsRemaining(thread)).toBe(0);
      const records = await readCanonical(thread);

      expect(records.filter(({ record }) => record.payload._tag === "ToolCallUnknown")).toEqual(
        unknownBefore,
      );
      expect(records.filter(({ record }) => record.payload._tag === "ToolCallResolved")).toEqual(
        [],
      );
      expect(records.filter(({ record }) => record.payload._tag === "ToolCallSettled")).toEqual([]);
      expect(
        records
          .filter(({ record }) => record.payload._tag === "AbortRequested")
          .map(({ record }) => record.payload),
      ).toEqual([
        expect.objectContaining({
          author: command.author,
          reason: command.reason,
          submissionId: receipt.submissionId,
        }),
      ]);

      const outcomes = await runClient(
        Effect.gen(function* () {
          const client = yield* CloudflareThreadClient;

          return [yield* client.awaitSettlement(receipt), yield* client.awaitSettlement(follower)];
        }),
      );

      expect(outcomes.map(({ outcome }) => outcome)).toEqual(["aborted", "completed"]);
    },
    30_000,
  );

  it("a typed failure inside the pass rejects the delivery and redelivery converges the lane", async () => {
    const thread = lane("throw-retry");

    // Armed BEFORE the submit: the FIRST delivery (the pool auto-fires due alarms in the
    // background) fails typed — the alarm handler rejects, which is exactly what makes
    // workerd redeliver under at-least-once semantics. Convergence despite the failed
    // delivery, with no client request, is the retry evidence.
    armedRuntimeFailures.set(thread, "claim:after-claim");
    await submitTo(plannerDefinition, thread);
    await drainAlarmsUntil(thread, () => Promise.resolve(!armedRuntimeFailures.has(thread)));
    await drainAlarmsUntil(thread, allSettled(thread));
    await assertConvergence(thread);
  }, 30_000);

  it("the alarm invariant quiesces an external wait and its resolving mutation restores liveness", async () => {
    const thread = lane("invariant");
    const receipt = await submitTo(approvalDefinition, thread);

    // A durably suspended lane is a stable externally-driven wait: elapsed time is not work.
    await drainAlarmsUntil(thread, anyInState(thread, "suspended"));
    let observedCleared = false;

    for (let round = 0; round < 100 && !observedCleared; round++) {
      observedCleared = (await scheduledAlarm(thread)) === null;
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    expect(observedCleared, "a suspended lane must quiesce its autonomous alarm").toBe(true);
    await runClient(
      Effect.gen(function* () {
        const client = yield* CloudflareThreadClient;

        return yield* client.resolveApproval(
          decodeThreadId(thread),
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
    await drainAlarmsUntil(thread, allSettled(thread));
    // All settled ⇒ the final pass cleared the slot.
    await drainAlarmsUntil(thread, async () => (await scheduledAlarm(thread)) === null);
    expect(await scheduledAlarm(thread)).toBeNull();
  }, 30_000);

  it("the alarm invariant survives an eviction mid-pass: the persisted alarm outlives the incarnation", async () => {
    const thread = lane("invariant-evict");

    armRuntimeEviction(thread, "claim:after-claim");
    await submitTo(plannerDefinition, thread);
    // Wait for the doomed pass (auto-fired or drain-fired) to pre-arm, claim, and die.
    await drainAlarmsUntil(thread, () => Promise.resolve(armedEvictionsRemaining(thread) === 0));
    // The alarm that outlives the dead incarnation (its deadline was committed BEFORE the
    // abort) is what converges the lane with no incoming request.
    let observedArmed = false;

    for (let round = 0; round < 100 && !observedArmed; round++) {
      observedArmed = (await scheduledAlarm(thread)) !== null;
      if (!observedArmed && (await laneRows(thread)).every((row) => row.state === "settled")) {
        // Converged before we sampled the slot: the persisted alarm did its work already.
        observedArmed = true;
      }
      await new Promise((resolve) => setTimeout(resolve, 10));
    }
    expect(observedArmed).toBe(true);
    await drainAlarmsUntil(thread, allSettled(thread));
    await assertConvergence(thread);
  }, 30_000);
});
