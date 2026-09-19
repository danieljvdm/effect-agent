import { runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { Clock, Effect } from "effect";
import { digestJson } from "effect-agent/digest";
import { type AgentId } from "effect-agent/identifiers";
import {
  MessageDeliveryDriver,
  MessageDeliveryStore,
  prepareMessageDelivery,
} from "effect-agent/message-delivery";
import { ApprovalDecisionCommand } from "effect-agent/submission-ledger";
import { DurableObject } from "effect-cf";
import { TestClock } from "effect/testing";
import { describe, expect, it } from "vite-plus/test";

import { CloudflareThreadClient } from "../src/CloudflareThreadClient.ts";
import {
  alarmAttemptHolds,
  approvalDefinition,
  BOOK_TOOL_CALL_ID,
  decodeIdempotencyKey,
  decodeThreadId,
  maintenanceClocks,
  plannerDefinition,
  submitOptions,
  supplierCountsFor,
} from "./fixtures.ts";
import {
  allSettled,
  anyInState,
  drainAlarmsUntil,
  laneRows,
  runClient,
  scheduledAlarm,
  stubFor,
} from "./harness.ts";
import {
  droppedMessageWakes,
  messageEvictions,
  messageClaimDelays,
  testMessageDriverLayer,
  messageInterruptions,
  messageDeliveryHolds,
  messageDeliveryResources,
} from "./message-delivery-fixture.ts";

const latch = () => {
  let resolve = () => {};

  const promise = new Promise<void>((complete) => {
    resolve = complete;
  });

  return { promise, resolve };
};

const submit = (thread: string, key: string) =>
  runClient(
    Effect.flatMap(CloudflareThreadClient, (client) =>
      client.submit(
        { definition: plannerDefinition },
        { question: "message maintenance", ref: thread },
        submitOptions(thread, key),
      ),
    ),
  );

const keyFor = (source: string, message = "message") => ({
  ownerThreadId: decodeThreadId(source),
  messageId: decodeIdempotencyKey(message),
});

const read = (source: string, message = "message") =>
  runInDurableObject(stubFor(source), (instance) =>
    instance[DurableObject.RunSymbol](
      Effect.flatMap(MessageDeliveryStore, (store) => store.get(keyFor(source, message))),
    ),
  );

const enqueue = (
  source: string,
  destination: string,
  now: number,
  message = "message",
  agentId: AgentId = plannerDefinition.id,
  attemptTimeoutMillis = 100,
) =>
  runInDurableObject(stubFor(source), (instance) =>
    instance[DurableObject.RunSymbol](
      Effect.gen(function* () {
        const store = yield* MessageDeliveryStore;
        const options = submitOptions(destination, `message:${source}:${message}`);
        const input = { question: "delivered later", ref: destination };

        const record = yield* prepareMessageDelivery({
          key: keyFor(source, message),
          createdAtMillis: now,
          deadlineAtMillis: now + Math.max(60_000, 4 * attemptTimeoutMillis),
          policy: {
            maxAutomaticAttempts: 3,
            attemptTimeoutMillis,
            retryBaseMillis: 10,
            retryMaxMillis: 50,
            settlementPollMillis: 20,
          },
          envelope: {
            schemaVersion: 1,
            threadId: options.threadId,
            deliveryPrincipal: options.principal,
            agentId,
            definitions: options.definitions,
            input,
            inputDigest: yield* digestJson(input),
            admissionKey: options.idempotencyKey,
            authorization: { policyId: "host-message-policy", decisionId: "host-message-allow" },
          },
        });

        return yield* store.insert(record);
      }),
    ),
  );

const withThreads = (
  body: (
    source: string,
    destination: string,
    now: number,
    advance: (millis: number) => Promise<void>,
  ) => Promise<void>,
) =>
  Effect.runPromise(
    Effect.gen(function* () {
      const source = `messages-source-${crypto.randomUUID()}`;
      const destination = `messages-destination-${crypto.randomUUID()}`;
      const now = Date.now() + 86_400_000;

      yield* TestClock.setTime(now);
      const clock = yield* Clock.Clock;
      const testClock = yield* TestClock.testClockWith(Effect.succeed);

      for (const thread of [source, destination]) {
        maintenanceClocks.set(thread, clock);
        droppedMessageWakes.add(thread);
      }

      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          for (const thread of [source, destination]) {
            maintenanceClocks.delete(thread);
            droppedMessageWakes.delete(thread);
            messageEvictions.delete(thread);
            messageClaimDelays.delete(thread);
            messageInterruptions.delete(thread);
            messageDeliveryHolds.delete(thread);
            messageDeliveryResources.delete(thread);
            alarmAttemptHolds.delete(thread);
          }
        }),
      );
      yield* Effect.promise(() =>
        body(source, destination, now, (millis) => Effect.runPromise(testClock.adjust(millis))),
      );
    }).pipe(Effect.scoped, Effect.provide(TestClock.layer())),
  );

describe("Thread Object message maintenance", () => {
  it.each(["driver", "alarm"] as const)(
    "preserves the actual Claim window and normal timeout/backoff with %s ownership",
    (owner) =>
      withThreads(async (source, destination, now, advance) => {
        await submit(source, "initial");
        await drainAlarmsUntil(source, allSettled(source));
        await enqueue(source, destination, now, "message", plannerDefinition.id, 1_000);
        const claim = latch();
        const claimRelease = latch();
        const entered = latch();
        const release = latch();

        messageClaimDelays.set(source, { release: claimRelease.promise, entered: claim.resolve });
        messageDeliveryHolds.set(source, {
          point: "message-delivery:admission:response",
          entered: entered.resolve,
          release: release.promise,
        });
        let retired = false;

        const running = (
          owner === "alarm"
            ? runDurableObjectAlarm(stubFor(source))
            : runInDurableObject(stubFor(source), (instance) =>
                instance[DurableObject.RunSymbol](
                  Effect.flatMap(MessageDeliveryDriver, (driver) => driver.runDue()).pipe(
                    Effect.provide(testMessageDriverLayer),
                  ),
                ),
              )
        ).then(() => {
          retired = true;
        });

        try {
          await claim.promise;
          await advance(500);
          claimRelease.resolve();
          await entered.promise;
          expect((await read(source))?.leaseUntilMillis).toBe(now + 1_500);
          await advance(500);
          expect(retired, "selection time must not shorten the authorized Claim window").toBe(
            false,
          );
          await advance(500);
          await running;
          const record = await read(source);

          expect(record?.leaseUntilMillis).toBeNull();
          expect(record?.retry.lastFailure).toBe("timeout");
          expect(record?.retry.nextAttemptAtMillis).toBe(now + 1_510);
          expect(record?.retry.attempts).toBe(1);
        } finally {
          messageClaimDelays.delete(source);
          claimRelease.resolve();
          messageDeliveryHolds.delete(source);
          release.resolve();
          await running;
        }
      }),
  );

  // Regression: https://github.com/danieljvdm/effect-agent/commit/4ff21e2a4c3735be34955e7d5caf64f623d33f81
  it("sleeps until the delivery deadline while its destination waits for external approval", () =>
    withThreads(async (source, destination, now, advance) => {
      await submit(source, "initial");
      await drainAlarmsUntil(source, allSettled(source));
      await drainAlarmsUntil(source, async () => (await scheduledAlarm(source)) === null);
      await enqueue(source, destination, now, "message", approvalDefinition.id);
      await runDurableObjectAlarm(stubFor(source));
      const accepted = await read(source);

      expect(accepted?.status).toBe("accepted");
      expect(accepted?.receipt).not.toBeNull();
      expect(await allSettled(source)()).toBe(true);
      expect(
        (await scheduledAlarm(source))! - now,
        "delivery bookkeeping must not schedule an execution recovery before its next due attempt",
      ).toBe(20);
      await drainAlarmsUntil(destination, anyInState(destination, "suspended"));
      await drainAlarmsUntil(destination, async () => (await scheduledAlarm(destination)) === null);
      expect(supplierCountsFor(destination)).toEqual({});

      await advance(20);
      await runDurableObjectAlarm(stubFor(source));
      expect((await read(source))?.receipt).toEqual(accepted?.receipt);
      expect((await read(source))?.status).toBe("accepted");
      expect((await scheduledAlarm(source))! - now).toBe(40);
      expect(await scheduledAlarm(destination)).toBeNull();
      await runClient(
        Effect.flatMap(CloudflareThreadClient, (client) =>
          client.resolveApproval(
            decodeThreadId(destination),
            ApprovalDecisionCommand.make({
              submissionId: accepted!.receipt!.submissionId,
              toolCallId: BOOK_TOOL_CALL_ID,
              decision: "approved",
              resolver: "message-delivery-approver",
              reason: "resume retained delivery",
            }),
          ),
        ),
      );
      await drainAlarmsUntil(destination, allSettled(destination));
      await advance(20);
      await runDurableObjectAlarm(stubFor(source));
      const processed = await read(source);

      expect(processed?.status).toBe("processed");
      expect(processed?.receipt).toEqual(accepted?.receipt);
      expect(supplierCountsFor(destination)).toEqual({ book: 1 });
      expect(await scheduledAlarm(source)).toBeNull();
    }));

  // Wake-driven overlap is required while the source still owns its native budget.
  it("delivers new wakes during source work and leaves retry deadlines to future alarms", () =>
    withThreads(async (source, destination, now, advance) => {
      await submit(source, "initial");
      await drainAlarmsUntil(source, allSettled(source));
      const entered = latch();
      const release = latch();
      const deliveryEntered = latch();
      const deliveryRelease = latch();

      alarmAttemptHolds.set(source, {
        location: "claim:after-claim",
        entered: Effect.sync(entered.resolve),
        release: Effect.promise(() => release.promise),
        finished: Effect.void,
      });
      await submit(source, "active-source");
      const running = runDurableObjectAlarm(stubFor(source));

      try {
        await entered.promise;
        messageDeliveryHolds.set(source, {
          point: "message-delivery:admission:response",
          entered: deliveryEntered.resolve,
          release: deliveryRelease.promise,
        });
        await enqueue(source, destination, now);
        await deliveryEntered.promise;
        for (let index = 1; index <= 8; index++)
          await enqueue(source, destination, now, `late-${index}`);
        messageDeliveryHolds.delete(source);
        deliveryRelease.resolve();
        for (
          let attempt = 0;
          attempt < 200 && (await read(source, "late-8"))?.status !== "accepted";
          attempt++
        )
          await Promise.resolve();
        expect((await read(source))?.status).toBe("accepted");
        expect((await read(source, "late-8"))?.status).toBe("accepted");
        expect(await laneRows(destination)).toHaveLength(9);
        const before = [await read(source), await read(source, "late-8")];

        await advance(200);
        expect([await read(source), await read(source, "late-8")]).toEqual(before);
        expect(await allSettled(source)()).toBe(false);
      } finally {
        messageDeliveryHolds.delete(source);
        deliveryRelease.resolve();
        release.resolve();
        await running;
        alarmAttemptHolds.delete(source);
      }
      expect(await allSettled(source)()).toBe(true);
      expect(await scheduledAlarm(source)).not.toBeNull();
    }));

  it("defers a late five-minute wave that cannot fit the physical event without claiming it", () =>
    withThreads(async (source, destination, now, advance) => {
      await submit(source, "initial");
      await drainAlarmsUntil(source, allSettled(source));
      const entered = latch();
      const release = latch();

      alarmAttemptHolds.set(source, {
        location: "claim:after-claim",
        entered: Effect.sync(entered.resolve),
        release: Effect.promise(() => release.promise),
        finished: Effect.void,
      });
      await submit(source, "active-source");
      const running = runDurableObjectAlarm(stubFor(source));

      try {
        await entered.promise;
        await advance(9 * 60_000 + 1);
        await enqueue(
          source,
          destination,
          now + 9 * 60_000 + 1,
          "late",
          plannerDefinition.id,
          300_000,
        );
        await advance(1);
        expect((await read(source, "late"))?.retry.attempts).toBe(0);
        expect(await laneRows(destination)).toHaveLength(0);
      } finally {
        release.resolve();
        await running;
        alarmAttemptHolds.delete(source);
      }
      expect(await scheduledAlarm(source)).not.toBeNull();
      await runDurableObjectAlarm(stubFor(source));
      expect((await read(source, "late"))?.status).toBe("accepted");
      expect((await read(source, "late"))?.retry.attempts).toBe(1);
    }));

  it("finishes a listener-started wave at the native yield deadline without admitting another wave", () =>
    withThreads(async (source, destination, now, advance) => {
      await submit(source, "initial");
      await drainAlarmsUntil(source, allSettled(source));
      const nativeEntered = latch();
      const nativeRelease = latch();
      const entered = latch();
      const release = latch();

      alarmAttemptHolds.set(source, {
        location: "claim:after-claim",
        entered: Effect.sync(nativeEntered.resolve),
        release: Effect.promise(() => nativeRelease.promise),
        finished: Effect.void,
      });
      await submit(source, "active-source");
      let retired = false;

      const running = runDurableObjectAlarm(stubFor(source)).then(() => {
        retired = true;
      });

      try {
        await nativeEntered.promise;
        const lateStart = 10 * 60_000 - 1_000;

        await advance(lateStart);
        messageDeliveryHolds.set(source, {
          point: "message-delivery:admission:response",
          entered: entered.resolve,
          release: release.promise,
        });
        await enqueue(source, destination, now + lateStart, "late", plannerDefinition.id, 30_000);
        await entered.promise;
        nativeRelease.resolve();
        for (let attempt = 0; attempt < 200 && !(await allSettled(source)()); attempt++)
          await Promise.resolve();
        await advance(1_500);
        expect(retired).toBe(false);
        expect((await read(source, "late"))?.retry.attempts).toBe(1);
        await enqueue(source, destination, now + lateStart + 1_500, "after-stop");
        await submit(source, "next-budget");
      } finally {
        nativeRelease.resolve();
        alarmAttemptHolds.delete(source);
        messageDeliveryHolds.delete(source);
        release.resolve();
        await running;
      }
      expect((await read(source, "late"))?.status).toBe("accepted");
      expect((await read(source, "after-stop"))?.retry.attempts).toBe(0);
      expect(await laneRows(destination)).toHaveLength(1);
      await runDurableObjectAlarm(stubFor(source));
      expect((await read(source, "after-stop"))?.status).toBe("accepted");
      expect((await read(source, "after-stop"))?.retry.attempts).toBe(1);
      expect(await allSettled(source)()).toBe(true);
    }));

  it.each(["delivery-only", "continuous native arrivals"])(
    "grants a healthy acknowledgement its declared opportunity with %s",
    (mode) =>
      withThreads(async (source, destination, now, advance) => {
        await submit(source, "initial");
        await drainAlarmsUntil(source, allSettled(source));
        await drainAlarmsUntil(source, async () => (await scheduledAlarm(source)) === null);
        await enqueue(source, destination, now, "message", plannerDefinition.id, 30_000);
        if (mode === "continuous native arrivals") await submit(source, "native-backlog");
        const entered = latch();
        const release = latch();
        let retired = false;

        messageDeliveryHolds.set(source, {
          point: "message-delivery:admission:response",
          entered: entered.resolve,
          release: release.promise,
        });

        const running = runDurableObjectAlarm(stubFor(source)).then(() => {
          retired = true;
        });

        try {
          await entered.promise;
          for (let arrival = 0; arrival < 3; arrival++) {
            await advance(500);
            if (mode === "continuous native arrivals") {
              await submit(source, `arrival-${arrival}`);
              await advance(100);
              expect(await allSettled(source)()).toBe(true);
            }
          }
          expect(
            retired,
            "new native debt cannot repeatedly cancel a healthy acknowledgement",
          ).toBe(false);
        } finally {
          messageDeliveryHolds.delete(source);
          release.resolve();
          await running;
        }
        expect((await read(source))?.status).toBe("accepted");
        expect(await laneRows(destination)).toHaveLength(1);
        expect(messageDeliveryResources.get(source)).toEqual({ acquired: 1, released: 1 });
        expect((await read(source))?.retry.attempts).toBe(1);
      }),
  );

  it("executes new input during a held delivery and retains its exact retry after timeout", () =>
    withThreads(async (source, destination, now, advance) => {
      await submit(source, "initial");
      await drainAlarmsUntil(source, allSettled(source));
      await enqueue(source, destination, now, "message", plannerDefinition.id, 10_000);
      await submit(source, "ready-during-delivery");

      const entered = latch();
      const release = latch();

      messageDeliveryHolds.set(source, {
        point: "message-delivery:admission:response",
        entered: entered.resolve,
        release: release.promise,
      });
      const running = runDurableObjectAlarm(stubFor(source));

      let retired = false;

      const outcome = running.then(
        () => {
          retired = true;

          return { interrupted: false };
        },
        () => ({ interrupted: true }),
      );

      try {
        await entered.promise;
        for (let attempt = 0; attempt < 200 && !(await allSettled(source)()); attempt += 1) {
          await Promise.resolve();
        }
        expect(await allSettled(source)()).toBe(true);
        expect((await laneRows(source)).length).toBe(2);
        expect((await read(source))?.status).toBe("pending");
        expect(messageDeliveryResources.get(source)).toEqual({ acquired: 1, released: 0 });

        const destinationRows = await laneRows(destination);

        expect(destinationRows).toHaveLength(1);
        await submit(source, "next-ready-continuation");
        await advance(100);
        expect(retired).toBe(false);
        expect(await allSettled(source)()).toBe(true);
        expect(messageDeliveryResources.get(source)).toEqual({ acquired: 1, released: 0 });
        await advance(9_900);
        expect(retired, "the destination response must not own the physical event").toBe(true);
        expect(await outcome).toEqual({ interrupted: false });
        expect(messageDeliveryResources.get(source)).toEqual({ acquired: 1, released: 1 });
        expect(await scheduledAlarm(source)).not.toBeNull();
        expect((await laneRows(source)).map((row) => row.state)).toEqual([
          "settled",
          "settled",
          "settled",
        ]);

        expect(await allSettled(source)()).toBe(true);
        expect((await read(source))?.status).toBe("pending");
        expect((await read(source))?.retry.lastFailure).toBe("timeout");
        expect((await read(source))?.leaseUntilMillis).toBeNull();
        // Retry only after the driver's durable backoff, using the same admission identity.
        messageDeliveryHolds.delete(source);
        await advance(10);
        await runDurableObjectAlarm(stubFor(source));
        expect((await read(source))?.status).toBe("accepted");
        expect((await read(source))?.receipt?.submissionId).toBe(destinationRows[0]?.submission_id);
        expect(await laneRows(destination)).toHaveLength(1);
        const recovered = await read(source);

        release.resolve();
        expect(await read(source)).toEqual(recovered);
        await drainAlarmsUntil(destination, allSettled(destination));
        await advance(20);
        await runDurableObjectAlarm(stubFor(source));
        expect((await read(source))?.status).toBe("processed");
      } finally {
        messageDeliveryHolds.delete(source);
        release.resolve();
        await outcome;
      }
    }));

  it("delivers after both lanes settle and lets source work run while destination processing is pending", () =>
    withThreads(async (source, destination, now, advance) => {
      await submit(source, "initial");
      await submit(destination, "initial");
      await drainAlarmsUntil(source, allSettled(source));
      await drainAlarmsUntil(destination, allSettled(destination));
      expect(await scheduledAlarm(source)).toBeNull();

      await enqueue(source, destination, now);
      expect(await scheduledAlarm(source)).not.toBeNull();
      await runDurableObjectAlarm(stubFor(source));
      const accepted = await read(source);

      expect(accepted?.status).toBe("accepted");
      expect(accepted?.settlement).toBeNull();
      expect((await laneRows(destination)).filter((row) => row.state !== "settled")).toHaveLength(
        1,
      );

      await submit(source, "independent-work");
      await drainAlarmsUntil(source, allSettled(source));
      expect((await read(source))?.status).toBe("accepted");
      expect(await scheduledAlarm(source)).not.toBeNull();

      await drainAlarmsUntil(destination, allSettled(destination));
      await advance(20);
      await runDurableObjectAlarm(stubFor(source));
      const processed = await read(source);

      expect(processed?.status).toBe("processed");
      expect(processed?.receipt).toEqual(accepted?.receipt);
      expect(processed?.settlement?.outcome).toBe("completed");
      await drainAlarmsUntil(source, async () => (await scheduledAlarm(source)) === null);
      expect(await scheduledAlarm(source)).toBeNull();
    }));

  it.each(["eviction", "interruption"] as const)(
    "recovers a lost admission acknowledgement after %s and reconstruction using the same destination Receipt",
    (failure) =>
      withThreads(async (source, destination, now, advance) => {
        await submit(source, "initial");
        await drainAlarmsUntil(source, allSettled(source));
        await enqueue(source, destination, now);
        if (failure === "eviction")
          messageEvictions.set(source, "message-delivery:admission:after");
        else messageInterruptions.add(source);
        await expect(runDurableObjectAlarm(stubFor(source))).rejects.toBeDefined();
        if (failure === "interruption") {
          await runInDurableObject(stubFor(source), (_, state) => {
            state.abort("reconstruct interrupted admission");
          }).catch(() => undefined);
        }
        expect(messageEvictions.has(source)).toBe(false);
        const destinationRows = await laneRows(destination);

        expect(destinationRows).toHaveLength(1);
        await drainAlarmsUntil(destination, allSettled(destination));
        await advance(100);
        await runDurableObjectAlarm(stubFor(source));
        const recovered = await read(source);

        expect(recovered?.receipt?.submissionId).toBe(destinationRows[0]?.submission_id);
        expect(await laneRows(destination)).toHaveLength(1);
        await advance(20);
        await runDurableObjectAlarm(stubFor(source));
        expect((await read(source))?.status).toBe("processed");
      }),
  );

  it("prearms before an insert crash and reconstructs pending delivery after a committed insert crash", () =>
    withThreads(async (source, destination, now) => {
      messageEvictions.set(source, "message-delivery:insert:before");
      await expect(enqueue(source, destination, now)).rejects.toThrow(/message delivery eviction/u);
      expect(await scheduledAlarm(source)).not.toBeNull();
      expect(await read(source)).toBeNull();

      messageEvictions.set(source, "message-delivery:insert:after");
      await expect(enqueue(source, destination, now)).rejects.toThrow(/message delivery eviction/u);
      expect((await read(source))?.status).toBe("pending");
      expect(await scheduledAlarm(source)).not.toBeNull();
      await runDurableObjectAlarm(stubFor(source));
      expect((await read(source))?.status).toBe("accepted");
    }));
});
