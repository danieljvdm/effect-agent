import { digestJson } from "@effect-agent/thread/Digest";
import { MessageDeliveryStore, prepareMessageDelivery } from "@effect-agent/thread/MessageDelivery";
import { runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { Clock, Effect } from "effect";
import { DurableObject } from "effect-cf";
import { TestClock } from "effect/testing";
import { describe, expect, it } from "vite-plus/test";

import { CloudflareThreadClient } from "../src/CloudflareThreadClient.ts";
import {
  alarmAttemptHolds,
  decodeIdempotencyKey,
  decodeThreadId,
  maintenanceClocks,
  plannerDefinition,
  submitOptions,
} from "./fixtures.ts";
import {
  allSettled,
  drainAlarmsUntil,
  laneRows,
  runClient,
  scheduledAlarm,
  stubFor,
} from "./harness.ts";
import {
  droppedMessageWakes,
  messageEvictions,
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

const enqueue = (source: string, destination: string, now: number, message = "message") =>
  runInDurableObject(stubFor(source), (instance) =>
    instance[DurableObject.RunSymbol](
      Effect.gen(function* () {
        const store = yield* MessageDeliveryStore;
        const options = submitOptions(destination, `message:${source}:${message}`);
        const input = { question: "delivered later", ref: destination };

        const record = yield* prepareMessageDelivery({
          key: keyFor(source, message),
          createdAtMillis: now,
          deadlineAtMillis: now + 60_000,
          policy: {
            maxAutomaticAttempts: 3,
            attemptTimeoutMillis: 100,
            retryBaseMillis: 10,
            retryMaxMillis: 50,
            settlementPollMillis: 20,
          },
          envelope: {
            schemaVersion: 1,
            threadId: options.threadId,
            deliveryPrincipal: options.principal,
            agentId: plannerDefinition.id,
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
  // Regression: https://github.com/danieljvdm/effect-agent/commit/4ff21e2a4c3735be34955e7d5caf64f623d33f81
  it("delivers inserted messages, queued waves and settlement polls while its source Attempt is running", () =>
    withThreads(async (source, destination, now, advance) => {
      await submit(source, "initial");
      await drainAlarmsUntil(source, allSettled(source));

      const entered = latch();
      const release = latch();
      const finished = latch();
      const deliveryRelease = latch();
      const messages = ["message", ...Array.from({ length: 8 }, (_, index) => `message-${index}`)];

      const statuses = () =>
        Promise.all(messages.map(async (message) => (await read(source, message))?.status));

      alarmAttemptHolds.set(source, {
        location: "claim:after-claim",
        entered: Effect.sync(entered.resolve),
        release: Effect.promise(() => release.promise),
        finished: Effect.sync(finished.resolve),
      });
      await submit(source, "active-source");
      const running = runDurableObjectAlarm(stubFor(source));

      try {
        await entered.promise;
        expect(await read(source)).toBeNull();
        messageDeliveryHolds.set(source, {
          point: "message-delivery:admission:after",
          entered: () => {},
          release: deliveryRelease.promise,
        });
        await enqueue(source, destination, now);
        for (
          let attempt = 0;
          attempt < 200 && messageDeliveryResources.get(source)?.acquired !== 1;
          attempt += 1
        ) {
          await read(source);
        }
        expect(messageDeliveryResources.get(source)?.acquired).toBe(1);
        // Coalesce a backlog behind the first delivery so it exceeds a single native wave.
        for (const message of messages.slice(1)) await enqueue(source, destination, now, message);
        messageDeliveryHolds.delete(source);
        deliveryRelease.resolve();
        for (
          let attempt = 0;
          attempt < 200 && (await statuses()).some((status) => status !== "accepted");
          attempt += 1
        ) {
          await advance(1);
        }
        expect(await statuses()).toEqual(messages.map(() => "accepted"));
        expect(await laneRows(destination)).toHaveLength(messages.length);

        await drainAlarmsUntil(destination, allSettled(destination));
        await advance(20);
        for (
          let attempt = 0;
          attempt < 200 && (await statuses()).some((status) => status !== "processed");
          attempt += 1
        ) {
          await advance(1);
        }
        expect(await statuses()).toEqual(messages.map(() => "processed"));
        expect(await allSettled(source)()).toBe(false);
      } finally {
        messageDeliveryHolds.delete(source);
        deliveryRelease.resolve();
        release.resolve();
        await running;
        await finished.promise;
      }
    }));

  it("settles ready source work while delivery is held and finalizes delivery on maintenance interruption", () =>
    withThreads(async (source, destination, now, advance) => {
      await submit(source, "initial");
      await drainAlarmsUntil(source, allSettled(source));
      await enqueue(source, destination, now);
      await submit(source, "ready-during-delivery");

      const entered = latch();
      const release = latch();

      messageDeliveryHolds.set(source, {
        point: "message-delivery:admission:after",
        entered: entered.resolve,
        release: release.promise,
      });
      const running = runDurableObjectAlarm(stubFor(source));

      // Attach the rejection observer before advancing the cooperative event deadline.
      const outcome = running.then(
        () => ({ interrupted: false }),
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

        await advance(14 * 60_000);
        expect(await outcome).toEqual({ interrupted: true });
        expect(messageDeliveryResources.get(source)).toEqual({ acquired: 1, released: 1 });
        expect(await scheduledAlarm(source)).not.toBeNull();
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

  it("recovers a lost admission acknowledgement after eviction using the same destination Receipt", () =>
    withThreads(async (source, destination, now, advance) => {
      await submit(source, "initial");
      await drainAlarmsUntil(source, allSettled(source));
      await enqueue(source, destination, now);
      messageEvictions.set(source, "message-delivery:admission:after");
      await expect(runDurableObjectAlarm(stubFor(source))).rejects.toThrow(
        /message delivery eviction/u,
      );
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
    }));

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
