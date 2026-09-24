import { CloudflareThreadClient } from "@effect-agent/platform-cloudflare/cloudflare-thread-client";
import { runInDurableObject } from "cloudflare:test";
import { Effect, Fiber } from "effect";
import { type Receipt } from "effect-agent/durable-agent-runtime";
import type { CanonicalSequence } from "effect-agent/records";
import { ApprovalDecisionCommand } from "effect-agent/submission-ledger";
import { describe, expect, it, onTestFinished } from "vite-plus/test";

import { ProgressWaitRegistry } from "../src/internal/progress-wait.ts";
import {
  decodeThreadId,
  BOOK_TOOL_CALL_ID,
  approvalDefinition,
  submitOptions,
} from "./fixtures.ts";
import {
  allSettled,
  anyInState,
  awaitReconstructedProgressWaiter,
  drainAlarmsUntil,
  readCanonical,
  runClient,
  runClientFiber,
  stubFor,
} from "./harness.ts";
import type { TestThreadObject } from "./worker.ts";
let laneCounter = 0;
const lane = (label: string): string => `cf-progress-${label}-${laneCounter++}`;

const progressStub = (thread: string) => stubFor(thread) as DurableObjectStub<TestThreadObject>;

const submitApproval = (thread: string) =>
  runClient(
    Effect.gen(function* () {
      const client = yield* CloudflareThreadClient;

      return yield* client.submit(
        { definition: approvalDefinition },
        { question: "hold for approval", ref: thread },
        submitOptions(thread, `${thread}-key`),
      );
    }),
  );

const prepareApproval = async (
  thread: string,
): Promise<{ readonly receipt: Receipt; readonly cursor: CanonicalSequence }> => {
  const receipt = await submitApproval(thread);

  await drainAlarmsUntil(thread, anyInState(thread, "suspended"));
  const cursor = (await readCanonical(thread)).at(-1)?.sequence;

  if (cursor === undefined) throw new Error("approval lane did not materialize canonical history");

  return { receipt, cursor };
};

const approve = (thread: string, receipt: Receipt) =>
  runClient(
    Effect.gen(function* () {
      const client = yield* CloudflareThreadClient;

      return yield* client.resolveApproval(
        decodeThreadId(thread),
        ApprovalDecisionCommand.make({
          submissionId: receipt.submissionId,
          toolCallId: BOOK_TOOL_CALL_ID,
          decision: "approved",
          resolver: "#94-test-approver",
          reason: "release the durable progress test",
        }),
      );
    }),
  );

const awaitProgressEffect = (thread: string, afterSequence: CanonicalSequence) =>
  Effect.gen(function* () {
    const client = yield* CloudflareThreadClient;

    yield* client.awaitProgress(decodeThreadId(thread), afterSequence);
  });

const runTrackedClientFiber = <A, E>(effect: Effect.Effect<A, E, CloudflareThreadClient>) => {
  const fiber = runClientFiber(effect);

  onTestFinished(() => Effect.runPromise(Fiber.interrupt(fiber)));

  return fiber;
};

describe("#94 Cloudflare durable progress wait", () => {
  it("broadcasts cancellation across duplicate and late transport attempts", async () => {
    const completed = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const registry = yield* ProgressWaitRegistry;
          const first = yield* registry.subscribe("duplicate-attempt");
          const second = yield* registry.subscribe("duplicate-attempt");

          yield* registry.cancel("duplicate-attempt");
          yield* Effect.all([first, second], { concurrency: "unbounded" });
          const retriedAfterCancel = yield* registry.subscribe("duplicate-attempt");

          yield* retriedAfterCancel;

          yield* registry.cancel("late-attempt");
          const lateFirst = yield* registry.subscribe("late-attempt");
          const lateSecond = yield* registry.subscribe("late-attempt");

          yield* Effect.all([lateFirst, lateSecond], { concurrency: "unbounded" });

          return true;
        }),
      ).pipe(Effect.provide(ProgressWaitRegistry.layer)),
    );

    expect(completed).toBe(true);
  });

  it("keeps cancellation tombstones after old attempt scopes close and removes only their waiters", () =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const registry = yield* ProgressWaitRegistry;
          const closed = yield* Effect.scoped(registry.subscribe("reused-id"));
          const live = yield* registry.subscribe("reused-id");

          yield* registry.cancel("reused-id");
          yield* live;
          const detached = yield* Effect.forkChild(closed, { startImmediately: true });

          expect(detached.pollUnsafe()).toBeUndefined();
          yield* Fiber.interrupt(detached);

          yield* Effect.scoped(
            Effect.gen(function* () {
              const active = yield* registry.subscribe("cancelled-before-close");

              yield* registry.cancel("cancelled-before-close");
              yield* active;
            }),
          );
          yield* yield* registry.subscribe("cancelled-before-close");
        }),
      ).pipe(Effect.provide(ProgressWaitRegistry.layer)),
    ));

  it("reconnects after eviction, reconstructs the wait, and rechecks durable authority", async () => {
    const thread = lane("eviction");
    const approval = await prepareApproval(thread);
    const cursor = approval.cursor;

    const waiting = runTrackedClientFiber(awaitProgressEffect(thread, cursor));

    await progressStub(thread).awaitProgressWaiterCount(1);
    const priorIncarnation = await progressStub(thread).progressIncarnation();

    await runInDurableObject(stubFor(thread), (_instance, state) => {
      state.abort("#94 forced wait eviction");
    }).catch(() => undefined);

    const reconstructedIncarnation = await awaitReconstructedProgressWaiter(
      thread,
      priorIncarnation,
      1,
    );

    expect(reconstructedIncarnation).not.toBe(priorIncarnation);
    await approve(thread, approval.receipt);
    await Effect.runPromise(Fiber.join(waiting));
    await drainAlarmsUntil(thread, allSettled(thread));
    const after = await readCanonical(thread);

    expect(after.some((record) => record.sequence > cursor)).toBe(true);
  }, 20_000);
});
