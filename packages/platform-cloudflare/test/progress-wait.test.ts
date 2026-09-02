import { ApprovalDecisionCommand, CanonicalSequence, type Receipt } from "@effect-agent/thread";
import { runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { Cause, Effect, Fiber, Option, Schema } from "effect";
import { describe, expect, it, onTestFinished } from "vite-plus/test";

import { CloudflareThreadClient, ProgressWaitRegistry } from "../src/index.ts";
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

const ZERO_SEQUENCE = Schema.decodeSync(CanonicalSequence)(0);
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

const awaitCanonicalTagEffect = (thread: string, afterSequence: CanonicalSequence, tag: string) =>
  Effect.gen(function* () {
    const client = yield* CloudflareThreadClient;
    const threadId = decodeThreadId(thread);
    let cursor = afterSequence;

    for (;;) {
      const records = yield* client.readPage(threadId, {
        afterSequence: cursor,
        limit: 1_024,
      });

      if (records.some((record) => record.record.payload._tag === tag)) return;
      const last = records.at(-1);

      if (last !== undefined) {
        cursor = last.sequence;
        continue;
      }
      yield* client.awaitProgress(threadId, cursor);
    }
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

  it("returns for committed history and wakes promptly after a canonical append", async () => {
    const thread = lane("append");
    const approval = await prepareApproval(thread);

    const committed = runTrackedClientFiber(awaitProgressEffect(thread, ZERO_SEQUENCE));

    await Effect.runPromise(Fiber.join(committed));
    const before = await readCanonical(thread);
    const cursor = before.at(-1)?.sequence;

    expect(cursor).toBeDefined();
    if (cursor === undefined) return;

    const waiting = runTrackedClientFiber(
      awaitCanonicalTagEffect(thread, cursor, "ToolApprovalDecided"),
    );

    await progressStub(thread).awaitProgressWaiterCount(1);
    await approve(thread, approval.receipt);
    await runDurableObjectAlarm(stubFor(thread));
    await Effect.runPromise(Fiber.join(waiting));

    const after = await readCanonical(thread);

    expect(after.some((record) => record.sequence > cursor)).toBe(true);
  }, 20_000);

  it("broadcasts to every waiter, isolates lanes, and cleans up an interrupted caller", async () => {
    const thread = lane("many");
    const unrelated = lane("unrelated");
    const main = await prepareApproval(thread);
    const other = await prepareApproval(unrelated);
    const cursor = main.cursor;
    const unrelatedCursor = other.cursor;

    const first = runTrackedClientFiber(
      awaitCanonicalTagEffect(thread, cursor, "ToolApprovalDecided"),
    );

    const second = runTrackedClientFiber(
      awaitCanonicalTagEffect(thread, cursor, "ToolApprovalDecided"),
    );

    const interruptedFiber = runTrackedClientFiber(awaitProgressEffect(thread, cursor));
    const unrelatedFiber = runTrackedClientFiber(awaitProgressEffect(unrelated, unrelatedCursor));

    await progressStub(thread).awaitProgressWaiterCount(3);
    await progressStub(unrelated).awaitProgressWaiterCount(1);

    await Effect.runPromise(Fiber.interrupt(interruptedFiber));
    await progressStub(thread).awaitProgressWaiterCount(2);
    expect(await progressStub(unrelated).progressWaiterCount()).toBe(1);

    await approve(thread, main.receipt);
    await Effect.runPromise(Effect.all([Fiber.join(first), Fiber.join(second)]));
    expect(await progressStub(unrelated).progressWaiterCount()).toBe(1);

    await Effect.runPromise(Fiber.interrupt(unrelatedFiber));
    await progressStub(unrelated).awaitProgressWaiterCount(0);
    await approve(unrelated, other.receipt);
    await drainAlarmsUntil(thread, allSettled(thread));
    await drainAlarmsUntil(unrelated, allSettled(unrelated));
  }, 20_000);

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

  it("delivers the remote wake seam used by child, parent, and host settlement", async () => {
    const thread = lane("remote");
    const approval = await prepareApproval(thread);
    const waiting = runTrackedClientFiber(awaitProgressEffect(thread, approval.cursor));

    await progressStub(thread).awaitProgressWaiterCount(1);

    await stubFor(thread).wake();
    await Effect.runPromise(Fiber.join(waiting));
    await progressStub(thread).awaitProgressWaiterCount(0);
    expect(await progressStub(thread).progressWaiterCount()).toBe(0);
    await approve(thread, approval.receipt);
    await drainAlarmsUntil(thread, allSettled(thread));
  });

  it("preserves the typed non-materialized failure across the RPC client boundary", async () => {
    const thread = lane("missing");

    const tag = await runClient(
      Effect.gen(function* () {
        const client = yield* CloudflareThreadClient;

        const exit = yield* Effect.exit(
          client.awaitProgress(decodeThreadId(thread), ZERO_SEQUENCE),
        );

        if (exit._tag === "Success") return "success";
        const error = Cause.findErrorOption(exit.cause);

        if (Option.isNone(error)) return "defect";
        const value: unknown = error.value;

        return typeof value === "object" && value !== null && "_tag" in value
          ? String(value._tag)
          : "unknown";
      }),
    );

    expect(tag).toBe("ThreadNotMaterialized");
  });

  it("preserves a typed authorization denial across the RPC client boundary", async () => {
    const thread = lane("denied");

    const tag = await runClient(
      Effect.gen(function* () {
        const client = yield* CloudflareThreadClient;

        const exit = yield* Effect.exit(
          client.awaitProgress(decodeThreadId(thread), ZERO_SEQUENCE),
        );

        if (exit._tag === "Success") return "success";
        const error = Cause.findErrorOption(exit.cause);

        if (Option.isNone(error)) return "defect";
        const value: unknown = error.value;

        return typeof value === "object" && value !== null && "_tag" in value
          ? String(value._tag)
          : "unknown";
      }),
      "DENIED",
    );

    expect(tag).toBe("OperationDenied");
  });
});
