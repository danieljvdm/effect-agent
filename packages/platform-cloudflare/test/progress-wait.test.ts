import { ApprovalDecisionCommand, CanonicalSequence, type Receipt } from "@effect-agent/session";
import { runDurableObjectAlarm, runInDurableObject } from "cloudflare:test";
import { Cause, Effect, Fiber, Option, Schema } from "effect";
import { describe, expect, it, onTestFinished } from "vite-plus/test";

import { CloudflareConversationClient, ProgressWaitRegistry } from "../src/index.ts";
import {
  TEST_CALLER,
  decodeConversationId,
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
import type { TestConversationObject } from "./worker.ts";

const ZERO_SEQUENCE = Schema.decodeSync(CanonicalSequence)(0);
let laneCounter = 0;
const lane = (label: string): string => `cf-progress-${label}-${laneCounter++}`;

const progressStub = (conversation: string) =>
  stubFor(conversation) as DurableObjectStub<TestConversationObject>;

const submitApproval = (conversation: string) =>
  runClient(
    Effect.gen(function* () {
      const client = yield* CloudflareConversationClient;
      return yield* client.submit(
        { definition: approvalDefinition },
        { question: "hold for approval", ref: conversation },
        submitOptions(conversation, `${conversation}-key`),
      );
    }),
  );

const prepareApproval = async (
  conversation: string,
): Promise<{ readonly receipt: Receipt; readonly cursor: CanonicalSequence }> => {
  const receipt = await submitApproval(conversation);
  await drainAlarmsUntil(conversation, anyInState(conversation, "suspended"));
  const cursor = (await readCanonical(conversation)).at(-1)?.sequence;
  if (cursor === undefined) throw new Error("approval lane did not materialize canonical history");
  return { receipt, cursor };
};

const approve = (conversation: string, receipt: Receipt) =>
  runClient(
    Effect.gen(function* () {
      const client = yield* CloudflareConversationClient;
      return yield* client.resolveApproval(
        decodeConversationId(conversation),
        ApprovalDecisionCommand.make({
          submissionId: receipt.submissionId,
          toolCallId: BOOK_TOOL_CALL_ID,
          decision: "approved",
          resolver: "#94-test-approver",
          reason: "release the durable progress test",
        }),
        TEST_CALLER,
      );
    }),
  );

const awaitProgressEffect = (conversation: string, afterSequence: CanonicalSequence) =>
  Effect.gen(function* () {
    const client = yield* CloudflareConversationClient;
    yield* client.awaitProgress(decodeConversationId(conversation), afterSequence, TEST_CALLER);
  });

const awaitCanonicalTagEffect = (
  conversation: string,
  afterSequence: CanonicalSequence,
  tag: string,
) =>
  Effect.gen(function* () {
    const client = yield* CloudflareConversationClient;
    const conversationId = decodeConversationId(conversation);
    let cursor = afterSequence;
    for (;;) {
      const records = yield* client.readPage(conversationId, TEST_CALLER, {
        afterSequence: cursor,
        limit: 1_024,
      });
      if (records.some((record) => record.record.payload._tag === tag)) return;
      const last = records.at(-1);
      if (last !== undefined) {
        cursor = last.sequence;
        continue;
      }
      yield* client.awaitProgress(conversationId, cursor, TEST_CALLER);
    }
  });

const runTrackedClientFiber = <A, E>(effect: Effect.Effect<A, E, CloudflareConversationClient>) => {
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
    const conversation = lane("append");
    const approval = await prepareApproval(conversation);

    const committed = runTrackedClientFiber(awaitProgressEffect(conversation, ZERO_SEQUENCE));
    await Effect.runPromise(Fiber.join(committed));
    const before = await readCanonical(conversation);
    const cursor = before.at(-1)?.sequence;
    expect(cursor).toBeDefined();
    if (cursor === undefined) return;

    const waiting = runTrackedClientFiber(
      awaitCanonicalTagEffect(conversation, cursor, "ToolApprovalDecided"),
    );
    await progressStub(conversation).awaitProgressWaiterCount(1);
    await approve(conversation, approval.receipt);
    await runDurableObjectAlarm(stubFor(conversation));
    await Effect.runPromise(Fiber.join(waiting));

    const after = await readCanonical(conversation);
    expect(after.some((record) => record.sequence > cursor)).toBe(true);
  }, 20_000);

  it("broadcasts to every waiter, isolates lanes, and cleans up an interrupted caller", async () => {
    const conversation = lane("many");
    const unrelated = lane("unrelated");
    const main = await prepareApproval(conversation);
    const other = await prepareApproval(unrelated);
    const cursor = main.cursor;
    const unrelatedCursor = other.cursor;

    const first = runTrackedClientFiber(
      awaitCanonicalTagEffect(conversation, cursor, "ToolApprovalDecided"),
    );
    const second = runTrackedClientFiber(
      awaitCanonicalTagEffect(conversation, cursor, "ToolApprovalDecided"),
    );
    const interruptedFiber = runTrackedClientFiber(awaitProgressEffect(conversation, cursor));
    const unrelatedFiber = runTrackedClientFiber(awaitProgressEffect(unrelated, unrelatedCursor));
    await progressStub(conversation).awaitProgressWaiterCount(3);
    await progressStub(unrelated).awaitProgressWaiterCount(1);

    await Effect.runPromise(Fiber.interrupt(interruptedFiber));
    await progressStub(conversation).awaitProgressWaiterCount(2);
    expect(await progressStub(unrelated).progressWaiterCount()).toBe(1);

    await approve(conversation, main.receipt);
    await Effect.runPromise(Effect.all([Fiber.join(first), Fiber.join(second)]));
    expect(await progressStub(unrelated).progressWaiterCount()).toBe(1);

    await Effect.runPromise(Fiber.interrupt(unrelatedFiber));
    await progressStub(unrelated).awaitProgressWaiterCount(0);
    await approve(unrelated, other.receipt);
    await drainAlarmsUntil(conversation, allSettled(conversation));
    await drainAlarmsUntil(unrelated, allSettled(unrelated));
  }, 20_000);

  it("reconnects after eviction, reconstructs the wait, and rechecks durable authority", async () => {
    const conversation = lane("eviction");
    const approval = await prepareApproval(conversation);
    const cursor = approval.cursor;

    const waiting = runTrackedClientFiber(awaitProgressEffect(conversation, cursor));
    await progressStub(conversation).awaitProgressWaiterCount(1);
    const priorIncarnation = await progressStub(conversation).progressIncarnation();
    await runInDurableObject(stubFor(conversation), (_instance, state) => {
      state.abort("#94 forced wait eviction");
    }).catch(() => undefined);

    const reconstructedIncarnation = await awaitReconstructedProgressWaiter(
      conversation,
      priorIncarnation,
      1,
    );
    expect(reconstructedIncarnation).not.toBe(priorIncarnation);
    await approve(conversation, approval.receipt);
    await Effect.runPromise(Fiber.join(waiting));
    await drainAlarmsUntil(conversation, allSettled(conversation));
    const after = await readCanonical(conversation);
    expect(after.some((record) => record.sequence > cursor)).toBe(true);
  }, 20_000);

  it("delivers the remote wake seam used by child, parent, and host settlement", async () => {
    const conversation = lane("remote");
    const approval = await prepareApproval(conversation);
    const waiting = runTrackedClientFiber(awaitProgressEffect(conversation, approval.cursor));
    await progressStub(conversation).awaitProgressWaiterCount(1);

    await stubFor(conversation).wake();
    await Effect.runPromise(Fiber.join(waiting));
    await progressStub(conversation).awaitProgressWaiterCount(0);
    expect(await progressStub(conversation).progressWaiterCount()).toBe(0);
    await approve(conversation, approval.receipt);
    await drainAlarmsUntil(conversation, allSettled(conversation));
  });

  it("preserves the typed non-materialized failure across the RPC client boundary", async () => {
    const conversation = lane("missing");
    const tag = await runClient(
      Effect.gen(function* () {
        const client = yield* CloudflareConversationClient;
        const exit = yield* Effect.exit(
          client.awaitProgress(decodeConversationId(conversation), ZERO_SEQUENCE, TEST_CALLER),
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
    expect(tag).toBe("ConversationNotMaterialized");
  });

  it("preserves a typed authorization denial across the RPC client boundary", async () => {
    const conversation = lane("denied");
    const tag = await runClient(
      Effect.gen(function* () {
        const client = yield* CloudflareConversationClient;
        const exit = yield* Effect.exit(
          client.awaitProgress(decodeConversationId(conversation), ZERO_SEQUENCE, TEST_CALLER),
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
