import { ApprovalDecisionCommand } from "@effect-agent/thread";
import { Effect } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { AdmissionLimitExceeded, CloudflareThreadClient } from "../src/index.ts";
import {
  BOOK_TOOL_CALL_ID,
  approvalDefinition,
  decodeThreadId,
  plannerDefinition,
  submitOptions,
} from "./fixtures.ts";
import {
  allSettled,
  anyInState,
  drainAlarmsUntil,
  laneRows,
  runClient,
  type TestNamespace,
} from "./harness.ts";

/**
 * Resource limits are checked BEFORE admission (exit gate; DEPLOY-007): the Thread
 * Object's submit entry point refuses over-limit work with the typed
 * `AdmissionLimitExceeded` and writes NOTHING — no ledger row ever exists for a refused
 * admission. The `LIMITED` class binds tight queue/input quotas; `TINYDB` binds a database
 * ceiling below any real database so every admission must refuse.
 */

let laneCounter = 0;
const lane = (label: string): string => `cf-limits-${label}-${laneCounter++}`;

const submitTo = (
  namespace: TestNamespace,
  definition: typeof plannerDefinition | typeof approvalDefinition,
  thread: string,
  key: string,
  question = "respect the quota",
) =>
  runClient(
    Effect.gen(function* () {
      const client = yield* CloudflareThreadClient;

      return yield* client.submit(
        { definition },
        { question, ref: thread },
        submitOptions(thread, key),
      );
    }),
    namespace,
  );

const submitRefusal = (
  namespace: TestNamespace,
  definition: typeof plannerDefinition | typeof approvalDefinition,
  thread: string,
  key: string,
  question?: string,
): Promise<unknown> =>
  runClient(
    Effect.gen(function* () {
      const client = yield* CloudflareThreadClient;

      return yield* client.submit(
        { definition },
        { question: question ?? "respect the quota", ref: thread },
        submitOptions(thread, key),
      );
    }).pipe(Effect.flip),
    namespace,
  );

describe("DC admission limits (before any ledger row exists)", () => {
  it("refuses the over-depth admission typed and admits again once the lane drains", async () => {
    const thread = lane("queue-depth");
    // S1 suspends durably on approval, S2 queues behind it: depth 2 = the configured max.
    const receipt1 = await submitTo("LIMITED", approvalDefinition, thread, "k1");

    await drainAlarmsUntil(thread, anyInState(thread, "suspended", "LIMITED"), {
      namespace: "LIMITED",
    });
    await submitTo("LIMITED", approvalDefinition, thread, "k2");

    const refusal = await submitRefusal("LIMITED", approvalDefinition, thread, "k3");

    expect(refusal).toBeInstanceOf(AdmissionLimitExceeded);
    if (refusal instanceof AdmissionLimitExceeded) {
      expect(refusal.limit).toBe("queue-depth");
      expect(refusal.actual).toBe(2);
      expect(refusal.maximum).toBe(2);
    }
    // NOTHING was admitted for the refused key: the lane still holds exactly two rows.
    expect(await laneRows(thread, "LIMITED")).toHaveLength(2);

    // A REPLAYED key is exempt: the accepted-work obligation already exists, and refusing
    // the replay would strand a client that merely lost the first Receipt.
    const replayed = await submitTo("LIMITED", approvalDefinition, thread, "k1");

    expect(replayed.submissionId).toBe(receipt1.submissionId);

    // Once the suspended head resolves and settles, the quota reopens.
    await runClient(
      Effect.gen(function* () {
        const client = yield* CloudflareThreadClient;

        return yield* client.resolveApproval(
          decodeThreadId(thread),
          ApprovalDecisionCommand.make({
            submissionId: receipt1.submissionId,
            toolCallId: BOOK_TOOL_CALL_ID,
            decision: "approved",
            resolver: "cf-limits-approver",
            reason: "reopen the lane quota",
          }),
        );
      }),
      "LIMITED",
    );
    await drainAlarmsUntil(
      thread,
      async () => {
        const rows = await laneRows(thread, "LIMITED");

        return rows.some((row) => row.state === "settled");
      },
      { namespace: "LIMITED" },
    );
    const receipt3 = await submitTo("LIMITED", approvalDefinition, thread, "k3");

    expect(receipt3.threadId).toBe(decodeThreadId(thread));
  }, 60_000);

  it("refuses over-limit input bytes typed before any ledger row exists", async () => {
    const thread = lane("input-bytes");

    const refusal = await submitRefusal(
      "LIMITED",
      plannerDefinition,
      thread,
      "k1",
      "x".repeat(2_000),
    );

    expect(refusal).toBeInstanceOf(AdmissionLimitExceeded);
    if (refusal instanceof AdmissionLimitExceeded) {
      expect(refusal.limit).toBe("input-bytes");
      expect(refusal.maximum).toBe(512);
      expect(refusal.actual).toBeGreaterThan(512);
    }
    expect(await laneRows(thread, "LIMITED")).toHaveLength(0);

    // The same lane still admits a bounded input afterwards: the refusal wrote nothing.
    await submitTo("LIMITED", plannerDefinition, thread, "k1");
    await drainAlarmsUntil(thread, allSettled(thread, "LIMITED"), {
      namespace: "LIMITED",
    });
  }, 30_000);

  it("refuses admissions typed when the database exceeds its configured ceiling", async () => {
    const thread = lane("database-bytes");
    const refusal = await submitRefusal("TINYDB", plannerDefinition, thread, "k1");

    expect(refusal).toBeInstanceOf(AdmissionLimitExceeded);
    if (refusal instanceof AdmissionLimitExceeded) {
      expect(refusal.limit).toBe("database-bytes");
      expect(refusal.maximum).toBe(1);
      expect(refusal.actual).toBeGreaterThan(1);
    }
    expect(await laneRows(thread, "TINYDB")).toHaveLength(0);
  }, 30_000);
});
