import { ApprovalDecisionCommand } from "@effect-agent/session";
import { Effect } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { AdmissionLimitExceeded, CloudflareConversationClient } from "../src/index.ts";
import {
  BOOK_TOOL_CALL_ID,
  TEST_CALLER,
  approvalDefinition,
  decodeConversationId,
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
 * Resource limits are checked BEFORE admission (exit gate; DEPLOY-007): the Conversation
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
  conversation: string,
  key: string,
  question = "respect the quota",
) =>
  runClient(
    Effect.gen(function* () {
      const client = yield* CloudflareConversationClient;
      return yield* client.submit(
        { definition },
        { question, ref: conversation },
        submitOptions(conversation, key),
      );
    }),
    namespace,
  );

const submitRefusal = (
  namespace: TestNamespace,
  definition: typeof plannerDefinition | typeof approvalDefinition,
  conversation: string,
  key: string,
  question?: string,
): Promise<unknown> =>
  runClient(
    Effect.gen(function* () {
      const client = yield* CloudflareConversationClient;
      return yield* client.submit(
        { definition },
        { question: question ?? "respect the quota", ref: conversation },
        submitOptions(conversation, key),
      );
    }).pipe(Effect.flip),
    namespace,
  );

describe("DC admission limits (before any ledger row exists)", () => {
  it("bounds readAll materialization while readPage remains available", async () => {
    const conversation = lane("read-all");
    await submitTo("CONVERSATIONS", plannerDefinition, conversation, "read-all-key");
    await drainAlarmsUntil(conversation, allSettled(conversation));

    const failure = await runClient(
      Effect.gen(function* () {
        const client = yield* CloudflareConversationClient;
        return yield* client
          .readAll(decodeConversationId(conversation), TEST_CALLER, { maxRecords: 1 })
          .pipe(Effect.flip);
      }),
    );
    expect(failure).toMatchObject({
      _tag: "ConversationReadLimitExceeded",
      maximum: 1,
      observed: 2,
    });

    const page = await runClient(
      Effect.gen(function* () {
        const client = yield* CloudflareConversationClient;
        return yield* client.readPage(decodeConversationId(conversation), TEST_CALLER, {
          limit: 1,
        });
      }),
    );
    expect(page).toHaveLength(1);
  });

  it("refuses the over-depth admission typed and admits again once the lane drains", async () => {
    const conversation = lane("queue-depth");
    // S1 suspends durably on approval, S2 queues behind it: depth 2 = the configured max.
    const receipt1 = await submitTo("LIMITED", approvalDefinition, conversation, "k1");
    await drainAlarmsUntil(conversation, anyInState(conversation, "suspended", "LIMITED"), {
      namespace: "LIMITED",
    });
    await submitTo("LIMITED", approvalDefinition, conversation, "k2");

    const refusal = await submitRefusal("LIMITED", approvalDefinition, conversation, "k3");
    expect(refusal).toBeInstanceOf(AdmissionLimitExceeded);
    if (refusal instanceof AdmissionLimitExceeded) {
      expect(refusal.limit).toBe("queue-depth");
      expect(refusal.actual).toBe(2);
      expect(refusal.maximum).toBe(2);
    }
    // NOTHING was admitted for the refused key: the lane still holds exactly two rows.
    expect(await laneRows(conversation, "LIMITED")).toHaveLength(2);

    // A REPLAYED key is exempt: the accepted-work obligation already exists, and refusing
    // the replay would strand a client that merely lost the first Receipt.
    const replayed = await submitTo("LIMITED", approvalDefinition, conversation, "k1");
    expect(replayed.submissionId).toBe(receipt1.submissionId);

    // Once the suspended head resolves and settles, the quota reopens.
    await runClient(
      Effect.gen(function* () {
        const client = yield* CloudflareConversationClient;
        return yield* client.resolveApproval(
          decodeConversationId(conversation),
          ApprovalDecisionCommand.make({
            submissionId: receipt1.submissionId,
            toolCallId: BOOK_TOOL_CALL_ID,
            decision: "approved",
            resolver: "cf-limits-approver",
            reason: "reopen the lane quota",
          }),
          TEST_CALLER,
        );
      }),
      "LIMITED",
    );
    await drainAlarmsUntil(
      conversation,
      async () => {
        const rows = await laneRows(conversation, "LIMITED");
        return rows.some((row) => row.state === "settled");
      },
      { namespace: "LIMITED" },
    );
    const receipt3 = await submitTo("LIMITED", approvalDefinition, conversation, "k3");
    expect(receipt3.conversationId).toBe(decodeConversationId(conversation));
  }, 60_000);

  it("refuses over-limit input bytes typed before any ledger row exists", async () => {
    const conversation = lane("input-bytes");
    const refusal = await submitRefusal(
      "LIMITED",
      plannerDefinition,
      conversation,
      "k1",
      "x".repeat(2_000),
    );
    expect(refusal).toBeInstanceOf(AdmissionLimitExceeded);
    if (refusal instanceof AdmissionLimitExceeded) {
      expect(refusal.limit).toBe("input-bytes");
      expect(refusal.maximum).toBe(512);
      expect(refusal.actual).toBeGreaterThan(512);
    }
    expect(await laneRows(conversation, "LIMITED")).toHaveLength(0);

    // The same lane still admits a bounded input afterwards: the refusal wrote nothing.
    await submitTo("LIMITED", plannerDefinition, conversation, "k1");
    await drainAlarmsUntil(conversation, allSettled(conversation, "LIMITED"), {
      namespace: "LIMITED",
    });
  }, 30_000);

  it("refuses admissions typed when the database exceeds its configured ceiling", async () => {
    const conversation = lane("database-bytes");
    const refusal = await submitRefusal("TINYDB", plannerDefinition, conversation, "k1");
    expect(refusal).toBeInstanceOf(AdmissionLimitExceeded);
    if (refusal instanceof AdmissionLimitExceeded) {
      expect(refusal.limit).toBe("database-bytes");
      expect(refusal.maximum).toBe(1);
      expect(refusal.actual).toBeGreaterThan(1);
    }
    expect(await laneRows(conversation, "TINYDB")).toHaveLength(0);
  }, 30_000);
});
