import { ApprovalDecisionCommand, RunJournalError } from "@effect-agent/session";
import { Effect } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { CloudflareConversationClient } from "../src/index.ts";
import {
  BOOK_TOOL_CALL_ID,
  TEST_CALLER,
  approvalDefinition,
  decodeConversationId,
  submitOptions,
} from "./fixtures.ts";
import { anyInState, drainAlarmsUntil, runClient } from "./harness.ts";

describe("Cloudflare Conversation Object typed client failures", () => {
  it("re-decodes RunJournalError from resolveApproval without erasing its error class", async () => {
    const conversation = "cf-run-journal-error-rpc";
    const receipt = await runClient(
      Effect.gen(function* () {
        const client = yield* CloudflareConversationClient;
        return yield* client.submit(
          { definition: approvalDefinition },
          { question: "journal failure boundary", ref: conversation },
          submitOptions(conversation, "run-journal-error-key"),
        );
      }),
      "RUN_JOURNAL_FAILURE",
    );
    await drainAlarmsUntil(
      conversation,
      anyInState(conversation, "suspended", "RUN_JOURNAL_FAILURE"),
      { namespace: "RUN_JOURNAL_FAILURE" },
    );

    const failure = await runClient(
      Effect.gen(function* () {
        const client = yield* CloudflareConversationClient;
        return yield* client
          .resolveApproval(
            decodeConversationId(conversation),
            ApprovalDecisionCommand.make({
              submissionId: receipt.submissionId,
              toolCallId: BOOK_TOOL_CALL_ID,
              decision: "approved",
              resolver: "journal-error-fixture",
              reason: "exercise typed error fidelity",
            }),
            TEST_CALLER,
          )
          .pipe(Effect.flip);
      }),
      "RUN_JOURNAL_FAILURE",
    );

    expect(failure).toBeInstanceOf(RunJournalError);
    expect(failure).toMatchObject({
      _tag: "RunJournalError",
      message: "fixture Run journal projection failure",
    });
  });
});
