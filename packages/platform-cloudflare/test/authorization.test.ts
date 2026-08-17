import { AbortCommand, OperationDenied } from "@effect-agent/session";
import { runInDurableObject } from "cloudflare:test";
import { Effect } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { CloudflareConversationClient } from "../src/index.ts";
import { TEST_CALLER, decodeConversationId, plannerDefinition, submitOptions } from "./fixtures.ts";
import { allSettled, drainAlarmsUntil, runClient, stubFor } from "./harness.ts";

describe("Cloudflare Conversation Object authorization", () => {
  it("re-decodes OperationDenied through the real DO RPC client", async () => {
    const conversation = "cf-operation-denied-rpc";
    const receipt = await runClient(
      Effect.gen(function* () {
        const client = yield* CloudflareConversationClient;
        return yield* client.submit(
          { definition: plannerDefinition },
          { question: "authorization boundary", ref: conversation },
          submitOptions(conversation, "denied-rpc-key"),
        );
      }),
      "DENIED",
    );

    const observeFailure = await runClient(
      Effect.gen(function* () {
        const client = yield* CloudflareConversationClient;
        return yield* client
          .readPage(decodeConversationId(conversation), TEST_CALLER)
          .pipe(Effect.flip);
      }),
      "DENIED",
    );
    expect(observeFailure).toBeInstanceOf(OperationDenied);
    expect(observeFailure).toMatchObject({
      _tag: "OperationDenied",
      operation: "observe",
      principal: TEST_CALLER.principal,
      reason: "denied by the platform RPC policy fixture",
    });

    const awaitFailure = await runClient(
      Effect.gen(function* () {
        const client = yield* CloudflareConversationClient;
        return yield* client.awaitSettlement(receipt, TEST_CALLER).pipe(Effect.flip);
      }),
      "DENIED",
    );
    expect(awaitFailure).toBeInstanceOf(OperationDenied);
    expect(awaitFailure).toMatchObject({
      _tag: "OperationDenied",
      operation: "awaitSettlement",
    });

    await drainAlarmsUntil(conversation, allSettled(conversation, "DENIED"), {
      namespace: "DENIED",
    });
    const alarmBefore = await runInDurableObject(
      stubFor(conversation, "DENIED"),
      (_instance, state) => state.storage.getAlarm(),
    );
    const abortFailure = await runClient(
      Effect.gen(function* () {
        const client = yield* CloudflareConversationClient;
        return yield* client
          .abort(
            decodeConversationId(conversation),
            AbortCommand.make({
              submissionId: receipt.submissionId,
              author: "denied-fixture",
              reason: "authorization must precede alarm mutation",
            }),
            TEST_CALLER,
          )
          .pipe(Effect.flip);
      }),
      "DENIED",
    );
    expect(abortFailure).toBeInstanceOf(OperationDenied);
    expect(abortFailure).toMatchObject({ _tag: "OperationDenied", operation: "abort" });
    const alarmAfter = await runInDurableObject(
      stubFor(conversation, "DENIED"),
      (_instance, state) => state.storage.getAlarm(),
    );
    expect(alarmAfter).toBe(alarmBefore);
  });
});
