import { OperationDenied } from "@effect-agent/session";
import { Effect } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { CloudflareConversationClient } from "../src/index.ts";
import { TEST_CALLER, decodeConversationId, plannerDefinition, submitOptions } from "./fixtures.ts";
import { runClient } from "./harness.ts";

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
  });
});
