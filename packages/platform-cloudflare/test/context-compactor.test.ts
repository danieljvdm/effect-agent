import { submissionSettlementRecordId } from "@effect-agent/session";
import { runInDurableObject } from "cloudflare:test";
import { Effect } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { CloudflareConversationClient } from "../src/index.ts";
import { contextCompactorDefinition, contextCompactorProbe, submitOptions } from "./fixtures.ts";
import {
  allSettled,
  drainAlarmsUntil,
  readCanonical,
  runClient,
  stubFor,
  type TestNamespace,
} from "./harness.ts";

let laneCounter = 0;
const lane = (label: string): string => `cf-context-compactor-${label}-${laneCounter++}`;

const submitAndSettle = async (
  conversation: string,
  question: string,
  key: string,
  namespace: TestNamespace,
) => {
  const receipt = await runClient(
    Effect.gen(function* () {
      const client = yield* CloudflareConversationClient;
      return yield* client.submit(
        { definition: contextCompactorDefinition },
        { question, ref: conversation },
        submitOptions(conversation, key),
      );
    }),
    namespace,
  );
  await drainAlarmsUntil(conversation, allSettled(conversation, namespace), { namespace });
  const settlement = await runClient(
    Effect.gen(function* () {
      const client = yield* CloudflareConversationClient;
      return yield* client.awaitSettlement(receipt);
    }),
    namespace,
  );
  const records = await readCanonical(conversation, namespace);
  const terminal = records.find(
    (envelope) => envelope.record.recordId === submissionSettlementRecordId(receipt.submissionId),
  )?.record.payload;
  if (terminal?._tag !== "SubmissionSettled") {
    throw new Error(`Missing canonical settlement for ${receipt.submissionId}`);
  }
  return { receipt, settlement, records, terminal };
};

const abortIncarnation = (conversation: string): Promise<void> =>
  runInDurableObject(stubFor(conversation, "CONTEXT_COMPACTOR"), (_instance, state) => {
    state.abort("issue #49 reconstruction probe");
  }).then(
    () => undefined,
    () => undefined,
  );

describe("Cloudflare host-supplied context preparation", () => {
  it("keeps the existing no-compactor construction behavior", async () => {
    const conversation = lane("default");
    const result = await submitAndSettle(
      conversation,
      "without a host compactor",
      `${conversation}-key`,
      "CONVERSATIONS",
    );

    expect(result.settlement.outcome).toBe("completed");
    expect(result.terminal.result).toEqual({ answer: "uncompacted" });
  });

  it("invokes the provided compactor without replacing canonical history", async () => {
    const conversation = lane("provided");
    const original = "preserve this exact canonical input";
    const result = await submitAndSettle(
      conversation,
      original,
      `${conversation}-key`,
      "CONTEXT_COMPACTOR",
    );

    expect(result.settlement.outcome).toBe("completed");
    expect(result.terminal.result).toEqual({ answer: "compacted" });
    expect(contextCompactorProbe(conversation)).toMatchObject({
      acquisitions: 1,
      invocations: 1,
    });
    const canonical = JSON.stringify(result.records);
    expect(canonical).toContain(original);
    expect(canonical).not.toContain("[host-compacted-context]");
  });

  it("settles a typed compactor failure without persisting its cause", async () => {
    const conversation = lane("typed-failure");
    const result = await submitAndSettle(
      conversation,
      "[host-compactor-failure] preserve the refused input",
      `${conversation}-key`,
      "CONTEXT_COMPACTOR",
    );

    expect(result.settlement.outcome).toBe("failed");
    expect(result.terminal.result).toMatchObject({
      errorTag: "RunContextPreparationError",
      message: "Context compaction failed (ContextTransformError)",
    });
    expect(result.terminal.result).not.toHaveProperty("cause");
    const canonical = JSON.stringify(result.records);
    expect(canonical).toContain("preserve the refused input");
    expect(canonical).not.toContain("the host compactor refused this context");
  });

  it("DEPLOY-013 acquires once per incarnation and reconstructs from canonical history after eviction", async () => {
    const conversation = lane("reconstruction");
    await submitAndSettle(conversation, "first", `${conversation}-first`, "CONTEXT_COMPACTOR");
    await submitAndSettle(conversation, "second", `${conversation}-second`, "CONTEXT_COMPACTOR");

    const beforeEviction = contextCompactorProbe(conversation);
    expect(beforeEviction.acquisitions).toBe(1);
    expect(beforeEviction.invocations).toBe(2);
    expect(beforeEviction.sourceMessageCounts[1]).toBeGreaterThan(
      beforeEviction.sourceMessageCounts[0] ?? 0,
    );

    await abortIncarnation(conversation);
    const reconstructed = await submitAndSettle(
      conversation,
      "third after eviction",
      `${conversation}-third`,
      "CONTEXT_COMPACTOR",
    );
    expect(reconstructed.terminal.result).toEqual({ answer: "compacted" });

    const afterEviction = contextCompactorProbe(conversation);
    expect(afterEviction.acquisitions).toBe(2);
    expect(afterEviction.invocations).toBe(3);
    expect(afterEviction.sourceMessageCounts[2]).toBeGreaterThan(
      afterEviction.sourceMessageCounts[1] ?? 0,
    );
    expect(JSON.stringify(reconstructed.records)).toContain("third after eviction");
    expect(JSON.stringify(reconstructed.records)).not.toContain("[host-compacted-context]");
  });
});
