import { submissionSettlementRecordId } from "@effect-agent/session";
import { runInDurableObject } from "cloudflare:test";
import { Effect } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { CloudflareConversationClient } from "../src/index.ts";
import {
  contextCompactorDefinition,
  contextCompactorProbe,
  contextAuthorizationProbe,
  searchDefinition,
  submitOptions,
} from "./fixtures.ts";
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

describe("Cloudflare replaceable compaction", () => {
  it("retains independent Tool authorization alongside a compactor after eviction", async () => {
    const conversation = lane("authorization");
    for (const incarnation of [1, 2]) {
      await submitAndSettle(
        conversation,
        "seed",
        `${conversation}-seed-${incarnation}`,
        "CONTEXT_COMPACTOR",
      );
      const compacted = await submitAndSettle(
        conversation,
        "compact",
        `${conversation}-compact-${incarnation}`,
        "CONTEXT_COMPACTOR",
      );
      expect(compacted.terminal.result).toEqual({ answer: "compacted" });
      const receipt = await runClient(
        CloudflareConversationClient.use((client) =>
          client.submit(
            { definition: searchDefinition },
            { question: "search", ref: conversation },
            submitOptions(conversation, `${conversation}-denied-${incarnation}`),
          ),
        ),
        "CONTEXT_COMPACTOR",
      );
      await drainAlarmsUntil(conversation, allSettled(conversation, "CONTEXT_COMPACTOR"), {
        namespace: "CONTEXT_COMPACTOR",
      });
      const settlement = await runClient(
        CloudflareConversationClient.use((client) => client.awaitSettlement(receipt)),
        "CONTEXT_COMPACTOR",
      );
      expect(settlement).toMatchObject({
        outcome: "failed",
        failure: {
          errorTag: "AgentToolAuthorizationDenied",
          message: "host denied Tool execution",
        },
      });
      expect(contextAuthorizationProbe(conversation)).toEqual({
        acquisitions: incarnation,
        calls: incarnation,
      });
      expect(contextCompactorProbe(conversation).acquisitions).toBe(incarnation);
      const records = await readCanonical(conversation, "CONTEXT_COMPACTOR");
      expect(records.some(({ record }) => record.payload._tag === "ToolCallSettled")).toBe(false);
      if (incarnation === 1) await abortIncarnation(conversation);
    }
  });

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
    await submitAndSettle(conversation, original, `${conversation}-seed`, "CONTEXT_COMPACTOR");
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
    expect(result.records.some((entry) => entry.record.payload._tag === "CompactionCreated")).toBe(
      true,
    );
  });

  it("settles a typed compactor failure without persisting its cause", async () => {
    const conversation = lane("typed-failure");
    await submitAndSettle(conversation, "seed", `${conversation}-seed`, "CONTEXT_COMPACTOR");
    const result = await submitAndSettle(
      conversation,
      "[host-compactor-failure] preserve the refused input",
      `${conversation}-key`,
      "CONTEXT_COMPACTOR",
    );

    expect(result.settlement.outcome).toBe("failed");
    expect(result.terminal.result).toMatchObject({
      errorTag: "CompactionError",
      message: "Context compaction refused",
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
    expect(beforeEviction.invocations).toBe(1);

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
    expect(afterEviction.invocations).toBe(2);
    expect(JSON.stringify(reconstructed.records)).toContain("third after eviction");
    expect(
      reconstructed.records.filter((entry) => entry.record.payload._tag === "CompactionCreated"),
    ).toHaveLength(2);
  });
});
