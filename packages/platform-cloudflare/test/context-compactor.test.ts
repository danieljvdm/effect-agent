import { submissionSettlementRecordId } from "@effect-agent/thread";
import { runInDurableObject } from "cloudflare:test";
import { Effect } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { CloudflareThreadClient } from "../src/index.ts";
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
  thread: string,
  question: string,
  key: string,
  namespace: TestNamespace,
) => {
  const receipt = await runClient(
    Effect.gen(function* () {
      const client = yield* CloudflareThreadClient;
      return yield* client.submit(
        { definition: contextCompactorDefinition },
        { question, ref: thread },
        submitOptions(thread, key),
      );
    }),
    namespace,
  );
  await drainAlarmsUntil(thread, allSettled(thread, namespace), { namespace });
  const settlement = await runClient(
    Effect.gen(function* () {
      const client = yield* CloudflareThreadClient;
      return yield* client.awaitSettlement(receipt);
    }),
    namespace,
  );
  const records = await readCanonical(thread, namespace);
  const terminal = records.find(
    (envelope) => envelope.record.recordId === submissionSettlementRecordId(receipt.submissionId),
  )?.record.payload;
  if (terminal?._tag !== "SubmissionSettled") {
    throw new Error(`Missing canonical settlement for ${receipt.submissionId}`);
  }
  return { receipt, settlement, records, terminal };
};

const abortIncarnation = (thread: string): Promise<void> =>
  runInDurableObject(stubFor(thread, "CONTEXT_COMPACTOR"), (_instance, state) => {
    state.abort("issue #49 reconstruction probe");
  }).then(
    () => undefined,
    () => undefined,
  );

describe("Cloudflare replaceable compaction", () => {
  it("retains independent Tool authorization alongside a compactor after eviction", async () => {
    const thread = lane("authorization");
    for (const incarnation of [1, 2]) {
      await submitAndSettle(thread, "seed", `${thread}-seed-${incarnation}`, "CONTEXT_COMPACTOR");
      const compacted = await submitAndSettle(
        thread,
        "compact",
        `${thread}-compact-${incarnation}`,
        "CONTEXT_COMPACTOR",
      );
      expect(compacted.terminal.result).toEqual({ answer: "compacted" });
      const receipt = await runClient(
        CloudflareThreadClient.use((client) =>
          client.submit(
            { definition: searchDefinition },
            { question: "search", ref: thread },
            submitOptions(thread, `${thread}-denied-${incarnation}`),
          ),
        ),
        "CONTEXT_COMPACTOR",
      );
      await drainAlarmsUntil(thread, allSettled(thread, "CONTEXT_COMPACTOR"), {
        namespace: "CONTEXT_COMPACTOR",
      });
      const settlement = await runClient(
        CloudflareThreadClient.use((client) => client.awaitSettlement(receipt)),
        "CONTEXT_COMPACTOR",
      );
      expect(settlement).toMatchObject({
        outcome: "failed",
        failure: {
          errorTag: "AgentToolAuthorizationDenied",
          message: "host denied Tool execution",
        },
      });
      expect(contextAuthorizationProbe(thread)).toEqual({
        acquisitions: incarnation,
        calls: incarnation,
      });
      expect(contextCompactorProbe(thread).acquisitions).toBe(incarnation);
      const records = await readCanonical(thread, "CONTEXT_COMPACTOR");
      expect(records.some(({ record }) => record.payload._tag === "ToolCallSettled")).toBe(false);
      if (incarnation === 1) await abortIncarnation(thread);
    }
  });

  it("keeps the existing no-compactor construction behavior", async () => {
    const thread = lane("default");
    const result = await submitAndSettle(
      thread,
      "without a host compactor",
      `${thread}-key`,
      "THREADS",
    );

    expect(result.settlement.outcome).toBe("completed");
    expect(result.terminal.result).toEqual({ answer: "uncompacted" });
  });

  it("invokes the provided compactor without replacing canonical history", async () => {
    const thread = lane("provided");
    const original = "preserve this exact canonical input";
    await submitAndSettle(thread, original, `${thread}-seed`, "CONTEXT_COMPACTOR");
    const result = await submitAndSettle(thread, original, `${thread}-key`, "CONTEXT_COMPACTOR");

    expect(result.settlement.outcome).toBe("completed");
    expect(result.terminal.result).toEqual({ answer: "compacted" });
    expect(contextCompactorProbe(thread)).toMatchObject({
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
    const thread = lane("typed-failure");
    await submitAndSettle(thread, "seed", `${thread}-seed`, "CONTEXT_COMPACTOR");
    const result = await submitAndSettle(
      thread,
      "[host-compactor-failure] preserve the refused input",
      `${thread}-key`,
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
    const thread = lane("reconstruction");
    await submitAndSettle(thread, "first", `${thread}-first`, "CONTEXT_COMPACTOR");
    await submitAndSettle(thread, "second", `${thread}-second`, "CONTEXT_COMPACTOR");

    const beforeEviction = contextCompactorProbe(thread);
    expect(beforeEviction.acquisitions).toBe(1);
    expect(beforeEviction.invocations).toBe(1);

    await abortIncarnation(thread);
    const reconstructed = await submitAndSettle(
      thread,
      "third after eviction",
      `${thread}-third`,
      "CONTEXT_COMPACTOR",
    );
    expect(reconstructed.terminal.result).toEqual({ answer: "compacted" });

    const afterEviction = contextCompactorProbe(thread);
    expect(afterEviction.acquisitions).toBe(2);
    expect(afterEviction.invocations).toBe(2);
    expect(JSON.stringify(reconstructed.records)).toContain("third after eviction");
    expect(
      reconstructed.records.filter((entry) => entry.record.payload._tag === "CompactionCreated"),
    ).toHaveLength(2);
  });
});
