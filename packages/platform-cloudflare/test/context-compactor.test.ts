import { CloudflareThreadClient } from "@effect-agent/platform-cloudflare/cloudflare-thread-client";
import { runInDurableObject } from "cloudflare:test";
import { Effect } from "effect";
import { submissionSettlementRecordId } from "effect-agent/submission-ledger";
import { describe, expect, it } from "vite-plus/test";

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
});
