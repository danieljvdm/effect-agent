import { AbortCommand } from "@effect-agent/thread";
import { Effect } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { CloudflareThreadClient, ThreadObject } from "../src/index.ts";
import { plannerDefinition, submitOptions } from "./fixtures.ts";
import { allSettled, drainAlarmsUntil, runClient, stubFor } from "./harness.ts";

/**
 * P7 WP1 — the four administrative entry points over the real Durable Object RPC boundary
 * (plan §3 surfaces): every request and response is a closed Schema envelope, typed failures
 * re-decode to identical tags (`AdminFailed`), and protocol anomalies answer typed instead of
 * throwing. `wake()` already exists as its own entry point and is exercised by the alarm suite.
 */

let laneCounter = 0;
const lane = (): string => `cf-admin-encoded-${laneCounter++}`;

const submitPlanner = (thread: string, key: string) =>
  runClient(
    Effect.gen(function* () {
      const client = yield* CloudflareThreadClient;

      return yield* client.submit(
        { definition: plannerDefinition },
        { question: "explain the settled lane", ref: thread },
        submitOptions(thread, key),
      );
    }),
  );

/**
 * Invoke one admin entry point and decode its envelope. The stub's RPC surface types the
 * methods' results as `unknown`, so the resolution is normalized through `Promise.resolve`
 * instead of awaiting an untyped value directly.
 */
const callAdmin = async (invoke: () => unknown): Promise<ThreadObject.AdminResponse> => {
  const raw: unknown = await Promise.resolve(invoke());

  return Effect.runPromise(ThreadObject.decodeAdminResponse(raw));
};

describe("P7 admin encoded entry points (DC)", () => {
  it("preserves settlement and abort authorization denials through the RPC client", async () => {
    const receipt = await submitPlanner(lane(), "deny-target");

    const operations = await runClient(
      Effect.gen(function* () {
        const client = yield* CloudflareThreadClient;

        const awaited = yield* client.awaitSettlement(receipt).pipe(
          Effect.match({
            onSuccess: () => "unexpected success",
            onFailure: (error) => error._tag,
          }),
        );

        const aborted = yield* client
          .abort(
            receipt.threadId,
            AbortCommand.make({
              submissionId: receipt.submissionId,
              author: "operator",
              reason: "stop",
            }),
          )
          .pipe(
            Effect.match({
              onSuccess: () => "unexpected success",
              onFailure: (error) => error._tag,
            }),
          );

        return [awaited, aborted];
      }),
      "DENIED",
    );

    expect(operations).toEqual(["OperationDenied", "OperationDenied"]);
  });
  it("explainEncoded answers typed explanations for one Submission and for the lane", async () => {
    const thread = lane();
    const receipt = await submitPlanner(thread, "admin-explain");

    await drainAlarmsUntil(thread, allSettled(thread));
    const stub = stubFor(thread);

    const explained = await callAdmin(() =>
      stub.explainEncoded({ submissionId: receipt.submissionId }),
    );

    expect(explained._tag).toBe("ExplainedRecovery");
    if (explained._tag === "ExplainedRecovery") {
      expect(explained.explanations).toHaveLength(1);
      const explanation = explained.explanations[0];

      expect(explanation?.submission.submissionId).toBe(receipt.submissionId);
      expect(explanation?.decision._tag).toBe("NoAction");
      expect(explanation?.disposition).toBe("none");
      expect(explanation?.decisionMeaning).toContain("settled");
    }

    // The lane form explains nonterminal members only: a settled lane owes nothing.
    const laneExplained = await callAdmin(() => stub.explainEncoded({}));

    expect(laneExplained._tag).toBe("ExplainedRecovery");
    if (laneExplained._tag === "ExplainedRecovery") {
      expect(laneExplained.explanations).toEqual([]);
    }
  });

  it("verifyEncoded answers the typed integrity report with honest digest-chain scoping", async () => {
    const thread = lane();

    await submitPlanner(thread, "admin-verify");
    await drainAlarmsUntil(thread, allSettled(thread));

    const verified = await callAdmin(() => stubFor(thread).verifyEncoded({}));

    expect(verified._tag).toBe("VerifiedIntegrity");
    if (verified._tag === "VerifiedIntegrity") {
      expect(verified.report.ok).toBe(true);
      expect(verified.report.submissionCount).toBe(1);
      const byName = new Map(verified.report.checks.map((check) => [check.name, check]));

      expect(byName.get("record-identity")?.status).toBe("passed");
      expect(byName.get("fifo-settlement-order")?.status).toBe("passed");
      expect(byName.get("ledger-canonical-agreement")?.status).toBe("passed");
      // The port does not export per-batch producer identity; the report says so instead of
      // silently claiming the chain (adapter-level verifyOnOpen is the storage-side audit).
      expect(byName.get("digest-chain")?.status).toBe("skipped");
    }
  });

  it("retryEncoded refuses settled work typed and answers protocol anomalies typed", async () => {
    const thread = lane();
    const receipt = await submitPlanner(thread, "admin-retry");

    await drainAlarmsUntil(thread, allSettled(thread));
    const stub = stubFor(thread);

    const refused = await callAdmin(() =>
      stub.retryEncoded({
        submissionId: receipt.submissionId,
        author: "operator",
        reason: "re-drive settled work over the wire",
      }),
    );

    expect(refused._tag).toBe("AdminFailed");
    if (refused._tag === "AdminFailed") {
      expect(refused.failure._tag).toBe("RetryRefused");
      if (refused.failure._tag === "RetryRefused") {
        expect(refused.failure.refusal).toBe("settled");
        expect(refused.failure.submissionId).toBe(receipt.submissionId);
      }
    }

    // A malformed envelope never throws across the RPC boundary: it answers typed.
    const anomaly = await callAdmin(() => stub.retryEncoded({ nope: true }));

    expect(anomaly._tag).toBe("AdminFailed");
    if (anomaly._tag === "AdminFailed") {
      expect(anomaly.failure._tag).toBe("HostProtocolError");
    }
  });

  it("obligationsEncoded answers the typed obligation report", async () => {
    const thread = lane();

    await submitPlanner(thread, "admin-obligations");
    await drainAlarmsUntil(thread, allSettled(thread));

    const scanned = await callAdmin(() =>
      stubFor(thread).obligationsEncoded({ agingSeconds: 60, overdueSeconds: 600 }),
    );

    expect(scanned._tag).toBe("ObligationsScanned");
    if (scanned._tag === "ObligationsScanned") {
      // The settled lane owes nothing; the thresholds round-trip through the report.
      expect(scanned.report.entries).toEqual([]);
      expect(scanned.report.thresholds.agingSeconds).toBe(60);
      expect(scanned.report.thresholds.overdueSeconds).toBe(600);
    }
  });
});
