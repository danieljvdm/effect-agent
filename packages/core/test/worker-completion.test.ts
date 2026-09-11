import { Schema } from "effect";
import { describe, expect, it } from "vite-plus/test";

import { InputMessage } from "../src/Messaging.ts";
import { WorkerCompletion, WorkerReport } from "../src/Worker.ts";

const identity = {
  _tag: "Settled",
  worker: {
    schemaVersion: 1,
    delegationId: "research",
    targetAgentId: "child",
    threadId: "child-thread",
  },
  receipt: {
    threadId: "child-thread",
    submissionId: "input-1",
    receiptId: "receipt-1",
    queueSequence: 1,
  },
  runId: "run-1",
  settlementId: "settlement-1",
};

const completed = {
  _tag: "WorkerCompletion",
  schemaVersion: 1,
  budgetExhausted: true,
  report: { ...identity, outcome: "completed", result: { activities: ["walk"], partial: true } },
};

const failed = {
  ...completed,
  budgetExhausted: false,
  report: {
    ...identity,
    outcome: "failed",
    failure: {
      _tag: "SubagentExecutionFailure",
      delegationId: "research",
      targetAgentId: "child",
      classification: "child-failed",
      childThreadId: "child-thread",
      childSubmissionId: "input-1",
      childRunId: "run-1",
      errorTag: "WorkerFailed",
      message: "Worker input failed",
    },
  },
};

describe("worker completion wire schema", () => {
  it.each([completed, failed])(
    "round-trips a standard completion through stored input metadata",
    (encoded) => {
      const decoded = Schema.decodeUnknownSync(InputMessage)(encoded);

      expect(Schema.encodeSync(InputMessage)(decoded)).toEqual(encoded);
    },
  );
  it("decodes the declaration's projected success with its exact inferred type", () => {
    const report = Schema.decodeUnknownSync(
      WorkerReport(
        Schema.Struct({ activities: Schema.Array(Schema.String), partial: Schema.Boolean }),
      ),
    )(completed.report);

    if (report.outcome !== "completed") throw new Error("Expected completion");
    const activities: ReadonlyArray<string> = report.result.activities;

    expect(activities).toEqual(["walk"]);
  });
  it("rejects mismatched receipts, missing run identities, and unbounded failures", () => {
    for (const encoded of [
      { ...completed, report: { ...completed.report, runId: undefined } },
      {
        ...completed,
        report: {
          ...completed.report,
          receipt: { ...identity.receipt, threadId: "another-child" },
        },
      },
      {
        ...failed,
        report: {
          ...failed.report,
          failure: { ...failed.report.failure, message: "x".repeat(4097) },
        },
      },
    ])
      expect(Schema.decodeUnknownExit(WorkerCompletion)(encoded)._tag).toBe("Failure");
  });
});
