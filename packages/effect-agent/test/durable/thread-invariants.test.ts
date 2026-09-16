import { NodeCrypto } from "@effect/platform-node";
import { describe, expect, it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import { EMPTY_TAIL_DIGEST } from "effect-agent/digest";
import { CanonicalRecordEnvelope, CanonicalSequence, RecordEnvelope } from "effect-agent/records";
import { runIdForSubmission } from "effect-agent/run-journal";
import {
  SubmissionSnapshot,
  submissionInputRecordId,
  submissionSettlementId,
  submissionSettlementRecordId,
} from "effect-agent/submission-ledger";
import { verifyThreadInvariants } from "effect-agent/thread-invariants";
import { ThreadExport } from "effect-agent/thread-store";

const digest = "a".repeat(64);
const createdAt = "2026-09-16T00:00:00.000Z";

const submission = (name: string, queueSequence: number) =>
  Schema.decodeSync(SubmissionSnapshot)({
    submissionId: name,
    threadId: "invariant-thread",
    queueSequence,
    principal: "test",
    idempotencyKey: name,
    agentId: "test",
    agentDigests: { agent: digest, model: digest, tools: digest },
    deploymentId: "test",
    inputPayload: name,
    inputDigest: digest,
    receiptId: `receipt-${name}`,
    state: "settled",
    settledOutcome: "completed",
    createdAt,
  });

const a = submission("a", 1);
const b = submission("b", 2);
const c = submission("c", 3);

const record = (recordId: string, payload: (typeof RecordEnvelope.Encoded)["payload"]) =>
  Schema.decodeSync(RecordEnvelope)({
    recordId,
    family: "thread",
    schemaVersion: 1,
    createdAt,
    deploymentId: "test",
    payload,
  });

const input = (row: SubmissionSnapshot) =>
  record(submissionInputRecordId(row.submissionId), {
    _tag: "UserInputRecorded",
    submissionId: row.submissionId,
    runId: runIdForSubmission(row.submissionId),
    kind: "user",
    input: row.inputPayload,
  });

const settlement = (row: SubmissionSnapshot) =>
  record(submissionSettlementRecordId(row.submissionId), {
    _tag: "SubmissionSettled",
    submissionId: row.submissionId,
    settlementId: submissionSettlementId(row.submissionId),
    receiptId: row.receiptId,
    runId: runIdForSubmission(row.submissionId),
    outcome: "completed",
    result: "done",
  });

const unknown = (row: SubmissionSnapshot, toolCallId = "call") =>
  record(`unknown-${row.submissionId}-${toolCallId}`, {
    _tag: "ToolCallUnknown",
    runId: runIdForSubmission(row.submissionId),
    turn: 1,
    toolCallId,
    toolName: "write",
    reason: "The external outcome is unknown",
  });

const resolved = (row: SubmissionSnapshot, toolCallId = "call") =>
  record(`resolved-${row.submissionId}-${toolCallId}`, {
    _tag: "ToolCallResolved",
    runId: runIdForSubmission(row.submissionId),
    toolCallId,
    resolution: "completed-with-result",
    author: "operator",
    reason: "The service confirmed the outcome",
  });

const toolSettled = (row: SubmissionSnapshot) =>
  record(`result-${row.submissionId}`, {
    _tag: "ToolCallSettled",
    runId: runIdForSubmission(row.submissionId),
    toolCallId: "call",
    toolName: "write",
    result: "done",
    isFailure: false,
  });

const verify = (source: ReadonlyArray<RecordEnvelope>, submissions = [a, b]) => {
  const records = source.map((entry, index) =>
    Schema.decodeSync(CanonicalRecordEnvelope)({
      threadId: a.threadId,
      batchId: `batch-${index + 1}`,
      sequence: index + 1,
      offset: `offset-${index + 1}`,
      record: Schema.encodeSync(RecordEnvelope)(entry),
    }),
  );

  return verifyThreadInvariants({
    export: ThreadExport.make({
      format: "effect-agent/thread@1",
      threadId: a.threadId,
      tailSequence: Schema.decodeSync(CanonicalSequence)(records.length),
      tailDigest: EMPTY_TAIL_DIGEST,
      records,
    }),
    submissions,
    requireAllSettled: true,
  }).pipe(Effect.provide(NodeCrypto.layer));
};

describe("Thread settlement ordering invariants", () => {
  it.effect("keeps a proven bypass after the older Run wakes during the later Run", () =>
    Effect.gen(function* () {
      const report = yield* verify([
        input(a),
        unknown(a),
        unknown(a, "sibling"),
        resolved(a, "sibling"),
        input(b),
        resolved(a),
        settlement(b),
        settlement(a),
      ]);

      expect(report.ok).toBe(true);
      expect(report.checks.find((check) => check.name === "fifo-settlement-order")?.status).toBe(
        "passed",
      );
    }),
  );

  const invalidBypasses = [
    { name: "no unknown evidence", prefix: [input(a), input(b)] },
    { name: "unknown evidence from another Run", prefix: [input(a), unknown(c), input(b)] },
    { name: "unknown recorded after later input", prefix: [input(a), input(b), unknown(a)] },
    {
      name: "resolution before later input",
      prefix: [input(a), unknown(a), resolved(a), input(b)],
    },
    {
      name: "settled Tool result before later input",
      prefix: [input(a), unknown(a), toolSettled(a), input(b)],
    },
    {
      name: "abort requested before later input",
      prefix: [
        input(a),
        unknown(a),
        record("abort-a", {
          _tag: "AbortRequested",
          submissionId: a.submissionId,
          author: "operator",
          reason: "stop",
        }),
        input(b),
      ],
    },
  ];

  for (const scenario of invalidBypasses) {
    it.effect(`rejects reversed settlements with ${scenario.name}`, () =>
      Effect.gen(function* () {
        const report = yield* verify([...scenario.prefix, settlement(b), settlement(a)]);

        expect(report.ok).toBe(false);
        expect(
          report.checks.filter((check) => check.status === "failed").map((check) => check.name),
        ).toEqual(["fifo-settlement-order"]);
      }),
    );
  }

  it.effect.each([
    { order: [b, c, a], expected: "passed" },
    { order: [c, b, a], expected: "failed" },
  ])("preserves FIFO between eligible followers: $expected", ({ order, expected }) =>
    Effect.gen(function* () {
      const report = yield* verify(
        [input(a), unknown(a), input(b), input(c), ...order.map(settlement)],
        [a, b, c],
      );

      expect(report.checks.find((check) => check.name === "fifo-settlement-order")?.status).toBe(
        expected,
      );
    }),
  );
});
