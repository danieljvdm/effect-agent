import { describe, expect, it } from "@effect/vitest";
import { Schema } from "effect";

import {
  RecoveryCheckpointContents,
  checkpointSuffixCompatible,
} from "../../src/durable/internal/journal-checkpoint.ts";
import { CanonicalRecordEnvelope } from "../../src/durable/Records.ts";
import { SaveRecoveryCheckpointRequest } from "../../src/durable/ThreadStore.ts";

// Encoded fixtures pin the disposable format independently of runtime constructors.
const digest = "a".repeat(64);
const runId = "run:checkpoint-fixture";

const envelope = (sequence: number, payload: unknown) => ({
  threadId: "checkpoint-fixture",
  batchId: `batch-${sequence}`,
  sequence,
  offset: `fixture:${sequence}`,
  record: {
    recordId: `record-${sequence}`,
    family: "thread",
    schemaVersion: 1,
    createdAt: "2026-01-01T00:00:00.000Z",
    deploymentId: "fixture",
    payload,
  },
});

const replacement = envelope(5, {
  _tag: "CompactionCreated",
  runId,
  turn: 3,
  kind: "rollover",
  coversThrough: 4,
  handoff: "Continue the original request.",
});

const state = {
  schemaVersion: 1,
  policyAccountingVersion: 1,
  submissionId: "checkpoint-fixture",
  submissionIds: ["checkpoint-fixture"],
  seed: {
    runId,
    throughSequence: 2,
    firstSequence: 1,
    committedTurns: 1,
    policyUsage: {
      committedTurns: 1,
      toolCalls: 0,
      programmaticToolCalls: 0,
      consecutiveToolFailures: 0,
      finalizationUsed: false,
    },
    modelCalls: 1,
    unobservedModelCalls: 0,
    inputTokens: 10,
    outputTokens: 2,
    lastInputTokens: 10,
    lastOutputTokens: 2,
    costMicrousd: 0,
    summarizedModelUsage: {
      modelCalls: 0,
      inputTokens: { total: 0, uncached: 0, cacheRead: 0, cacheWrite: 0 },
      outputTokens: { total: 0, text: 0, reasoning: 0 },
      costMicrousd: 0,
      byModel: [],
      usageStatus: "unknown",
      pricingStatus: "unknown",
      unobservedModelCalls: 0,
    },
    protectedContext: {
      content: [{ role: "system", content: "Original instructions.", options: {} }],
    },
    contextWindowId: "context:checkpoint-fixture:3",
    frontier: { sequence: 2, tag: "ModelResponseRecorded" },
    compaction: replacement,
  },
  records: [replacement],
};

const contents = { state, digest };

it("requires canonical target proof for sparse selections in both cached records and new suffixes", () => {
  const decoded = Schema.decodeUnknownSync(RecoveryCheckpointContents)(contents);

  const selection = decodeEnvelope(
    envelope(9, {
      _tag: "CompactionCreated",
      runId,
      turn: 4,
      kind: "clear-tool-results",
      coversThrough: 8,
      toolResultRecordIds: ["record-7"],
    }),
  );

  expect(checkpointSuffixCompatible(decoded.state.seed, [selection], [])).toBe(false);
  expect(checkpointSuffixCompatible(decoded.state.seed, [], [selection])).toBe(false);
  expect(checkpointSuffixCompatible(decoded.state.seed, [selection], [settled(7)])).toBe(true);
});

const checkpoint = {
  schemaVersion: 1,
  threadId: "checkpoint-fixture",
  throughSequence: 5,
  tailDigest: digest,
  engineVersion: "effect-agent/recovery@1",
  agentDefinitionDigest: digest,
  modelDigest: digest,
  toolDigest: digest,
  state: contents,
  createdAt: "2026-01-01T00:00:00.000Z",
};

const decodeEnvelope = Schema.decodeUnknownSync(CanonicalRecordEnvelope);

const response = (sequence: number) =>
  decodeEnvelope(
    envelope(sequence, {
      _tag: "ModelResponseRecorded",
      runId,
      turnId: `turn:${runId}:2`,
      turn: 2,
      messages: {
        content: [
          {
            role: "assistant",
            options: {},
            content: [
              {
                type: "tool-call",
                id: "call",
                name: "write",
                params: {},
                providerExecuted: false,
                options: {},
              },
            ],
          },
        ],
      },
      messagesDigest: digest,
    }),
  );

const settled = (sequence: number) =>
  decodeEnvelope(
    envelope(sequence, {
      _tag: "ToolCallSettled",
      runId,
      toolCallId: "call",
      toolName: "write",
      result: "recorded",
      isFailure: false,
    }),
  );

describe("recovery checkpoint compatibility", () => {
  it("round-trips the fenced request and versioned state as JSON", () => {
    const codec = Schema.fromJsonString(SaveRecoveryCheckpointRequest);
    const encoded = JSON.stringify({ checkpoint, producerEpoch: 7 });
    const decoded = Schema.decodeSync(codec)(encoded);

    expect(JSON.parse(Schema.encodeSync(codec)(decoded))).toEqual(JSON.parse(encoded));
    const saved = Schema.decodeUnknownSync(RecoveryCheckpointContents)(decoded.checkpoint.state);

    expect(Schema.encodeSync(RecoveryCheckpointContents)(saved)).toEqual(contents);
  });

  it.each(["schemaVersion", "policyAccountingVersion"])("rejects incompatible %s", (field) => {
    expect(
      Schema.decodeUnknownExit(RecoveryCheckpointContents)({
        ...contents,
        state: { ...state, [field]: 2 },
      })._tag,
    ).toBe("Failure");
  });

  it("rejects late results whose declarations are already covered, missing, or in the future", () => {
    const seed = Schema.decodeUnknownSync(RecoveryCheckpointContents)(contents).state.seed;

    expect(checkpointSuffixCompatible(seed, [settled(7)], [response(3)])).toBe(false);
    expect(checkpointSuffixCompatible(seed, [settled(7)], [])).toBe(false);
    expect(checkpointSuffixCompatible(seed, [settled(7), response(8)], [])).toBe(false);
    expect(checkpointSuffixCompatible(seed, [response(6), settled(7)], [])).toBe(true);
  });
});

it("falls back to canonical replay for auxiliary accounting outside the checkpoint seed", () => {
  const decoded = Schema.decodeUnknownSync(RecoveryCheckpointContents)(contents);

  const reservation = decodeEnvelope(
    envelope(6, {
      _tag: "CompactionEvaluationReserved",
      runId,
      turn: 4,
      inputTokensEstimate: 100,
    }),
  );

  const accounting = decodeEnvelope(
    envelope(7, {
      _tag: "CompactionEvaluationRecorded",
      runId,
      turn: 4,
      usage: {
        provider: "selector",
        model: "test",
        purpose: "compaction",
        inputTokens: { total: 70, uncached: 70, cacheRead: 0, cacheWrite: 0 },
        outputTokens: { total: 2, text: 2, reasoning: 0 },
        costMicrousd: 0,
      },
    }),
  );

  for (const record of [reservation, accounting]) {
    expect(checkpointSuffixCompatible(decoded.state.seed, [record], [])).toBe(false);
    expect(checkpointSuffixCompatible(decoded.state.seed, [], [record])).toBe(false);
  }
});
