import { NodeCrypto } from "@effect/platform-node";
import { describe, expect, it, layer } from "@effect/vitest";
import { Effect, Schema } from "effect";
import { digestJson } from "effect-agent/digest";
import { ThreadId } from "effect-agent/identifiers";
import {
  CanonicalRecordEnvelope,
  MAX_PERSISTED_JSON_BYTES,
  PersistedJson,
  RecordEnvelope,
} from "effect-agent/records";
import {
  ThreadProjection,
  replayThread,
  replayThreadFromCheckpoint,
} from "effect-agent/thread-projection";

const SHA_256_A = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const SHA_256_B = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const SHA_256_C = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

const decodeRecord = (recordId: string, payload: (typeof RecordEnvelope.Encoded)["payload"]) =>
  Schema.decodeSync(RecordEnvelope)({
    recordId,
    family: "thread",
    schemaVersion: 1,
    createdAt: "2026-07-29T12:00:00.000Z",
    deploymentId: "test-deployment",
    payload,
  });

const decodeEnvelope = (sequence: number, record: RecordEnvelope): CanonicalRecordEnvelope =>
  Schema.decodeSync(CanonicalRecordEnvelope)({
    threadId: "travel-thread",
    batchId: "travel-batch",
    sequence,
    offset: `memory:${sequence}`,
    record: Schema.encodeSync(RecordEnvelope)(record),
  });

describe("thread canonical contracts", () => {
  layer(NodeCrypto.layer)((it) => {
    it.effect("digests object keys by UTF-16 code units, independent of insertion order", () =>
      Effect.gen(function* () {
        const ordered = yield* digestJson({
          alpha: 1,
          beta: { gamma: [true, null], delta: "d" },
        });

        const reordered = yield* digestJson({
          beta: { delta: "d", gamma: [true, null] },
          alpha: 1,
        });

        expect(reordered).toBe(ordered);

        // Precomposed U+00E0 and decomposed U+0061 U+0300 are canonically equivalent
        // but distinct code-unit sequences. Locale-aware collation treats them as equal,
        // so a stable sort would leak insertion order; code-unit ordering must not.
        const precomposedFirst = yield* digestJson({ "\u00e0": 1, "a\u0300": 2 });
        const decomposedFirst = yield* digestJson({ "a\u0300": 2, "\u00e0": 1 });

        expect(decomposedFirst).toBe(precomposedFirst);

        // Locale-aware collation orders "\u00e4" before "z" in en but after "z" in sv.
        // Code-unit ordering is the same on every host, whatever the process locale.
        const umlautFirst = yield* digestJson({ "\u00e4": 1, z: 2 });
        const umlautLast = yield* digestJson({ z: 2, "\u00e4": 1 });

        expect(umlautLast).toBe(umlautFirst);
        expect(yield* digestJson({ z: "\ud800", value: "😀é" })).toBe(
          "a2ae949a2f22f7fc31138231e3db19f57f017e0d8a09f7b2b049398f9346f57c",
        );
        expect(
          yield* digestJson([
            [{ z: "last", a: [false, null, 2.5] }],
            [],
            { nested: [[1], { b: 2, a: 3 }] },
          ]),
        ).toBe("8f7312be37bd42bc167b496ac42e8e8289a845c133097cf7ab5c8e5f7e38f5ab");
      }),
    );
  });

  it("accepts shared acyclic JSON while rejecting object and array ancestor cycles", () => {
    const shared = { amenities: ["hot tub", "kitchen"] };
    const dag = { first: shared, nested: { second: shared }, list: [shared, shared.amenities] };

    expect(Schema.decodeSync(PersistedJson)(dag)).toEqual(dag);
    const cycle: unknown[] = [];
    const parent = { shared, cycle };

    cycle.push(parent);
    expect(Schema.decodeUnknownExit(PersistedJson)(parent)._tag).toBe("Failure");
    expect(Schema.decodeUnknownExit(PersistedJson)(cycle)._tag).toBe("Failure");
  });

  it("charges every shared occurrence to traversal and byte limits", () => {
    // Fifteen binary levels expand to 65,535 nodes despite containing only fifteen arrays.
    let sharedTree: Schema.Json = null;

    for (let level = 0; level < 15; level++) sharedTree = [sharedTree, sharedTree];
    expect(Schema.decodeExit(PersistedJson)([sharedTree])._tag).toBe("Success");
    expect(Schema.decodeExit(PersistedJson)([sharedTree, null])._tag).toBe("Failure");

    const largeShared = { text: "x".repeat(MAX_PERSISTED_JSON_BYTES / 2) };

    expect(Schema.decodeExit(PersistedJson)(largeShared)._tag).toBe("Success");
    expect(Schema.decodeExit(PersistedJson)([largeShared, largeShared])._tag).toBe("Failure");
  });
});

describe("phase 5 durable canonical payloads", () => {
  const encodedToolCallPrepared = {
    _tag: "ToolCallPrepared",
    runId: "run-1",
    turnId: "turn-1",
    turn: 2,
    toolCallId: "call-1",
    toolName: "book_flight",
    parameters: { destination: "Kyoto", travelerRef: "traveler-7" },
    parametersDigest: SHA_256_A,
  } as const;

  const encodedToolCallUnknown = {
    _tag: "ToolCallUnknown",
    runId: "run-1",
    turn: 2,
    toolCallId: "call-1",
    toolName: "book_flight",
    reason: "worker lost after preparation without a canonical outcome",
  } as const;

  const encodedToolCallResolved = {
    _tag: "ToolCallResolved",
    runId: "run-1",
    toolCallId: "call-1",
    resolution: "completed-with-result",
    author: "operator",
    reason: "supplier store shows the booking",
  } as const;

  it("keeps an aborted Run's unknown call open when a later Run reuses its call ID", () => {
    const threadId = Schema.decodeSync(ThreadId)("travel-thread");
    const prepared = decodeEnvelope(1, decodeRecord("run-1-prepared", encodedToolCallPrepared));
    const unknown = decodeEnvelope(2, decodeRecord("run-1-unknown", encodedToolCallUnknown));

    const aborted = decodeEnvelope(
      3,
      decodeRecord("run-1-aborted", {
        _tag: "SubmissionSettled",
        submissionId: "submission-1",
        settlementId: "settlement:submission-1",
        receiptId: "receipt-1",
        runId: "run-1",
        outcome: "aborted",
      }),
    );

    const preparedAgain = decodeEnvelope(
      4,
      decodeRecord("run-2-prepared", {
        ...encodedToolCallPrepared,
        runId: "run-2",
        turnId: "turn-2",
      }),
    );

    const prefix = [prepared, unknown, aborted, preparedAgain];

    const checkpoint = Schema.decodeSync(ThreadProjection)(
      Schema.encodeSync(ThreadProjection)(replayThread(threadId, prefix)),
    );

    expect(checkpoint.openToolCalls.map((call) => [call.runId, call.toolCallId])).toEqual([
      ["run-1", "call-1"],
      ["run-2", "call-1"],
    ]);

    for (const terminal of [
      decodeRecord("run-2-settled", {
        _tag: "ToolCallSettled",
        runId: "run-2",
        toolCallId: "call-1",
        toolName: "book_flight",
        result: { bookingRef: "booking-43" },
        isFailure: false,
      }),
      decodeRecord("run-2-resolved", { ...encodedToolCallResolved, runId: "run-2" }),
    ]) {
      const suffix = [decodeEnvelope(5, terminal)];
      const full = replayThread(threadId, [...prefix, ...suffix]);

      expect(full.openToolCalls.map((call) => [call.runId, call.toolCallId])).toEqual([
        ["run-1", "call-1"],
      ]);
      expect(full.unknownToolCalls).toEqual([unknown.record.payload]);
      expect(replayThreadFromCheckpoint(checkpoint, suffix)).toEqual(full);
    }
  });
});

describe("S2 durable subagent canonical payloads", () => {
  const encodedSubagentRequested = {
    _tag: "SubagentRequested",
    runId: "run:submission-parent",
    turnId: "turn:run:submission-parent:1",
    turn: 1,
    toolCallId: "call-delegate-1",
    delegationId: "delegation-destination-research",
    targetAgentId: "destination-researcher",
    targetDigests: { agent: SHA_256_A, model: SHA_256_B, tools: SHA_256_C },
    childInput: { destination: "Kyoto", month: "October" },
    childInputDigest: SHA_256_A,
    grantDigest: SHA_256_B,
    reservationId: "run%3Asubmission-parent:call-delegate-1",
    reservationDigest: SHA_256_C,
    childThreadId: "subagent:submission-parent:call-delegate-1",
    childPrincipal: "tenant-a",
    childIdempotencyKey: "subagent:run:submission-parent:call-delegate-1",
  } as const;

  const encodedSubagentStarted = {
    _tag: "SubagentStarted",
    runId: "run:submission-parent",
    toolCallId: "call-delegate-1",
    childThreadId: "subagent:submission-parent:call-delegate-1",
    childSubmissionId: "submission-child-1",
    childReceiptId: "receipt-child-1",
    childRunId: "run:submission-child-1",
  } as const;

  const encodedSubagentJoined = {
    _tag: "SubagentJoined",
    runId: "run:submission-parent",
    toolCallId: "call-delegate-1",
    childSubmissionId: "submission-child-1",
    childSettlementId: "settlement:submission-child-1",
    childOutcome: "completed",
    childResultDigest: SHA_256_A,
    projectedResultDigest: SHA_256_B,
    usageSummary: { turns: 1, toolCalls: 0 },
    reservationId: "run%3Asubmission-parent:call-delegate-1",
    finalAccounting: { consumed: { turns: 1 }, released: { turns: 3 } },
  } as const;

  it("keeps subagent invocations separate when sequential Runs reuse a Tool Call ID", () => {
    const threadId = Schema.decodeSync(ThreadId)("travel-thread");
    const requested = decodeEnvelope(1, decodeRecord("first-requested", encodedSubagentRequested));
    const started = decodeEnvelope(2, decodeRecord("first-started", encodedSubagentStarted));
    const joined = decodeEnvelope(3, decodeRecord("first-joined", encodedSubagentJoined));

    const completed = decodeEnvelope(
      4,
      decodeRecord("first-completed", {
        _tag: "RunCompleted",
        runId: encodedSubagentRequested.runId,
        output: { answer: "Kyoto" },
      }),
    );

    const requestedAgain = decodeEnvelope(
      5,
      decodeRecord("second-requested", {
        ...encodedSubagentRequested,
        runId: "run:submission-second",
        turnId: "turn:run:submission-second:1",
        childInput: { destination: "Osaka", month: "November" },
        childThreadId: "subagent:submission-second:call-delegate-1",
        childIdempotencyKey: "subagent:run:submission-second:call-delegate-1",
        reservationId: "run%3Asubmission-second:call-delegate-1",
      }),
    );

    const startedAgain = decodeEnvelope(
      6,
      decodeRecord("second-started", {
        ...encodedSubagentStarted,
        runId: "run:submission-second",
        childThreadId: "subagent:submission-second:call-delegate-1",
        childSubmissionId: "submission-child-2",
        childReceiptId: "receipt-child-2",
        childRunId: "run:submission-child-2",
      }),
    );

    const joinedAgain = decodeEnvelope(
      7,
      decodeRecord("second-joined", {
        ...encodedSubagentJoined,
        runId: "run:submission-second",
        childSubmissionId: "submission-child-2",
        childSettlementId: "settlement:submission-child-2",
        reservationId: "run%3Asubmission-second:call-delegate-1",
      }),
    );

    const prefix = [requested, started, joined, completed, requestedAgain];

    const checkpoint = Schema.decodeSync(ThreadProjection)(
      Schema.encodeSync(ThreadProjection)(replayThread(threadId, prefix)),
    );

    const suffix = [startedAgain, joinedAgain];
    const full = replayThread(threadId, [...prefix, ...suffix]);

    expect(full.subagentInvocations).toEqual([
      {
        runId: encodedSubagentRequested.runId,
        toolCallId: encodedSubagentRequested.toolCallId,
        requested: requested.record.payload,
        started: started.record.payload,
        joined: joined.record.payload,
      },
      {
        runId: "run:submission-second",
        toolCallId: encodedSubagentRequested.toolCallId,
        requested: requestedAgain.record.payload,
        started: startedAgain.record.payload,
        joined: joinedAgain.record.payload,
      },
    ]);
    expect(replayThreadFromCheckpoint(checkpoint, suffix)).toEqual(full);
  });
});
