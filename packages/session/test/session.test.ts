import { NodeCrypto } from "@effect/platform-node";
import { describe, expect, it, layer } from "@effect/vitest";
import { Duration, Effect, Schema } from "effect";
import { ConversationId, RunId, SubmissionId, ToolCallId } from "@effect-agent/core";

import {
  AbortCommand,
  AbortIntent,
  AdmissionConflict,
  AdmissionRequest,
  AdmissionResult,
  ApprovalConflict,
  ApprovalDecisionCommand,
  ApprovalDecisionIntent,
  CanonicalBatch,
  CanonicalRecordEnvelope,
  Claim,
  ClaimJoiningRequest,
  ConversationProjection,
  DEFAULT_OWNERSHIP_LEASE_DURATION,
  DefinitionDigestInput,
  DefinitionDigests,
  Digest,
  digestCanonicalBatch,
  digestDefinitions,
  digestJson,
  EMPTY_TAIL_DIGEST,
  JoinedToHost,
  JoiningClaim,
  LedgerCapabilities,
  MarkJoinedRequest,
  MarkUnknownRequest,
  MAX_PERSISTED_JSON_BYTES,
  MAX_PERSISTED_JSON_DEPTH,
  OwnershipLost,
  PersistedJson,
  PreparedToolCallEvidence,
  ProducerEpoch,
  ReconciliationDecision,
  RecordEnvelope,
  RecoverySnapshot,
  replayConversation,
  replayConversationFromCheckpoint,
  ReservedSettlement,
  RevertJoiningRequest,
  Settlement,
  SettlementConflict,
  SettlementReservation,
  SubmissionLookup,
  SubmissionSnapshot,
  submissionAbortBatchId,
  submissionAbortRecordId,
  submissionInputBatchId,
  submissionInputRecordId,
  submissionSettlementBatchId,
  submissionSettlementId,
  submissionSettlementRecordId,
  SuspendRequest,
  ToolReconciler,
  UnknownResolution,
  UnknownResolutionCommand,
  UnknownResolutionConflict,
  UnknownResolutionIntent,
  WakeScheduler,
  approvalDecisionBatchId,
  markUnknownBatchId,
  modelResponseInterruptedBatchId,
  modelResponseInterruptedRecordId,
  toolApprovalDecisionRecordId,
  toolApprovalRequestRecordId,
  toolCallPreparedRecordId,
  toolCallResolutionBatchId,
  toolCallResolvedRecordId,
  toolCallResultBatchId,
  toolCallSettledRecordId,
  toolCallUnknownRecordId,
  toolStepSettledBatchId,
  toolStepSettledRecordId,
  turnApprovalsBatchId,
  turnPreparedBatchId,
  turnResponseBatchId,
  turnResultsBatchId,
} from "../src/index.ts";

const SHA_256_A = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const SHA_256_B = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const SHA_256_C = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";

const definitionDigests = DefinitionDigests.make({
  agent: Schema.decodeSync(Digest)(SHA_256_A),
  model: Schema.decodeSync(Digest)(SHA_256_B),
  tools: Schema.decodeSync(Digest)(SHA_256_C),
});

const decodeRecord = (recordId: string, payload: (typeof RecordEnvelope.Encoded)["payload"]) =>
  Schema.decodeSync(RecordEnvelope)({
    recordId,
    family: "conversation",
    schemaVersion: 1,
    createdAt: "2026-07-29T12:00:00.000Z",
    deploymentId: "test-deployment",
    payload,
  });

const decodeEnvelope = (sequence: number, record: RecordEnvelope): CanonicalRecordEnvelope =>
  Schema.decodeSync(CanonicalRecordEnvelope)({
    conversationId: "travel-conversation",
    batchId: "travel-batch",
    sequence,
    offset: `memory:${sequence}`,
    record: Schema.encodeSync(RecordEnvelope)(record),
  });

describe("session canonical contracts", () => {
  const createdRecord = decodeRecord("record-created", {
    _tag: "ConversationCreated",
    agentId: "travel-planner",
    definitions: Schema.encodeSync(DefinitionDigests)(definitionDigests),
  });

  const createdEnvelope = decodeEnvelope(1, createdRecord);

  layer(NodeCrypto.layer)((it) => {
    it.effect("round-trips the current canonical record version", () =>
      Effect.gen(function* () {
        const encoded = yield* Schema.encodeEffect(CanonicalRecordEnvelope)(createdEnvelope);
        const decoded = yield* Schema.decodeUnknownEffect(CanonicalRecordEnvelope)(encoded);

        expect(decoded).toEqual(createdEnvelope);
        expect(decoded.record.schemaVersion).toBe(1);
        expect(decoded.offset).toBe("memory:1");
      }),
    );

    it.effect("produces stable definition and chained batch digests", () =>
      Effect.gen(function* () {
        const firstDefinitions = yield* digestDefinitions(
          DefinitionDigestInput.make({
            agent: { name: "travel-planner", revision: 1 },
            model: { model: "scripted", options: { temperature: 0, seed: 7 } },
            tools: [{ name: "search" }, { name: "hold" }],
          }),
        );
        const reorderedDefinitions = yield* digestDefinitions(
          DefinitionDigestInput.make({
            agent: { revision: 1, name: "travel-planner" },
            model: { options: { seed: 7, temperature: 0 }, model: "scripted" },
            tools: [{ name: "search" }, { name: "hold" }],
          }),
        );

        expect(reorderedDefinitions).toEqual(firstDefinitions);

        const encodedCreatedRecord = yield* Schema.encodeEffect(RecordEnvelope)(createdRecord);
        const batch = yield* Schema.decodeEffect(CanonicalBatch)({
          batchId: "travel-batch",
          producerId: "travel-producer",
          records: [encodedCreatedRecord],
        });
        const firstBatchDigest = yield* digestCanonicalBatch(EMPTY_TAIL_DIGEST, batch);
        const repeatedBatchDigest = yield* digestCanonicalBatch(EMPTY_TAIL_DIGEST, batch);

        expect(repeatedBatchDigest).toBe(firstBatchDigest);
        expect(firstBatchDigest).toMatch(/^[a-f0-9]{64}$/);
      }),
    );

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
      }),
    );
  });

  it("rejects an unsupported canonical record version", () => {
    const encoded = Schema.encodeSync(RecordEnvelope)(createdRecord);

    expect(() =>
      Schema.decodeUnknownSync(RecordEnvelope)({
        ...encoded,
        schemaVersion: 2,
      }),
    ).toThrow();
  });

  it("round-trips bounded persisted JSON and rejects resource-exhausting values", () => {
    const valid = {
      destination: "Kyoto",
      dates: ["2026-10-10", "2026-10-15"],
      preferences: { quiet: true, budget: 2_000 },
    };
    const decoded = Schema.decodeUnknownSync(PersistedJson)(valid);
    expect(Schema.encodeSync(PersistedJson)(decoded)).toEqual(valid);

    let tooDeep: unknown = "leaf";
    for (let depth = 0; depth <= MAX_PERSISTED_JSON_DEPTH; depth++) {
      tooDeep = { next: tooDeep };
    }
    expect(Schema.decodeUnknownExit(PersistedJson)(tooDeep)._tag).toBe("Failure");
    expect(
      Schema.decodeUnknownExit(PersistedJson)("x".repeat(MAX_PERSISTED_JSON_BYTES + 1))._tag,
    ).toBe("Failure");

    const cyclic: Record<string, unknown> = {};
    cyclic.self = cyclic;
    expect(Schema.decodeUnknownExit(PersistedJson)(cyclic)._tag).toBe("Failure");
  });

  it("replays a checkpoint suffix equivalently to full replay", () => {
    const userInput = decodeEnvelope(
      2,
      decodeRecord("record-user-input", {
        _tag: "UserInputRecorded",
        submissionId: "submission-1",
        kind: "user",
        input: { destination: "Kyoto", dates: ["2026-10-10", "2026-10-15"] },
      }),
    );
    const modelCompleted = decodeEnvelope(
      3,
      decodeRecord("record-model-completed", {
        _tag: "ModelCompleted",
        runId: "run-1",
        output: { itinerary: ["Kyoto"], nextAction: "review" },
      }),
    );
    const runCompleted = decodeEnvelope(
      4,
      decodeRecord("record-run-completed", {
        _tag: "RunCompleted",
        runId: "run-1",
        output: { itinerary: ["Kyoto"], nextAction: "review" },
      }),
    );
    const finalDigest = Schema.decodeSync(Digest)(
      "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
    );
    const records = [createdEnvelope, userInput, modelCompleted, runCompleted];

    const full = replayConversation(
      Schema.decodeSync(CanonicalRecordEnvelope)(
        Schema.encodeSync(CanonicalRecordEnvelope)(createdEnvelope),
      ).conversationId,
      records,
      finalDigest,
    );
    const checkpoint = replayConversation(
      full.conversationId,
      records.slice(0, 2),
      EMPTY_TAIL_DIGEST,
    );
    const resumed = replayConversationFromCheckpoint(checkpoint, records.slice(2), finalDigest);

    expect(resumed).toEqual(full);
    expect(
      Schema.decodeSync(ConversationProjection)(Schema.encodeSync(ConversationProjection)(resumed)),
    ).toEqual(full);
  });
});

describe("phase 4 durable canonical payloads", () => {
  const encodedAbortRequested = {
    _tag: "AbortRequested",
    submissionId: "submission-1",
    author: "operator",
    reason: "user cancelled the trip",
  } as const;
  const encodedSubmissionSettled = {
    _tag: "SubmissionSettled",
    submissionId: "submission-1",
    settlementId: "settlement:submission-1",
    receiptId: "receipt-1",
    outcome: "completed",
    runId: "run-1",
    result: { itinerary: ["Kyoto"] },
  } as const;
  const encodedModelResponseRecorded = {
    _tag: "ModelResponseRecorded",
    runId: "run-1",
    turnId: "turn-1",
    turn: 1,
    messages: [{ role: "assistant", content: "One quiet day in Kyoto." }],
    messagesDigest: SHA_256_A,
  } as const;

  it("round-trips the three new payload tags through the version-1 record envelope", () => {
    const payloads = [
      encodedAbortRequested,
      encodedSubmissionSettled,
      encodedModelResponseRecorded,
    ] as const;
    for (const [index, payload] of payloads.entries()) {
      const record = decodeRecord(`record-p4-${index}`, payload);
      expect(record.schemaVersion).toBe(1);
      const encoded = Schema.encodeSync(RecordEnvelope)(record);
      expect(encoded.payload).toEqual(payload);
      expect(Schema.decodeUnknownSync(RecordEnvelope)(encoded)).toEqual(record);
    }
  });

  it("decodes a settlement without a run or result", () => {
    const { runId: _runId, result: _result, ...withoutOptional } = encodedSubmissionSettled;
    const record = decodeRecord("record-p4-minimal-settlement", withoutOptional);
    expect(Schema.encodeSync(RecordEnvelope)(record).payload).toEqual(withoutOptional);
  });

  it("rejects malformed durable payloads", () => {
    const envelope = (payload: unknown): unknown => ({
      recordId: "record-p4-invalid",
      family: "conversation",
      schemaVersion: 1,
      createdAt: "2026-07-29T12:00:00.000Z",
      deploymentId: "test-deployment",
      payload,
    });
    const failures: ReadonlyArray<unknown> = [
      { ...encodedSubmissionSettled, outcome: "cancelled" },
      { ...encodedSubmissionSettled, settlementId: "" },
      { ...encodedSubmissionSettled, receiptId: "" },
      { ...encodedAbortRequested, author: "" },
      { ...encodedAbortRequested, reason: "x".repeat(64 * 1024 + 1) },
      { ...encodedModelResponseRecorded, turn: 0 },
      { ...encodedModelResponseRecorded, turn: 1.5 },
      { ...encodedModelResponseRecorded, messagesDigest: "not-a-digest" },
    ];
    for (const payload of failures) {
      expect(Schema.decodeUnknownExit(RecordEnvelope)(envelope(payload))._tag).toBe("Failure");
    }
  });

  it("reduces settlements and abort requests while model responses only advance the sequence", () => {
    const created = decodeEnvelope(
      1,
      decodeRecord("record-p4-created", {
        _tag: "ConversationCreated",
        agentId: "travel-planner",
        definitions: Schema.encodeSync(DefinitionDigests)(definitionDigests),
      }),
    );
    const response = decodeEnvelope(
      2,
      decodeRecord("record-p4-response", encodedModelResponseRecorded),
    );
    const abort = decodeEnvelope(3, decodeRecord("record-p4-abort", encodedAbortRequested));
    const settled = decodeEnvelope(4, decodeRecord("record-p4-settled", encodedSubmissionSettled));

    const afterResponse = replayConversation(created.conversationId, [created, response]);
    expect(afterResponse.throughSequence).toBe(2);
    expect(afterResponse.inputs).toEqual([]);
    expect(afterResponse.modelOutputs).toEqual([]);
    expect(afterResponse.settlements).toEqual([]);
    expect(afterResponse.abortRequests).toEqual([]);

    const full = replayConversation(created.conversationId, [created, response, abort, settled]);
    expect(full.throughSequence).toBe(4);
    expect(full.abortRequests).toEqual([abort.record.payload]);
    expect(full.settlements).toEqual([settled.record.payload]);

    const resumed = replayConversationFromCheckpoint(afterResponse, [abort, settled]);
    expect(resumed).toEqual(full);
    expect(
      Schema.decodeSync(ConversationProjection)(Schema.encodeSync(ConversationProjection)(full)),
    ).toEqual(full);
  });

  it("rejects a Phase 3 checkpoint projection state so callers rebuild from canonical records", () => {
    const phase3State = {
      conversationId: "travel-conversation",
      throughSequence: 2,
      tailDigest: SHA_256_A,
      inputs: [{ destination: "Kyoto" }],
      modelOutputs: [],
      completedRuns: [],
      failedRuns: [],
    };
    expect(Schema.decodeUnknownExit(ConversationProjection)(phase3State)._tag).toBe("Failure");
  });
});

describe("SubmissionLedger port schemas", () => {
  const submissionId = Schema.decodeSync(SubmissionId)("submission-1");
  const encodedAdmissionRequest = {
    conversationId: "travel-conversation",
    principal: "tenant-a",
    idempotencyKey: "client-key-1",
    agentId: "travel-planner",
    agentDigests: { agent: SHA_256_A, model: SHA_256_B, tools: SHA_256_C },
    deploymentId: "test-deployment",
    inputPayload: { destination: "Kyoto" },
    inputDigest: SHA_256_A,
  } as const;
  const encodedAdmissionResult = {
    submissionId: "submission-1",
    receiptId: "receipt-1",
    queueSequence: 0,
    state: "admitted",
    replayed: false,
  } as const;
  const encodedSubmissionSnapshot = {
    ...encodedAdmissionRequest,
    submissionId: "submission-1",
    queueSequence: 0,
    receiptId: "receipt-1",
    state: "running",
    createdAt: "2026-08-12T00:00:00.000Z",
  } as const;

  it("round-trips admission values and rejects malformed ones", () => {
    const request = Schema.decodeUnknownSync(AdmissionRequest)(encodedAdmissionRequest);
    expect(Schema.encodeSync(AdmissionRequest)(request)).toEqual(encodedAdmissionRequest);
    const result = Schema.decodeUnknownSync(AdmissionResult)(encodedAdmissionResult);
    expect(Schema.encodeSync(AdmissionResult)(result)).toEqual(encodedAdmissionResult);

    const requestFailures: ReadonlyArray<unknown> = [
      { ...encodedAdmissionRequest, idempotencyKey: "" },
      { ...encodedAdmissionRequest, principal: "x".repeat(257) },
      { ...encodedAdmissionRequest, inputDigest: "not-a-digest" },
    ];
    for (const encoded of requestFailures) {
      expect(Schema.decodeUnknownExit(AdmissionRequest)(encoded)._tag).toBe("Failure");
    }
    const resultFailures: ReadonlyArray<unknown> = [
      { ...encodedAdmissionResult, state: "unknown-state" },
      { ...encodedAdmissionResult, queueSequence: -1 },
    ];
    for (const encoded of resultFailures) {
      expect(Schema.decodeUnknownExit(AdmissionResult)(encoded)._tag).toBe("Failure");
    }
  });

  it("round-trips lookups, claims, and recovery snapshots", () => {
    const byId = Schema.decodeUnknownSync(SubmissionLookup)({
      _tag: "SubmissionLookupById",
      submissionId: "submission-1",
    });
    const byKey = Schema.decodeUnknownSync(SubmissionLookup)({
      _tag: "SubmissionLookupByKey",
      conversationId: "travel-conversation",
      principal: "tenant-a",
      idempotencyKey: "client-key-1",
    });
    expect(byId._tag).toBe("SubmissionLookupById");
    expect(byKey._tag).toBe("SubmissionLookupByKey");
    expect(Schema.decodeUnknownExit(SubmissionLookup)({ _tag: "LookupByGuess" })._tag).toBe(
      "Failure",
    );

    const encodedClaim = {
      submissionId: "submission-1",
      attemptId: "attempt-1",
      ownershipToken: "token-1",
      producerEpoch: 2,
      leaseExpiresAt: "2026-08-12T00:00:30.000Z",
      inputPayload: { destination: "Kyoto" },
    } as const;
    const claim = Schema.decodeUnknownSync(Claim)(encodedClaim);
    expect(Schema.encodeSync(Claim)(claim)).toEqual(encodedClaim);
    expect(Duration.toMillis(DEFAULT_OWNERSHIP_LEASE_DURATION)).toBe(30_000);

    const snapshot = Schema.decodeUnknownSync(SubmissionSnapshot)(encodedSubmissionSnapshot);
    expect(Schema.encodeSync(SubmissionSnapshot)(snapshot)).toEqual(encodedSubmissionSnapshot);

    const encodedRecovery = {
      submission: encodedSubmissionSnapshot,
      ownership: {
        attemptId: "attempt-1",
        ownerProducerId: "producer-1",
        producerEpoch: 2,
        leaseExpiresAt: "2026-08-12T00:00:30.000Z",
      },
      inputApplied: { recordId: "input:submission-1", sequence: 2 },
      joins: [{ submissionId: "submission-2", state: "joined", hostSubmissionId: "submission-1" }],
      approvalDecisions: [
        {
          submissionId: "submission-1",
          toolCallId: "call-1",
          decision: "approved",
          resolver: "operator",
          reason: "reviewed",
          decidedAt: "2026-08-12T00:00:20.000Z",
        },
      ],
      unknownResolutions: [
        {
          submissionId: "submission-1",
          toolCallId: "call-2",
          author: "operator",
          reason: "supplier store checked",
          resolution: { _tag: "SafeToRetry" },
          resolvedAt: "2026-08-12T00:00:25.000Z",
        },
      ],
      suspension: {
        reason: { _tag: "ApprovalPending", toolCallIds: ["call-1"] },
        suspendedAt: "2026-08-12T00:00:15.000Z",
      },
    };
    const recovery = Schema.decodeUnknownSync(RecoverySnapshot)(encodedRecovery);
    expect(Schema.encodeSync(RecoverySnapshot)(recovery)).toEqual(encodedRecovery);
    const minimalRecovery = Schema.decodeUnknownSync(RecoverySnapshot)({
      submission: encodedSubmissionSnapshot,
      joins: [],
      approvalDecisions: [],
      unknownResolutions: [],
    });
    expect(minimalRecovery.ownership).toBeUndefined();
    expect(minimalRecovery.reservation).toBeUndefined();
    expect(minimalRecovery.abortIntent).toBeUndefined();
    expect(minimalRecovery.hostSubmissionId).toBeUndefined();
    expect(minimalRecovery.suspension).toBeUndefined();
    // The P5 snapshot fields are required: a P4-shaped snapshot value no longer decodes.
    expect(
      Schema.decodeUnknownExit(RecoverySnapshot)({ submission: encodedSubmissionSnapshot })._tag,
    ).toBe("Failure");
  });

  it("round-trips settlement reservation values carrying the exact canonical record", () => {
    const settledRecord = decodeRecord("settlement:submission-1", {
      _tag: "SubmissionSettled",
      submissionId: "submission-1",
      settlementId: "settlement:submission-1",
      receiptId: "receipt-1",
      outcome: "completed",
      runId: "run-1",
      result: { itinerary: ["Kyoto"] },
    });
    const encodedReservation = {
      submissionId: "submission-1",
      ownershipToken: "token-1",
      settlementId: "settlement:submission-1",
      outcome: "completed",
      record: Schema.encodeSync(RecordEnvelope)(settledRecord),
      recordDigest: SHA_256_B,
    };
    const reservation = Schema.decodeUnknownSync(SettlementReservation)(encodedReservation);
    expect(Schema.encodeSync(SettlementReservation)(reservation)).toEqual(encodedReservation);
    expect(reservation.record).toEqual(settledRecord);

    const reserved = Schema.decodeUnknownSync(ReservedSettlement)({
      submissionId: "submission-1",
      settlementId: "settlement:submission-1",
      outcome: "completed",
      record: Schema.encodeSync(RecordEnvelope)(settledRecord),
      recordDigest: SHA_256_B,
      replayed: true,
    });
    expect(reserved.replayed).toBe(true);

    const settlement = Schema.decodeUnknownSync(Settlement)({
      submissionId: "submission-1",
      settlementId: "settlement:submission-1",
      receiptId: "receipt-1",
      outcome: "completed",
      settledAt: "2026-08-12T00:01:00.000Z",
    });
    expect(settlement.outcome).toBe("completed");
    expect(
      Schema.decodeUnknownExit(SettlementReservation)({
        ...encodedReservation,
        outcome: "unknown",
      })._tag,
    ).toBe("Failure");
  });

  it("round-trips abort commands, intents, capabilities, and typed errors", () => {
    const command = Schema.decodeUnknownSync(AbortCommand)({
      submissionId: "submission-1",
      author: "operator",
      reason: "user cancelled the trip",
    });
    expect(command.author).toBe("operator");
    expect(
      Schema.decodeUnknownExit(AbortCommand)({
        submissionId: "submission-1",
        author: "",
        reason: "user cancelled the trip",
      })._tag,
    ).toBe("Failure");

    const encodedIntent = {
      submissionId: "submission-1",
      author: "operator",
      reason: "user cancelled the trip",
      requestedAt: "2026-08-12T00:00:10.000Z",
      canonicalRecordId: "abort:submission-1",
    };
    const intent = Schema.decodeUnknownSync(AbortIntent)(encodedIntent);
    expect(Schema.encodeSync(AbortIntent)(intent)).toEqual(encodedIntent);

    expect(Schema.decodeUnknownSync(LedgerCapabilities)({ durability: "durable-node" })).toEqual({
      durability: "durable-node",
    });
    expect(
      Schema.decodeUnknownExit(LedgerCapabilities)({ durability: "durable-sqlite" })._tag,
    ).toBe("Failure");

    const conflict = AdmissionConflict.make({
      conversationId: Schema.decodeSync(ConversationId)("travel-conversation"),
      principal: Schema.decodeSync(AdmissionRequest.fields.principal)("tenant-a"),
      idempotencyKey: Schema.decodeSync(AdmissionRequest.fields.idempotencyKey)("client-key-1"),
      existingInputDigest: Schema.decodeSync(Digest)(SHA_256_A),
      attemptedInputDigest: Schema.decodeSync(Digest)(SHA_256_B),
    });
    const encodedConflict = Schema.encodeSync(AdmissionConflict)(conflict);
    expect(Schema.decodeUnknownSync(AdmissionConflict)(encodedConflict)).toEqual(conflict);

    const lost = OwnershipLost.make({
      submissionId,
      actualEpoch: Schema.decodeSync(ProducerEpoch)(3),
    });
    expect(lost.actualEpoch).toBe(3);
    const settlementConflict = SettlementConflict.make({
      submissionId,
      existingOutcome: "completed",
    });
    expect(settlementConflict.existingOutcome).toBe("completed");
  });

  it("derives the deterministic identities shared by adapters and the coordinator", () => {
    expect(submissionInputBatchId(submissionId)).toBe("submission-input:submission-1");
    expect(submissionInputRecordId(submissionId)).toBe("input:submission-1");
    expect(submissionSettlementId(submissionId)).toBe("settlement:submission-1");
    expect(submissionSettlementBatchId(submissionId)).toBe("submission-settlement:submission-1");
    expect(submissionSettlementRecordId(submissionId)).toBe("settlement:submission-1");
    expect(submissionAbortBatchId(submissionId)).toBe("submission-abort:submission-1");
    expect(submissionAbortRecordId(submissionId)).toBe("abort:submission-1");
  });

  const wakeConversationId = Schema.decodeSync(ConversationId)("travel-conversation");

  it.effect("accepts wake notifications on the noop scheduler without failing", () =>
    Effect.gen(function* () {
      const scheduler = yield* WakeScheduler;
      yield* scheduler.notify(wakeConversationId);
      yield* scheduler.notify(wakeConversationId);
    }).pipe(Effect.provide(WakeScheduler.layerNoop)),
  );
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
  const encodedToolStepSettled = {
    _tag: "ToolStepSettled",
    runId: "run-1",
    toolCallId: "call-1",
    stepName: "reserve-flight",
    output: { bookingRef: "booking-42" },
    outputDigest: SHA_256_B,
  } as const;
  const encodedApprovalRequested = {
    _tag: "ToolApprovalRequested",
    runId: "run-1",
    turnId: "turn-1",
    turn: 2,
    toolCallId: "call-1",
    toolName: "book_flight",
    parametersDigest: SHA_256_A,
  } as const;
  const encodedApprovalDecided = {
    _tag: "ToolApprovalDecided",
    runId: "run-1",
    turn: 2,
    toolCallId: "call-1",
    decision: "approved",
    resolver: "operator",
    reason: "reviewed and approved",
  } as const;
  const encodedInterrupted = {
    _tag: "ModelResponseInterrupted",
    runId: "run-1",
    supersededEpoch: 3,
    attemptId: "attempt-1",
    reason: "superseded a prior owner without a complete canonical turn",
  } as const;

  const payloads = [
    encodedToolCallPrepared,
    encodedToolCallUnknown,
    encodedToolCallResolved,
    encodedToolStepSettled,
    encodedApprovalRequested,
    encodedApprovalDecided,
    encodedInterrupted,
  ] as const;

  it("round-trips the seven new payload tags through the version-1 record envelope", () => {
    for (const [index, payload] of payloads.entries()) {
      const record = decodeRecord(`record-p5-${index}`, payload);
      expect(record.schemaVersion).toBe(1);
      const encoded = Schema.encodeSync(RecordEnvelope)(record);
      expect(encoded.payload).toEqual(payload);
      expect(Schema.decodeUnknownSync(RecordEnvelope)(encoded)).toEqual(record);
    }
  });

  it("rejects malformed durable-tool payloads", () => {
    const envelope = (payload: unknown): unknown => ({
      recordId: "record-p5-invalid",
      family: "conversation",
      schemaVersion: 1,
      createdAt: "2026-08-12T12:00:00.000Z",
      deploymentId: "test-deployment",
      payload,
    });
    const failures: ReadonlyArray<unknown> = [
      { ...encodedToolCallPrepared, turn: 0 },
      { ...encodedToolCallPrepared, parametersDigest: "not-a-digest" },
      { ...encodedToolCallPrepared, toolName: "" },
      { ...encodedToolCallUnknown, reason: "x".repeat(64 * 1024 + 1) },
      { ...encodedToolCallResolved, resolution: "made-up-result" },
      { ...encodedToolCallResolved, author: "" },
      { ...encodedToolStepSettled, stepName: "" },
      { ...encodedToolStepSettled, outputDigest: "not-a-digest" },
      { ...encodedApprovalDecided, decision: "maybe" },
      { ...encodedInterrupted, supersededEpoch: -1 },
    ];
    for (const payload of failures) {
      expect(Schema.decodeUnknownExit(RecordEnvelope)(envelope(payload))._tag).toBe("Failure");
    }
  });

  it("folds prepared/settled/resolved, unknown, and approval records into the projection", () => {
    const created = decodeEnvelope(
      1,
      decodeRecord("record-p5-created", {
        _tag: "ConversationCreated",
        agentId: "travel-planner",
        definitions: Schema.encodeSync(DefinitionDigests)(definitionDigests),
      }),
    );
    const prepared = decodeEnvelope(2, decodeRecord("record-p5-prepared", encodedToolCallPrepared));
    const preparedTwo = decodeEnvelope(
      3,
      decodeRecord("record-p5-prepared-2", { ...encodedToolCallPrepared, toolCallId: "call-2" }),
    );
    const requested = decodeEnvelope(
      4,
      decodeRecord("record-p5-requested", encodedApprovalRequested),
    );
    const decided = decodeEnvelope(5, decodeRecord("record-p5-decided", encodedApprovalDecided));
    const unknown = decodeEnvelope(6, decodeRecord("record-p5-unknown", encodedToolCallUnknown));
    const settled = decodeEnvelope(
      7,
      decodeRecord("record-p5-settled", {
        _tag: "ToolCallSettled",
        runId: "run-1",
        toolCallId: "call-2",
        toolName: "book_flight",
        result: { bookingRef: "booking-43" },
        isFailure: false,
      }),
    );
    const resolved = decodeEnvelope(8, decodeRecord("record-p5-resolved", encodedToolCallResolved));

    const afterPrepared = replayConversation(created.conversationId, [
      created,
      prepared,
      preparedTwo,
      requested,
      decided,
      unknown,
    ]);
    expect(afterPrepared.openToolCalls.map((call) => call.toolCallId)).toEqual([
      "call-1",
      "call-2",
    ]);
    expect(afterPrepared.unknownToolCalls).toEqual([unknown.record.payload]);
    expect(afterPrepared.approvals).toEqual([requested.record.payload, decided.record.payload]);

    // `ToolCallSettled` and `ToolCallResolved` close their calls; `ToolCallUnknown` does not.
    const full = replayConversation(created.conversationId, [
      created,
      prepared,
      preparedTwo,
      requested,
      decided,
      unknown,
      settled,
      resolved,
    ]);
    expect(full.openToolCalls).toEqual([]);

    const resumed = replayConversationFromCheckpoint(afterPrepared, [settled, resolved]);
    expect(resumed).toEqual(full);
    expect(
      Schema.decodeSync(ConversationProjection)(Schema.encodeSync(ConversationProjection)(full)),
    ).toEqual(full);
  });

  it("rejects a Phase 4 checkpoint projection state so callers rebuild from canonical records", () => {
    const phase4State = {
      conversationId: "travel-conversation",
      throughSequence: 2,
      tailDigest: SHA_256_A,
      inputs: [{ destination: "Kyoto" }],
      modelOutputs: [],
      completedRuns: [],
      failedRuns: [],
      settlements: [],
      abortRequests: [],
    };
    expect(Schema.decodeUnknownExit(ConversationProjection)(phase4State)._tag).toBe("Failure");
  });
});

describe("phase 5 ledger port schemas", () => {
  const submissionId = Schema.decodeSync(SubmissionId)("submission-1");
  const hostSubmissionId = Schema.decodeSync(SubmissionId)("submission-host");
  const toolCallId = Schema.decodeSync(ToolCallId)("call-1");

  it("round-trips joining requests and claims", () => {
    const encodedClaimJoining = {
      conversationId: "travel-conversation",
      hostSubmissionId: "submission-host",
      ownershipToken: "token-1",
      maxCount: 1,
    } as const;
    const request = Schema.decodeUnknownSync(ClaimJoiningRequest)(encodedClaimJoining);
    expect(Schema.encodeSync(ClaimJoiningRequest)(request)).toEqual(encodedClaimJoining);
    expect(
      Schema.decodeUnknownExit(ClaimJoiningRequest)({ ...encodedClaimJoining, maxCount: 0 })._tag,
    ).toBe("Failure");

    const claim = Schema.decodeUnknownSync(JoiningClaim)({
      submissionId: "submission-2",
      queueSequence: 2,
      inputPayload: { note: "also add a museum day" },
    });
    expect(claim.queueSequence).toBe(2);

    const marked = Schema.decodeUnknownSync(MarkJoinedRequest)({
      submissionId: "submission-2",
      ownershipToken: "token-1",
      recordId: "input:submission-2",
      sequence: 9,
    });
    expect(marked.recordId).toBe("input:submission-2");
    expect(
      Schema.decodeUnknownSync(RevertJoiningRequest)({ submissionId: "submission-2" }).submissionId,
    ).toBe("submission-2");
  });

  it("round-trips suspension requests and approval decisions with typed conflicts", () => {
    const encodedSuspend = {
      submissionId: "submission-1",
      ownershipToken: "token-1",
      reason: { _tag: "ApprovalPending", toolCallIds: ["call-1", "call-2"] },
    } as const;
    const suspend = Schema.decodeUnknownSync(SuspendRequest)(encodedSuspend);
    expect(Schema.encodeSync(SuspendRequest)(suspend)).toEqual(encodedSuspend);
    expect(
      Schema.decodeUnknownExit(SuspendRequest)({
        ...encodedSuspend,
        reason: { _tag: "ApprovalPending", toolCallIds: [] },
      })._tag,
    ).toBe("Failure");

    const encodedCommand = {
      submissionId: "submission-1",
      toolCallId: "call-1",
      decision: "approved",
      resolver: "operator",
      reason: "reviewed",
    } as const;
    const command = Schema.decodeUnknownSync(ApprovalDecisionCommand)(encodedCommand);
    expect(Schema.encodeSync(ApprovalDecisionCommand)(command)).toEqual(encodedCommand);
    expect(
      Schema.decodeUnknownExit(ApprovalDecisionCommand)({
        ...encodedCommand,
        decision: "vetoed",
      })._tag,
    ).toBe("Failure");

    const encodedIntent = {
      ...encodedCommand,
      decidedAt: "2026-08-12T00:00:10.000Z",
      canonicalRecordId: "approval-decision:run-1:2:call-1",
    };
    const intent = Schema.decodeUnknownSync(ApprovalDecisionIntent)(encodedIntent);
    expect(Schema.encodeSync(ApprovalDecisionIntent)(intent)).toEqual(encodedIntent);

    const conflict = ApprovalConflict.make({
      submissionId,
      toolCallId,
      existingDecision: "denied",
    });
    expect(conflict.existingDecision).toBe("denied");
  });

  it("round-trips unknown marking, resolutions, and the joined-abort conflict", () => {
    const marked = Schema.decodeUnknownSync(MarkUnknownRequest)({
      submissionId: "submission-1",
      toolCallIds: ["call-1"],
      reason: "prepared without a canonical outcome",
    });
    expect(marked.toolCallIds).toEqual(["call-1"]);
    expect(
      Schema.decodeUnknownExit(MarkUnknownRequest)({
        submissionId: "submission-1",
        toolCallIds: [],
        reason: "empty",
      })._tag,
    ).toBe("Failure");

    const resolutions: ReadonlyArray<unknown> = [
      { _tag: "CompletedWithResult", result: { bookingRef: "booking-42" }, isFailure: false },
      { _tag: "NeverHappened" },
      { _tag: "SafeToRetry" },
      { _tag: "AbortSubmission" },
    ];
    for (const encoded of resolutions) {
      const resolution = Schema.decodeUnknownSync(UnknownResolution)(encoded);
      expect(Schema.encodeSync(UnknownResolution)(resolution)).toEqual(encoded);
    }
    expect(Schema.decodeUnknownExit(UnknownResolution)({ _tag: "JustGuess" })._tag).toBe("Failure");

    const encodedCommand = {
      submissionId: "submission-1",
      toolCallId: "call-1",
      author: "operator",
      reason: "supplier store shows the booking",
      resolution: {
        _tag: "CompletedWithResult",
        result: { bookingRef: "booking-42" },
        isFailure: false,
      },
    } as const;
    const command = Schema.decodeUnknownSync(UnknownResolutionCommand)(encodedCommand);
    expect(Schema.encodeSync(UnknownResolutionCommand)(command)).toEqual(encodedCommand);

    const intent = Schema.decodeUnknownSync(UnknownResolutionIntent)({
      ...encodedCommand,
      resolvedAt: "2026-08-12T00:00:30.000Z",
    });
    expect(intent.resolution._tag).toBe("CompletedWithResult");

    const conflict = UnknownResolutionConflict.make({ submissionId, toolCallId });
    expect(conflict.toolCallId).toBe("call-1");
    const joined = JoinedToHost.make({ submissionId, hostSubmissionId });
    expect(joined.hostSubmissionId).toBe("submission-host");
  });

  it("derives the deterministic Phase 5 identities shared by adapters and the coordinator", () => {
    const runId = Schema.decodeSync(RunId)("run:submission-1");
    expect(turnResponseBatchId(runId, 2)).toBe("turn-response:run:submission-1:2");
    expect(turnResultsBatchId(runId, 2)).toBe("turn-results:run:submission-1:2");
    expect(turnPreparedBatchId(runId, 2)).toBe("turn-prepared:run:submission-1:2");
    expect(toolCallResultBatchId(runId, 2, toolCallId)).toBe(
      "turn-results:run:submission-1:2:call-1",
    );
    expect(toolCallPreparedRecordId(runId, 2, toolCallId)).toBe(
      "tool-prepared:run:submission-1:2:call-1",
    );
    expect(toolCallSettledRecordId(runId, 2, toolCallId)).toBe(
      "tool-settled:run:submission-1:2:call-1",
    );
    expect(markUnknownBatchId(submissionId, 2)).toBe("mark-unknown:submission-1:2");
    expect(toolCallUnknownRecordId(runId, 2, toolCallId)).toBe(
      "tool-unknown:run:submission-1:2:call-1",
    );
    expect(toolCallResolutionBatchId(submissionId, toolCallId)).toBe("resolve:submission-1:call-1");
    expect(toolCallResolvedRecordId(runId, 2, toolCallId)).toBe(
      "tool-resolved:run:submission-1:2:call-1",
    );
    expect(toolStepSettledRecordId(runId, toolCallId, "reserve-flight")).toBe(
      "step:run:submission-1:call-1:reserve-flight",
    );
    expect(toolStepSettledBatchId(runId, toolCallId, "reserve-flight")).toBe(
      toolStepSettledRecordId(runId, toolCallId, "reserve-flight"),
    );
    expect(turnApprovalsBatchId(runId, 2)).toBe("turn-approvals:run:submission-1:2");
    expect(toolApprovalRequestRecordId(runId, 2, toolCallId)).toBe(
      "approval-request:run:submission-1:2:call-1",
    );
    expect(approvalDecisionBatchId(submissionId, toolCallId)).toBe(
      "approval-decision:submission-1:call-1",
    );
    expect(toolApprovalDecisionRecordId(runId, 2, toolCallId)).toBe(
      "approval-decision:run:submission-1:2:call-1",
    );
    expect(modelResponseInterruptedRecordId(runId, 3)).toBe("interrupted:run:submission-1:3");
    expect(modelResponseInterruptedBatchId(runId, 3)).toBe(
      modelResponseInterruptedRecordId(runId, 3),
    );
  });
});

describe("ToolReconciler", () => {
  const preparedEvidence = Schema.decodeUnknownSync(PreparedToolCallEvidence)({
    conversationId: "travel-conversation",
    submissionId: "submission-1",
    runId: "run:submission-1",
    turn: 2,
    toolCallId: "call-1",
    toolName: "book_flight",
    parameters: { destination: "Kyoto" },
    parametersDigest: SHA_256_A,
  });

  it("round-trips reconciliation decisions", () => {
    const decisions: ReadonlyArray<unknown> = [
      { _tag: "NeverStarted" },
      { _tag: "CompletedWithResult", result: { bookingRef: "booking-42" }, isFailure: false },
      { _tag: "SafeToRetry" },
      { _tag: "Uncertain", reason: "supplier unreachable" },
    ];
    for (const encoded of decisions) {
      const decision = Schema.decodeUnknownSync(ReconciliationDecision)(encoded);
      expect(Schema.encodeSync(ReconciliationDecision)(decision)).toEqual(encoded);
    }
    expect(Schema.decodeUnknownExit(ReconciliationDecision)({ _tag: "Probably" })._tag).toBe(
      "Failure",
    );
  });

  it.effect("defaults to Uncertain for every open call (fail-closed)", () =>
    Effect.gen(function* () {
      const reconciler = yield* ToolReconciler;
      const decision = yield* reconciler.reconcile(preparedEvidence);
      expect(decision._tag).toBe("Uncertain");
    }).pipe(Effect.provide(ToolReconciler.uncertain)),
  );
});
