import { NodeCrypto } from "@effect/platform-node";
import { describe, expect, it, layer } from "@effect/vitest";
import { Duration, Effect, Schema } from "effect";
import { ConversationId, SubmissionId } from "@effect-agent/core";

import {
  AbortCommand,
  AbortIntent,
  AdmissionConflict,
  AdmissionRequest,
  AdmissionResult,
  CanonicalBatch,
  CanonicalRecordEnvelope,
  Claim,
  ConversationProjection,
  DEFAULT_OWNERSHIP_LEASE_DURATION,
  DefinitionDigestInput,
  DefinitionDigests,
  Digest,
  digestCanonicalBatch,
  digestDefinitions,
  digestJson,
  EMPTY_TAIL_DIGEST,
  LedgerCapabilities,
  MAX_PERSISTED_JSON_BYTES,
  MAX_PERSISTED_JSON_DEPTH,
  OwnershipLost,
  PersistedJson,
  ProducerEpoch,
  RecordEnvelope,
  RecoverySnapshot,
  replayConversation,
  replayConversationFromCheckpoint,
  ReservedSettlement,
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
  WakeScheduler,
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
    };
    const recovery = Schema.decodeUnknownSync(RecoverySnapshot)(encodedRecovery);
    expect(Schema.encodeSync(RecoverySnapshot)(recovery)).toEqual(encodedRecovery);
    const minimalRecovery = Schema.decodeUnknownSync(RecoverySnapshot)({
      submission: encodedSubmissionSnapshot,
    });
    expect(minimalRecovery.ownership).toBeUndefined();
    expect(minimalRecovery.reservation).toBeUndefined();
    expect(minimalRecovery.abortIntent).toBeUndefined();
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
