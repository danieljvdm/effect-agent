import { NodeCrypto } from "@effect/platform-node";
import { describe, expect, it, layer } from "@effect/vitest";
import { Effect, Schema } from "effect";

import {
  CanonicalBatch,
  CanonicalRecordEnvelope,
  ConversationProjection,
  DefinitionDigestInput,
  DefinitionDigests,
  Digest,
  digestCanonicalBatch,
  digestDefinitions,
  EMPTY_TAIL_DIGEST,
  MAX_PERSISTED_JSON_BYTES,
  MAX_PERSISTED_JSON_DEPTH,
  PersistedJson,
  RecordEnvelope,
  replayConversation,
  replayConversationFromCheckpoint,
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
