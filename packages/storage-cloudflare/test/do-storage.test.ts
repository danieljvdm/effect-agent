import {
  CanonicalBatch,
  CanonicalRecord,
  ConversationMaterialization,
  ConversationStore,
  ConversationStoreError,
  conversationStoreConformanceCases,
  EMPTY_TAIL_DIGEST,
  FencedAppendRequest,
  UserInputRecorded,
} from "@effect-agent/session";
import { BrowserCrypto } from "@effect/platform-browser";
import { SqliteClient } from "@effect/sql-sqlite-do";
import { Cause, Crypto, Effect, Exit, Layer, Schema } from "effect";
import * as SqlClientService from "effect/unstable/sql/SqlClient";
import { describe, expect, it } from "vite-plus/test";

import {
  conversationStoreLayer,
  DoStorageConfig,
  DoStorageFailpoint,
  DoValueBoundExceeded,
  layer,
  storageConfigLayer,
  type DoStorageInitializationError,
} from "../src/index.ts";
import {
  conversation,
  epoch,
  id,
  at,
  sequence,
  TEST_DEPLOYMENT,
  TEST_PRODUCER,
  withConversationStorage,
} from "./harness.ts";

type Equal<Left, Right> =
  (<Value>() => Value extends Left ? 1 : 2) extends <Value>() => Value extends Right ? 1 : 2
    ? (<Value>() => Value extends Right ? 1 : 2) extends <Value>() => Value extends Left ? 1 : 2
      ? true
      : false
    : false;
type Assert<Value extends true> = Value;
type ConversationStoreLayerRequirementsProof = Assert<
  Equal<
    Layer.Services<typeof conversationStoreLayer>,
    DoStorageConfig | DoStorageFailpoint | SqlClientService.SqlClient | Crypto.Crypto
  >
>;
type ConversationStoreLayerErrorProof = Assert<
  Equal<Layer.Error<typeof conversationStoreLayer>, DoStorageInitializationError>
>;

const inputRecord = (recordId: string, input: string): CanonicalRecord =>
  CanonicalRecord.make({
    recordId: id(
      Schema.NonEmptyString.pipe(Schema.brand("@effect-agent/session/RecordId")),
      recordId,
    ),
    family: "conversation",
    schemaVersion: 1,
    createdAt: at(1),
    deploymentId: TEST_DEPLOYMENT,
    payload: UserInputRecorded.make({
      submissionId: id(UserInputRecorded.fields.submissionId, "submission-do-store"),
      kind: "user",
      input,
    }),
  });

const batch = (
  batchId: string,
  records: readonly [CanonicalRecord, ...Array<CanonicalRecord>],
): CanonicalBatch =>
  CanonicalBatch.make({
    batchId: id(Schema.NonEmptyString.pipe(Schema.brand("@effect-agent/session/BatchId")), batchId),
    producerId: TEST_PRODUCER,
    records,
  });

describe("DoConversationStore", () => {
  // The SAME adapter-neutral contract suite the Node/SQLite and in-memory adapters run,
  // executed in-workerd against a real SQLite-backed Durable Object's storage. One Durable
  // Object per case: the 0.21.x pool shares storage across tests within a run.
  describe("shared ConversationStore conformance", () => {
    for (const conformanceCase of conversationStoreConformanceCases) {
      it(conformanceCase.name, () =>
        withConversationStorage(`wp1-store:${conformanceCase.name}`, (storage) =>
          conformanceCase.run.pipe(Effect.provide(layer({ storage, observationPollInterval: 1 }))),
        ),
      );
    }
  });

  it("keeps configuration, failpoint, SQL, and Crypto authority in the named Layer input", () => {
    const requirementsProof: ConversationStoreLayerRequirementsProof = true;
    const errorProof: ConversationStoreLayerErrorProof = true;

    expect(requirementsProof).toBe(true);
    expect(errorProof).toBe(true);
  });

  it("refuses an over-bound canonical append typed before any write", () =>
    withConversationStorage("wp1-store-value-bound", (storage) =>
      Effect.gen(function* () {
        const store = yield* ConversationStore;
        const sql = yield* SqlClientService.SqlClient;
        const conversationId = "conversation-value-bound";

        yield* store.materialize(
          ConversationMaterialization.make({
            conversationId: conversation(conversationId),
            producerEpoch: epoch(1),
          }),
        );

        // 2,048 bytes of record content against a 1,024-byte configured bound (the platform
        // analogue is ~1.9 MB under the 2 MB per-value limit; a small bound keeps the test
        // payload honest without allocating megabytes inside workerd).
        const exit = yield* store
          .append(
            FencedAppendRequest.make({
              conversationId: conversation(conversationId),
              batch: batch("value-bound-batch", [
                inputRecord("value-bound-record", "x".repeat(2_048)),
              ]),
              expectedTailSequence: sequence(0),
              expectedTailDigest: EMPTY_TAIL_DIGEST,
              producerEpoch: epoch(1),
            }),
          )
          .pipe(Effect.exit);
        expect(Exit.isFailure(exit)).toBe(true);
        if (Exit.isFailure(exit)) {
          const error = Cause.squash(exit.cause);
          expect(error).toBeInstanceOf(ConversationStoreError);
          if (error instanceof ConversationStoreError) {
            expect(error.cause).toBeInstanceOf(DoValueBoundExceeded);
            if (error.cause instanceof DoValueBoundExceeded) {
              expect(error.cause.maxBytes).toBe(1_024);
              expect(error.cause.actualBytes).toBeGreaterThan(1_024);
              expect(error.cause.message).toContain("R2");
            }
          }
        }

        // Nothing was written: the refusal happened BEFORE any durable mutation.
        const batchRows = yield* sql<Record<string, unknown>>`
          SELECT batch_id FROM effect_agent_canonical_batches
        `;
        expect(batchRows).toEqual([]);
        const recordRows = yield* sql<Record<string, unknown>>`
          SELECT record_id FROM effect_agent_canonical_records
        `;
        expect(recordRows).toEqual([]);
      }).pipe(
        Effect.provide(
          conversationStoreLayer.pipe(
            Layer.provideMerge(
              Layer.mergeAll(
                storageConfigLayer({ storage, maxStoredValueBytes: 1_024 }),
                DoStorageFailpoint.layer,
                SqliteClient.layer({ storage }),
                BrowserCrypto.layer,
              ),
            ),
          ),
        ),
      ),
    ));
});
