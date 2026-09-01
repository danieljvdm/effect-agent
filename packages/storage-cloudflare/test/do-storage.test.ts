import {
  CanonicalBatch,
  CanonicalRecord,
  ThreadMaterialization,
  ThreadStore,
  ThreadStoreError,
  EMPTY_TAIL_DIGEST,
  FencedAppendRequest,
  UserInputRecorded,
} from "@effect-agent/thread";
import {
  threadStoreConformanceCases,
  threadCheckpointConformanceCases,
} from "@effect-agent/thread/testing";
import { BrowserCrypto } from "@effect/platform-browser";
import { SqliteClient } from "@effect/sql-sqlite-do";
import type { Crypto } from "effect";
import { Cause, Effect, Exit, Layer, Option, Schema, Tracer } from "effect";
import * as SqlClientService from "effect/unstable/sql/SqlClient";
import { describe, expect, it } from "vite-plus/test";

import {
  CurrentDoStorageVersion,
  type DoStorageConfig,
  threadStoreLayer,
  DoStorageCompatibilityError,
  DoStorageError,
  DoStorageFailpoint,
  DoValueBoundExceeded,
  layer,
  storageConfigLayer,
  type DoStorageInitializationError,
} from "../src/index.ts";
import {
  thread,
  epoch,
  id,
  at,
  sequence,
  TEST_DEPLOYMENT,
  TEST_PRODUCER,
  withThreadStorage,
} from "./harness.ts";

type Equal<Left, Right> =
  (<Value>() => Value extends Left ? 1 : 2) extends <Value>() => Value extends Right ? 1 : 2
    ? (<Value>() => Value extends Right ? 1 : 2) extends <Value>() => Value extends Left ? 1 : 2
      ? true
      : false
    : false;
type Assert<Value extends true> = Value;
type ThreadStoreLayerRequirementsProof = Assert<
  Equal<
    Layer.Services<typeof threadStoreLayer>,
    DoStorageConfig | DoStorageFailpoint | SqlClientService.SqlClient | Crypto.Crypto
  >
>;
type ThreadStoreLayerErrorProof = Assert<
  Equal<Layer.Error<typeof threadStoreLayer>, DoStorageInitializationError>
>;
const isThreadStoreError = Schema.is(ThreadStoreError);
const isDoStorageCompatibilityError = Schema.is(DoStorageCompatibilityError);
const isDoStorageError = Schema.is(DoStorageError);
const isDoValueBoundExceeded = Schema.is(DoValueBoundExceeded);

const inputRecord = (recordId: string, input: string): CanonicalRecord =>
  CanonicalRecord.make({
    recordId: id(
      Schema.NonEmptyString.pipe(Schema.brand("@effect-agent/thread/RecordId")),
      recordId,
    ),
    family: "thread",
    schemaVersion: 1,
    createdAt: at(1),
    deploymentId: TEST_DEPLOYMENT,
    payload: UserInputRecorded.make({
      submissionId: id(SubmissionId, "submission-do-store"),
      kind: "user",
      input,
    }),
  });

const batch = (
  batchId: string,
  records: readonly [CanonicalRecord, ...Array<CanonicalRecord>],
): CanonicalBatch =>
  CanonicalBatch.make({
    batchId: id(Schema.NonEmptyString.pipe(Schema.brand("@effect-agent/thread/BatchId")), batchId),
    producerId: TEST_PRODUCER,
    records,
  });

describe("DoThreadStore", () => {
  // The SAME adapter-neutral contract suite the Node/SQLite and in-memory adapters run,
  // executed in-workerd against a real SQLite-backed Durable Object's storage. One Durable
  // Object per case: the 0.21.x pool shares storage across tests within a run.
  describe("shared ThreadStore conformance", () => {
    for (const conformanceCase of [
      ...threadStoreConformanceCases,
      ...threadCheckpointConformanceCases,
    ]) {
      it(conformanceCase.name, () => {
        const spanNames: Array<string> = [];
        const tracer = Tracer.make({
          span(options) {
            spanNames.push(options.name);
            return new Tracer.NativeSpan(options);
          },
        });
        return withThreadStorage(`wp1-store:${conformanceCase.name}`, (storage) =>
          conformanceCase.run.pipe(
            Effect.provide(layer({ storage, observationPollInterval: 1 })),
            Effect.provideService(Tracer.Tracer, tracer),
            Effect.withTracerEnabled(true),
            Effect.tap(() =>
              Effect.sync(() => {
                expect(spanNames).toContain("sql.execute");
                for (const helper of [
                  "DoJournal.decodeRows",
                  "DoJournal.decodeSingleRow",
                  "DoThreadStore.makeOffset",
                  "DoThreadStore.parseOffset",
                  "DoThreadStore.encodeCanonicalRecord",
                  "DoThreadStore.encodeCanonicalBatch",
                  "DoThreadStore.encodeCheckpoint",
                  "DoThreadStore.decodeEnvelope",
                  "DoThreadStore.decodeCheckpoint",
                  "DoThreadStore.hitFailpoint",
                ]) {
                  expect(spanNames).not.toContain(helper);
                }
              }),
            ),
          ),
        );
      });
    }
  });

  it("keeps configuration, failpoint, SQL, and Crypto authority in the named Layer input", () => {
    const requirementsProof: ThreadStoreLayerRequirementsProof = true;
    const errorProof: ThreadStoreLayerErrorProof = true;

    expect(requirementsProof).toBe(true);
    expect(errorProof).toBe(true);
  });

  it("validates convenience-layer configuration before initializing storage", () =>
    withThreadStorage("wp1-store-invalid-config", (storage) =>
      Effect.gen(function* () {
        const opened = yield* ThreadStore.pipe(
          Effect.provide(layer({ storage, observationPollInterval: -1 })),
          Effect.exit,
        );

        expect(Exit.isFailure(opened)).toBe(true);
        if (Exit.isFailure(opened)) {
          const failure = Cause.findErrorOption(opened.cause);
          expect(Option.isSome(failure)).toBe(true);
          if (Option.isSome(failure)) {
            expect(isDoStorageError(failure.value)).toBe(true);
            if (isDoStorageError(failure.value)) {
              expect(failure.value.operation).toBe("configure Durable Object storage");
            }
          }
        }
        const tables = storage.sql
          .exec<{ name: string }>(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'effect_agent_%'",
          )
          .toArray();
        expect(tables).toEqual([]);
      }),
    ));

  it("rejects an unsupported storage version without mutating its tables", () =>
    withThreadStorage("wp1-store-unsupported-version", (storage) =>
      Effect.gen(function* () {
        const previousVersion = CurrentDoStorageVersion - 1;
        storage.sql.exec(`
          CREATE TABLE effect_agent_meta (
            key TEXT PRIMARY KEY NOT NULL,
            value TEXT NOT NULL
          );
          INSERT INTO effect_agent_meta (key, value)
          VALUES ('storage_version', '${previousVersion}');
          CREATE TABLE effect_agent_threads (
            thread_id TEXT PRIMARY KEY NOT NULL
          );
          INSERT INTO effect_agent_threads (thread_id)
          VALUES ('retained-thread');
        `);

        const opened = yield* ThreadStore.pipe(
          Effect.provide(layer({ storage, observationPollInterval: 1 })),
          Effect.exit,
        );
        expect(Exit.isFailure(opened)).toBe(true);
        if (Exit.isFailure(opened)) {
          const failure = Cause.findErrorOption(opened.cause);
          expect(Option.isSome(failure)).toBe(true);
          if (Option.isSome(failure)) {
            expect(isDoStorageCompatibilityError(failure.value)).toBe(true);
            if (isDoStorageCompatibilityError(failure.value)) {
              expect(failure.value.actualVersion).toBe(previousVersion);
              expect(failure.value.supportedVersion).toBe(CurrentDoStorageVersion);
              expect(failure.value.message).toContain(
                "Replace the development namespace explicitly",
              );
            }
          }
        }

        const rows = storage.sql
          .exec<{ thread_id: string }>("SELECT thread_id FROM effect_agent_threads")
          .toArray();
        expect(rows).toEqual([{ thread_id: "retained-thread" }]);
      }),
    ));

  it("refuses an over-bound canonical append typed before any write", () =>
    withThreadStorage("wp1-store-value-bound", (storage) =>
      Effect.gen(function* () {
        const store = yield* ThreadStore;
        const sql = yield* SqlClientService.SqlClient;
        const threadId = "thread-value-bound";

        yield* store.materialize(
          ThreadMaterialization.make({
            threadId: thread(threadId),
            producerEpoch: epoch(1),
          }),
        );

        // 2,048 bytes of record content against a 1,024-byte configured bound (the platform
        // analogue is ~1.9 MB under the 2 MB per-value limit; a small bound keeps the test
        // payload honest without allocating megabytes inside workerd).
        const exit = yield* store
          .append(
            FencedAppendRequest.make({
              threadId: thread(threadId),
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
          expect(error).toBeInstanceOf(ThreadStoreError);
          if (isThreadStoreError(error)) {
            expect(error.cause).toBeInstanceOf(DoValueBoundExceeded);
            if (isDoValueBoundExceeded(error.cause)) {
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
          threadStoreLayer.pipe(
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
import { SubmissionId } from "@effect-agent/core";
