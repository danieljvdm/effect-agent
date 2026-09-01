import {
  MemoryConflict,
  MemoryKey,
  MemoryMutationFailpoint,
  MemoryMutationFailure,
  MemoryOperationConflict,
  MemoryReader,
  MemoryStorageError,
  MemoryWithdrawn,
  MemoryWrite,
  MemoryWriter,
} from "@effect-agent/core";
import { NodeFileSystem } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { describe, expect, it } from "@effect/vitest";
import type { PlatformError } from "effect";
import { Cause, Deferred, Effect, Exit, Fiber, FileSystem, Layer, Result, Schema } from "effect";
import { TestClock } from "effect/testing";
import * as SqlClientService from "effect/unstable/sql/SqlClient";

import {
  memoryReaderLayer,
  memoryStoreLayer,
  memoryStoreLayerWithFailpoints,
} from "../src/index.ts";

const key = MemoryKey.make({ namespace: "tenant-a", id: "source-1" });

const putFor = (
  memoryKey: MemoryKey,
  operationId: string,
  expectedRevision: string | null,
  text: string,
  scopes: ReadonlyArray<string> = ["team"],
) =>
  Schema.decodeSync(MemoryWrite)({
    _tag: "Put",
    key: memoryKey,
    operationId,
    expectedRevision,
    locator: `memory://${memoryKey.id}`,
    content: {
      text,
      attributions: [
        {
          originId: "origin-1",
          speaker: "dan",
          observers: ["agent"],
          locator: "chat://message-1",
          activityAt: 50,
          interpretation: "direct statement",
        },
      ],
      metadata: { kind: "preference" },
      recordedAt: 75,
      extractedAt: 80,
    },
    scopes,
  });

const put = (
  operationId: string,
  expectedRevision: string | null,
  text: string,
  scopes: ReadonlyArray<string> = ["team"],
) => putFor(key, operationId, expectedRevision, text, scopes);

const withdraw = (operationId: string, expectedRevision: string) =>
  Schema.decodeSync(MemoryWrite)({
    _tag: "Withdraw",
    key,
    operationId,
    expectedRevision,
    reason: "source withdrawn",
  });

const storedJsonCodeUnitLimit = 16 * 1024 * 1024;
const boundaryObservers = Array.from({ length: 128 }, (_, index) => {
  const prefix = `observer-${String(index).padStart(3, "0")}-`;
  return `${prefix}${"x".repeat(1_024 - prefix.length)}`;
});

const boundaryPut = (operationId: string, attributionCount: number) =>
  Schema.decodeSync(MemoryWrite)({
    _tag: "Put",
    key,
    operationId,
    expectedRevision: null,
    locator: "memory://source-1",
    content: {
      text: "encoded boundary",
      attributions: Array.from({ length: attributionCount }, (_, index) => ({
        originId: `origin-${index}`,
        speaker: "dan",
        observers: boundaryObservers,
        locator: "chat://message-1",
        activityAt: 50,
        interpretation: "direct statement",
      })),
      metadata: { kind: "boundary" },
      recordedAt: 75,
      extractedAt: 80,
    },
    scopes: ["team"],
  });

const storeLayer = (filename: string) =>
  memoryStoreLayer.pipe(Layer.provide(SqliteClient.layer({ filename, busyTimeout: 5_000 })));

const readerLayer = (filename: string) =>
  memoryReaderLayer.pipe(
    Layer.provide(SqliteClient.layer({ filename, readonly: true, busyTimeout: 5_000 })),
  );

const failpointLayer = (filename: string, handler: MemoryMutationFailpoint["Service"]["hit"]) =>
  memoryStoreLayerWithFailpoints.pipe(
    Layer.provide(
      Layer.mergeAll(
        SqliteClient.layer({ filename, busyTimeout: 5_000 }),
        Layer.succeed(MemoryMutationFailpoint)({ hit: handler }),
      ),
    ),
  );

const withTemporaryDatabase = <A, E>(
  use: (filename: string) => Effect.Effect<A, E>,
): Effect.Effect<A, E | PlatformError.PlatformError> =>
  Effect.scoped(
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const directory = yield* fs.makeTempDirectoryScoped({
        prefix: "effect-agent-memory-sqlite-",
      });
      return yield* use(`${directory}/memory.sqlite`);
    }),
  ).pipe(Effect.provide(NodeFileSystem.layer));

const readCurrent = (filename: string) =>
  Effect.gen(function* () {
    const reader = yield* MemoryReader;
    return yield* reader.get(key);
  }).pipe(Effect.provide(readerLayer(filename)));

const runRaw = <A, E>(filename: string, effect: Effect.Effect<A, E, SqlClientService.SqlClient>) =>
  effect.pipe(Effect.provide(SqliteClient.layer({ filename, busyTimeout: 5_000 })));

describe("SQLite memory store", () => {
  it.effect("rejects uninitialized and incompatible reader schemas without creating tables", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        yield* runRaw(filename, Effect.void);
        expect(yield* readCurrent(filename).pipe(Effect.flip)).toEqual(
          MemoryStorageError.make({ operation: "open memory reader", reason: "unavailable" }),
        );
        const tables = yield* runRaw(
          filename,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;
            return yield* sql`SELECT name FROM sqlite_master WHERE type = 'table'`;
          }),
        );
        expect(tables).toEqual([]);
        yield* Effect.void.pipe(Effect.provide(storeLayer(filename)));
        expect(yield* readCurrent(filename)).toBeNull();
        yield* runRaw(
          filename,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;
            yield* sql`UPDATE effect_agent_memory_metadata SET version = 2`;
          }),
        );
        expect(yield* readCurrent(filename).pipe(Effect.flip)).toEqual(
          MemoryStorageError.make({ operation: "open memory reader", reason: "incompatible" }),
        );
      }),
    ),
  );

  it.effect("round trips and replays a change near the encoded JSON boundary", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        const command = boundaryPut("boundary-roundtrip", 126);
        const commandLength = JSON.stringify({ version: 1, value: command }).length;
        expect(commandLength).toBeGreaterThan(storedJsonCodeUnitLimit - 512 * 1024);
        expect(commandLength).toBeLessThanOrEqual(storedJsonCodeUnitLimit);

        const stored = yield* Effect.gen(function* () {
          const writer = yield* MemoryWriter;
          return yield* writer.change(command);
        }).pipe(Effect.provide(storeLayer(filename)));

        yield* Effect.gen(function* () {
          const reader = yield* MemoryReader;
          const writer = yield* MemoryWriter;
          expect(yield* reader.get(key)).toEqual(stored);
          expect(yield* writer.change(command)).toEqual(stored);
        }).pipe(Effect.provide(storeLayer(filename)));
      }),
    ),
  );

  it.effect("rejects an oversized encoded change before document or receipt mutation", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        const command = boundaryPut("boundary-rejected", 128);
        expect(JSON.stringify({ version: 1, value: command }).length).toBeGreaterThan(
          storedJsonCodeUnitLimit,
        );

        yield* Effect.gen(function* () {
          const reader = yield* MemoryReader;
          const writer = yield* MemoryWriter;
          expect(yield* writer.change(command).pipe(Effect.flip)).toEqual(
            MemoryStorageError.make({
              operation: "change memory document",
              reason: "invalid-input",
            }),
          );
          expect(yield* reader.get(key)).toBeNull();
          expect(
            yield* writer.change(put(command.operationId, null, "accepted retry")),
          ).toMatchObject({ generation: 1 });
        }).pipe(Effect.provide(storeLayer(filename)));
      }),
    ),
  );

  it.effect("persists corrections, exact receipts, scopes, and a terminal withdrawal", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        const firstPut = put("put-1", null, "prefers tea", ["profile"]);
        const correction = put("put-2", "1", "prefers coffee", ["private", "profile"]);
        const withdrawal = withdraw("withdraw-1", "2");

        const initial = yield* Effect.gen(function* () {
          const writer = yield* MemoryWriter;
          const reader = yield* MemoryReader;
          yield* TestClock.setTime(1_000);
          const first = yield* writer.change(firstPut);
          yield* TestClock.setTime(2_000);
          const corrected = yield* writer.change(correction);
          const replayed = yield* writer.change(firstPut);
          expect(replayed).toEqual(first);
          expect(yield* reader.get(key)).toEqual(corrected);
          const divergent = yield* writer
            .change(put("put-1", null, "different command", ["profile"]))
            .pipe(Effect.flip);
          expect(divergent).toEqual(MemoryOperationConflict.make({ key, operationId: "put-1" }));
          return { first, corrected };
        }).pipe(Effect.provide(storeLayer(filename)));

        expect(initial.first.modifiedAt).toBe(1_000);
        expect(initial.corrected.modifiedAt).toBe(2_000);
        expect(initial.corrected._tag).toBe("ActiveMemoryDocument");
        if (initial.corrected._tag === "ActiveMemoryDocument") {
          expect(initial.corrected.scopes).toEqual(["private", "profile"]);
          expect(initial.corrected.content.recordedAt).toBe(75);
          expect(initial.corrected.content.extractedAt).toBe(80);
          expect(initial.corrected.content.attributions[0].activityAt).toBe(50);
        }

        const withdrawn = yield* Effect.gen(function* () {
          const reader = yield* MemoryReader;
          const writer = yield* MemoryWriter;
          expect(yield* reader.get(key)).toEqual(initial.corrected);
          yield* TestClock.setTime(3_000);
          const tombstone = yield* writer.change(withdrawal);
          expect(yield* writer.change(correction)).toEqual(initial.corrected);
          expect(yield* reader.get(key)).toEqual(tombstone);
          const delayed = yield* writer
            .change(put("delayed-put", "2", "stale resurrection"))
            .pipe(Effect.flip);
          expect(delayed).toEqual(MemoryWithdrawn.make({ key, revision: "3" }));
          return tombstone;
        }).pipe(Effect.provide(storeLayer(filename)));

        expect(withdrawn._tag).toBe("WithdrawnMemoryDocument");
        expect(withdrawn.generation).toBe(3);
        expect(withdrawn.modifiedAt).toBe(3_000);
        expect(yield* readCurrent(filename)).toEqual(withdrawn);
      }),
    ),
  );

  it.effect("serializes competing conditional edits across independent connections", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        yield* Effect.gen(function* () {
          const writer = yield* MemoryWriter;
          yield* writer.change(put("seed", null, "seed"));
        }).pipe(Effect.provide(storeLayer(filename)));

        const edit = (command: MemoryWrite) =>
          Effect.gen(function* () {
            const writer = yield* MemoryWriter;
            return yield* writer.change(command);
          }).pipe(Effect.provide(storeLayer(filename)), Effect.result);
        const outcomes = yield* Effect.all(
          [put("race-a", "1", "edit a"), put("race-b", "1", "edit b")].map(edit),
          { concurrency: "unbounded" },
        );
        expect(outcomes.filter(Result.isSuccess)).toHaveLength(1);
        expect(outcomes.filter(Result.isFailure)).toHaveLength(1);
        const failure = outcomes.find(Result.isFailure);
        expect(failure?.failure).toEqual(
          MemoryConflict.make({ key, expectedRevision: "1", actualRevision: "2" }),
        );
        expect((yield* readCurrent(filename))?.generation).toBe(2);
      }),
    ),
  );

  it.effect("isolates identical source and operation IDs by namespace", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        const otherKey = MemoryKey.make({ namespace: "tenant-b", id: key.id });
        const firstCommand = putFor(key, "shared-operation", null, "tenant a");
        const secondCommand = putFor(otherKey, "shared-operation", null, "tenant b");
        yield* Effect.gen(function* () {
          const reader = yield* MemoryReader;
          const writer = yield* MemoryWriter;
          const first = yield* writer.change(firstCommand);
          const second = yield* writer.change(secondCommand);
          expect(first.key).toEqual(key);
          expect(second.key).toEqual(otherKey);
          expect(second).not.toEqual(first);
          expect(yield* writer.change(firstCommand)).toEqual(first);
          expect(yield* writer.change(secondCommand)).toEqual(second);
          expect(yield* reader.get(key)).toEqual(first);
          expect(yield* reader.get(otherKey)).toEqual(second);
        }).pipe(Effect.provide(storeLayer(filename)));
      }),
    ),
  );

  for (const point of [
    "memory:change:before",
    "memory:change:after-state",
    "memory:change:after-receipt",
  ] as const) {
    it.effect(`rolls back ${point}`, () =>
      withTemporaryDatabase((filename) =>
        Effect.gen(function* () {
          const command = put(`operation-${point}`, null, point);
          const failed = yield* Effect.gen(function* () {
            const writer = yield* MemoryWriter;
            return yield* writer.change(command);
          }).pipe(
            Effect.provide(
              failpointLayer(filename, (current) =>
                current === point
                  ? Effect.fail(MemoryMutationFailure.make({ point: current }))
                  : Effect.void,
              ),
            ),
            Effect.flip,
          );
          expect(failed).toEqual(MemoryMutationFailure.make({ point }));
          expect(yield* readCurrent(filename)).toBeNull();
          const recovered = yield* Effect.gen(function* () {
            const writer = yield* MemoryWriter;
            return yield* writer.change(command);
          }).pipe(Effect.provide(storeLayer(filename)));
          expect(recovered.generation).toBe(1);
        }),
      ),
    );
  }

  for (const point of ["memory:change:after-state", "memory:change:after-receipt"] as const) {
    it.effect(`rolls back withdrawal at ${point}`, () =>
      withTemporaryDatabase((filename) =>
        Effect.gen(function* () {
          const active = yield* Effect.gen(function* () {
            const writer = yield* MemoryWriter;
            return yield* writer.change(put("withdraw-seed", null, "still active"));
          }).pipe(Effect.provide(storeLayer(filename)));
          const command = withdraw(`withdraw-${point}`, "1");
          const failed = yield* Effect.gen(function* () {
            const writer = yield* MemoryWriter;
            return yield* writer.change(command);
          }).pipe(
            Effect.provide(
              failpointLayer(filename, (current) =>
                current === point
                  ? Effect.fail(MemoryMutationFailure.make({ point: current }))
                  : Effect.void,
              ),
            ),
            Effect.flip,
          );
          expect(failed).toEqual(MemoryMutationFailure.make({ point }));
          expect(yield* readCurrent(filename)).toEqual(active);
          const recovered = yield* Effect.gen(function* () {
            const writer = yield* MemoryWriter;
            return yield* writer.change(command);
          }).pipe(Effect.provide(storeLayer(filename)));
          expect(recovered._tag).toBe("WithdrawnMemoryDocument");
          expect(recovered.generation).toBe(2);
        }),
      ),
    );
  }

  it.effect("replays the committed receipt after the lost-ack failpoint", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        const command = put("lost-ack", null, "committed once");
        yield* TestClock.setTime(10_000);
        const failure = yield* Effect.gen(function* () {
          const writer = yield* MemoryWriter;
          return yield* writer.change(command);
        }).pipe(
          Effect.provide(
            failpointLayer(filename, (point) =>
              point === "memory:change:after"
                ? Effect.fail(MemoryMutationFailure.make({ point }))
                : Effect.void,
            ),
          ),
          Effect.flip,
        );
        expect(failure).toEqual(MemoryMutationFailure.make({ point: "memory:change:after" }));
        const committed = yield* readCurrent(filename);
        yield* TestClock.setTime(20_000);
        const replayed = yield* Effect.gen(function* () {
          const writer = yield* MemoryWriter;
          return yield* writer.change(command);
        }).pipe(Effect.provide(storeLayer(filename)));
        expect(replayed).toEqual(committed);
        expect(replayed.modifiedAt).toBe(10_000);
      }),
    ),
  );

  it.effect("replays an exact committed tombstone after lost acknowledgement", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        yield* Effect.gen(function* () {
          const writer = yield* MemoryWriter;
          yield* writer.change(put("withdraw-lost-ack-seed", null, "active"));
        }).pipe(Effect.provide(storeLayer(filename)));
        const command = withdraw("withdraw-lost-ack", "1");
        yield* TestClock.setTime(30_000);
        const failure = yield* Effect.gen(function* () {
          const writer = yield* MemoryWriter;
          return yield* writer.change(command);
        }).pipe(
          Effect.provide(
            failpointLayer(filename, (point) =>
              point === "memory:change:after"
                ? Effect.fail(MemoryMutationFailure.make({ point }))
                : Effect.void,
            ),
          ),
          Effect.flip,
        );
        expect(failure).toEqual(MemoryMutationFailure.make({ point: "memory:change:after" }));
        const committed = yield* readCurrent(filename);
        expect(committed?._tag).toBe("WithdrawnMemoryDocument");
        expect(committed?.modifiedAt).toBe(30_000);

        yield* TestClock.setTime(40_000);
        yield* Effect.gen(function* () {
          const reader = yield* MemoryReader;
          const writer = yield* MemoryWriter;
          expect(yield* writer.change(command)).toEqual(committed);
          const delayed = yield* writer
            .change(put("withdraw-delayed-put", "1", "stale resurrection"))
            .pipe(Effect.flip);
          expect(delayed).toEqual(MemoryWithdrawn.make({ key, revision: "2" }));
          expect(yield* reader.get(key)).toEqual(committed);
        }).pipe(Effect.provide(storeLayer(filename)));
      }),
    ),
  );

  for (const point of ["memory:initialize:before", "memory:initialize:after"] as const) {
    it.effect(`recovers initialization at ${point} without canonical storage tables`, () =>
      withTemporaryDatabase((filename) =>
        Effect.gen(function* () {
          const opened = yield* Effect.void.pipe(
            Effect.provide(
              failpointLayer(filename, (current) =>
                current === point
                  ? Effect.fail(MemoryMutationFailure.make({ point: current }))
                  : Effect.void,
              ),
            ),
            Effect.exit,
          );
          expect(Exit.isFailure(opened)).toBe(true);
          yield* Effect.void.pipe(Effect.provide(storeLayer(filename)));
          expect(yield* readCurrent(filename)).toBeNull();
          const names = yield* runRaw(
            filename,
            Effect.gen(function* () {
              const sql = yield* SqlClientService.SqlClient;
              return yield* sql<{ name: string }>`
                SELECT name FROM sqlite_master
                WHERE type = 'table' AND name LIKE 'effect_agent_%'
                ORDER BY name
              `;
            }),
          );
          expect(names.map((row) => row.name)).toEqual([
            "effect_agent_memory_documents_v1",
            "effect_agent_memory_metadata",
            "effect_agent_memory_receipts_v1",
          ]);
        }),
      ),
    );
  }

  it.effect("rolls back defects, timeouts, and interruption after the state write", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        const defectExit = yield* Effect.gen(function* () {
          const writer = yield* MemoryWriter;
          return yield* writer.change(put("defect", null, "defect"));
        }).pipe(
          Effect.provide(
            failpointLayer(filename, (point) =>
              point === "memory:change:after-state" ? Effect.die("injected defect") : Effect.void,
            ),
          ),
          Effect.exit,
        );
        expect(Exit.isFailure(defectExit) && Cause.hasDies(defectExit.cause)).toBe(true);
        expect(yield* readCurrent(filename)).toBeNull();

        const timeoutFile = `${filename}-timeout`;
        const timeoutReached = yield* Deferred.make<void>();
        const timeoutFiber = yield* Effect.gen(function* () {
          const writer = yield* MemoryWriter;
          return yield* writer.change(put("timeout", null, "timeout"));
        }).pipe(
          Effect.provide(
            failpointLayer(timeoutFile, (point) =>
              point === "memory:change:after-state"
                ? Deferred.succeed(timeoutReached, undefined).pipe(Effect.andThen(Effect.never))
                : Effect.void,
            ),
          ),
          Effect.timeout("1 second"),
          Effect.forkChild,
        );
        yield* Deferred.await(timeoutReached);
        yield* TestClock.adjust("1 second");
        expect(Exit.isFailure(yield* Fiber.await(timeoutFiber))).toBe(true);
        expect(yield* readCurrent(timeoutFile)).toBeNull();

        const interruptedFile = `${filename}-interrupted`;
        const interruptionReached = yield* Deferred.make<void>();
        const interruptedFiber = yield* Effect.gen(function* () {
          const writer = yield* MemoryWriter;
          return yield* writer.change(put("interrupted", null, "interrupted"));
        }).pipe(
          Effect.provide(
            failpointLayer(interruptedFile, (point) =>
              point === "memory:change:after-state"
                ? Deferred.succeed(interruptionReached, undefined).pipe(
                    Effect.andThen(Effect.never),
                  )
                : Effect.void,
            ),
          ),
          Effect.forkChild,
        );
        yield* Deferred.await(interruptionReached);
        yield* Fiber.interrupt(interruptedFiber);
        const interruptedExit = yield* Fiber.await(interruptedFiber);
        expect(Exit.isFailure(interruptedExit) && Cause.hasInterrupts(interruptedExit.cause)).toBe(
          true,
        );
        expect(yield* readCurrent(interruptedFile)).toBeNull();
      }),
    ),
  );

  it.effect("rejects incompatible and corrupt stored documents and receipts", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        const command = put("stored-row", null, "stored row");
        yield* Effect.gen(function* () {
          const writer = yield* MemoryWriter;
          yield* writer.change(command);
        }).pipe(Effect.provide(storeLayer(filename)));

        yield* runRaw(
          filename,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;
            yield* sql`
              UPDATE effect_agent_memory_documents_v1
              SET document_json = '{"version":2}'
              WHERE namespace = ${key.namespace} AND source_id = ${key.id}
            `;
          }),
        );
        const incompatible = yield* readCurrent(filename).pipe(Effect.flip);
        expect(incompatible._tag).toBe("MemoryStorageError");
        if (incompatible._tag === "MemoryStorageError") {
          expect(incompatible.reason).toBe("incompatible");
        }

        const receiptFile = `${filename}-receipt`;
        yield* Effect.gen(function* () {
          const writer = yield* MemoryWriter;
          yield* writer.change(command);
        }).pipe(Effect.provide(storeLayer(receiptFile)));
        yield* runRaw(
          receiptFile,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;
            yield* sql`
              UPDATE effect_agent_memory_receipts_v1
              SET result_json = '{"version":1,"value":null}'
              WHERE namespace = ${key.namespace} AND operation_id = ${command.operationId}
            `;
          }),
        );
        const corruptReceipt = yield* Effect.gen(function* () {
          const writer = yield* MemoryWriter;
          return yield* writer.change(command);
        }).pipe(Effect.provide(storeLayer(receiptFile)), Effect.flip);
        expect(corruptReceipt._tag).toBe("MemoryStorageError");
        if (corruptReceipt._tag === "MemoryStorageError") {
          expect(corruptReceipt.reason).toBe("corrupt");
        }
      }),
    ),
  );

  it.effect("rejects canonical receipt results forged independently of their commands", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        const created = put("forged-created", null, "created", ["created-scope"]);
        const corrected = put("forged-corrected", "1", "corrected", ["corrected-scope"]);
        const withdrawn = withdraw("forged-withdrawn", "2");
        yield* Effect.gen(function* () {
          const writer = yield* MemoryWriter;
          yield* writer.change(created);
          yield* writer.change(corrected);
          yield* writer.change(withdrawn);
        }).pipe(Effect.provide(storeLayer(filename)));

        const replayFailure = (command: MemoryWrite) =>
          Effect.gen(function* () {
            const writer = yield* MemoryWriter;
            return yield* writer.change(command);
          }).pipe(Effect.provide(storeLayer(filename)), Effect.flip);

        const receiptResults = yield* runRaw(
          filename,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;
            return yield* sql<{ operation_id: string; result_json: string }>`
              SELECT operation_id, result_json
              FROM effect_agent_memory_receipts_v1
              WHERE namespace = ${key.namespace}
                AND operation_id IN (${created.operationId}, ${corrected.operationId}, ${withdrawn.operationId})
            `;
          }),
        );
        const resultFor = (operationId: string) =>
          receiptResults.find((row) => row.operation_id === operationId)?.result_json;
        const createdResult = resultFor(created.operationId);
        const withdrawnResult = resultFor(withdrawn.operationId);
        expect(createdResult).toBeDefined();
        expect(withdrawnResult).toBeDefined();
        if (createdResult === undefined || withdrawnResult === undefined) return;

        for (const [original, replacement] of [
          ['"text":"created"', '"text":"forged"'],
          ['"scopes":["created-scope"]', '"scopes":["forged-scope"]'],
          ['"locator":"memory://source-1"', '"locator":"memory://forged"'],
        ]) {
          yield* runRaw(
            filename,
            Effect.gen(function* () {
              const sql = yield* SqlClientService.SqlClient;
              yield* sql`
                UPDATE effect_agent_memory_receipts_v1
                SET result_json = replace(${createdResult}, ${original}, ${replacement})
                WHERE namespace = ${key.namespace} AND operation_id = ${created.operationId}
              `;
            }),
          );
          expect(yield* replayFailure(created)).toEqual(
            MemoryStorageError.make({ operation: "change memory document", reason: "corrupt" }),
          );
        }

        yield* runRaw(
          filename,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;
            yield* sql`
              UPDATE effect_agent_memory_receipts_v1
              SET result_json = ${createdResult}
              WHERE namespace = ${key.namespace} AND operation_id = ${corrected.operationId}
            `;
          }),
        );
        expect(yield* replayFailure(corrected)).toEqual(
          MemoryStorageError.make({ operation: "change memory document", reason: "corrupt" }),
        );

        yield* runRaw(
          filename,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;
            yield* sql`
              UPDATE effect_agent_memory_receipts_v1
              SET result_json = ${createdResult}
              WHERE namespace = ${key.namespace} AND operation_id = ${withdrawn.operationId}
            `;
          }),
        );
        expect(yield* replayFailure(withdrawn)).toEqual(
          MemoryStorageError.make({ operation: "change memory document", reason: "corrupt" }),
        );

        yield* runRaw(
          filename,
          Effect.gen(function* () {
            const sql = yield* SqlClientService.SqlClient;
            yield* sql`
              UPDATE effect_agent_memory_receipts_v1
              SET result_json = replace(
                ${withdrawnResult},
                ${'"reason":"source withdrawn"'},
                ${'"reason":"forged withdrawal"'}
              )
              WHERE namespace = ${key.namespace} AND operation_id = ${withdrawn.operationId}
            `;
          }),
        );
        expect(yield* replayFailure(withdrawn)).toEqual(
          MemoryStorageError.make({ operation: "change memory document", reason: "corrupt" }),
        );
      }),
    ),
  );
});
