import * as MemoryNamespace from "@effect-agent/core/MemoryNamespace";
import {
  MemoryScope,
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
} from "@effect-agent/core/MemoryStore";
import {
  SqlMemoryLimits,
  memoryReaderLayer,
  memoryStoreLayer,
  memoryStoreLayerWithFailpoints,
} from "@effect-agent/thread/SqlMemoryStore";
import { NodeFileSystem } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { describe, expect, it } from "@effect/vitest";
import {
  Schema as NamespaceSchema,
  Cause,
  Deferred,
  Effect,
  Exit,
  Fiber,
  FileSystem,
  Layer,
  Result,
  Schema,
} from "effect";
import type { PlatformError } from "effect";
import { TestClock } from "effect/testing";
import * as SqlClientService from "effect/unstable/sql/SqlClient";

const TestNamespace = MemoryNamespace.define({
  name: "test/memory",
  version: 1,
  identity: NamespaceSchema.String,
});

const key = MemoryKey.make({ namespace: TestNamespace.make("tenant-a"), id: "source-1" });

const putFor = <Namespace extends MemoryNamespace.Any>(
  memoryKey: MemoryKey<Namespace>,
  operationId: string,
  expectedRevision: string | null,
  text: string,
  scopes: ReadonlyArray<MemoryScope> = [MemoryScope.make("team")],
) =>
  MemoryWrite.make({
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
  scopes: ReadonlyArray<MemoryScope> = [MemoryScope.make("team")],
) => putFor(key, operationId, expectedRevision, text, scopes);

const withdraw = (operationId: string, expectedRevision: string) =>
  Schema.decodeSync(MemoryWrite.Wire)({
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
  Schema.decodeSync(MemoryWrite.Wire)({
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

// Derive the oracle from retained rows, independently of the accounting triggers.
const checkUsage = (filename: string) =>
  runRaw(
    filename,
    Effect.gen(function* () {
      const sql = yield* SqlClientService.SqlClient;

      const documents = yield* sql<{ namespace: string; source_id: string; document_json: string }>`
    SELECT namespace, source_id, document_json FROM effect_agent_memory_documents_v1
  `;

      const receipts = yield* sql<{
        namespace: string;
        source_id: string;
        operation_id: string;
        command_json: string;
        result_json: string;
      }>`
    SELECT namespace, source_id, operation_id, command_json, result_json FROM effect_agent_memory_receipts_v1
  `;

      const bytes = [...documents, ...receipts].reduce(
        (total, row) =>
          total +
          128 +
          Object.values(row).reduce(
            (size, value) => size + new TextEncoder().encode(value).length,
            0,
          ),
        0,
      );

      const usage = yield* sql<{ documents: number; receipts: number; bytes: number }>`
    SELECT documents, receipts, bytes FROM effect_agent_memory_usage_v1
  `;

      expect(usage).toEqual([{ documents: documents.length, receipts: receipts.length, bytes }]);

      return { documents: documents.length, receipts: receipts.length, bytes };
    }),
  );

// Reproduce the beta.47 layout without touching canonical document/receipt bytes.
const legacyLayout = (filename: string) =>
  runRaw(
    filename,
    Effect.gen(function* () {
      const sql = yield* SqlClientService.SqlClient;

      for (const table of ["documents", "receipts"]) {
        for (const event of ["insert", "update", "delete"]) {
          yield* sql.unsafe(`DROP TRIGGER effect_agent_memory_${table}_v1_usage_${event}`);
        }
      }
      yield* sql`DROP TABLE effect_agent_memory_usage_v1`;
      yield* sql`DELETE FROM effect_agent_memory_metadata WHERE component = 'memory-usage'`;
    }),
  );

describe("SQLite memory store", () => {
  it.effect("admits shrinking UTF-8 replacements by their retained byte delta", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        const first = put("large", null, "🌱".repeat(1_000));
        const replacement = put("small", "1", "é");
        const defaults = yield* SqlMemoryLimits;

        yield* Effect.flatMap(MemoryWriter, (writer) => writer.change(first)).pipe(
          Effect.provide(storeLayer(filename)),
        );
        const before = yield* checkUsage(filename);

        // The smaller head releases more bytes than the new small receipt consumes.
        const bounded = storeLayer(filename).pipe(
          Layer.provide(
            Layer.succeed(SqlMemoryLimits, {
              ...defaults,
              maxStorageBytes: before.bytes,
              maxDocuments: 1,
            }),
          ),
        );

        const result = yield* Effect.flatMap(MemoryWriter, (writer) =>
          writer.change(replacement),
        ).pipe(Effect.provide(bounded));

        expect(result.generation).toBe(2);
        const after = yield* checkUsage(filename);

        expect(after.bytes).toBeLessThan(before.bytes);
        expect(after).toMatchObject({ documents: 1, receipts: 2 });
        yield* Effect.gen(function* () {
          const writer = yield* MemoryWriter;

          expect(yield* writer.change(replacement)).toEqual(result);
          expect(
            yield* writer.change(put("small", "1", "changed")).pipe(Effect.flip),
          ).toBeInstanceOf(MemoryOperationConflict);
        }).pipe(Effect.provide(bounded));
        expect(yield* checkUsage(filename)).toEqual(after);
      }),
    ),
  );

  it.effect(
    "reserves receipt capacity for terminal withdrawal and permits exact replay at the hard limit",
    () =>
      withTemporaryDatabase((filename) =>
        Effect.gen(function* () {
          const defaults = yield* SqlMemoryLimits;

          const bounded = storeLayer(filename).pipe(
            Layer.provide(
              Layer.succeed(SqlMemoryLimits, {
                ...defaults,
                maxDocuments: 1,
                maxReceipts: 2,
                reservedWithdrawalReceipts: 1,
              }),
            ),
          );

          yield* Effect.gen(function* () {
            const writer = yield* MemoryWriter;

            expect(yield* writer.change(withdraw("missing", "1")).pipe(Effect.flip)).toBeInstanceOf(
              MemoryConflict,
            );
            const original = yield* writer.change(put("original", null, "retained"));

            expect(
              yield* writer.change(put("correction", "1", "new")).pipe(Effect.flip),
            ).toMatchObject({ reason: "invalid-input" });
            const tombstone = yield* writer.change(withdraw("withdraw", "1"));

            expect(yield* writer.change(withdraw("withdraw", "1"))).toEqual(tombstone);
            expect(yield* writer.change(put("original", null, "retained"))).toEqual(original);
            expect(
              yield* writer.change(put("late", "2", "resurrect")).pipe(Effect.flip),
            ).toBeInstanceOf(MemoryWithdrawn);
          }).pipe(Effect.provide(bounded));
          expect(yield* checkUsage(filename)).toMatchObject({ documents: 1, receipts: 2 });
        }),
      ),
  );

  it.effect("uses finite byte headroom after migration above the ordinary threshold", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        const defaults = yield* SqlMemoryLimits;

        yield* Effect.flatMap(MemoryWriter, (writer) =>
          writer.change(put("seed", null, "short")),
        ).pipe(Effect.provide(storeLayer(filename)));
        const before = yield* checkUsage(filename);

        yield* legacyLayout(filename);

        const limits = {
          ...defaults,
          maxStorageBytes: before.bytes + 5_000,
          reservedWithdrawalBytes: 5_000,
        };

        const bounded = storeLayer(filename).pipe(
          Layer.provide(Layer.succeed(SqlMemoryLimits, limits)),
        );

        yield* Effect.gen(function* () {
          const writer = yield* MemoryWriter;

          expect(
            yield* writer.change(put("ordinary", "1", "short")).pipe(Effect.flip),
          ).toMatchObject({ reason: "invalid-input" });
          expect(
            yield* writer
              .change(
                MemoryWrite.make({
                  _tag: "Withdraw",
                  key,
                  operationId: "cleanup",
                  expectedRevision: "1",
                  reason: "€".repeat(4_096),
                }),
              )
              .pipe(Effect.flip),
          ).toMatchObject({ operation: "memory storage limit", reason: "invalid-input" });
          expect(yield* writer.change(withdraw("cleanup", "1"))).toMatchObject({
            _tag: "WithdrawnMemoryDocument",
          });
        }).pipe(Effect.provide(bounded));
        expect((yield* checkUsage(filename)).bytes).toBeLessThanOrEqual(limits.maxStorageBytes);
      }),
    ),
  );

  it.effect("keeps trusted cleanup Put within the same hard limit over one SQL client", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        const defaults = yield* SqlMemoryLimits;
        const sqlLayer = SqliteClient.layer({ filename });

        const ordinary = memoryStoreLayer.pipe(
          Layer.provide(
            Layer.succeed(SqlMemoryLimits, {
              ...defaults,
              maxReceipts: 2,
              reservedWithdrawalReceipts: 1,
            }),
          ),
        );

        const cleanup = memoryStoreLayer.pipe(
          Layer.provide(
            Layer.succeed(SqlMemoryLimits, {
              ...defaults,
              maxReceipts: 2,
            }),
          ),
        );

        yield* Effect.gen(function* () {
          yield* Effect.gen(function* () {
            const writer = yield* MemoryWriter;

            yield* writer.change(put("profile", null, "source-owned and unrelated"));
            expect(
              yield* writer.change(put("cleanup", "1", "unrelated")).pipe(Effect.flip),
            ).toMatchObject({ reason: "invalid-input" });
          }).pipe(Effect.provide(ordinary));
          yield* Effect.gen(function* () {
            const writer = yield* MemoryWriter;

            expect(yield* writer.change(put("cleanup", "1", "unrelated"))).toMatchObject({
              generation: 2,
            });
            expect(
              yield* writer.change(put("overflow", "2", "more")).pipe(Effect.flip),
            ).toMatchObject({ reason: "invalid-input" });
          }).pipe(Effect.provide(cleanup));
        }).pipe(Effect.provide(sqlLayer));
        expect(yield* checkUsage(filename)).toMatchObject({ documents: 1, receipts: 2 });
      }),
    ),
  );

  for (const point of [
    "memory:initialize:before-accounting",
    "memory:initialize:after-accounting",
  ] as const) {
    it.effect(`migrates legacy receipts atomically after ${point}`, () =>
      withTemporaryDatabase((filename) =>
        Effect.gen(function* () {
          const old = put("legacy", null, "old plaintext 🌱");

          const otherKey = MemoryKey.make({
            namespace: TestNamespace.make("tenant-b"),
            id: key.id,
          });

          const other = putFor(otherKey, "legacy", null, "different namespace");

          const results = yield* Effect.gen(function* () {
            const writer = yield* MemoryWriter;
            const original = yield* writer.change(old);

            yield* writer.change(put("correction", "1", "corrected"));
            const tombstone = yield* writer.change(withdraw("terminal", "2"));
            const second = yield* writer.change(other);

            return { original, tombstone, second };
          }).pipe(Effect.provide(storeLayer(filename)));

          const snapshot = () =>
            runRaw(
              filename,
              Effect.gen(function* () {
                const sql = yield* SqlClientService.SqlClient;

                return yield* sql`SELECT * FROM effect_agent_memory_receipts_v1 ORDER BY namespace, operation_id`;
              }),
            );

          const receipts = yield* snapshot();

          yield* legacyLayout(filename);
          expect(yield* readCurrent(filename)).toEqual(results.tombstone);

          const failed = yield* Effect.void.pipe(
            Effect.provide(
              failpointLayer(filename, (current) =>
                current === point
                  ? Effect.fail(MemoryMutationFailure.make({ point }))
                  : Effect.void,
              ),
            ),
            Effect.flip,
          );

          expect(failed).toEqual(MemoryMutationFailure.make({ point }));

          const tables = yield* runRaw(
            filename,
            Effect.gen(function* () {
              const sql = yield* SqlClientService.SqlClient;

              return yield* sql`SELECT name FROM sqlite_master WHERE name = 'effect_agent_memory_usage_v1'`;
            }),
          );

          expect(tables).toEqual([]);
          yield* TestClock.setTime(50_000);
          yield* Effect.gen(function* () {
            const writer = yield* MemoryWriter;

            expect(yield* writer.change(old)).toEqual(results.original);
            expect(yield* writer.change(other)).toEqual(results.second);
            expect(yield* writer.change(withdraw("terminal", "2"))).toEqual(results.tombstone);
            expect(
              yield* writer.change(put("legacy", null, "changed")).pipe(Effect.flip),
            ).toBeInstanceOf(MemoryOperationConflict);
            expect(
              yield* writer.change(put("late", "3", "resurrect")).pipe(Effect.flip),
            ).toBeInstanceOf(MemoryWithdrawn);
          }).pipe(Effect.provide(storeLayer(filename)));
          expect(yield* snapshot()).toEqual(receipts);
          expect(yield* checkUsage(filename)).toMatchObject({ documents: 2, receipts: 4 });
        }),
      ),
    );
  }

  it.effect("serializes independent connections against the last ordinary receipt", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        const defaults = yield* SqlMemoryLimits;

        yield* Effect.void.pipe(Effect.provide(storeLayer(filename)));

        const writes = ["a", "b"].map((id) =>
          Effect.flatMap(MemoryWriter, (writer) =>
            writer.change(
              putFor(MemoryKey.make({ namespace: key.namespace, id }), id, null, "text"),
            ),
          ).pipe(
            Effect.provide(
              storeLayer(filename).pipe(
                Layer.provide(
                  Layer.succeed(SqlMemoryLimits, {
                    ...defaults,
                    maxReceipts: 2,
                    reservedWithdrawalReceipts: 1,
                  }),
                ),
              ),
            ),
            Effect.result,
          ),
        );

        const results = yield* Effect.all(writes, { concurrency: 2 });

        expect(results.filter(Result.isSuccess)).toHaveLength(1);
        expect(results.filter(Result.isFailure)).toHaveLength(1);
        expect(yield* checkUsage(filename)).toMatchObject({ documents: 1, receipts: 1 });
      }),
    ),
  );

  for (const damage of ["row", "marker", "trigger", "counter"] as const) {
    it.effect(`rejects damaged established accounting: ${damage}`, () =>
      withTemporaryDatabase((filename) =>
        Effect.gen(function* () {
          yield* Effect.flatMap(MemoryWriter, (writer) =>
            writer.change(put("retained", null, "text")),
          ).pipe(Effect.provide(storeLayer(filename)));
          yield* runRaw(
            filename,
            Effect.gen(function* () {
              const sql = yield* SqlClientService.SqlClient;

              if (damage === "row") yield* sql`DELETE FROM effect_agent_memory_usage_v1`;
              if (damage === "marker")
                yield* sql`DELETE FROM effect_agent_memory_metadata WHERE component = 'memory-usage'`;
              if (damage === "trigger")
                yield* sql`DROP TRIGGER effect_agent_memory_documents_v1_usage_update`;
              if (damage === "counter") {
                yield* sql`PRAGMA ignore_check_constraints = ON`;
                yield* sql`UPDATE effect_agent_memory_usage_v1 SET receipts = -1`;
              }
            }),
          );
          expect(
            yield* Effect.void.pipe(Effect.provide(storeLayer(filename)), Effect.flip),
          ).toMatchObject({ _tag: "MemoryStorageError", reason: "corrupt" });
          expect(yield* readCurrent(filename)).toMatchObject({ generation: 1 });
        }),
      ),
    );
  }

  it.effect("rejects invalid reserves before creating storage", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        const defaults = yield* SqlMemoryLimits;

        for (const reserve of [-1, 1.5, 3]) {
          expect(
            yield* Effect.void.pipe(
              Effect.provide(
                storeLayer(filename).pipe(
                  Layer.provide(
                    Layer.succeed(SqlMemoryLimits, {
                      ...defaults,
                      maxReceipts: 2,
                      reservedWithdrawalReceipts: reserve,
                    }),
                  ),
                ),
              ),
              Effect.flip,
            ),
          ).toMatchObject({ reason: "invalid-input" });
        }
      }),
    ),
  );

  for (const limit of ["maxDocuments", "maxReceipts", "maxStorageBytes"] as const) {
    it.effect(`enforces ${limit} independently without a row byte limit`, () =>
      withTemporaryDatabase((filename) =>
        Effect.gen(function* () {
          const defaults = yield* SqlMemoryLimits;

          const bounded = storeLayer(filename).pipe(
            Layer.provide(
              Layer.succeed(SqlMemoryLimits, {
                ...defaults,
                [limit]: limit === "maxStorageBytes" ? 16_384 : 1,
              }),
            ),
          );

          const first = put("first", null, "x".repeat(4_096));
          const secondKey = MemoryKey.make({ namespace: key.namespace, id: "source-2" });

          // Corrections add receipts but not documents. Each byte-limited write fits alone.
          const rejected =
            limit === "maxReceipts"
              ? put("second", "1", "correction")
              : putFor(secondKey, "second", null, "x".repeat(4_096));

          yield* Effect.gen(function* () {
            const writer = yield* MemoryWriter;
            const reader = yield* MemoryReader;
            const stored = yield* writer.change(first);

            expect(yield* writer.change(rejected).pipe(Effect.flip)).toEqual(
              MemoryStorageError.make({
                operation: "memory storage limit",
                reason: "invalid-input",
              }),
            );
            expect(yield* reader.get(key)).toEqual(stored);
            expect(yield* reader.get(secondKey)).toBeNull();
            expect(yield* writer.change(first)).toEqual(stored);
            if (limit === "maxDocuments") {
              expect(yield* writer.change(put("correction", "1", "corrected"))).toMatchObject({
                generation: 2,
              });
            }
          }).pipe(Effect.provide(bounded));
          // Reopen without the capacity limit: a rejected command must not retain a receipt.
          yield* Effect.gen(function* () {
            const writer = yield* MemoryWriter;
            const reader = yield* MemoryReader;
            const retried = yield* writer.change(rejected);

            expect(retried.generation).toBe(limit === "maxReceipts" ? 2 : 1);
            expect(yield* reader.get(rejected.key)).toEqual(retried);
          }).pipe(Effect.provide(storeLayer(filename)));
        }),
      ),
    );
  }

  it.effect(
    "isolates structured namespace documents, receipts, and tombstones across restart",
    () =>
      withTemporaryDatabase((filename) =>
        Effect.gen(function* () {
          const identity = Schema.Struct({ tenantId: Schema.String, userId: Schema.String });
          const users = MemoryNamespace.define({ name: "app/users", version: 1, identity });
          const other = MemoryNamespace.define({ name: "app/other", version: 1, identity });
          const newer = MemoryNamespace.define({ name: "app/users", version: 2, identity });

          const namespaces = [
            users.make({ tenantId: "a", userId: "one" }),
            users.make({ tenantId: "a", userId: "two" }),
            users.make({ tenantId: "b", userId: "one" }),
            other.make({ tenantId: "a", userId: "one" }),
            newer.make({ tenantId: "a", userId: "one" }),
          ];

          for (const [index, namespace] of namespaces.entries()) {
            const command = putFor(
              MemoryKey.make({ namespace, id: "shared-source" }),
              "shared-operation",
              null,
              `source-${index}`,
            );

            const write = Effect.flatMap(MemoryWriter, (writer) => writer.change(command));

            if (index === 0) {
              expect(
                yield* write.pipe(
                  Effect.provide(
                    failpointLayer(filename, (point) =>
                      point === "memory:change:after"
                        ? Effect.fail(MemoryMutationFailure.make({ point }))
                        : Effect.void,
                    ),
                  ),
                  Effect.flip,
                ),
              ).toMatchObject({ point: "memory:change:after" });
            } else yield* write.pipe(Effect.provide(storeLayer(filename)));
          }

          const reconstructed = MemoryNamespace.define({
            name: "app/users",
            version: 1,
            identity: Schema.Struct({ userId: Schema.String, tenantId: Schema.String }),
          });

          const first = reconstructed.make({ userId: "one", tenantId: "a" });

          expect(first).not.toBe(namespaces[0]);
          expect(first.address).toBe(namespaces[0].address);
          yield* Effect.gen(function* () {
            const writer = yield* MemoryWriter;
            const reader = yield* MemoryReader;

            for (const [index, namespace] of [first, ...namespaces.slice(1)].entries()) {
              const memoryKey = MemoryKey.make({ namespace, id: "shared-source" });

              const receipt = yield* writer.change(
                putFor(memoryKey, "shared-operation", null, `source-${index}`),
              );

              expect(receipt.generation).toBe(1);
              expect(yield* reader.get(memoryKey)).toEqual(receipt);
              if (receipt._tag === "ActiveMemoryDocument")
                expect(receipt.content.text).toBe(`source-${index}`);
              expect(
                yield* writer
                  .change(putFor(memoryKey, "shared-operation", null, "changed payload"))
                  .pipe(Effect.flip),
              ).toMatchObject({ _tag: "MemoryOperationConflict" });
            }
            const firstKey = MemoryKey.make({ namespace: first, id: "shared-source" });

            yield* writer.change(
              MemoryWrite.make({
                _tag: "Withdraw",
                key: firstKey,
                operationId: "withdraw",
                expectedRevision: "1",
                reason: "removed",
              }),
            );
            expect(
              yield* writer
                .change(putFor(firstKey, "later", "2", "resurrection"))
                .pipe(Effect.flip),
            ).toMatchObject({ _tag: "MemoryWithdrawn" });
            for (const namespace of namespaces.slice(1))
              expect(
                (yield* reader.get(MemoryKey.make({ namespace, id: "shared-source" })))?._tag,
              ).toBe("ActiveMemoryDocument");
          }).pipe(Effect.provide(storeLayer(filename)));

          const tombstone = yield* Effect.flatMap(MemoryReader, (reader) =>
            reader.get(MemoryKey.make({ namespace: first, id: "shared-source" })),
          ).pipe(Effect.provide(readerLayer(filename)));

          expect(tombstone?._tag).toBe("WithdrawnMemoryDocument");
        }),
      ),
  );

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

            yield* sql`UPDATE effect_agent_memory_metadata SET version = 999`;
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
        const firstPut = put("put-1", null, "prefers tea", [MemoryScope.make("profile")]);

        const correction = put("put-2", "1", "prefers coffee", [
          MemoryScope.make("private"),
          MemoryScope.make("profile"),
        ]);

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
            .change(put("put-1", null, "different command", [MemoryScope.make("profile")]))
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
        expect(yield* checkUsage(filename)).toMatchObject({ documents: 1, receipts: 2 });
      }),
    ),
  );

  it.effect("isolates identical source and operation IDs by namespace", () =>
    withTemporaryDatabase((filename) =>
      Effect.gen(function* () {
        const otherKey = MemoryKey.make({ namespace: TestNamespace.make("tenant-b"), id: key.id });
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
          expect(yield* checkUsage(filename)).toEqual({ documents: 0, receipts: 0, bytes: 0 });

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
          expect(yield* checkUsage(filename)).toMatchObject({ documents: 1, receipts: 1 });

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

        // Upgrade the persisted beta.47 layout while the caller still lacks its acknowledgement.
        yield* legacyLayout(filename);

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

  for (const point of [
    "memory:initialize:before",
    "memory:initialize:before-accounting",
    "memory:initialize:after-accounting",
    "memory:initialize:after",
  ] as const) {
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
            "effect_agent_memory_usage_v1",
          ]);
        }),
      ),
    );
  }

  for (const mode of ["defect", "timeout", "interruption"] as const) {
    it.effect(`rolls back accounting migration on ${mode}`, () =>
      withTemporaryDatabase((filename) =>
        Effect.gen(function* () {
          yield* Effect.flatMap(MemoryWriter, (writer) =>
            writer.change(put("retained", null, "text")),
          ).pipe(Effect.provide(storeLayer(filename)));
          yield* legacyLayout(filename);
          const reached = yield* Deferred.make<void>();

          const opening = Effect.void.pipe(
            Effect.provide(
              failpointLayer(filename, (point) =>
                point === "memory:initialize:after-accounting"
                  ? mode === "defect"
                    ? Effect.die("injected")
                    : Deferred.succeed(reached, undefined).pipe(Effect.andThen(Effect.never))
                  : Effect.void,
              ),
            ),
          );

          if (mode === "defect") {
            const result = yield* Effect.exit(opening);

            expect(Exit.isFailure(result) && Cause.hasDies(result.cause)).toBe(true);
          } else {
            const fiber = yield* (
              mode === "timeout" ? opening.pipe(Effect.timeout("1 second")) : opening
            ).pipe(Effect.forkChild);

            yield* Deferred.await(reached);
            if (mode === "timeout") yield* TestClock.adjust("1 second");
            else yield* Fiber.interrupt(fiber);
            expect(Exit.isFailure(yield* Fiber.await(fiber))).toBe(true);
          }

          const partial = yield* runRaw(
            filename,
            Effect.gen(function* () {
              const sql = yield* SqlClientService.SqlClient;

              return yield* sql`SELECT name FROM sqlite_master WHERE name = 'effect_agent_memory_usage_v1'`;
            }),
          );

          expect(partial).toEqual([]);
          yield* Effect.void.pipe(Effect.provide(storeLayer(filename)));
          expect(yield* checkUsage(filename)).toMatchObject({ documents: 1, receipts: 1 });
          expect(yield* readCurrent(filename)).toMatchObject({ generation: 1 });
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
        expect(yield* checkUsage(filename)).toEqual({ documents: 0, receipts: 0, bytes: 0 });

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
        expect(yield* checkUsage(timeoutFile)).toEqual({ documents: 0, receipts: 0, bytes: 0 });

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
        expect(yield* checkUsage(interruptedFile)).toEqual({ documents: 0, receipts: 0, bytes: 0 });
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
              SET document_json = '{"version":999}'
              WHERE namespace = ${key.namespace.address} AND source_id = ${key.id}
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
              SET result_json = '{"version":2,"value":null}'
              WHERE namespace = ${key.namespace.address} AND operation_id = ${command.operationId}
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
        const created = put("forged-created", null, "created", [MemoryScope.make("created-scope")]);

        const corrected = put("forged-corrected", "1", "corrected", [
          MemoryScope.make("corrected-scope"),
        ]);

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
              WHERE namespace = ${key.namespace.address}
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
                WHERE namespace = ${key.namespace.address} AND operation_id = ${created.operationId}
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
              WHERE namespace = ${key.namespace.address} AND operation_id = ${corrected.operationId}
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
              WHERE namespace = ${key.namespace.address} AND operation_id = ${withdrawn.operationId}
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
              WHERE namespace = ${key.namespace.address} AND operation_id = ${withdrawn.operationId}
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
