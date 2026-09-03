import { MemoryStorageError, MemoryMutationFailpoint } from "@effect-agent/core/MemoryStore";
import {
  memoryStoreLayerWithFailpoints,
  SqlMemoryLimits,
} from "@effect-agent/thread/SqlMemoryStore";
import { SqliteClient } from "@effect/sql-sqlite-do";
import { Effect, Layer, Schema } from "effect";

export class DoMemoryStorageLimits extends Schema.Class<DoMemoryStorageLimits>(
  "@effect-agent/storage-cloudflare/DoMemoryStorageLimits",
)({
  maxRowBytes: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 1_900_000 })),
  maxStorageBytes: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 536_870_912 })),
  maxDocuments: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 100_000 })),
  maxReceipts: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 1_000_000 })),
}) {}

export const defaultDoMemoryStorageLimits = DoMemoryStorageLimits.make({
  maxRowBytes: 1_900_000,
  maxStorageBytes: 536_870_912,
  maxDocuments: 10_000,
  maxReceipts: 100_000,
});

/**
 * Local memory only, without Thread tables. Keep one SQL client per owner and pass the
 * full storage handle: sql-only handles cannot provide atomic receipts and revisions.
 * Byte limits conservatively count encoded rows, not SQLite page/index overhead.
 */
export const doMemoryStoreLayerWithFailpoints = (
  storage: NonNullable<SqliteClient.SqliteClientConfig["storage"]>,
  limits: DoMemoryStorageLimits = defaultDoMemoryStorageLimits,
) =>
  Layer.unwrap(
    Schema.decodeUnknownEffect(DoMemoryStorageLimits)(limits).pipe(
      Effect.mapError(() =>
        MemoryStorageError.make({ operation: "memory storage limits", reason: "invalid-input" }),
      ),
      Effect.map((validated) =>
        memoryStoreLayerWithFailpoints.pipe(
          Layer.provide(Layer.succeed(SqlMemoryLimits, validated)),
          Layer.provide(SqliteClient.layer({ storage })),
        ),
      ),
    ),
  );

export const doMemoryStoreLayer = (
  storage: NonNullable<SqliteClient.SqliteClientConfig["storage"]>,
  limits: DoMemoryStorageLimits = defaultDoMemoryStorageLimits,
) =>
  doMemoryStoreLayerWithFailpoints(storage, limits).pipe(
    Layer.provide(MemoryMutationFailpoint.layer),
  );
