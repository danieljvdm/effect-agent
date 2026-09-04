import { MemoryNamespaceAddress } from "@effect-agent/core/MemoryNamespace";
import {
  applyMemoryWrite,
  MemoryDocument,
  MemoryKey,
  MemoryMutationFailpoint,
  type MemoryMutationFailure,
  MemoryOperationConflict,
  MemoryReader,
  MemoryStorageError,
  MemoryWrite,
  MemoryWriter,
} from "@effect-agent/core/MemoryStore";
import { Clock, Context, Effect, Layer, Schema } from "effect";
import * as SqlClientService from "effect/unstable/sql/SqlClient";
import type { SqlError } from "effect/unstable/sql/SqlError";

import { utf8ByteLength } from "./internal/utf8.ts";

const STORAGE_VERSION = 2 as const;
const METADATA_COMPONENT = "memory";
const DOCUMENT_TABLE = "effect_agent_memory_documents_v1";
const RECEIPT_TABLE = "effect_agent_memory_receipts_v1";
const USAGE_COMPONENT = "memory-usage";
const USAGE_TABLE = "effect_agent_memory_usage_v1";
const MAX_STORED_JSON_CODE_UNITS = 16 * 1024 * 1024;
const StoredJson = Schema.String.check(Schema.isMaxLength(MAX_STORED_JSON_CODE_UNITS));

const EncodedMemoryChange = Schema.Struct({
  commandJson: StoredJson,
  documentJson: StoredJson,
  resultJson: StoredJson,
});

const equivalentContent = Schema.toEquivalence(MemoryWrite.Wire.members[0].fields.content);
const equivalentScopes = Schema.toEquivalence(MemoryWrite.Wire.members[0].fields.scopes);

/**
 * Independent adapter limits. Counts and encoded bytes include durable operation receipts.
 * Bytes count UTF-8 row payloads plus 128 bytes per row, excluding SQLite pages/indexes.
 * Replacements charge their byte delta; receipts and tombstones are never pruned.
 * Optional reserves (default zero) withhold capacity from Put, within the hard totals.
 * They require exclusive upgraded writer ownership: older writers count toward usage
 * but do not honor reserves. Limits are captured when the writer Layer is built.
 */
export const SqlMemoryLimits = Context.Reference<{
  readonly maxRowBytes: number;
  readonly maxStorageBytes: number;
  readonly maxDocuments: number;
  readonly maxReceipts: number;
  readonly reservedWithdrawalBytes?: number;
  readonly reservedWithdrawalReceipts?: number;
}>("@effect-agent/thread/SqlMemoryLimits", {
  defaultValue: () => ({
    maxRowBytes: Number.MAX_SAFE_INTEGER,
    maxStorageBytes: Number.MAX_SAFE_INTEGER,
    maxDocuments: Number.MAX_SAFE_INTEGER,
    maxReceipts: Number.MAX_SAFE_INTEGER,
  }),
});

const Limits = Schema.Struct({
  maxRowBytes: Schema.Natural,
  maxStorageBytes: Schema.Natural,
  maxDocuments: Schema.Natural,
  maxReceipts: Schema.Natural,
  reservedWithdrawalBytes: Schema.optional(Schema.Natural),
  reservedWithdrawalReceipts: Schema.optional(Schema.Natural),
}).check(
  Schema.makeFilter(
    (limits) =>
      (limits.reservedWithdrawalBytes ?? 0) <= limits.maxStorageBytes &&
      (limits.reservedWithdrawalReceipts ?? 0) <= limits.maxReceipts,
    { expected: "withdrawal reserves within hard storage limits" },
  ),
);

const UsageRow = Schema.Struct({
  documents: Schema.Natural,
  receipts: Schema.Natural,
  bytes: Schema.Natural,
});

// These expressions deliberately match the legacy aggregate's logical byte accounting.
const documentBytesSql = (row: string) =>
  `length(CAST(${row}.namespace AS BLOB)) + length(CAST(${row}.source_id AS BLOB)) + length(CAST(${row}.document_json AS BLOB)) + 128`;

const receiptBytesSql = (row: string) =>
  `length(CAST(${row}.namespace AS BLOB)) + length(CAST(${row}.source_id AS BLOB)) + length(CAST(${row}.operation_id AS BLOB)) + length(CAST(${row}.command_json AS BLOB)) + length(CAST(${row}.result_json AS BLOB)) + 128`;

// Triggers keep already-open version-2 writers accounted for. Their admission policy
// is still the old policy; deployment must drain them before relying on reserves.
const usageTriggers = [
  { table: DOCUMENT_TABLE, count: "documents", bytes: documentBytesSql },
  { table: RECEIPT_TABLE, count: "receipts", bytes: receiptBytesSql },
].flatMap(({ table, count, bytes }) =>
  ["INSERT", "UPDATE", "DELETE"].map((event) => ({
    name: `${table}_usage_${event.toLowerCase()}`,
    sql: `CREATE TRIGGER ${table}_usage_${event.toLowerCase()} AFTER ${event} ON ${table}
      BEGIN
        UPDATE ${USAGE_TABLE} SET
          ${count} = ${count} + ${event === "INSERT" ? 1 : event === "DELETE" ? -1 : 0},
          bytes = bytes + (${event === "DELETE" ? "0" : bytes("NEW")}) - (${event === "INSERT" ? "0" : bytes("OLD")})
        WHERE singleton = 1;
        SELECT CASE WHEN changes() <> 1 THEN RAISE(ABORT, 'missing memory usage') END;
      END`,
  })),
);

class MemoryMetadataRow extends Schema.Class<MemoryMetadataRow>(
  "@effect-agent/storage-sqlite/MemoryMetadataRow",
)({
  version: Schema.Int,
}) {}

class MemoryTableRow extends Schema.Class<MemoryTableRow>(
  "@effect-agent/storage-sqlite/MemoryTableRow",
)({
  name: Schema.NonEmptyString,
}) {}

class MemoryDocumentRow extends Schema.Class<MemoryDocumentRow>(
  "@effect-agent/storage-sqlite/MemoryDocumentRow",
)({
  namespace: MemoryNamespaceAddress,
  source_id: MemoryKey.Wire.fields.id,
  format_version: Schema.Int,
  generation: Schema.Int,
  revision: Schema.NonEmptyString,
  document_json: StoredJson,
  stored_bytes: Schema.Natural,
}) {}

class MemoryReceiptRow extends Schema.Class<MemoryReceiptRow>(
  "@effect-agent/storage-sqlite/MemoryReceiptRow",
)({
  namespace: MemoryNamespaceAddress,
  operation_id: MemoryWrite.Wire.members[0].fields.operationId,
  source_id: MemoryKey.Wire.fields.id,
  format_version: Schema.Int,
  command_json: StoredJson,
  result_json: StoredJson,
}) {}

class MemoryChangeCountRow extends Schema.Class<MemoryChangeCountRow>(
  "@effect-agent/storage-sqlite/MemoryChangeCountRow",
)({
  changed: Schema.Int,
}) {}

class StoredMemoryCommand extends Schema.Class<StoredMemoryCommand>(
  "@effect-agent/storage-sqlite/StoredMemoryCommand",
)({
  version: Schema.Literal(STORAGE_VERSION),
  value: MemoryWrite.Wire,
}) {}

class StoredMemoryResult extends Schema.Class<StoredMemoryResult>(
  "@effect-agent/storage-sqlite/StoredMemoryResult",
)({
  version: Schema.Literal(STORAGE_VERSION),
  value: MemoryDocument.Wire,
}) {}

const StoredVersionHeader = Schema.Struct({ version: Schema.Int });

export type SqliteMemoryInitializationError = MemoryStorageError | MemoryMutationFailure;

const storageError = (
  operation: string,
  reason: MemoryStorageError["reason"] = "unavailable",
): MemoryStorageError => MemoryStorageError.make({ operation, reason });

const query = <A extends object>(
  effect: Effect.Effect<ReadonlyArray<A>, SqlError>,
  operation: string,
) => effect.pipe(Effect.mapError(() => storageError(operation)));

const decodeRows = Effect.fn("SqliteMemoryStore.decodeRows")(function* <A, I>(
  schema: Schema.Codec<A, I, never>,
  rows: ReadonlyArray<unknown>,
  operation: string,
): Effect.fn.Return<ReadonlyArray<A>, MemoryStorageError> {
  return yield* Schema.decodeUnknownEffect(Schema.Array(schema))(rows).pipe(
    Effect.mapError(() => storageError(operation, "corrupt")),
  );
});

const decodeInput = Effect.fn("SqliteMemoryStore.decodeInput")(function* <A, I>(
  schema: Schema.Codec<A, I, never>,
  value: unknown,
  operation: string,
): Effect.fn.Return<A, MemoryStorageError> {
  return yield* Schema.decodeUnknownEffect(schema)(value).pipe(
    Effect.mapError(() => storageError(operation, "invalid-input")),
  );
});

const encodeJson = Effect.fn("SqliteMemoryStore.encodeJson")(function* <A, I>(
  schema: Schema.Codec<A, I, never>,
  value: A,
  operation: string,
): Effect.fn.Return<string, MemoryStorageError> {
  return yield* Schema.encodeEffect(Schema.fromJsonString(schema))(value).pipe(
    Effect.mapError(() => storageError(operation, "corrupt")),
  );
});

const validateEncodedChange = Effect.fn("SqliteMemoryStore.validateEncodedChange")(function* (
  encoded: typeof EncodedMemoryChange.Type,
  operation: string,
): Effect.fn.Return<void, MemoryStorageError> {
  yield* Schema.decodeEffect(EncodedMemoryChange)(encoded).pipe(
    Effect.mapError(() => storageError(operation, "invalid-input")),
  );
});

const decodeVersionedJson = Effect.fn("SqliteMemoryStore.decodeVersionedJson")(function* <A, I>(
  schema: Schema.Codec<A, I, never>,
  value: string,
  operation: string,
): Effect.fn.Return<A, MemoryStorageError> {
  const header = yield* Schema.decodeEffect(Schema.fromJsonString(StoredVersionHeader))(value).pipe(
    Effect.mapError(() => storageError(operation, "corrupt")),
  );

  if (header.version !== STORAGE_VERSION) {
    return yield* storageError(operation, "incompatible");
  }

  const decoded = yield* Schema.decodeEffect(Schema.fromJsonString(schema))(value).pipe(
    Effect.mapError(() => storageError(operation, "corrupt")),
  );

  const canonical = yield* encodeJson(schema, decoded, operation);

  if (canonical !== value) return yield* storageError(operation, "corrupt");

  return decoded;
});

const validateDocument = Effect.fn("SqliteMemoryStore.validateDocument")(function* (
  document: MemoryDocument,
  key: MemoryKey,
  operation: string,
): Effect.fn.Return<MemoryDocument, MemoryStorageError> {
  if (
    document.key.namespace.address !== key.namespace.address ||
    document.key.id !== key.id ||
    document.source.id !== key.id ||
    document.source.revision !== String(document.generation) ||
    (document.generation === 1) !== (document.predecessor === null) ||
    (document.predecessor !== null &&
      (document.predecessor.id !== key.id ||
        document.predecessor.revision !== String(document.generation - 1)))
  ) {
    return yield* storageError(operation, "corrupt");
  }

  return document;
});

const validateReceiptResult = Effect.fn("SqliteMemoryStore.validateReceiptResult")(function* (
  command: MemoryWrite,
  result: MemoryDocument,
  operation: string,
): Effect.fn.Return<void, MemoryStorageError> {
  if ((result.predecessor?.revision ?? null) !== command.expectedRevision) {
    return yield* storageError(operation, "corrupt");
  }
  if (command._tag === "Put") {
    if (result._tag !== "ActiveMemoryDocument" || result.source.locator !== command.locator) {
      return yield* storageError(operation, "corrupt");
    }
    if (
      !equivalentContent(command.content, result.content) ||
      !equivalentScopes(command.scopes, result.scopes)
    ) {
      return yield* storageError(operation, "corrupt");
    }

    return;
  }
  if (
    result._tag !== "WithdrawnMemoryDocument" ||
    result.reason !== command.reason ||
    result.predecessor === null ||
    result.source.locator !== result.predecessor.locator
  ) {
    return yield* storageError(operation, "corrupt");
  }
});

const readUsage = Effect.fn("SqliteMemoryStore.readUsage")(function* () {
  const sql = yield* SqlClientService.SqlClient;
  const operation = "read memory usage";

  const rows = yield* query(
    sql<Record<string, unknown>>`
      SELECT documents, receipts, bytes FROM effect_agent_memory_usage_v1 WHERE singleton = 1
    `,
    operation,
  ).pipe(Effect.flatMap((rows) => decodeRows(UsageRow, rows, operation)));

  if (rows.length !== 1) return yield* storageError(operation, "corrupt");

  return rows[0];
});

// Called inside schema initialization's transaction. The separate marker distinguishes
// a legacy store from damaged established accounting; reopening never rebuilds counters.
const initializeMemoryUsage = Effect.fn("SqliteMemoryStore.initializeUsage")(function* () {
  const sql = yield* SqlClientService.SqlClient;
  const failpoint = yield* MemoryMutationFailpoint;
  const operation = "initialize memory usage";

  const metadata = yield* sql<Record<string, unknown>>`
    SELECT version FROM effect_agent_memory_metadata WHERE component = ${USAGE_COMPONENT}
  `.pipe(Effect.flatMap((rows) => decodeRows(MemoryMetadataRow, rows, operation)));

  const objects = yield* sql<Record<string, unknown>>`
    SELECT name FROM sqlite_master
    WHERE name = ${USAGE_TABLE} OR name IN ${sql.in(usageTriggers.map((trigger) => trigger.name))}
  `.pipe(Effect.flatMap((rows) => decodeRows(MemoryTableRow, rows, operation)));

  if (metadata.length === 0) {
    if (objects.length !== 0) return yield* storageError(operation, "corrupt");
    yield* failpoint.hit("memory:initialize:before-accounting");
    yield* sql`
      CREATE TABLE effect_agent_memory_usage_v1 (
        singleton INTEGER PRIMARY KEY NOT NULL CHECK (singleton = 1),
        documents INTEGER NOT NULL CHECK (typeof(documents) = 'integer' AND documents BETWEEN 0 AND 9007199254740991),
        receipts INTEGER NOT NULL CHECK (typeof(receipts) = 'integer' AND receipts BETWEEN 0 AND 9007199254740991),
        bytes INTEGER NOT NULL CHECK (typeof(bytes) = 'integer' AND bytes BETWEEN 0 AND 9007199254740991)
      )
    `;
    yield* sql`
      INSERT INTO effect_agent_memory_usage_v1 (singleton, documents, receipts, bytes)
      SELECT 1,
        (SELECT COUNT(*) FROM effect_agent_memory_documents_v1),
        (SELECT COUNT(*) FROM effect_agent_memory_receipts_v1),
        COALESCE((SELECT SUM(length(CAST(namespace AS BLOB)) + length(CAST(source_id AS BLOB)) +
          length(CAST(document_json AS BLOB)) + 128) FROM effect_agent_memory_documents_v1), 0) +
        COALESCE((SELECT SUM(length(CAST(namespace AS BLOB)) + length(CAST(source_id AS BLOB)) +
          length(CAST(operation_id AS BLOB)) + length(CAST(command_json AS BLOB)) +
          length(CAST(result_json AS BLOB)) + 128) FROM effect_agent_memory_receipts_v1), 0)
    `;
    for (const trigger of usageTriggers) yield* sql.unsafe(trigger.sql);
    yield* sql`
      INSERT INTO effect_agent_memory_metadata (component, version) VALUES (${USAGE_COMPONENT}, 1)
    `;
    yield* failpoint.hit("memory:initialize:after-accounting");
  } else {
    if (metadata.length !== 1 || metadata[0].version !== 1) {
      return yield* storageError(operation, "incompatible");
    }
    if (objects.length !== usageTriggers.length + 1) {
      return yield* storageError(operation, "corrupt");
    }
  }
  yield* readUsage();
});

const initializeMemorySchema = Effect.fn("SqliteMemoryStore.initialize")(function* () {
  const sql = yield* SqlClientService.SqlClient;
  const failpoint = yield* MemoryMutationFailpoint;

  yield* failpoint.hit("memory:initialize:before");
  yield* sql
    .withTransaction(
      Effect.gen(function* () {
        yield* sql`
          CREATE TABLE IF NOT EXISTS effect_agent_memory_metadata (
            component TEXT PRIMARY KEY NOT NULL,
            version INTEGER NOT NULL
          )
        `;

        const metadataRows = yield* sql<Record<string, unknown>>`
          SELECT version
          FROM effect_agent_memory_metadata
          WHERE component = ${METADATA_COMPONENT}
        `;

        const metadata = yield* decodeRows(
          MemoryMetadataRow,
          metadataRows,
          "decode memory schema version",
        );

        if (metadata.length > 1)
          return yield* storageError("decode memory schema version", "corrupt");
        const currentVersion = metadata[0]?.version;

        if (currentVersion !== undefined && currentVersion !== STORAGE_VERSION) {
          return yield* storageError("initialize memory schema", "incompatible");
        }
        if (currentVersion === undefined) {
          const tableRows = yield* sql<Record<string, unknown>>`
            SELECT name
            FROM sqlite_master
            WHERE type = 'table' AND name IN (${DOCUMENT_TABLE}, ${RECEIPT_TABLE})
          `;

          const existingTables = yield* decodeRows(
            MemoryTableRow,
            tableRows,
            "inspect memory schema",
          );

          if (existingTables.length > 0) {
            return yield* storageError("initialize memory schema", "incompatible");
          }
          yield* sql`
            CREATE TABLE effect_agent_memory_documents_v1 (
              namespace TEXT NOT NULL,
              source_id TEXT NOT NULL,
              format_version INTEGER NOT NULL,
              generation INTEGER NOT NULL,
              revision TEXT NOT NULL,
              document_json TEXT NOT NULL,
              PRIMARY KEY (namespace, source_id)
            )
          `;
          yield* sql`
            CREATE TABLE effect_agent_memory_receipts_v1 (
              namespace TEXT NOT NULL,
              operation_id TEXT NOT NULL,
              source_id TEXT NOT NULL,
              format_version INTEGER NOT NULL,
              command_json TEXT NOT NULL,
              result_json TEXT NOT NULL,
              PRIMARY KEY (namespace, operation_id)
            )
          `;
          yield* sql`
            INSERT INTO effect_agent_memory_metadata (component, version)
            VALUES (${METADATA_COMPONENT}, ${STORAGE_VERSION})
          `;
        }
        yield* sql`
          SELECT namespace, source_id, format_version, generation, revision, document_json
          FROM effect_agent_memory_documents_v1
          LIMIT 0
        `;
        yield* sql`
          SELECT namespace, operation_id, source_id, format_version, command_json, result_json
          FROM effect_agent_memory_receipts_v1
          LIMIT 0
        `;
        yield* initializeMemoryUsage();
      }),
    )
    .pipe(Effect.catchTag("SqlError", () => Effect.fail(storageError("initialize memory schema"))));
  yield* failpoint.hit("memory:initialize:after");
});

const makeMemoryReader = Effect.fn("SqliteMemoryStore.makeReader")(function* () {
  const sql = yield* SqlClientService.SqlClient;

  const readDocument = Effect.fn("SqliteMemoryStore.readDocument")(function* (
    key: MemoryKey,
    operation: string,
  ): Effect.fn.Return<
    { readonly document: MemoryDocument; readonly bytes: number } | null,
    MemoryStorageError
  > {
    const rawRows = yield* query(
      sql<Record<string, unknown>>`
        SELECT namespace, source_id, format_version, generation, revision, document_json,
          length(CAST(namespace AS BLOB)) + length(CAST(source_id AS BLOB)) +
          length(CAST(document_json AS BLOB)) + 128 AS stored_bytes
        FROM effect_agent_memory_documents_v1
        WHERE namespace = ${key.namespace.address} AND source_id = ${key.id}
      `,
      operation,
    );

    const rows = yield* decodeRows(MemoryDocumentRow, rawRows, operation);

    if (rows.length === 0) return null;
    if (rows.length !== 1) return yield* storageError(operation, "corrupt");
    const row = rows[0];

    if (row.format_version !== STORAGE_VERSION) {
      return yield* storageError(operation, "incompatible");
    }
    const stored = yield* decodeVersionedJson(StoredMemoryResult, row.document_json, operation);
    const document = stored.value;

    yield* validateDocument(document, key, operation);
    if (
      row.namespace !== key.namespace.address ||
      row.source_id !== key.id ||
      row.generation !== document.generation ||
      row.revision !== document.source.revision
    ) {
      return yield* storageError(operation, "corrupt");
    }

    return { document, bytes: row.stored_bytes };
  });

  const get = Effect.fn("SqliteMemoryStore.get")(function* (key: MemoryKey) {
    const decodedKey = yield* decodeInput(MemoryKey.Wire, key, "get memory document");

    return (yield* readDocument(decodedKey, "get memory document"))?.document ?? null;
  });

  return { get, readDocument };
});

const makeMemoryServices = Effect.fn("SqliteMemoryStore.make")(function* () {
  const sql = yield* SqlClientService.SqlClient;
  const usage = readUsage().pipe(Effect.provideService(SqlClientService.SqlClient, sql));
  const failpoint = yield* MemoryMutationFailpoint;
  const limits = yield* decodeInput(Limits, yield* SqlMemoryLimits, "memory storage limits");

  yield* initializeMemorySchema();
  const { get, readDocument } = yield* makeMemoryReader();

  const readReceipt = Effect.fn("SqliteMemoryStore.readReceipt")(function* (
    write: MemoryWrite,
    operation: string,
  ) {
    const rawRows = yield* query(
      sql<Record<string, unknown>>`
        SELECT namespace, operation_id, source_id, format_version, command_json, result_json
        FROM effect_agent_memory_receipts_v1
        WHERE namespace = ${write.key.namespace.address} AND operation_id = ${write.operationId}
      `,
      operation,
    );

    const rows = yield* decodeRows(MemoryReceiptRow, rawRows, operation);

    if (rows.length === 0) return null;
    if (rows.length !== 1) return yield* storageError(operation, "corrupt");
    const row = rows[0];

    if (row.format_version !== STORAGE_VERSION) {
      return yield* storageError(operation, "incompatible");
    }

    const command = yield* decodeVersionedJson(
      StoredMemoryCommand,
      row.command_json,
      `${operation} command`,
    );

    const result = yield* decodeVersionedJson(
      StoredMemoryResult,
      row.result_json,
      `${operation} result`,
    );

    if (
      row.namespace !== command.value.key.namespace.address ||
      row.operation_id !== command.value.operationId ||
      row.source_id !== command.value.key.id ||
      result.value.key.namespace.address !== command.value.key.namespace.address ||
      result.value.key.id !== command.value.key.id
    ) {
      return yield* storageError(operation, "corrupt");
    }
    yield* validateDocument(result.value, command.value.key, operation);
    yield* validateReceiptResult(command.value, result.value, operation);

    return { commandJson: row.command_json, result: result.value };
  });

  const change = Effect.fn("SqliteMemoryStore.change")(function* (write: MemoryWrite) {
    const operation = "change memory document";
    const decodedWrite = yield* decodeInput(MemoryWrite.Wire, write, operation);

    const commandJson = yield* encodeJson(
      StoredMemoryCommand,
      StoredMemoryCommand.make({ version: STORAGE_VERSION, value: decodedWrite }),
      "encode memory command",
    );

    yield* failpoint.hit("memory:change:before");

    // The adapter must serialize before reading receipts and state. Node uses BEGIN
    // IMMEDIATE; Durable Objects use the driver's storage-backed transaction and permit.
    const transactionResult = yield* sql
      .withTransaction(
        Effect.gen(function* () {
          const receipt = yield* readReceipt(decodedWrite, operation);

          if (receipt !== null) {
            if (receipt.commandJson !== commandJson) {
              return yield* MemoryOperationConflict.make({
                key: decodedWrite.key,
                operationId: decodedWrite.operationId,
              });
            }

            return { document: receipt.result, changed: false } as const;
          }

          const currentRow = yield* readDocument(decodedWrite.key, operation);
          const current = currentRow?.document ?? null;
          const modifiedAt = yield* Clock.currentTimeMillis;
          const next = yield* applyMemoryWrite(current, decodedWrite, modifiedAt);

          const resultJson = yield* encodeJson(
            StoredMemoryResult,
            StoredMemoryResult.make({ version: STORAGE_VERSION, value: next }),
            "encode memory result",
          );

          const documentJson = resultJson;

          yield* validateEncodedChange({ commandJson, documentJson, resultJson }, operation);

          const bytes = utf8ByteLength;
          const identityBytes = bytes(next.key.namespace.address) + bytes(next.key.id);
          const documentBytes = identityBytes + bytes(documentJson) + 128;

          const receiptBytes =
            identityBytes +
            bytes(decodedWrite.operationId) +
            bytes(commandJson) +
            bytes(resultJson) +
            128;

          if (Math.max(documentBytes, receiptBytes) > limits.maxRowBytes) {
            return yield* storageError("memory row byte limit", "invalid-input");
          }

          const used = yield* usage;

          if (used.bytes < (currentRow?.bytes ?? 0) || (current !== null && used.documents === 0)) {
            return yield* storageError("read memory usage", "corrupt");
          }

          const maxReceipts =
            limits.maxReceipts -
            (decodedWrite._tag === "Put" ? (limits.reservedWithdrawalReceipts ?? 0) : 0);

          const maxStorageBytes =
            limits.maxStorageBytes -
            (decodedWrite._tag === "Put" ? (limits.reservedWithdrawalBytes ?? 0) : 0);

          // Subtract the persisted old row before adding its replacement. Subtraction
          // comparisons avoid overflowing safe integer budgets near unbounded defaults.
          if (
            used.documents > limits.maxDocuments - (current === null ? 1 : 0) ||
            used.receipts >= maxReceipts ||
            used.bytes - (currentRow?.bytes ?? 0) > maxStorageBytes - documentBytes - receiptBytes
          ) {
            return yield* storageError("memory storage limit", "invalid-input");
          }

          if (current === null) {
            yield* sql`
                INSERT INTO effect_agent_memory_documents_v1 (
                  namespace, source_id, format_version, generation, revision, document_json
                ) VALUES (
                  ${next.key.namespace.address}, ${next.key.id}, ${STORAGE_VERSION},
                  ${next.generation}, ${next.source.revision}, ${documentJson}
                )
              `;
          } else {
            yield* sql`
                UPDATE effect_agent_memory_documents_v1
                SET format_version = ${STORAGE_VERSION},
                    generation = ${next.generation},
                    revision = ${next.source.revision},
                    document_json = ${documentJson}
                WHERE namespace = ${next.key.namespace.address}
                  AND source_id = ${next.key.id}
                  AND generation = ${current.generation}
                  AND revision = ${current.source.revision}
              `;
          }

          const changedRows = yield* sql<Record<string, unknown>>`
              SELECT changes() AS changed
            `;

          const changed = yield* decodeRows(MemoryChangeCountRow, changedRows, operation);

          if (changed.length !== 1 || changed[0].changed !== 1) {
            return yield* storageError(operation, "corrupt");
          }
          yield* failpoint.hit("memory:change:after-state");
          yield* sql`
              INSERT INTO effect_agent_memory_receipts_v1 (
                namespace, operation_id, source_id, format_version, command_json, result_json
              ) VALUES (
                ${decodedWrite.key.namespace.address}, ${decodedWrite.operationId}, ${decodedWrite.key.id},
                ${STORAGE_VERSION}, ${commandJson}, ${resultJson}
              )
            `;
          yield* failpoint.hit("memory:change:after-receipt");

          return { document: next, changed: true } as const;
        }),
      )
      .pipe(Effect.catchTag("SqlError", () => Effect.fail(storageError(operation))));

    if (transactionResult.changed) yield* failpoint.hit("memory:change:after");

    return transactionResult.document;
  });

  return Context.make(MemoryReader, MemoryReader.fromAdapter({ get })).pipe(
    Context.add(MemoryWriter, MemoryWriter.fromAdapter({ change })),
  );
});

/** SQLite memory ports with the mutation failpoint kept injectable for recovery tests. */
export const memoryStoreLayerWithFailpoints: Layer.Layer<
  MemoryReader | MemoryWriter,
  SqliteMemoryInitializationError,
  SqlClientService.SqlClient | MemoryMutationFailpoint
> = Layer.effectContext(makeMemoryServices());

/** SQLite memory reader and writer with the production no-op mutation failpoint. */
export const memoryStoreLayer: Layer.Layer<
  MemoryReader | MemoryWriter,
  SqliteMemoryInitializationError,
  SqlClientService.SqlClient
> = memoryStoreLayerWithFailpoints.pipe(Layer.provide(MemoryMutationFailpoint.layer));

/** Reads an existing memory schema without writes, transactions, or mutation failpoints. */
export const memoryReaderLayer: Layer.Layer<
  MemoryReader,
  MemoryStorageError,
  SqlClientService.SqlClient
> = Layer.effect(
  MemoryReader,
  Effect.gen(function* () {
    const sql = yield* SqlClientService.SqlClient;
    const operation = "open memory reader";

    const tables = yield* query(
      sql<Record<string, unknown>>`
        SELECT name FROM sqlite_master
        WHERE type = 'table' AND name IN (
          'effect_agent_memory_metadata', ${DOCUMENT_TABLE}, ${RECEIPT_TABLE}
        )
      `,
      operation,
    ).pipe(Effect.flatMap((rows) => decodeRows(MemoryTableRow, rows, operation)));

    if (tables.length !== 3) {
      return yield* storageError(operation, tables.length === 0 ? "unavailable" : "incompatible");
    }

    const metadata = yield* query(
      sql<Record<string, unknown>>`
        SELECT version FROM effect_agent_memory_metadata WHERE component = ${METADATA_COMPONENT}
      `,
      operation,
    ).pipe(Effect.flatMap((rows) => decodeRows(MemoryMetadataRow, rows, operation)));

    if (metadata.length !== 1 || metadata[0].version !== STORAGE_VERSION) {
      return yield* storageError(operation, "incompatible");
    }
    yield* query(
      sql`
        SELECT namespace, source_id, format_version, generation, revision, document_json
        FROM effect_agent_memory_documents_v1 LIMIT 0
      `,
      operation,
    );
    const { get } = yield* makeMemoryReader();

    return MemoryReader.fromAdapter({ get });
  }),
);
