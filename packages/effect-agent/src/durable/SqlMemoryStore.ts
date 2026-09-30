import { Clock, Context, Effect, Layer, Schema } from "effect";
import * as SqlClientService from "effect/unstable/sql/SqlClient";
import type { SqlError } from "effect/unstable/sql/SqlError";

import { utf8ByteLength } from "../core/internal/utf8.ts";
import { type Any as MemoryNamespace, MemoryNamespaceAddress } from "../core/MemoryNamespace.ts";
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
  type MemoryWriteError,
  MemoryWriter,
} from "../core/MemoryStore.ts";

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

const equivalentContent = Schema.toEquivalence(
  Schema.toEncoded(MemoryWrite.Wire.members[0].fields.content),
);

const equivalentScopes = Schema.toEquivalence(MemoryWrite.Wire.members[0].fields.scopes);

/** Ordered commands committed atomically by the SQL memory adapter. */
export const SqlMemoryWriteBatch = Schema.Array(MemoryWrite.Wire).check(Schema.isMaxLength(128));

/**
 * SQL-only bulk writing. Results preserve input order, including exact operation replays.
 * Commands see earlier staged revisions; any refusal rolls back the entire batch. Storage
 * limits apply to every intermediate transition. Empty batches succeed without mutation.
 * Supplied by the memory store Layers alongside MemoryWriter's single-command API.
 */
export class SqlMemoryBatchWriter extends Context.Service<
  SqlMemoryBatchWriter,
  {
    readonly changeMany: <Namespace extends MemoryNamespace>(
      writes: ReadonlyArray<MemoryWrite<Namespace>>,
    ) => Effect.Effect<ReadonlyArray<MemoryDocument<Namespace>>, MemoryWriteError>;
  }
>()("@effect-agent/thread/SqlMemoryBatchWriter") {}

/**
 * An exclusive database owner's transaction gate. All cached services over that database
 * must share this identity, and every failed transaction must run its invalidators before
 * releasing readers. Hosts with independent database writers must leave this unset.
 */
export interface SqlStorageOwner {
  readonly read: <A, E, R>(effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>;
  readonly transaction: <A, E, R>(
    effect: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | SqlError, R | SqlClientService.SqlClient>;
  readonly invalidators: Set<() => void>;
}

export const SqlStorageOwner = Context.Reference<SqlStorageOwner | undefined>(
  "@effect-agent/thread/SqlStorageOwner",
  { defaultValue: () => undefined },
);

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

type DocumentView = { readonly document: MemoryDocument; readonly bytes: number } | null;
type ReceiptView = { readonly commandJson: string; readonly result: MemoryDocument } | null;
type MemoryView =
  | { readonly _tag: "Document"; readonly value: DocumentView }
  | { readonly _tag: "Receipt"; readonly value: ReceiptView }
  | { readonly _tag: "Usage"; readonly value: typeof UsageRow.Type };

// Demand-loaded views are disposable. The shared owner's gate keeps writes invisible
// until commit, and its rollback invalidator discards every speculative view together.
class MemoryViews {
  private readonly entries = new Map<
    string,
    { readonly view: MemoryView; readonly bytes: number }
  >();
  private bytes = 0;

  readonly clear = () => {
    this.entries.clear();
    this.bytes = 0;
  };

  get(key: string): MemoryView | undefined {
    const entry = this.entries.get(key);

    if (entry === undefined) return undefined;
    this.entries.delete(key);
    this.entries.set(key, entry);

    return entry.view;
  }

  set(key: string, view: MemoryView): void {
    const previous = this.entries.get(key);

    if (previous !== undefined) this.bytes -= previous.bytes;
    this.entries.delete(key);
    const bytes = utf8ByteLength(key) + utf8ByteLength(JSON.stringify(view)) + 128;

    if (bytes > 4 * 1024 * 1024) return;
    this.entries.set(key, { view, bytes });
    this.bytes += bytes;
    while (this.entries.size > 128 || this.bytes > 4 * 1024 * 1024) {
      const oldest = this.entries.entries().next().value;

      if (oldest === undefined) break;
      this.bytes -= oldest[1].bytes;
      this.entries.delete(oldest[0]);
    }
  }
}

const ownedViews = new WeakMap<SqlStorageOwner, MemoryViews>();

const memoryViews = Effect.fn("SqliteMemoryStore.memoryViews")(function* () {
  const owner = yield* SqlStorageOwner;

  if (owner === undefined) return undefined;
  let views = ownedViews.get(owner);

  if (views === undefined) {
    views = new MemoryViews();
    ownedViews.set(owner, views);
    owner.invalidators.add(views.clear);
  }

  return views;
});

const documentViewKey = (key: MemoryKey) =>
  JSON.stringify(["document", key.namespace.address, key.id]);

const receiptViewKey = (write: MemoryWrite) =>
  JSON.stringify(["receipt", write.key.namespace.address, write.operationId]);

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
  const views = yield* memoryViews();
  const operation = "read memory usage";

  const rows = yield* query(
    sql<Record<string, unknown>>`
      SELECT documents, receipts, bytes FROM effect_agent_memory_usage_v1 WHERE singleton = 1
    `,
    operation,
  ).pipe(Effect.flatMap((rows) => decodeRows(UsageRow, rows, operation)));

  if (rows.length !== 1) return yield* storageError(operation, "corrupt");

  views?.set("usage", { _tag: "Usage", value: rows[0] });

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
  const owner = yield* SqlStorageOwner;
  const transaction = owner === undefined ? sql.withTransaction : owner.transaction;
  const failpoint = yield* MemoryMutationFailpoint;

  yield* failpoint.hit("memory:initialize:before");
  yield* transaction(
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
  ).pipe(Effect.catchTag("SqlError", () => Effect.fail(storageError("initialize memory schema"))));
  yield* failpoint.hit("memory:initialize:after");
});

const makeMemoryReader = Effect.fn("SqliteMemoryStore.makeReader")(function* () {
  const sql = yield* SqlClientService.SqlClient;
  const owner = yield* SqlStorageOwner;
  const views = yield* memoryViews();

  const readDocuments = Effect.fn("SqliteMemoryStore.readDocuments")(
    function* (
      keys: ReadonlyArray<MemoryKey>,
      operation: string,
    ): Effect.fn.Return<ReadonlyMap<string, DocumentView>, MemoryStorageError> {
      const found = new Map<string, DocumentView>();
      const missing = new Map<string, MemoryKey>();

      for (const key of keys) {
        const viewKey = documentViewKey(key);
        const cached = views?.get(viewKey);

        if (cached?._tag === "Document") found.set(viewKey, cached.value);
        else missing.set(viewKey, key);
      }
      const pending = Array.from(missing);

      // Each exact composite key uses two bindings. No history or unrelated rows are loaded.
      for (let index = 0; index < pending.length; index += 50) {
        const requested = new Map(pending.slice(index, index + 50));

        const rawRows = yield* query(
          sql<Record<string, unknown>>`
            SELECT namespace, source_id, format_version, generation, revision, document_json,
              length(CAST(namespace AS BLOB)) + length(CAST(source_id AS BLOB)) +
              length(CAST(document_json AS BLOB)) + 128 AS stored_bytes
            FROM effect_agent_memory_documents_v1
            WHERE ${sql.or(
              Array.from(
                requested.values(),
                (key) => sql`(namespace = ${key.namespace.address} AND source_id = ${key.id})`,
              ),
            )}
          `,
          operation,
        );

        const rows = yield* decodeRows(MemoryDocumentRow, rawRows, operation);

        for (const row of rows) {
          const viewKey = JSON.stringify(["document", row.namespace, row.source_id]);
          const key = requested.get(viewKey);

          if (key === undefined || found.has(viewKey))
            return yield* storageError(operation, "corrupt");
          if (row.format_version !== STORAGE_VERSION)
            return yield* storageError(operation, "incompatible");

          const stored = yield* decodeVersionedJson(
            StoredMemoryResult,
            row.document_json,
            operation,
          );

          const document = stored.value;

          yield* validateDocument(document, key, operation);
          if (row.generation !== document.generation || row.revision !== document.source.revision)
            return yield* storageError(operation, "corrupt");
          found.set(viewKey, { document, bytes: row.stored_bytes });
        }

        for (const viewKey of requested.keys()) {
          const value = found.get(viewKey) ?? null;

          found.set(viewKey, value);
          views?.set(viewKey, { _tag: "Document", value });
        }
      }

      return found;
    },
    (effect) => (owner === undefined ? effect : owner.read(effect)),
  );

  const get = Effect.fn("SqliteMemoryStore.get")(function* (key: MemoryKey) {
    const decodedKey = yield* decodeInput(MemoryKey.Wire, key, "get memory document");

    const documents = yield* readDocuments([decodedKey], "get memory document");

    return documents.get(documentViewKey(decodedKey))?.document ?? null;
  });

  return { get, readDocuments };
});

const makeMemoryServices = Effect.fn("SqliteMemoryStore.make")(function* () {
  const sql = yield* SqlClientService.SqlClient;
  const owner = yield* SqlStorageOwner;
  const views = yield* memoryViews();

  const usage = readUsage().pipe(
    Effect.provideService(SqlClientService.SqlClient, sql),
    Effect.provideService(SqlStorageOwner, owner),
  );

  const transaction = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    owner === undefined
      ? sql.withTransaction(effect)
      : owner.transaction(effect).pipe(Effect.provideService(SqlClientService.SqlClient, sql));

  const failpoint = yield* MemoryMutationFailpoint;
  const limits = yield* decodeInput(Limits, yield* SqlMemoryLimits, "memory storage limits");

  yield* initializeMemorySchema();
  const { get, readDocuments } = yield* makeMemoryReader();

  const readReceipts = Effect.fn("SqliteMemoryStore.readReceipts")(function* (
    writes: ReadonlyArray<MemoryWrite>,
    operation: string,
  ): Effect.fn.Return<ReadonlyMap<string, ReceiptView>, MemoryStorageError> {
    const found = new Map<string, ReceiptView>();
    const missing = new Map<string, MemoryWrite>();

    for (const write of writes) {
      const viewKey = receiptViewKey(write);
      const cached = views?.get(viewKey);

      if (cached?._tag === "Receipt") found.set(viewKey, cached.value);
      else missing.set(viewKey, write);
    }
    const pending = Array.from(missing);

    for (let index = 0; index < pending.length; index += 50) {
      const requested = new Map(pending.slice(index, index + 50));

      const rawRows = yield* query(
        sql<Record<string, unknown>>`
          SELECT namespace, operation_id, source_id, format_version, command_json, result_json
          FROM effect_agent_memory_receipts_v1
          WHERE ${sql.or(
            Array.from(
              requested.values(),
              (write) =>
                sql`(namespace = ${write.key.namespace.address} AND operation_id = ${write.operationId})`,
            ),
          )}
        `,
        operation,
      );

      const rows = yield* decodeRows(MemoryReceiptRow, rawRows, operation);

      for (const row of rows) {
        const viewKey = JSON.stringify(["receipt", row.namespace, row.operation_id]);

        if (!requested.has(viewKey) || found.has(viewKey))
          return yield* storageError(operation, "corrupt");
        if (row.format_version !== STORAGE_VERSION)
          return yield* storageError(operation, "incompatible");

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
        found.set(viewKey, { commandJson: row.command_json, result: result.value });
      }

      for (const viewKey of requested.keys()) {
        const value = found.get(viewKey) ?? null;

        found.set(viewKey, value);
        views?.set(viewKey, { _tag: "Receipt", value });
      }
    }

    return found;
  });

  const changeMany = Effect.fn("SqliteMemoryStore.changeMany")(function* (
    writes: ReadonlyArray<MemoryWrite>,
  ) {
    const operation = "change memory document";
    const decodedWrites = yield* decodeInput(SqlMemoryWriteBatch, writes, operation);

    if (decodedWrites.length === 0) return [];

    const commands = yield* Effect.forEach(decodedWrites, (write) =>
      encodeJson(
        StoredMemoryCommand,
        StoredMemoryCommand.make({ version: STORAGE_VERSION, value: write }),
        "encode memory command",
      ).pipe(Effect.map((commandJson) => ({ write, commandJson }))),
    );

    yield* failpoint.hit("memory:change:before");

    // The adapter must serialize before reading receipts and state. Node uses BEGIN
    // IMMEDIATE; Durable Objects use the driver's storage-backed transaction and permit.
    const transactionResult = yield* transaction(
      Effect.gen(function* () {
        const retainedReceipts = yield* readReceipts(decodedWrites, operation);

        const retainedDocuments = yield* readDocuments(
          decodedWrites
            .filter((write) => retainedReceipts.get(receiptViewKey(write)) === null)
            .map((write) => write.key),
          operation,
        );

        const documents = new Map<
          string,
          {
            readonly document: MemoryDocument;
            readonly bytes: number;
            readonly documentJson: string;
          }
        >();

        const receipts = new Map<
          string,
          {
            readonly write: MemoryWrite;
            readonly commandJson: string;
            readonly resultJson: string;
            readonly result: MemoryDocument;
          }
        >();

        const results: Array<MemoryDocument> = [];
        let projectedUsage: typeof UsageRow.Type | undefined;

        for (const { write, commandJson } of commands) {
          const receiptKey = receiptViewKey(write);
          const receipt = receipts.get(receiptKey) ?? retainedReceipts.get(receiptKey) ?? null;

          if (receipt !== null) {
            if (receipt.commandJson !== commandJson) {
              return yield* MemoryOperationConflict.make({
                key: write.key,
                operationId: write.operationId,
              });
            }
            results.push(receipt.result);
            continue;
          }

          const documentKey = documentViewKey(write.key);

          const currentRow =
            documents.get(documentKey) ?? retainedDocuments.get(documentKey) ?? null;

          const current = currentRow?.document ?? null;
          const modifiedAt = yield* Clock.currentTimeMillis;
          const next = yield* applyMemoryWrite(current, write, modifiedAt);

          const resultJson = yield* encodeJson(
            StoredMemoryResult,
            StoredMemoryResult.make({ version: STORAGE_VERSION, value: next }),
            "encode memory result",
          );

          const documentJson = resultJson;

          yield* validateEncodedChange({ commandJson, documentJson, resultJson }, operation);

          const bytes = utf8ByteLength;
          const identityBytes = bytes(next.key.namespace.address) + bytes(next.key.id);
          const resultBytes = bytes(resultJson);
          const documentBytes = identityBytes + resultBytes + 128;

          const receiptBytes =
            identityBytes + bytes(write.operationId) + bytes(commandJson) + resultBytes + 128;

          if (Math.max(documentBytes, receiptBytes) > limits.maxRowBytes) {
            return yield* storageError("memory row byte limit", "invalid-input");
          }

          const cachedUsage = views?.get("usage");

          const used =
            projectedUsage ?? (cachedUsage?._tag === "Usage" ? cachedUsage.value : yield* usage);

          if (used.bytes < (currentRow?.bytes ?? 0) || (current !== null && used.documents === 0)) {
            return yield* storageError("read memory usage", "corrupt");
          }

          const maxReceipts =
            limits.maxReceipts -
            (write._tag === "Put" ? (limits.reservedWithdrawalReceipts ?? 0) : 0);

          const maxStorageBytes =
            limits.maxStorageBytes -
            (write._tag === "Put" ? (limits.reservedWithdrawalBytes ?? 0) : 0);

          // Check every staged transition, including intermediate document sizes and
          // receipts. A later shrinking command cannot evade the earlier write's limits.
          if (
            used.documents > limits.maxDocuments - (current === null ? 1 : 0) ||
            used.receipts >= maxReceipts ||
            used.bytes - (currentRow?.bytes ?? 0) > maxStorageBytes - documentBytes - receiptBytes
          ) {
            return yield* storageError("memory storage limit", "invalid-input");
          }

          projectedUsage = {
            documents: used.documents + (current === null ? 1 : 0),
            receipts: used.receipts + 1,
            bytes: used.bytes - (currentRow?.bytes ?? 0) + documentBytes + receiptBytes,
          };
          documents.set(documentKey, { document: next, bytes: documentBytes, documentJson });
          receipts.set(receiptKey, { write, commandJson, resultJson, result: next });
          results.push(next);
        }

        if (projectedUsage === undefined) return { documents: results, changed: false };

        // Both persisted rows have six bound columns: sixteen rows stay below the DO
        // driver's 100-parameter limit. The surrounding transaction owns all revision
        // checks, and only the final row for each key needs physical writeback.
        const documentRows = Array.from(documents.values(), ({ document, documentJson }) => ({
          namespace: document.key.namespace.address,
          source_id: document.key.id,
          format_version: STORAGE_VERSION,
          generation: document.generation,
          revision: document.source.revision,
          document_json: documentJson,
        }));

        for (let index = 0; index < documentRows.length; index += 16) {
          const chunk = documentRows.slice(index, index + 16);

          const changedRows = yield* sql<Record<string, unknown>>`
            INSERT INTO effect_agent_memory_documents_v1 ${sql.insert(chunk)}
            ON CONFLICT (namespace, source_id) DO UPDATE SET
              format_version = excluded.format_version,
              generation = excluded.generation,
              revision = excluded.revision,
              document_json = excluded.document_json
            RETURNING 1 AS changed
          `;

          const changed = yield* decodeRows(MemoryChangeCountRow, changedRows, operation);

          if (changed.length !== chunk.length || changed.some((row) => row.changed !== 1)) {
            return yield* storageError(operation, "corrupt");
          }
        }
        yield* failpoint.hit("memory:change:after-state");

        const receiptRows = Array.from(receipts.values(), ({ write, commandJson, resultJson }) => ({
          namespace: write.key.namespace.address,
          operation_id: write.operationId,
          source_id: write.key.id,
          format_version: STORAGE_VERSION,
          command_json: commandJson,
          result_json: resultJson,
        }));

        for (let index = 0; index < receiptRows.length; index += 16) {
          yield* sql`
            INSERT INTO effect_agent_memory_receipts_v1 ${sql.insert(receiptRows.slice(index, index + 16))}
          `;
        }
        yield* failpoint.hit("memory:change:after-receipt");

        for (const [key, { document, bytes }] of documents)
          views?.set(key, { _tag: "Document", value: { document, bytes } });
        for (const [key, { commandJson, result }] of receipts)
          views?.set(key, { _tag: "Receipt", value: { commandJson, result } });
        views?.set("usage", { _tag: "Usage", value: projectedUsage });

        return { documents: results, changed: true };
      }),
    ).pipe(Effect.catchTag("SqlError", () => Effect.fail(storageError(operation))));

    if (transactionResult.changed) yield* failpoint.hit("memory:change:after");

    return transactionResult.documents;
  });

  const change = Effect.fn("SqliteMemoryStore.change")(function* (write: MemoryWrite) {
    return (yield* changeMany([write]))[0];
  });

  const batchWriter = SqlMemoryBatchWriter.of({
    changeMany: Effect.fn("SqlMemoryBatchWriter.changeMany")(function* <
      Namespace extends MemoryNamespace,
    >(writes: ReadonlyArray<MemoryWrite<Namespace>>) {
      const documents = yield* changeMany(writes);

      return yield* Effect.forEach(documents, (document, index) =>
        MemoryDocument.restore(writes[index].key.namespace, document),
      );
    }),
  });

  return Context.make(MemoryReader, MemoryReader.fromAdapter({ get })).pipe(
    Context.add(MemoryWriter, MemoryWriter.fromAdapter({ change })),
    Context.add(SqlMemoryBatchWriter, batchWriter),
  );
});

/** SQLite memory ports with the mutation failpoint kept injectable for recovery tests. */
export const memoryStoreLayerWithFailpoints: Layer.Layer<
  MemoryReader | MemoryWriter | SqlMemoryBatchWriter,
  SqliteMemoryInitializationError,
  SqlClientService.SqlClient | MemoryMutationFailpoint
> = Layer.effectContext(makeMemoryServices());

/** SQLite memory reader, single writer and batch writer with the production no-op failpoint. */
export const memoryStoreLayer: Layer.Layer<
  MemoryReader | MemoryWriter | SqlMemoryBatchWriter,
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
