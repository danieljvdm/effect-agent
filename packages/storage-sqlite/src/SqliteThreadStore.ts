import { makeSqlTransaction, SqlInteger } from "@effect-agent/storage-sql/sql-storage";
import { makeSqlThreadStore } from "@effect-agent/storage-sql/sql-thread-store";
import { NodeCrypto } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import type { Crypto } from "effect";
import { Duration, Effect, Layer, Schema } from "effect";
import { ThreadId } from "effect-agent/identifiers";
import { ProducerEpoch } from "effect-agent/records";
import { DEFAULT_OWNERSHIP_LEASE_DURATION } from "effect-agent/submission-ledger";
import { ThreadStore } from "effect-agent/thread-store";
import * as SqlClientService from "effect/unstable/sql/SqlClient";

import { CurrentSqliteStorageVersion } from "./internal/migrations.ts";
import { initializeSqliteJournal, sqliteErrors } from "./internal/sqlite-journal.ts";
import { SqliteStorageConfig, SqliteStorageConfigValue } from "./SqliteStorageConfig.ts";
import {
  SqliteStorageCompatibilityError,
  SqliteStorageCorruptionError,
  SqliteStorageError,
} from "./SqliteStorageError.ts";
import {
  SqliteStorageFailpoint,
  type SqliteStorageFailpointHandler,
} from "./SqliteStorageFailpoint.ts";

export interface SqliteStorageOptions {
  readonly filename: string;
  readonly observationPollInterval?: number | undefined;
  /** Bounded SQLITE_BUSY retry window for write-lock acquisition, in milliseconds. */
  readonly busyTimeout?: number | undefined;
  /**
   * Submission ownership lease duration in milliseconds (D5). Defaults to
   * `DEFAULT_OWNERSHIP_LEASE_DURATION` from `effect-agent/submission-ledger`.
   */
  readonly ownershipLeaseDuration?: number | undefined;
  /**
   * Re-verify every stored payload and digest chain while opening the store. Defaults to
   * off: per-operation Schema decoding and the digest chain already fail clearly on corrupt
   * rows without scanning the whole database on every open.
   */
  readonly verifyOnOpen?: boolean | undefined;
  readonly failpoint?: SqliteStorageFailpointHandler | undefined;
}

export type SqliteStorageInitializationError =
  | SqliteStorageCompatibilityError
  | SqliteStorageCorruptionError
  | SqliteStorageError;

const ExclusiveMode = Schema.Tuple([Schema.Struct({ locking_mode: Schema.Literal("exclusive") })]);
const JournalMode = Schema.Tuple([Schema.Struct({ journal_mode: Schema.String })]);

const DatabaseHeader = Schema.Tuple([
  Schema.Struct({ user_version: SqlInteger, table_count: SqlInteger }),
]);

const RetainedOwnership = Schema.Array(
  Schema.Struct({
    thread_id: ThreadId,
    current_epoch: SqlInteger.pipe(
      Schema.decodeTo(ProducerEpoch.check(Schema.isLessThan(Number.MAX_SAFE_INTEGER))),
    ),
    ownership_epoch: SqlInteger.pipe(Schema.decodeTo(ProducerEpoch)),
  }),
);

/**
 * Acquire process-lifetime authority over a dedicated SQLite client before exposing it to a
 * managed host. Open the client with `disableWAL: true`: WAL setup and lock contention must
 * fail through this Layer's typed initialization error. The client Scope owns the lock;
 * never change its locking mode or close it while host work is alive.
 *
 * This excludes ALL other database connections, including readers. After compatibility checks,
 * retire retained ownership and advance its Thread epochs in one transaction, then recover
 * through the ordinary ledger protocol. Producer names confer no takeover authority. Journals,
 * receipts, queue states and unresolved external effects are left intact.
 */
export const exclusiveHostClientLayer: Layer.Layer<
  SqlClientService.SqlClient,
  SqliteStorageInitializationError,
  SqliteStorageConfig | SqliteStorageFailpoint | SqlClientService.SqlClient | Crypto.Crypto
> = Layer.effect(SqlClientService.SqlClient)(
  Effect.gen(function* () {
    const sql = (yield* SqlClientService.SqlClient).withoutTransforms();

    const acquireError = (cause: unknown) =>
      SqliteStorageError.make({
        operation: "acquire exclusive host",
        message:
          "Cannot acquire exclusive SQLite host authority; close other database connections.",
        cause,
      });

    yield* Effect.gen(function* () {
      yield* sql`PRAGMA busy_timeout = 0`;
      const mode = yield* sql`PRAGMA main.locking_mode = EXCLUSIVE`;

      yield* Schema.decodeUnknownEffect(ExclusiveMode)(mode);

      const [journal] = yield* Schema.decodeUnknownEffect(JournalMode)(
        yield* sql`PRAGMA journal_mode`,
      );

      if (journal.journal_mode !== "wal") {
        const [header] = yield* Schema.decodeUnknownEffect(DatabaseHeader)(
          yield* sql`
          SELECT (SELECT user_version FROM pragma_user_version) AS user_version,
                 (SELECT COUNT(*) FROM sqlite_master WHERE type = 'table') AS table_count
        `,
        );

        // Only a new file may change journal mode before compatibility checks. Existing
        // stores must already satisfy the adapter's WAL contract, including predecessors.
        if (header.user_version !== 0 || header.table_count !== 0) {
          return yield* SqliteStorageCompatibilityError.make({
            actualVersion: header.user_version,
            supportedVersion: CurrentSqliteStorageVersion,
            message:
              "An existing managed-host database must use SQLite WAL mode; no data was changed.",
          });
        }
        yield* sql`PRAGMA journal_mode = WAL`;
      }
      // A mode setting alone is not authority. Acquire the write lock now; EXCLUSIVE mode
      // retains it after commit/rollback until this client's enclosing Scope closes.
      yield* makeSqlTransaction(sql, { begin: "BEGIN IMMEDIATE" })(Effect.void);
    }).pipe(
      Effect.mapError((error) =>
        Schema.is(SqliteStorageCompatibilityError)(error) ? error : acquireError(error),
      ),
    );

    // Supported upgrades and malformed/unsupported storage checks precede ownership mutation.
    yield* initializeSqliteJournal();
    yield* sql
      .withTransaction(
        Effect.gen(function* () {
          const rows = yield* sql`
            SELECT s.thread_id, t.producer_epoch AS current_epoch,
                   o.producer_epoch AS ownership_epoch
            FROM effect_agent_submission_ownership o
            LEFT JOIN effect_agent_submissions s ON s.submission_id = o.submission_id
            LEFT JOIN effect_agent_threads t ON t.thread_id = s.thread_id
          `;

          const retained = yield* Schema.decodeUnknownEffect(RetainedOwnership)(rows).pipe(
            Effect.mapError((error) =>
              SqliteStorageCorruptionError.make({
                table: "effect_agent_submission_ownership",
                rowKey: "exclusive-host-startup",
                message: error.message,
              }),
            ),
          );

          for (const row of retained) {
            if (row.ownership_epoch > row.current_epoch) {
              return yield* SqliteStorageCorruptionError.make({
                table: "effect_agent_submission_ownership",
                rowKey: row.thread_id,
                message: "Retained ownership is ahead of the Thread's producer epoch.",
              });
            }
          }

          yield* sql`
            UPDATE effect_agent_threads SET producer_epoch = producer_epoch + 1
            WHERE thread_id IN (
              SELECT s.thread_id FROM effect_agent_submissions s
              JOIN effect_agent_submission_ownership o ON o.submission_id = s.submission_id
            )
          `;
          yield* sql`DELETE FROM effect_agent_submission_ownership`;
        }),
      )
      .pipe(
        Effect.catchTag("SqlError", (cause) =>
          SqliteStorageError.make({
            operation: "retire departed host ownership",
            message: cause.message,
            cause,
          }),
        ),
      );

    return sql;
  }),
);

const makeServices = Effect.fn("SqliteThreadStore.makeServices")(function* () {
  const config = yield* SqliteStorageConfig;
  const failpoint = yield* SqliteStorageFailpoint;
  const journal = yield* initializeSqliteJournal();

  return yield* makeSqlThreadStore(journal, {
    ...config,
    errors: sqliteErrors,
    hitFailpoint: failpoint.hit,
    offsetPrefix: "effect-agent-sqlite@1:",
  });
});

/**
 * SQLite Thread Store implementation with configuration, failpoint, SQL, and Crypto
 * authority kept visible in its input channel.
 */
export const threadStoreLayer: Layer.Layer<
  ThreadStore,
  SqliteStorageInitializationError,
  SqliteStorageConfig | SqliteStorageFailpoint | SqlClientService.SqlClient | Crypto.Crypto
> = Layer.effect(ThreadStore, makeServices());

/**
 * Validated SQLite storage configuration Layer with the documented defaults applied. Shared
 * by the ThreadStore and SubmissionLedger convenience layers so their defaults cannot
 * drift.
 */
export const storageConfigLayer = (
  options: SqliteStorageOptions,
): Layer.Layer<SqliteStorageConfig, SqliteStorageError> =>
  Layer.effect(SqliteStorageConfig)(
    Schema.decodeEffect(SqliteStorageConfigValue)({
      observationPollInterval: options.observationPollInterval ?? 25,
      busyTimeout: options.busyTimeout ?? 5_000,
      ownershipLeaseDuration:
        options.ownershipLeaseDuration ?? Duration.toMillis(DEFAULT_OWNERSHIP_LEASE_DURATION),
      verifyOnOpen: options.verifyOnOpen ?? false,
    }).pipe(
      Effect.mapError((error) =>
        SqliteStorageError.make({
          cause: error,
          operation: "configure SQLite storage",
          message: error.message,
        }),
      ),
    ),
  );

/** The failpoint Layer selected by convenience options: explicit handler or the no-op default. */
export const storageFailpointLayer = (
  options: SqliteStorageOptions,
): Layer.Layer<SqliteStorageFailpoint> =>
  options.failpoint === undefined
    ? SqliteStorageFailpoint.layer
    : Layer.succeed(SqliteStorageFailpoint)({ hit: options.failpoint });

/**
 * A composition-root convenience Layer for canonical Threads. Durable accepted work is
 * served by the separate SubmissionLedger port.
 */
export const layer = (
  options: SqliteStorageOptions,
): Layer.Layer<ThreadStore, SqliteStorageInitializationError> =>
  Layer.unwrap(
    Effect.map(SqliteStorageConfig, (config) =>
      threadStoreLayer.pipe(
        Layer.provide(
          Layer.mergeAll(
            Layer.succeed(SqliteStorageConfig)(config),
            storageFailpointLayer(options),
            SqliteClient.layer({ filename: options.filename }),
            NodeCrypto.layer,
          ),
        ),
      ),
    ),
  ).pipe(Layer.provide(storageConfigLayer(options)));
