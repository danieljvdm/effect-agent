import { Array, Context, Crypto, DateTime, Effect, Option, Schema } from "effect";
import { digestJson } from "effect-agent/digest";
import type { ThreadId } from "effect-agent/identifiers";
import {
  LifecyclePublication,
  LifecyclePublicationBatch,
  LifecyclePublicationConfig,
  LifecyclePublicationError,
  lifecyclePublicationBatchMaxFacts,
  type LifecyclePublicationStorage,
} from "effect-agent/lifecycle-publication";
import { SqlStorageOwner } from "effect-agent/sql-memory-store";
import { SqlClient } from "effect/unstable/sql/SqlClient";

import { makeSqlQuery, SqlInteger } from "./SqlStorage.ts";

const codec = Schema.fromJsonString(LifecyclePublication);
const maxPendingPayloadBytes = 4 * 1024 * 1024;

export type SqlLifecyclePublicationInput = Omit<LifecyclePublication, "ordinal" | "id"> & {
  readonly id?: string;
};

export type SqlLifecycleRetainMany = (
  publications: ReadonlyArray<SqlLifecyclePublicationInput>,
) => Effect.Effect<void, LifecyclePublicationError>;

/** Retain source facts in the source owner's current transaction, without invoking its hooks. */
export class SqlLifecycleRetainer extends Context.Service<
  SqlLifecycleRetainer,
  { readonly retainMany: SqlLifecycleRetainMany }
>()("@effect-agent/storage-sql/SqlLifecycleRetainer") {}

/** Adapter-owned journal intent materialization, independent of publication dispatch. */
export interface SqlLifecycleSource {
  /** Retain the earlier canonical prefix before assigning a noncanonical fact's ordinal. */
  readonly beforeRetain: (
    ownerThreadId: ThreadId,
  ) => Effect.Effect<void, LifecyclePublicationError, SqlLifecycleRetainer>;
  /** Materialize a bounded canonical suffix before selecting the next owner batches. */
  readonly beforePending: Effect.Effect<void, LifecyclePublicationError, SqlLifecycleRetainer>;
}

export const SqlLifecycleSource = Context.Reference<SqlLifecycleSource | undefined>(
  "@effect-agent/storage-sql/SqlLifecycleSource",
  { defaultValue: () => undefined },
);

const Row = Schema.Struct({
  id: Schema.String,
  owner_thread_id: Schema.String,
  ordinal: SqlInteger,
  fingerprint: Schema.String,
  payload_json: Schema.NullOr(Schema.String),
  due_at_millis: Schema.NullOr(SqlInteger),
});

const Attempts = Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 8 }));

const PendingRow = Schema.Struct({
  ...Row.fields,
  attempts: SqlInteger.pipe(Schema.decodeTo(Attempts)),
});

type OwnerView = {
  ordinal?: number;
  readonly rows: Map<string, typeof Row.Type>;
  readonly batches: Map<string, ReadonlyArray<string>>;
  readonly attempts: Map<string, number>;
  complete: boolean;
  head?: number;
  headKnown: boolean;
};

type OwnedView = {
  readonly owners: Map<string, OwnerView>;
  deadline?: Option.Option<number>;
};

const ownedViews = new WeakMap<object, Map<string, OwnedView>>();

const failure = (cause: unknown) =>
  LifecyclePublicationError.make({ reason: "unavailable", cause });

/**
 * Optional native recovery obligations. Retain runs inside the caller's existing source write
 * transaction. Acknowledgement clears private payloads and retry state; the stable
 * identity/fingerprint remains. Receipts must never be cascade-deleted with a Thread or its
 * projections.
 * The additive table has its own closed Schema; it does not change a native format in place.
 * Selection reads sizes before payloads, with a 4 MiB budget across selected owners. An
 * individually larger valid fact runs alone; no pending tail is loaded or acknowledged early.
 */
export const makeSqlLifecyclePublication = Effect.fn("SqlLifecyclePublication.make")(function* (
  namespace?: string,
  maxStoredValueBytes = 16 * 1024 * 1024,
) {
  const active = yield* LifecyclePublicationConfig;

  if (Option.isNone(active)) return undefined;
  const sql = yield* SqlClient;
  const crypto = active.value;
  const { table, execute } = yield* makeSqlQuery(namespace);
  const relation = table("effect_agent_lifecycle_publications");
  const retries = table("effect_agent_lifecycle_publication_retries");
  const owner = yield* SqlStorageOwner;
  const source = yield* SqlLifecycleSource;
  let owned: OwnedView | undefined;

  if (owner !== undefined) {
    let namespaces = ownedViews.get(owner);

    if (namespaces === undefined) {
      namespaces = new Map();
      ownedViews.set(owner, namespaces);
    }
    const key = namespace ?? "";

    owned = namespaces.get(key);
    if (owned === undefined) {
      const view: OwnedView = { owners: new Map() };

      owned = view;
      namespaces.set(key, view);
      owner.invalidators.add(() => {
        view.owners.clear();
        delete view.deadline;
      });
    }
  }

  const read = <A, E>(body: Effect.Effect<A, E>) => owner?.read(body) ?? body;

  const transaction = <A, E>(body: Effect.Effect<A, E>) =>
    (owner === undefined ? sql.withTransaction(body) : owner.transaction(body)).pipe(
      Effect.provideService(SqlClient, sql),
      Effect.catchTag("SqlError", failure),
    );

  const viewFor = (ownerThreadId: string) => {
    if (owned === undefined) return undefined;
    let view = owned.owners.get(ownerThreadId);

    if (view === undefined) {
      view = {
        rows: new Map(),
        batches: new Map(),
        attempts: new Map(),
        complete: false,
        headKnown: false,
      };
      owned.owners.set(ownerThreadId, view);
    }

    return view;
  };

  const trim = () => {
    if (owned === undefined) return;
    let bytes = 0;

    for (const view of owned.owners.values()) {
      if (view.rows.size > 128) {
        view.rows.clear();
        view.batches.clear();
        view.attempts.clear();
        view.complete = false;
        view.headKnown = false;
      }
      for (const row of view.rows.values())
        bytes += new TextEncoder().encode(JSON.stringify(row)).byteLength;
    }
    while (owned.owners.size > 128 || bytes > maxPendingPayloadBytes) {
      const oldest = owned.owners.entries().next().value;

      if (oldest === undefined) break;
      for (const row of oldest[1].rows.values())
        bytes -= new TextEncoder().encode(JSON.stringify(row)).byteLength;
      owned.owners.delete(oldest[0]);
    }
  };

  const remember = (rows: ReadonlyArray<typeof Row.Type>) => {
    for (const row of rows)
      viewFor(row.owner_thread_id)?.rows.set(row.id, {
        id: row.id,
        owner_thread_id: row.owner_thread_id,
        ordinal: row.ordinal,
        fingerprint: row.fingerprint,
        payload_json: row.payload_json,
        due_at_millis: row.due_at_millis,
      });
    trim();
  };

  const batchKey = (batch: LifecyclePublicationBatch) =>
    JSON.stringify(batch.map((fact) => fact.id));

  const rememberBatch = (
    batch: LifecyclePublicationBatch,
    rows: ReadonlyArray<typeof Row.Type>,
  ) => {
    remember(rows);
    const view = owned?.owners.get(batch[0].ownerThreadId);

    if (view !== undefined && rows.every((row) => view.rows.has(row.id))) {
      view.batches.clear();
      view.batches.set(
        batchKey(batch),
        rows.map((row) => row.id),
      );
    }
  };

  const invalidateDeadline = () => {
    if (owned !== undefined) delete owned.deadline;
  };

  // Additive retry state leaves existing payloads and acknowledgement receipts unchanged.
  yield* sql`CREATE TABLE IF NOT EXISTS ${retries} (
    id TEXT PRIMARY KEY, attempts BIGINT NOT NULL
  )`.pipe(execute, Effect.mapError(failure));

  yield* sql`CREATE TABLE IF NOT EXISTS ${relation} (
    id TEXT PRIMARY KEY, owner_thread_id TEXT NOT NULL, ordinal BIGINT NOT NULL,
    fingerprint TEXT NOT NULL, payload_json TEXT, due_at_millis BIGINT,
    UNIQUE(owner_thread_id, ordinal)
  )`.pipe(execute, Effect.mapError(failure));
  yield* sql`CREATE INDEX IF NOT EXISTS effect_agent_lifecycle_pending ON ${relation}(due_at_millis, owner_thread_id, ordinal) WHERE due_at_millis IS NOT NULL`.pipe(
    execute,
    Effect.mapError(failure),
  );
  yield* sql`CREATE INDEX IF NOT EXISTS effect_agent_lifecycle_owner_retained ON ${relation}(owner_thread_id, ordinal) WHERE payload_json IS NOT NULL`.pipe(
    execute,
    Effect.mapError(failure),
  );

  const encode = (publication: LifecyclePublication) =>
    Schema.encodeEffect(codec)(publication).pipe(Effect.mapError(failure));

  const fingerprint = (text: string) =>
    digestJson(text).pipe(Effect.provideService(Crypto.Crypto, crypto), Effect.mapError(failure));

  const decodeRows = (rows: unknown) =>
    Schema.decodeUnknownEffect(Schema.Array(Row))(rows).pipe(Effect.mapError(failure));

  const retainMany: SqlLifecycleRetainMany = Effect.fn("SqlLifecyclePublication.retainMany")(
    function* (inputs) {
      const existing = new Map<string, typeof Row.Type>();
      const missing: Array<string> = [];

      for (const input of inputs) {
        if (input.id === undefined) continue;
        const row = owned?.owners.get(input.ownerThreadId)?.rows.get(input.id);

        if (row === undefined) missing.push(input.id);
        else existing.set(row.id, row);
      }
      // Six bound parameters per inserted row; identity reads leave the same 100-parameter
      // ceiling intact. Existing fingerprints remain authoritative after acknowledgement.
      for (let index = 0; index < missing.length; index += 100) {
        const rows =
          yield* sql`SELECT id, owner_thread_id, ordinal, fingerprint, payload_json, due_at_millis FROM ${relation} WHERE id IN ${sql.in(missing.slice(index, index + 100))}`.pipe(
            execute,
            Effect.mapError(failure),
            Effect.flatMap(decodeRows),
          );

        for (const row of rows) existing.set(row.id, row);
        remember(rows);
      }

      const counters = new Map<string, number>();
      const insert: Array<typeof Row.Type> = [];

      for (const input of inputs) {
        const previous = input.id === undefined ? undefined : existing.get(input.id);

        let current =
          counters.get(input.ownerThreadId) ?? owned?.owners.get(input.ownerThreadId)?.ordinal;

        if (previous === undefined && current === undefined) {
          const rows =
            yield* sql`SELECT COALESCE(MAX(ordinal), 0) AS ordinal FROM ${relation} WHERE owner_thread_id = ${input.ownerThreadId}`.pipe(
              execute,
              Effect.mapError(failure),
              Effect.flatMap(
                Schema.decodeUnknownEffect(Schema.Array(Schema.Struct({ ordinal: SqlInteger }))),
              ),
              Effect.mapError(failure),
            );

          current = rows[0]?.ordinal ?? 0;
          const view = viewFor(input.ownerThreadId);

          if (view !== undefined) {
            view.ordinal = current;
            if (current === 0) {
              view.complete = true;
              view.headKnown = true;
            }
          }
        }
        const ordinal = previous?.ordinal ?? (current ?? 0) + 1;

        const publication = yield* LifecyclePublication.makeEffect({
          ...input,
          id: input.id ?? JSON.stringify([input.ownerThreadId, "lifecycle", ordinal]),
          ordinal,
        }).pipe(Effect.mapError(failure));

        const text = yield* encode(publication);

        if (new TextEncoder().encode(text).byteLength > maxStoredValueBytes || ordinal > 1_000_000)
          return yield* LifecyclePublicationError.make({ reason: "capacity" });
        const digest = yield* fingerprint(text);

        if (previous !== undefined) {
          if (previous.fingerprint !== digest)
            return yield* LifecyclePublicationError.make({ reason: "conflict" });
          continue;
        }

        const row: typeof Row.Type = {
          id: publication.id,
          owner_thread_id: input.ownerThreadId,
          ordinal,
          fingerprint: digest,
          payload_json: text,
          due_at_millis: DateTime.toEpochMillis(input.createdAt),
        };

        counters.set(input.ownerThreadId, ordinal);
        existing.set(row.id, row);
        insert.push(row);
      }
      for (let index = 0; index < insert.length; index += 16) {
        const rows = insert.slice(index, index + 16);

        yield* sql`INSERT INTO ${relation} ${sql.insert(rows)}`.pipe(
          execute,
          Effect.mapError(failure),
        );
        for (const row of rows) {
          const view = viewFor(row.owner_thread_id);

          if (view === undefined) continue;
          view.ordinal = row.ordinal;
          view.attempts.set(row.id, 0);
          if (view.headKnown && view.head === undefined) view.head = row.ordinal;
        }
        remember(rows);
      }
      if (insert.length > 0) invalidateDeadline();
    },
    read,
  );

  const retain = Effect.fn("SqlLifecyclePublication.retain")(function* (
    input: SqlLifecyclePublicationInput,
  ) {
    yield* (source?.beforeRetain(input.ownerThreadId) ?? Effect.void).pipe(
      Effect.provideService(SqlLifecycleRetainer, { retainMany }),
    );
    yield* retainMany([input]);
  }, read);

  const verify = Effect.fnUntraced(function* (
    publication: LifecyclePublication,
    row: typeof Row.Type | undefined,
  ) {
    if (
      row === undefined ||
      row.id !== publication.id ||
      row.owner_thread_id !== publication.ownerThreadId ||
      row.ordinal !== publication.ordinal ||
      row.fingerprint !== (yield* fingerprint(yield* encode(publication)))
    )
      return yield* LifecyclePublicationError.make({ reason: "conflict" });
  });

  const verifyBatch = Effect.fnUntraced(function* (input: LifecyclePublicationBatch) {
    const batch = yield* Schema.decodeEffect(Schema.toType(LifecyclePublicationBatch))(input).pipe(
      Effect.mapError(() => LifecyclePublicationError.make({ reason: "conflict" })),
    );

    const first = batch[0];
    const last = Array.lastNonEmpty(batch);
    const view = owned?.owners.get(first.ownerThreadId);
    const retained = view?.batches.get(batchKey(batch));

    if (retained !== undefined && view !== undefined) {
      const rows = retained.map((id) => view.rows.get(id));

      if (rows.every((row) => row !== undefined)) {
        yield* Effect.forEach(batch, (publication, index) => verify(publication, rows[index]));
        if (rows[0] !== undefined) return rows[0];
      }
    }

    const identities = new Set(batch.map((publication) => publication.id));

    const rows =
      (yield* sql`SELECT id, owner_thread_id, ordinal, fingerprint, payload_json, due_at_millis FROM ${relation} WHERE owner_thread_id = ${first.ownerThreadId} AND ordinal BETWEEN ${first.ordinal} AND ${last.ordinal} ORDER BY ordinal`.pipe(
        execute,
        Effect.mapError(failure),
        Effect.flatMap(decodeRows),
      )).filter((row) => row.payload_json !== null || identities.has(row.id));

    const head = rows[0];

    if (head === undefined || rows.length !== batch.length)
      return yield* LifecyclePublicationError.make({ reason: "conflict" });
    yield* Effect.forEach(batch, (publication, index) => verify(publication, rows[index]));
    rememberBatch(batch, rows);

    return head;
  });

  const storage: LifecyclePublicationStorage = {
    pending: (nowMillis, limit) =>
      Effect.gen(function* () {
        yield* Schema.decodeEffect(
          Schema.Struct({
            nowMillis: Schema.Natural,
            limit: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 100 })),
          }),
        )({ nowMillis, limit }).pipe(Effect.mapError(failure));
        yield* (source?.beforePending ?? Effect.void).pipe(
          Effect.provideService(SqlLifecycleRetainer, { retainMany }),
        );

        // A deferred or parked prefix blocks its whole owner. Select metadata before payloads.
        const owners = yield* sql`SELECT p.owner_thread_id FROM ${relation} p
          WHERE p.payload_json IS NOT NULL AND p.due_at_millis <= ${nowMillis}
          AND NOT EXISTS (SELECT 1 FROM ${relation} before_p WHERE before_p.owner_thread_id = p.owner_thread_id AND before_p.ordinal < p.ordinal AND before_p.payload_json IS NOT NULL)
          ORDER BY p.due_at_millis, p.owner_thread_id LIMIT ${limit}`.pipe(
          execute,
          Effect.mapError(failure),
          Effect.flatMap(
            Schema.decodeUnknownEffect(
              Schema.Array(Schema.Struct({ owner_thread_id: Schema.String })),
            ),
          ),
          Effect.mapError(failure),
        );

        const batches: Array<LifecyclePublicationBatch> = [];
        let selectedBytes = 0;

        for (const owner of owners) {
          const view = owned?.owners.get(owner.owner_thread_id);

          const cachedRows =
            view?.complete === true
              ? [...view.rows.values()]
                  .filter((row) => row.payload_json !== null)
                  .sort((left, right) => left.ordinal - right.ordinal)
                  .slice(0, lifecyclePublicationBatchMaxFacts)
              : undefined;

          const sizes =
            cachedRows === undefined
              ? yield* sql`SELECT ordinal, ${sql.onDialectOrElse({
                  pg: () => sql`octet_length(payload_json)`,
                  orElse: () => sql`length(CAST(payload_json AS BLOB))`,
                })} AS payload_bytes FROM ${relation}
            WHERE owner_thread_id = ${owner.owner_thread_id} AND payload_json IS NOT NULL
            ORDER BY ordinal LIMIT ${lifecyclePublicationBatchMaxFacts}`.pipe(
                  execute,
                  Effect.mapError(failure),
                  Effect.flatMap(
                    Schema.decodeUnknownEffect(
                      Schema.Array(
                        Schema.Struct({ ordinal: SqlInteger, payload_bytes: SqlInteger }),
                      ),
                    ),
                  ),
                  Effect.mapError(failure),
                )
              : cachedRows.map((row) => ({
                  ordinal: row.ordinal,
                  payload_bytes: new TextEncoder().encode(row.payload_json ?? "").byteLength,
                }));

          let lastOrdinal: number | undefined;

          for (const row of sizes) {
            // An individually larger valid fact runs alone, so it cannot strand its owner.
            if (
              selectedBytes + row.payload_bytes > maxPendingPayloadBytes &&
              (lastOrdinal !== undefined || batches.length > 0)
            )
              break;
            lastOrdinal = row.ordinal;
            selectedBytes += row.payload_bytes;
          }
          if (lastOrdinal === undefined) continue;

          const selected =
            cachedRows === undefined
              ? yield* sql`SELECT p.id, p.owner_thread_id, p.ordinal, p.fingerprint, p.payload_json, p.due_at_millis, COALESCE(r.attempts, 0) AS attempts
            FROM ${relation} p LEFT JOIN ${retries} r ON r.id = p.id WHERE p.owner_thread_id = ${owner.owner_thread_id}
            AND p.ordinal <= ${lastOrdinal} AND p.payload_json IS NOT NULL ORDER BY p.ordinal`.pipe(
                  execute,
                  Effect.mapError(failure),
                  Effect.flatMap(Schema.decodeUnknownEffect(Schema.Array(PendingRow))),
                  Effect.mapError(failure),
                )
              : cachedRows.filter((row) => row.ordinal <= lastOrdinal);

          for (const row of selected) {
            if ("attempts" in row) viewFor(row.owner_thread_id)?.attempts.set(row.id, row.attempts);
          }
          const rows = selected;

          const facts = yield* Effect.forEach(rows, (row) =>
            Effect.gen(function* () {
              if (row.payload_json === null)
                return yield* LifecyclePublicationError.make({ reason: "corrupt" });

              const publication = yield* Schema.decodeEffect(codec)(row.payload_json).pipe(
                Effect.mapError(failure),
              );

              yield* verify(publication, row);

              return publication;
            }),
          );

          const batch = yield* Schema.decodeUnknownEffect(Schema.toType(LifecyclePublicationBatch))(
            facts,
          ).pipe(Effect.mapError(failure));

          rememberBatch(batch, rows);
          const retained = owned?.owners.get(owner.owner_thread_id);

          if (retained !== undefined) {
            retained.head = batch[0].ordinal;
            retained.headKnown = true;
            if (
              sizes.length < lifecyclePublicationBatchMaxFacts &&
              sizes.length === rows.length &&
              rows.every((row) => retained.rows.has(row.id))
            )
              retained.complete = true;
          }
          batches.push(batch);
          if (selectedBytes >= maxPendingPayloadBytes) break;
        }

        return batches;
      }).pipe(source === undefined ? read : transaction),
    acknowledge: (batch) =>
      transaction(
        Effect.gen(function* () {
          yield* verifyBatch(batch);
          yield* sql`UPDATE ${relation} SET payload_json = NULL, due_at_millis = NULL WHERE owner_thread_id = ${batch[0].ownerThreadId} AND ordinal BETWEEN ${batch[0].ordinal} AND ${Array.lastNonEmpty(batch).ordinal}`.pipe(
            execute,
            Effect.mapError(failure),
          );
          yield* sql`DELETE FROM ${retries} WHERE id IN (
            SELECT id FROM ${relation} WHERE owner_thread_id = ${batch[0].ownerThreadId}
            AND ordinal BETWEEN ${batch[0].ordinal} AND ${Array.lastNonEmpty(batch).ordinal}
          )`.pipe(execute, Effect.mapError(failure));
          const view = owned?.owners.get(batch[0].ownerThreadId);

          if (view !== undefined) {
            for (const publication of batch) {
              const row = view.rows.get(publication.id);

              if (row !== undefined)
                view.rows.set(row.id, { ...row, payload_json: null, due_at_millis: null });
              view.attempts.delete(publication.id);
            }
            if (view.complete) {
              view.head = [...view.rows.values()]
                .filter((row) => row.payload_json !== null)
                .sort((left, right) => left.ordinal - right.ordinal)[0]?.ordinal;
              view.headKnown = true;
            } else view.headKnown = false;
          }
          invalidateDeadline();
        }),
      ),
    claim: (batch, nowMillis, timeoutMillis) =>
      transaction(
        Effect.gen(function* () {
          yield* Schema.decodeEffect(
            Schema.Struct({
              nowMillis: Schema.Natural,
              timeoutMillis: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 300_000 })),
            }),
          )({ nowMillis, timeoutMillis }).pipe(Effect.mapError(failure));
          yield* Schema.decodeEffect(Schema.toType(LifecyclePublicationBatch))(batch).pipe(
            Effect.mapError(failure),
          );
          const first = batch[0];

          const cachedAttempt = owned?.owners.get(first.ownerThreadId)?.attempts.get(first.id);

          // Shared SQL still locks the retry row. A physical single-owner adapter already
          // holds its transaction gate and can reuse the retry read with its pending prefix.
          const attempts =
            cachedAttempt === undefined
              ? yield* sql`INSERT INTO ${retries}(id, attempts) VALUES (${first.id}, 0)
        ON CONFLICT(id) DO UPDATE SET attempts = ${retries}.attempts RETURNING attempts`.pipe(
                  execute,
                  Effect.mapError(failure),
                  Effect.flatMap(
                    Schema.decodeUnknownEffect(
                      Schema.NonEmptyArray(
                        Schema.Struct({
                          attempts: SqlInteger.pipe(Schema.decodeTo(Attempts)),
                        }),
                      ),
                    ),
                  ),
                  Effect.mapError(failure),
                )
              : [{ attempts: cachedAttempt }];

          const head = yield* verifyBatch(batch);

          if (
            head.payload_json === null ||
            head.due_at_millis === null ||
            head.due_at_millis > nowMillis
          )
            return false;

          const view = owned?.owners.get(first.ownerThreadId);

          const blocked =
            view?.headKnown === true
              ? view.head !== first.ordinal
              : (yield* sql`SELECT id FROM ${relation} WHERE owner_thread_id = ${first.ownerThreadId} AND ordinal < ${first.ordinal} AND payload_json IS NOT NULL LIMIT 1`.pipe(
                  execute,
                  Effect.mapError(failure),
                )).length > 0;

          if (blocked) return false;
          const attempt = attempts[0].attempts + 1;

          if (attempt > 8) return false;

          const deadline =
            attempt === 8
              ? null
              : nowMillis + timeoutMillis + Math.min(60_000, 1_000 * 2 ** (attempt - 1));

          yield* (
            cachedAttempt === undefined
              ? sql`UPDATE ${retries} SET attempts = ${attempt} WHERE id = ${first.id}`
              : sql`INSERT INTO ${retries}(id, attempts) VALUES (${first.id}, ${attempt}) ON CONFLICT(id) DO UPDATE SET attempts = excluded.attempts`
          ).pipe(execute, Effect.mapError(failure));
          yield* sql`UPDATE ${relation} SET due_at_millis = ${deadline} WHERE owner_thread_id = ${first.ownerThreadId} AND ordinal BETWEEN ${first.ordinal} AND ${Array.lastNonEmpty(batch).ordinal} AND payload_json IS NOT NULL`.pipe(
            execute,
            Effect.mapError(failure),
          );
          const retained = owned?.owners.get(first.ownerThreadId);

          if (retained !== undefined) {
            retained.attempts.set(first.id, attempt);
            for (const publication of batch) {
              const row = retained.rows.get(publication.id);

              if (row !== undefined && row.payload_json !== null)
                retained.rows.set(row.id, { ...row, due_at_millis: deadline });
            }
          }
          invalidateDeadline();

          return true;
        }),
      ),
    pendingDeadline: read(
      Effect.suspend(() =>
        owned?.deadline !== undefined
          ? Effect.succeed(owned.deadline)
          : sql`SELECT MIN(due_at_millis) AS deadline FROM ${relation} p WHERE p.due_at_millis IS NOT NULL AND p.payload_json IS NOT NULL AND NOT EXISTS (SELECT 1 FROM ${relation} before_p WHERE before_p.owner_thread_id = p.owner_thread_id AND before_p.ordinal < p.ordinal AND before_p.payload_json IS NOT NULL)`.pipe(
              execute,
              Effect.mapError(failure),
              Effect.flatMap(
                Schema.decodeUnknownEffect(
                  Schema.Array(Schema.Struct({ deadline: Schema.NullOr(SqlInteger) })),
                ),
              ),
              Effect.mapError(failure),
              Effect.map((rows) => Option.fromNullishOr(rows[0]?.deadline)),
              Effect.tap((deadline) =>
                Effect.sync(() => {
                  if (owned !== undefined) owned.deadline = deadline;
                }),
              ),
            ),
      ),
    ),
    retryParked: (ownerThreadId, nowMillis) =>
      transaction(
        Effect.gen(function* () {
          yield* Schema.decodeEffect(Schema.Natural)(nowMillis).pipe(Effect.mapError(failure));

          const rows =
            yield* sql`SELECT id, owner_thread_id, ordinal, fingerprint, payload_json, due_at_millis FROM ${relation} WHERE owner_thread_id = ${ownerThreadId} AND payload_json IS NOT NULL ORDER BY ordinal LIMIT 1`.pipe(
              execute,
              Effect.mapError(failure),
              Effect.flatMap(decodeRows),
            );

          const head = rows[0];

          if (head === undefined || head.due_at_millis !== null) return;
          yield* sql`DELETE FROM ${retries} WHERE id = ${head.id}`.pipe(
            execute,
            Effect.mapError(failure),
          );
          yield* sql`UPDATE ${relation} SET due_at_millis = ${nowMillis} WHERE owner_thread_id = ${ownerThreadId} AND payload_json IS NOT NULL`.pipe(
            execute,
            Effect.mapError(failure),
          );
          const view = owned?.owners.get(ownerThreadId);

          if (view !== undefined) {
            view.attempts.set(head.id, 0);
            for (const row of view.rows.values()) {
              if (row.payload_json !== null)
                view.rows.set(row.id, { ...row, due_at_millis: nowMillis });
            }
          }
          invalidateDeadline();
        }),
      ),
  };

  return { retain, retainMany, storage };
});
