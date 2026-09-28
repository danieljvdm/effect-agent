import { Array, Crypto, DateTime, Effect, Option, Schema } from "effect";
import { digestJson } from "effect-agent/digest";
import {
  LifecyclePublication,
  LifecyclePublicationBatch,
  LifecyclePublicationConfig,
  LifecyclePublicationError,
  lifecyclePublicationBatchMaxFacts,
  type LifecyclePublicationStorage,
} from "effect-agent/lifecycle-publication";
import { SqlClient } from "effect/unstable/sql/SqlClient";

import { makeSqlQuery, SqlInteger } from "./SqlStorage.ts";

const codec = Schema.fromJsonString(LifecyclePublication);
const maxPendingPayloadBytes = 4 * 1024 * 1024;

const Row = Schema.Struct({
  id: Schema.String,
  owner_thread_id: Schema.String,
  ordinal: SqlInteger,
  fingerprint: Schema.String,
  payload_json: Schema.NullOr(Schema.String),
  due_at_millis: Schema.NullOr(SqlInteger),
});

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

  const get = (id: string) =>
    sql`SELECT id, owner_thread_id, ordinal, fingerprint, payload_json, due_at_millis FROM ${relation} WHERE id = ${id}`.pipe(
      execute,
      Effect.mapError(failure),
      Effect.flatMap(decodeRows),
    );

  const retain = Effect.fn("SqlLifecyclePublication.retain")(function* (
    input: Omit<LifecyclePublication, "ordinal" | "id"> & { readonly id?: string },
  ) {
    const existing = input.id === undefined ? undefined : (yield* get(input.id))[0];

    const counters =
      yield* sql`SELECT COALESCE(MAX(ordinal), 0) AS ordinal FROM ${relation} WHERE owner_thread_id = ${input.ownerThreadId}`.pipe(
        execute,
        Effect.mapError(failure),
        Effect.flatMap(
          Schema.decodeUnknownEffect(Schema.Array(Schema.Struct({ ordinal: SqlInteger }))),
        ),
        Effect.mapError(failure),
      );

    const ordinal = existing?.ordinal ?? (counters[0]?.ordinal ?? 0) + 1;

    const publication = LifecyclePublication.make({
      ...input,
      id: input.id ?? JSON.stringify([input.ownerThreadId, "lifecycle", ordinal]),
      ordinal,
    });

    const text = yield* encode(publication);

    if (new TextEncoder().encode(text).byteLength > maxStoredValueBytes || ordinal > 1_000_000)
      return yield* LifecyclePublicationError.make({ reason: "capacity" });
    const digest = yield* fingerprint(text);

    if (existing !== undefined) {
      if (existing.fingerprint !== digest)
        return yield* LifecyclePublicationError.make({ reason: "conflict" });

      return;
    }
    yield* sql`INSERT INTO ${relation}(id, owner_thread_id, ordinal, fingerprint, payload_json, due_at_millis) VALUES (${publication.id}, ${input.ownerThreadId}, ${ordinal}, ${digest}, ${text}, ${DateTime.toEpochMillis(input.createdAt)})`.pipe(
      execute,
      Effect.mapError(failure),
    );
  });

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

    return head;
  });

  const transaction = <A, E>(body: Effect.Effect<A, E>) =>
    sql.withTransaction(body).pipe(Effect.catchTag("SqlError", failure));

  const storage: LifecyclePublicationStorage = {
    pending: (nowMillis, limit) =>
      Effect.gen(function* () {
        yield* Schema.decodeEffect(
          Schema.Struct({
            nowMillis: Schema.Natural,
            limit: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 100 })),
          }),
        )({ nowMillis, limit }).pipe(Effect.mapError(failure));

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
          const sizes = yield* sql`SELECT ordinal, ${sql.onDialectOrElse({
            pg: () => sql`octet_length(payload_json)`,
            orElse: () => sql`length(CAST(payload_json AS BLOB))`,
          })} AS payload_bytes FROM ${relation}
            WHERE owner_thread_id = ${owner.owner_thread_id} AND payload_json IS NOT NULL
            ORDER BY ordinal LIMIT ${lifecyclePublicationBatchMaxFacts}`.pipe(
            execute,
            Effect.mapError(failure),
            Effect.flatMap(
              Schema.decodeUnknownEffect(
                Schema.Array(Schema.Struct({ ordinal: SqlInteger, payload_bytes: SqlInteger })),
              ),
            ),
            Effect.mapError(failure),
          );

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

          const rows =
            yield* sql`SELECT id, owner_thread_id, ordinal, fingerprint, payload_json, due_at_millis
            FROM ${relation} WHERE owner_thread_id = ${owner.owner_thread_id}
            AND ordinal <= ${lastOrdinal} AND payload_json IS NOT NULL ORDER BY ordinal`.pipe(
              execute,
              Effect.mapError(failure),
              Effect.flatMap(decodeRows),
            );

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

          batches.push(
            yield* Schema.decodeUnknownEffect(Schema.toType(LifecyclePublicationBatch))(facts).pipe(
              Effect.mapError(failure),
            ),
          );
          if (selectedBytes >= maxPendingPayloadBytes) break;
        }

        return batches;
      }),
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

          // Serialize claim attempts for the retained head, including on PostgreSQL.
          const attempts = yield* sql`INSERT INTO ${retries}(id, attempts) VALUES (${first.id}, 0)
        ON CONFLICT(id) DO UPDATE SET attempts = ${retries}.attempts RETURNING attempts`.pipe(
            execute,
            Effect.mapError(failure),
            Effect.flatMap(
              Schema.decodeUnknownEffect(
                Schema.NonEmptyArray(
                  Schema.Struct({
                    attempts: SqlInteger.pipe(
                      Schema.decodeTo(
                        Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 8 })),
                      ),
                    ),
                  }),
                ),
              ),
            ),
            Effect.mapError(failure),
          );

          const head = yield* verifyBatch(batch);

          if (
            head.payload_json === null ||
            head.due_at_millis === null ||
            head.due_at_millis > nowMillis
          )
            return false;

          const earlier =
            yield* sql`SELECT id FROM ${relation} WHERE owner_thread_id = ${first.ownerThreadId} AND ordinal < ${first.ordinal} AND payload_json IS NOT NULL LIMIT 1`.pipe(
              execute,
              Effect.mapError(failure),
            );

          if (earlier.length > 0) return false;
          const attempt = attempts[0].attempts + 1;

          if (attempt > 8) return false;

          const deadline =
            attempt === 8
              ? null
              : nowMillis + timeoutMillis + Math.min(60_000, 1_000 * 2 ** (attempt - 1));

          yield* sql`UPDATE ${retries} SET attempts = ${attempt} WHERE id = ${first.id}`.pipe(
            execute,
            Effect.mapError(failure),
          );
          yield* sql`UPDATE ${relation} SET due_at_millis = ${deadline} WHERE owner_thread_id = ${first.ownerThreadId} AND ordinal BETWEEN ${first.ordinal} AND ${Array.lastNonEmpty(batch).ordinal} AND payload_json IS NOT NULL`.pipe(
            execute,
            Effect.mapError(failure),
          );

          return true;
        }),
      ),
    pendingDeadline:
      sql`SELECT MIN(due_at_millis) AS deadline FROM ${relation} p WHERE p.due_at_millis IS NOT NULL AND p.payload_json IS NOT NULL AND NOT EXISTS (SELECT 1 FROM ${relation} before_p WHERE before_p.owner_thread_id = p.owner_thread_id AND before_p.ordinal < p.ordinal AND before_p.payload_json IS NOT NULL)`.pipe(
        execute,
        Effect.mapError(failure),
        Effect.flatMap(
          Schema.decodeUnknownEffect(
            Schema.Array(Schema.Struct({ deadline: Schema.NullOr(SqlInteger) })),
          ),
        ),
        Effect.mapError(failure),
        Effect.map((rows) => Option.fromNullishOr(rows[0]?.deadline)),
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
        }),
      ),
  };

  return { retain, storage };
});
