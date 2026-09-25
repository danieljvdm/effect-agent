import { Crypto, DateTime, Effect, Option, Schema } from "effect";
import { digestJson } from "effect-agent/digest";
import {
  LifecyclePublication,
  LifecyclePublicationConfig,
  LifecyclePublicationError,
  type LifecyclePublicationStorage,
} from "effect-agent/lifecycle-publication";
import { SqlClient } from "effect/unstable/sql/SqlClient";

import { makeSqlQuery, SqlInteger } from "./SqlStorage.ts";

const codec = Schema.fromJsonString(LifecyclePublication);

const Row = Schema.Struct({
  id: Schema.String,
  owner_thread_id: Schema.String,
  ordinal: SqlInteger,
  fingerprint: Schema.String,
  payload_json: Schema.NullOr(Schema.String),
});

const failure = (cause: unknown) =>
  LifecyclePublicationError.make({ reason: "unavailable", cause });

/**
 * Optional native recovery obligations. Retain runs inside the caller's existing source write
 * transaction. Acknowledgement only drops the private payload; the stable identity/fingerprint
 * remains. These rows must never be cascade-deleted with a Thread or its projections.
 * The additive table has its own closed Schema; it does not change a native format in place.
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

  yield* sql`CREATE TABLE IF NOT EXISTS ${relation} (
    id TEXT PRIMARY KEY, owner_thread_id TEXT NOT NULL, ordinal BIGINT NOT NULL,
    fingerprint TEXT NOT NULL, payload_json TEXT, due_at_millis BIGINT,
    UNIQUE(owner_thread_id, ordinal)
  )`.pipe(execute, Effect.mapError(failure));
  yield* sql`CREATE INDEX IF NOT EXISTS effect_agent_lifecycle_pending ON ${relation}(due_at_millis, owner_thread_id, ordinal) WHERE due_at_millis IS NOT NULL`.pipe(
    execute,
    Effect.mapError(failure),
  );
  yield* sql`CREATE INDEX IF NOT EXISTS effect_agent_lifecycle_owner_pending ON ${relation}(owner_thread_id, ordinal) WHERE due_at_millis IS NOT NULL`.pipe(
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
    sql`SELECT id, owner_thread_id, ordinal, fingerprint, payload_json FROM ${relation} WHERE id = ${id}`.pipe(
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

  const verify = Effect.fn("SqlLifecyclePublication.verify")(function* (
    publication: LifecyclePublication,
  ) {
    const row = (yield* get(publication.id))[0];

    if (
      row === undefined ||
      row.owner_thread_id !== publication.ownerThreadId ||
      row.ordinal !== publication.ordinal ||
      row.fingerprint !== (yield* fingerprint(yield* encode(publication)))
    )
      return yield* LifecyclePublicationError.make({ reason: "conflict" });

    return row;
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

        // One oldest obligation per owner; a deferred predecessor blocks later facts even if due.
        const rows =
          yield* sql`SELECT p.id, p.owner_thread_id, p.ordinal, p.fingerprint, p.payload_json FROM ${relation} p WHERE p.due_at_millis <= ${nowMillis} AND NOT EXISTS (SELECT 1 FROM ${relation} before_p WHERE before_p.owner_thread_id = p.owner_thread_id AND before_p.ordinal < p.ordinal AND before_p.due_at_millis IS NOT NULL) ORDER BY p.due_at_millis, p.owner_thread_id, p.ordinal LIMIT ${limit}`.pipe(
            execute,
            Effect.mapError(failure),
            Effect.flatMap(decodeRows),
          );

        return yield* Effect.forEach(rows, (row) =>
          Effect.gen(function* () {
            if (row.payload_json === null)
              return yield* LifecyclePublicationError.make({ reason: "corrupt" });

            const publication = yield* Schema.decodeEffect(codec)(row.payload_json).pipe(
              Effect.mapError(failure),
            );

            yield* verify(publication);

            return publication;
          }),
        );
      }),
    acknowledge: (publication) =>
      Effect.gen(function* () {
        yield* verify(publication);
        yield* sql`UPDATE ${relation} SET payload_json = NULL, due_at_millis = NULL WHERE id = ${publication.id}`.pipe(
          execute,
          Effect.mapError(failure),
        );
      }),
    defer: (publication, untilMillis) =>
      Effect.gen(function* () {
        yield* Schema.decodeEffect(Schema.Natural)(untilMillis).pipe(Effect.mapError(failure));
        yield* verify(publication);
        yield* sql`UPDATE ${relation} SET due_at_millis = ${untilMillis} WHERE id = ${publication.id} AND payload_json IS NOT NULL`.pipe(
          execute,
          Effect.mapError(failure),
        );
      }),
    pendingDeadline:
      sql`SELECT MIN(due_at_millis) AS deadline FROM ${relation} p WHERE p.due_at_millis IS NOT NULL AND NOT EXISTS (SELECT 1 FROM ${relation} before_p WHERE before_p.owner_thread_id = p.owner_thread_id AND before_p.ordinal < p.ordinal AND before_p.due_at_millis IS NOT NULL)`.pipe(
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
    pendingDeadlineFor: (ownerThreadId) =>
      sql`SELECT due_at_millis AS deadline FROM ${relation} WHERE owner_thread_id = ${ownerThreadId} AND due_at_millis IS NOT NULL ORDER BY ordinal LIMIT 1`.pipe(
        execute,
        Effect.mapError(failure),
        Effect.flatMap(
          Schema.decodeUnknownEffect(Schema.Array(Schema.Struct({ deadline: SqlInteger }))),
        ),
        Effect.mapError(failure),
        Effect.map((rows) => Option.fromNullishOr(rows[0]?.deadline)),
      ),
  };

  return { retain, storage };
});
