import { Context, Effect, Layer, Schema } from "effect";
import { SqlClient } from "effect/unstable/sql/SqlClient";

import {
  ConversationId,
  ConversationSummary,
  PlannerError,
  PublishedSite,
  Trip,
  TripSiteStore,
  type PublishTripRequest,
  type SaveTripRequest,
} from "../domain.ts";

const StoredTrip = Schema.Struct({ version: Schema.Literal(1), trip: Trip });
const StoredPublication = Schema.Struct({ version: Schema.Literal(1), site: PublishedSite });

const StoredConversation = Schema.Struct({
  version: Schema.Literal(1),
  conversation: ConversationSummary,
});

const Row = Schema.Struct({ value: Schema.String });
const Rows = Schema.Array(Row);

/** Test-only crash seams surround each application mutation; production is a no-op. */
export const TripFailpoint = Context.Reference<{
  readonly hit: (point: string) => Effect.Effect<void, PlannerError>;
}>("travel-planner/TripFailpoint", {
  defaultValue: () => ({ hit: (_point: string): Effect.Effect<void, PlannerError> => Effect.void }),
});

export class TripRepository extends Context.Service<
  TripRepository,
  {
    readonly list: Effect.Effect<ReadonlyArray<Trip>, PlannerError>;
    readonly listConversations: Effect.Effect<ReadonlyArray<ConversationSummary>, PlannerError>;
    readonly rememberConversation: (
      conversation: ConversationSummary,
    ) => Effect.Effect<void, PlannerError>;
    readonly get: (id: string) => Effect.Effect<Trip, PlannerError>;
    readonly conversationId: (id: string) => Effect.Effect<string, PlannerError>;
    readonly save: (
      request: SaveTripRequest,
      conversationId: string,
    ) => Effect.Effect<Trip, PlannerError>;
    readonly recordPublication: (site: PublishedSite) => Effect.Effect<void, PlannerError>;
  }
>()("travel-planner/TripRepository") {}

const storageError = () =>
  new PlannerError({
    code: "storage",
    message: "Trip storage is unavailable or contains unsupported data.",
  });

const conflict = () =>
  new PlannerError({
    code: "conflict",
    message: "This trip changed. Refresh it before saving or publishing.",
  });

/** Append-only revision rows. All local mutations share the Thread Object's SQL client. */
export const TripRepositoryLive = Layer.effect(
  TripRepository,
  Effect.gen(function* () {
    const sql = yield* SqlClient;
    const failpoint = yield* TripFailpoint;

    yield* failpoint.hit("schema:before");
    yield* sql
      .withTransaction(
        Effect.gen(function* () {
          yield* sql`CREATE TABLE IF NOT EXISTS travel_trip_revisions (
      trip_id TEXT NOT NULL, revision INTEGER NOT NULL, value TEXT NOT NULL,
      PRIMARY KEY (trip_id, revision)
    )`;
          yield* sql`CREATE TABLE IF NOT EXISTS travel_trip_publications (
      trip_id TEXT NOT NULL, revision INTEGER NOT NULL, value TEXT NOT NULL,
      PRIMARY KEY (trip_id, revision)
    )`;
          yield* sql`CREATE TABLE IF NOT EXISTS travel_trip_conversations (
      trip_id TEXT PRIMARY KEY, conversation_id TEXT NOT NULL
    )`;
          yield* sql`CREATE TABLE IF NOT EXISTS travel_conversations (
      conversation_id TEXT PRIMARY KEY, value TEXT NOT NULL
    )`;
        }),
      )
      .pipe(Effect.mapError(storageError));
    yield* failpoint.hit("schema:after");

    const decodeConversation = (value: string) =>
      Schema.decodeUnknownEffect(Schema.fromJsonString(StoredConversation))(value).pipe(
        Effect.map((stored) => stored.conversation),
        Effect.mapError(storageError),
      );

    const listConversations = sql`SELECT value FROM travel_conversations ORDER BY rowid DESC`.pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(Rows)),
      Effect.flatMap((rows) => Effect.forEach(rows, (row) => decodeConversation(row.value))),
      Effect.mapError(storageError),
    );

    // Reserve navigation before admission: even a lost admission response cannot hide the thread.
    // Retries retain the original title, and never replace unsupported stored data.
    const rememberConversation = Effect.fn("TripRepository.rememberConversation")(function* (
      conversation: ConversationSummary,
    ) {
      yield* failpoint.hit("conversation:before");

      const value = yield* Schema.encodeEffect(Schema.fromJsonString(StoredConversation))({
        version: 1,
        conversation,
      }).pipe(Effect.mapError(storageError));

      yield* sql
        .withTransaction(
          Effect.gen(function* () {
            const rows =
              yield* sql`SELECT value FROM travel_conversations WHERE conversation_id = ${conversation.conversationId}`.pipe(
                Effect.flatMap(Schema.decodeUnknownEffect(Rows)),
              );

            if (rows[0] !== undefined) {
              yield* decodeConversation(rows[0].value);

              return;
            }
            yield* sql`INSERT INTO travel_conversations (conversation_id, value) VALUES (${conversation.conversationId}, ${value})`;
          }),
        )
        .pipe(
          Effect.catchTag("SqlError", storageError),
          Effect.catchTag("SchemaError", storageError),
        );
      yield* failpoint.hit("conversation:after");
    });

    const decodeTrip = (value: string) =>
      Schema.decodeUnknownEffect(Schema.fromJsonString(StoredTrip))(value).pipe(
        Effect.map((stored) => stored.trip),
        Effect.mapError(storageError),
      );

    const publication = Effect.fn("TripRepository.publication")(function* (id: string) {
      const rows =
        yield* sql`SELECT value FROM travel_trip_publications WHERE trip_id = ${id} ORDER BY revision DESC LIMIT 1`.pipe(
          Effect.flatMap(Schema.decodeUnknownEffect(Rows)),
          Effect.mapError(storageError),
        );

      if (rows[0] === undefined) return null;

      return yield* Schema.decodeUnknownEffect(Schema.fromJsonString(StoredPublication))(
        rows[0].value,
      ).pipe(
        Effect.map((stored) => stored.site),
        Effect.mapError(storageError),
      );
    });

    const get = Effect.fn("TripRepository.get")(function* (id: string) {
      const rows =
        yield* sql`SELECT value FROM travel_trip_revisions WHERE trip_id = ${id} ORDER BY revision DESC LIMIT 1`.pipe(
          Effect.flatMap(Schema.decodeUnknownEffect(Rows)),
          Effect.mapError(storageError),
        );

      if (rows[0] === undefined)
        return yield* new PlannerError({ code: "not-found", message: "Trip not found." });
      const trip = yield* decodeTrip(rows[0].value);

      return { ...trip, published: yield* publication(id) };
    });

    const list = Effect.gen(function* () {
      const rows = yield* sql`SELECT value FROM travel_trip_revisions r WHERE revision = (
      SELECT MAX(revision) FROM travel_trip_revisions WHERE trip_id = r.trip_id
    ) ORDER BY trip_id`.pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(Rows)),
        Effect.mapError(storageError),
      );

      return yield* Effect.forEach(
        rows,
        Effect.fn("TripRepository.listItem")(function* (row) {
          const trip = yield* decodeTrip(row.value);

          return { ...trip, published: yield* publication(trip.id) };
        }),
      );
    });

    const conversationId = Effect.fn("TripRepository.conversationId")(function* (id: string) {
      const rows =
        yield* sql`SELECT conversation_id AS value FROM travel_trip_conversations WHERE trip_id = ${id}`.pipe(
          Effect.flatMap(Schema.decodeUnknownEffect(Rows)),
          Effect.mapError(storageError),
        );

      // Older trips get their own conversation without rewriting the original shared log.
      return yield* Schema.decodeUnknownEffect(ConversationId)(rows[0]?.value ?? `trip-${id}`).pipe(
        Effect.mapError(storageError),
      );
    });

    const save = Effect.fn("TripRepository.save")(function* (
      request: SaveTripRequest,
      conversation: string,
    ) {
      yield* failpoint.hit("save:before");

      const trip = yield* sql
        .withTransaction(
          Effect.gen(function* () {
            const previous = request.tripId === null ? null : yield* get(request.tripId);

            if (previous !== null && (yield* conversationId(previous.id)) !== conversation)
              return yield* new PlannerError({
                code: "invalid",
                message: "Open this trip's conversation before changing it.",
              });

            if (
              (previous === null && request.expectedRevision !== null) ||
              (previous !== null && previous.revision !== request.expectedRevision)
            )
              return yield* conflict();

            const next = yield* Schema.decodeUnknownEffect(Trip)({
              title: request.title,
              destination: request.destination,
              summary: request.summary,
              startDate: request.startDate,
              endDate: request.endDate,
              travelers: request.travelers,
              days: request.days,
              notes: request.notes,
              places: request.places ?? previous?.places ?? [],
              id: previous?.id ?? crypto.randomUUID(),
              revision: (previous?.revision ?? 0) + 1,
              published: previous?.published ?? null,
            }).pipe(
              Effect.mapError(
                () => new PlannerError({ code: "invalid", message: "Invalid trip details." }),
              ),
            );

            const valueJson = yield* Schema.encodeEffect(Schema.fromJsonString(StoredTrip))({
              version: 1,
              trip: next,
            }).pipe(Effect.mapError(storageError));

            yield* sql`INSERT INTO travel_trip_revisions (trip_id, revision, value) VALUES (${next.id}, ${next.revision}, ${valueJson})`;
            if (previous === null)
              yield* sql`INSERT INTO travel_trip_conversations (trip_id, conversation_id) VALUES (${next.id}, ${conversation})`;

            return next;
          }),
        )
        .pipe(Effect.catchTag("SqlError", () => storageError()));

      yield* failpoint.hit("save:after");

      return trip;
    });

    const recordPublication = Effect.fn("TripRepository.recordPublication")(function* (
      site: PublishedSite,
    ) {
      yield* failpoint.hit("publication:before");

      const value = yield* Schema.encodeEffect(Schema.fromJsonString(StoredPublication))({
        version: 1,
        site,
      }).pipe(Effect.mapError(storageError));

      yield* sql
        .withTransaction(
          Effect.gen(function* () {
            const previous =
              yield* sql`SELECT value FROM travel_trip_publications WHERE trip_id = ${site.tripId} AND revision = ${site.revision}`.pipe(
                Effect.flatMap(Schema.decodeUnknownEffect(Rows)),
                Effect.mapError(storageError),
              );

            if (previous[0] !== undefined) {
              const stored = yield* Schema.decodeUnknownEffect(
                Schema.fromJsonString(StoredPublication),
              )(previous[0].value).pipe(Effect.mapError(storageError));

              if (stored.site.commitId !== site.commitId) return yield* conflict();

              return;
            }
            yield* sql`INSERT INTO travel_trip_publications (trip_id, revision, value) VALUES (${site.tripId}, ${site.revision}, ${value})`;
          }),
        )
        .pipe(Effect.catchTag("SqlError", () => storageError()));
      yield* failpoint.hit("publication:after");
    });

    return TripRepository.of({
      list,
      listConversations,
      rememberConversation,
      get,
      conversationId,
      save,
      recordPublication,
    });
  }),
);

/** Shared by the native agent tool and RPC; never silently publishes a newer revision. */
export const publishTrip = Effect.fn("publishTrip")(function* (request: PublishTripRequest) {
  const repository = yield* TripRepository;
  const store = yield* TripSiteStore;
  const trip = yield* repository.get(request.tripId);

  if (trip.revision !== request.expectedRevision) return yield* conflict();
  if (trip.published?.revision === trip.revision) return trip.published;
  const site = yield* store.publish({ trip });

  if (site.tripId !== trip.id || site.revision !== trip.revision)
    return yield* new PlannerError({
      code: "publication",
      message: "The publication returned a different trip revision.",
    });
  yield* repository.recordPublication(site);

  return site;
});
