import { Context, Effect, Layer, Schema } from "effect";
import { SqlClient } from "effect/unstable/sql/SqlClient";

import { AppId, PlannerError, Revision, TripApp, TripId } from "../domain.ts";
import { TripFailpoint } from "../server/trips.ts";

export class AppRepository extends Context.Service<
  AppRepository,
  {
    readonly get: (tripId: string) => Effect.Effect<TripApp | null, PlannerError>;
    readonly getById: (appId: string) => Effect.Effect<TripApp | null, PlannerError>;
    readonly save: (
      app: TripApp,
      expectedRevision: number | null,
    ) => Effect.Effect<TripApp, PlannerError>;
  }
>()("travel-planner/trip-app/AppRepository") {}

const StoredApp = Schema.fromJsonString(
  Schema.Struct({ version: Schema.Literal(1), app: TripApp }).pipe(
    Schema.annotate({ parseOptions: { onExcessProperty: "error" } }),
  ),
);

const Rows = Schema.Array(
  Schema.Struct({ app_id: AppId, trip_id: TripId, revision: Revision, value: Schema.String }),
);

const storageError = () =>
  new PlannerError({
    code: "storage",
    message: "Trip app storage is unavailable or contains unsupported data.",
  });

const conflict = () =>
  new PlannerError({
    code: "conflict",
    message: "This trip app changed. Refresh it before saving.",
  });

const invalid = () => new PlannerError({ code: "invalid", message: "Invalid trip app." });

/** One host-owned account partition. Revisions are append-only; exact retries never restore an older head. */
export const AppRepositoryLive = Layer.effect(
  AppRepository,
  Effect.gen(function* () {
    const sql = yield* SqlClient;
    const failpoint = yield* TripFailpoint;

    yield* failpoint.hit("app:schema:before");
    yield* sql`CREATE TABLE IF NOT EXISTS travel_app_revisions (
    app_id TEXT NOT NULL, trip_id TEXT NOT NULL, revision INTEGER NOT NULL,
    value TEXT NOT NULL, PRIMARY KEY (app_id, revision), UNIQUE (trip_id, revision)
  )`.pipe(Effect.mapError(storageError));
    yield* failpoint.hit("app:schema:after");

    const decodeRows = Effect.fn("AppRepository.decodeRows")(function* (input: unknown) {
      const rows = yield* Schema.decodeUnknownEffect(Rows)(input).pipe(
        Effect.mapError(storageError),
      );

      return yield* Effect.forEach(rows, (row) =>
        Schema.decodeUnknownEffect(StoredApp)(row.value).pipe(
          Effect.flatMap(({ app }) =>
            app.id === row.app_id && app.tripId === row.trip_id && app.revision === row.revision
              ? Effect.succeed(app)
              : Effect.fail(storageError()),
          ),
          Effect.mapError(storageError),
        ),
      );
    });

    const get = Effect.fn("AppRepository.get")(function* (input: string) {
      const tripId = yield* Schema.decodeUnknownEffect(TripId)(input).pipe(
        Effect.mapError(invalid),
      );

      const rows =
        yield* sql`SELECT * FROM travel_app_revisions WHERE trip_id = ${tripId} ORDER BY revision DESC LIMIT 1`.pipe(
          Effect.flatMap(decodeRows),
          Effect.catchTag("SqlError", storageError),
        );

      return rows[0] ?? null;
    });

    const getById = Effect.fn("AppRepository.getById")(function* (input: string) {
      const appId = yield* Schema.decodeUnknownEffect(AppId)(input).pipe(Effect.mapError(invalid));

      const rows =
        yield* sql`SELECT * FROM travel_app_revisions WHERE app_id = ${appId} ORDER BY revision DESC LIMIT 1`.pipe(
          Effect.flatMap(decodeRows),
          Effect.catchTag("SqlError", storageError),
        );

      return rows[0] ?? null;
    });

    const save = Effect.fn("AppRepository.save")(function* (
      candidate: TripApp,
      expected: number | null,
    ) {
      const app = yield* Schema.decodeUnknownEffect(TripApp)(candidate).pipe(
        Effect.mapError(invalid),
      );

      const expectedRevision = yield* Schema.decodeUnknownEffect(Schema.NullOr(Revision))(
        expected,
      ).pipe(Effect.mapError(invalid));

      const value = yield* Schema.encodeEffect(StoredApp)({ version: 1, app }).pipe(
        Effect.mapError(invalid),
      );

      yield* failpoint.hit("app:save:before");

      const saved = yield* sql
        .withTransaction(
          Effect.gen(function* () {
            const previous = yield* get(app.tripId);
            const byId = yield* getById(app.id);

            if (
              (previous !== null && previous.id !== app.id) ||
              (byId !== null && byId.tripId !== app.tripId)
            )
              return yield* conflict();

            const repeated =
              yield* sql`SELECT * FROM travel_app_revisions WHERE app_id = ${app.id} AND revision = ${app.revision}`.pipe(
                Effect.flatMap(decodeRows),
              );

            if (repeated[0] !== undefined) {
              const encoded = yield* Schema.encodeEffect(StoredApp)({
                version: 1,
                app: repeated[0],
              }).pipe(Effect.mapError(storageError));

              if (encoded !== value) return yield* conflict();

              return repeated[0];
            }
            if (
              (previous?.revision ?? null) !== expectedRevision ||
              app.revision !== (expectedRevision ?? 0) + 1
            )
              return yield* conflict();
            yield* sql`INSERT INTO travel_app_revisions (app_id, trip_id, revision, value) VALUES (${app.id}, ${app.tripId}, ${app.revision}, ${value})`;

            return app;
          }),
        )
        .pipe(Effect.catchTag("SqlError", storageError));

      yield* failpoint.hit("app:save:after");

      return saved;
    });

    return AppRepository.of({ get, getById, save });
  }),
);
