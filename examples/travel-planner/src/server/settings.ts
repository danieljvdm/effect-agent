import { Context, Effect, Layer, Schema } from "effect";
import { SqlClient } from "effect/unstable/sql/SqlClient";

import { PlannerError, PlannerSettings, defaultPlannerSettings } from "../domain.ts";

const StoredSettings = Schema.Struct({ version: Schema.Literal(1), settings: PlannerSettings });
const Rows = Schema.Array(Schema.Struct({ value: Schema.String }));

/** Application mutation seams; production does not inject failures. */
export const SettingsFailpoint = Context.Reference<{
  readonly hit: (
    point: "schema:before" | "schema:after" | "save:before" | "save:after",
  ) => Effect.Effect<void, PlannerError>;
}>("travel-planner/SettingsFailpoint", {
  defaultValue: () => ({ hit: () => Effect.void }),
});

export class PlannerSettingsStore extends Context.Service<
  PlannerSettingsStore,
  {
    readonly get: Effect.Effect<PlannerSettings, PlannerError>;
    readonly save: (settings: PlannerSettings) => Effect.Effect<PlannerSettings, PlannerError>;
  }
>()("travel-planner/PlannerSettingsStore") {}

const storageError = () =>
  new PlannerError({
    code: "storage",
    message:
      "Model preferences are unavailable or contain unsupported data. Refresh before retrying.",
  });

/** One account preference row; canonical submissions retain their own admitted settings. */
export const PlannerSettingsStoreLive = Layer.effect(
  PlannerSettingsStore,
  Effect.gen(function* () {
    const sql = yield* SqlClient;
    const failpoint = yield* SettingsFailpoint;

    yield* failpoint.hit("schema:before");
    yield* sql`CREATE TABLE IF NOT EXISTS travel_planner_settings (
    id INTEGER PRIMARY KEY CHECK (id = 1), value TEXT NOT NULL
  )`.pipe(Effect.mapError(storageError));
    yield* failpoint.hit("schema:after");

    const get = Effect.gen(function* () {
      const rows = yield* sql`SELECT value FROM travel_planner_settings WHERE id = 1`.pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(Rows)),
        Effect.mapError(storageError),
      );

      if (rows[0] === undefined) return defaultPlannerSettings;

      const stored = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(StoredSettings))(
        rows[0].value,
      ).pipe(Effect.mapError(storageError));

      return stored.settings;
    });

    const save = Effect.fn("PlannerSettingsStore.save")(function* (candidate: PlannerSettings) {
      const settings = yield* Schema.decodeUnknownEffect(PlannerSettings)(candidate).pipe(
        Effect.mapError(
          () => new PlannerError({ code: "invalid", message: "Invalid model preferences." }),
        ),
      );

      const encoded = yield* Schema.encodeEffect(Schema.fromJsonString(StoredSettings))({
        version: 1,
        settings,
      }).pipe(Effect.mapError(storageError));

      yield* failpoint.hit("save:before");
      yield* sql
        .withTransaction(
          Effect.gen(function* () {
            // Unsupported existing data must never be silently replaced by a default or new save.
            yield* get;
            yield* sql`INSERT INTO travel_planner_settings (id, value) VALUES (1, ${encoded})
        ON CONFLICT (id) DO UPDATE SET value = excluded.value`;
          }),
        )
        .pipe(Effect.catchTag("SqlError", storageError));
      yield* failpoint.hit("save:after");

      return settings;
    });

    return PlannerSettingsStore.of({ get, save });
  }),
);
