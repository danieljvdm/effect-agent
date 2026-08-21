import { Context, Effect, Layer, Ref } from "effect";

import type { SqliteStorageFailpointError, SqliteStorageFailpointLocation } from "./errors.ts";

export type SqliteStorageFailpointHandler = (
  location: SqliteStorageFailpointLocation,
) => Effect.Effect<void, SqliteStorageFailpointError>;

const noFailpoint: SqliteStorageFailpointHandler = () => Effect.void;

/** Test-only control for replacing the active SQLite failpoint handler. */
export class SqliteStorageFailpointTestControl extends Context.Service<
  SqliteStorageFailpointTestControl,
  {
    readonly clear: Effect.Effect<void>;
    readonly setHandler: (handler: SqliteStorageFailpointHandler) => Effect.Effect<void>;
  }
>()("@effect-agent/storage-sqlite/SqliteStorageFailpointTestControl") {}

/** Explicit fault-injection authority used at SQLite operation boundaries. */
export class SqliteStorageFailpoint extends Context.Service<
  SqliteStorageFailpoint,
  {
    readonly hit: SqliteStorageFailpointHandler;
  }
>()("@effect-agent/storage-sqlite/SqliteStorageFailpoint") {
  /** Production default: no fault injection. */
  static readonly layer = Layer.succeed(this)({ hit: noFailpoint });

  /** Reusable test Layer with a control service backed by the same handler Ref. */
  static readonly layerTest = Layer.effectContext(
    Effect.gen(function* () {
      const handler = yield* Ref.make<SqliteStorageFailpointHandler>(noFailpoint);
      return Context.make(
        SqliteStorageFailpoint,
        SqliteStorageFailpoint.of({
          hit: (location) => Ref.get(handler).pipe(Effect.flatMap((current) => current(location))),
        }),
      ).pipe(
        Context.add(
          SqliteStorageFailpointTestControl,
          SqliteStorageFailpointTestControl.of({
            clear: Ref.set(handler, noFailpoint),
            setHandler: (next) => Ref.set(handler, next),
          }),
        ),
      );
    }),
  );
}
