import { Context, Effect, Layer } from "effect";

import type {
  SqliteStorageFailpointError,
  SqliteStorageFailpointLocation,
} from "./SqliteStorageError.ts";

export type SqliteStorageFailpointHandler = (
  location: SqliteStorageFailpointLocation,
) => Effect.Effect<void, SqliteStorageFailpointError>;

const noFailpoint: SqliteStorageFailpointHandler = () => Effect.void;

/** Explicit fault-injection authority used at SQLite operation boundaries. */
export class SqliteStorageFailpoint extends Context.Service<
  SqliteStorageFailpoint,
  {
    readonly hit: SqliteStorageFailpointHandler;
  }
>()("@effect-agent/storage-sqlite/SqliteStorageFailpoint") {
  /** Production default: no fault injection. */
  static readonly layer = Layer.succeed(this)({ hit: noFailpoint });
}
