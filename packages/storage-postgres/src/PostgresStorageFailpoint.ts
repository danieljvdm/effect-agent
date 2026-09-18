import { Context, Effect, Layer } from "effect";

import type {
  PostgresStorageFailpointError,
  PostgresStorageFailpointLocation,
} from "./PostgresStorageError.ts";

export type PostgresStorageFailpointHandler = (
  location: PostgresStorageFailpointLocation,
) => Effect.Effect<void, PostgresStorageFailpointError>;

const noFailpoint: PostgresStorageFailpointHandler = () => Effect.void;

/** Explicit fault-injection authority used at Postgres operation boundaries. */
export class PostgresStorageFailpoint extends Context.Service<
  PostgresStorageFailpoint,
  {
    readonly hit: PostgresStorageFailpointHandler;
  }
>()("@effect-agent/storage-postgres/PostgresStorageFailpoint") {
  /** Production default: no fault injection. */
  static readonly layer = Layer.succeed(this)({ hit: noFailpoint });
}
