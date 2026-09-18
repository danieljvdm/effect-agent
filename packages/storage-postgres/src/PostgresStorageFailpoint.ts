import { Context, Effect, Layer } from "effect";

import type { PostgresStorageOptions } from "./PostgresStorageConfig.ts";
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

/** The failpoint Layer selected by convenience options: explicit handler or the no-op default. */
export const layerFailpoint = (
  options: PostgresStorageOptions,
): Layer.Layer<PostgresStorageFailpoint> =>
  options.failpoint === undefined
    ? PostgresStorageFailpoint.layer
    : Layer.succeed(PostgresStorageFailpoint)({ hit: options.failpoint });
