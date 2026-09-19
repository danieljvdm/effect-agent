import { Context, Effect, Layer, Ref } from "effect";

import {
  PostgresStorageFailpoint,
  type PostgresStorageFailpointHandler,
} from "./PostgresStorageFailpoint.ts";

const noFailpoint: PostgresStorageFailpointHandler = () => Effect.void;

/** Test-only control for replacing the active Postgres failpoint handler. */
export class PostgresStorageFailpointTestControl extends Context.Service<
  PostgresStorageFailpointTestControl,
  {
    readonly clear: Effect.Effect<void>;
    readonly setHandler: (handler: PostgresStorageFailpointHandler) => Effect.Effect<void>;
  }
>()("@effect-agent/storage-postgres/PostgresStorageFailpointTestControl") {
  /** Reusable test Layer with a control service backed by the same handler Ref. */
  static readonly layer = Layer.effectContext(
    Effect.gen(function* () {
      const handler = yield* Ref.make<PostgresStorageFailpointHandler>(noFailpoint);

      return Context.make(
        PostgresStorageFailpoint,
        PostgresStorageFailpoint.of({
          hit: (location) => Ref.get(handler).pipe(Effect.flatMap((current) => current(location))),
        }),
      ).pipe(
        Context.add(
          PostgresStorageFailpointTestControl,
          PostgresStorageFailpointTestControl.of({
            clear: Ref.set(handler, noFailpoint),
            setHandler: (next) => Ref.set(handler, next),
          }),
        ),
      );
    }),
  );
}
