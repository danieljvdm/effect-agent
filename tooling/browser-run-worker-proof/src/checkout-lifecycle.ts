import { Effect } from "effect";

/** Register retirement before deployment, including partially acknowledged provisioning. */
export const withRetirement = <A, E, R, E2, R2>(
  run: Effect.Effect<A, E, R>,
  retire: Effect.Effect<void, E2, R2>,
) => run.pipe(Effect.ensuring(retire.pipe(Effect.orDie)));
