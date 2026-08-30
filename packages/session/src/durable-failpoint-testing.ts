import { Context, Effect, Layer, Ref } from "effect";

import {
  DurableRuntimeFailpoint,
  type DurableRuntimeFailpointHandler,
} from "./durable-failpoint.ts";

const noFailpoint: DurableRuntimeFailpointHandler = () => Effect.void;

/** Test-only control for replacing the active coordinator failpoint handler. */
export class DurableRuntimeFailpointTestControl extends Context.Service<
  DurableRuntimeFailpointTestControl,
  {
    readonly clear: Effect.Effect<void>;
    readonly setHandler: (handler: DurableRuntimeFailpointHandler) => Effect.Effect<void>;
  }
>()("@effect-agent/session/DurableRuntimeFailpointTestControl") {
  /** Reusable test Layer with a control service backed by the same handler Ref. */
  static readonly layer = Layer.effectContext(
    Effect.gen(function* () {
      const handler = yield* Ref.make<DurableRuntimeFailpointHandler>(noFailpoint);
      return Context.make(
        DurableRuntimeFailpoint,
        DurableRuntimeFailpoint.of({
          hit: (location) => Ref.get(handler).pipe(Effect.flatMap((current) => current(location))),
        }),
      ).pipe(
        Context.add(
          DurableRuntimeFailpointTestControl,
          DurableRuntimeFailpointTestControl.of({
            clear: Ref.set(handler, noFailpoint),
            setHandler: (next) => Ref.set(handler, next),
          }),
        ),
      );
    }),
  );
}
