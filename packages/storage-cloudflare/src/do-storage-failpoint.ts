import { Context, Effect, Layer } from "effect";

import type { DoStorageFailpointError, DoStorageFailpointLocation } from "./errors.ts";

export type DoStorageFailpointHandler = (
  location: DoStorageFailpointLocation,
) => Effect.Effect<void, DoStorageFailpointError>;

const noFailpoint: DoStorageFailpointHandler = () => Effect.void;

/** Explicit fault-injection authority used at Durable Object storage operation boundaries. */
export class DoStorageFailpoint extends Context.Service<
  DoStorageFailpoint,
  {
    readonly hit: DoStorageFailpointHandler;
  }
>()("@effect-agent/storage-cloudflare/DoStorageFailpoint") {
  /** Production default: no fault injection. */
  static readonly layer = Layer.succeed(this)({ hit: noFailpoint });
}
