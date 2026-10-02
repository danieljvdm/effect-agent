import { BunRuntime, BunServices } from "@effect/platform-bun";
import { Effect, Layer } from "effect";

import { journey, telemetry } from "./journey.ts";

BunRuntime.runMain(
  journey.pipe(Effect.scoped, Effect.provide(Layer.merge(telemetry, BunServices.layer))),
);
