import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Effect, Layer } from "effect";
import { Command } from "effect/unstable/cli";

import { command, measurementHttpLayer } from "./measure.ts";

NodeRuntime.runMain(
  Command.run(command, { version: "0.0.0" }).pipe(
    Effect.scoped,
    Effect.provide(Layer.mergeAll(measurementHttpLayer, NodeServices.layer)),
  ),
);
