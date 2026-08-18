import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Effect, Layer } from "effect";
import { FetchHttpClient } from "effect/unstable/http";

import { runActionsDelivery } from "./run-delivery.ts";

const program = runActionsDelivery().pipe(
  Effect.scoped,
  Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer)),
);

NodeRuntime.runMain(program, { disableErrorReporting: true });
