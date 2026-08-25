import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { ConfigProvider, Effect, Layer } from "effect";
import { FetchHttpClient } from "effect/unstable/http";

import { reviewActionProgram, withActionInputs } from "./action.ts";

const configProvider = withActionInputs(ConfigProvider.fromEnv());

NodeRuntime.runMain(
  reviewActionProgram.pipe(
    Effect.scoped,
    Effect.provideService(ConfigProvider.ConfigProvider, configProvider),
    Effect.provide(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer)),
  ),
  { disableErrorReporting: true },
);
