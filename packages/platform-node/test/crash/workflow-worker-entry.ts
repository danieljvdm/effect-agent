import { NodeCrypto, NodeRuntime, NodeServices } from "@effect/platform-node";
import { Effect, Layer } from "effect";

import { workflowCrashWorker } from "./workflow-worker.ts";

NodeRuntime.runMain(
  workflowCrashWorker.pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
  { disableErrorReporting: true },
);
