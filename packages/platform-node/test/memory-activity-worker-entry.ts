import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Effect } from "effect";

import { memoryActivityWorker } from "./memory-activity-worker.ts";

NodeRuntime.runMain(memoryActivityWorker.pipe(Effect.provide(NodeServices.layer)), {
  disableErrorReporting: false,
});
