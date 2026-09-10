import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Console, Effect, Layer } from "effect";
import { FetchHttpClient } from "effect/unstable/http";

import { runWorkerProof, workerDeploymentLayer } from "./workflow.ts";

NodeRuntime.runMain(
  runWorkerProof.pipe(
    Effect.tap(({ name, result }) =>
      Console.log(
        `Browser Run Worker proof passed for ${result.fact}; temporary Worker ${name} was deleted`,
      ),
    ),
    Effect.provide(
      workerDeploymentLayer.pipe(
        Layer.provideMerge(Layer.mergeAll(FetchHttpClient.layer, NodeServices.layer)),
      ),
    ),
  ),
  { disableErrorReporting: false },
);
