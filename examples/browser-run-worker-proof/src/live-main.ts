import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Console, Effect } from "effect";

import { liveWorkerProof } from "./workflow.ts";

NodeRuntime.runMain(
  liveWorkerProof.pipe(
    Effect.tap(({ name, result }) =>
      Console.log(
        `Browser Run Worker proof passed for ${result.fact}; temporary Worker ${name} was deleted`,
      ),
    ),
    Effect.provide(NodeServices.layer),
  ),
  { disableErrorReporting: false },
);
