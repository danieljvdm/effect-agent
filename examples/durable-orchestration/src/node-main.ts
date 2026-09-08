import { NodeRuntime } from "@effect/platform-node";
import { Effect, References } from "effect";

import { nodeDemonstration, nodeHost } from "./node.ts";

NodeRuntime.runMain(
  nodeDemonstration.pipe(
    Effect.tap((state) =>
      Effect.sync(() => process.stdout.write(`${JSON.stringify(state, null, 2)}\n`)),
    ),
    Effect.provide(nodeHost("orchestration.sqlite")),
    Effect.provideService(References.MinimumLogLevel, "Warn"),
    Effect.scoped,
  ),
);
