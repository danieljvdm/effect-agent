import { NodeRuntime } from "@effect/platform-node";
import { Config, Effect, References } from "effect";

import { nodeDemonstration, nodeHost } from "./node.ts";

NodeRuntime.runMain(
  Effect.gen(function* () {
    const filename = yield* Config.nonEmptyString("ORCHESTRATION_DATABASE").pipe(
      Config.withDefault("orchestration-gpt-5.6-sol.sqlite"),
    );

    yield* Effect.sync(() =>
      process.stderr.write(`Running OpenAI orchestration; state: ${filename}\n`),
    );
    const state = yield* nodeDemonstration.pipe(Effect.provide(nodeHost(filename)));

    yield* Effect.sync(() => process.stdout.write(`${JSON.stringify(state, null, 2)}\n`));
  }).pipe(
    Effect.provideService(References.MinimumLogLevel, "Info"),
    Effect.provideService(References.LogToStderr, true),
    Effect.scoped,
  ),
);
