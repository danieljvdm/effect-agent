import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Effect, Schema } from "effect";
import { Command, Flag } from "effect/unstable/cli";

import { benchmark } from "./benchmark.ts";

const command = Command.make(
  "perf:tool-selection",
  {
    output: Flag.String("output").pipe(
      Flag.withDescription("New JSON evidence file; parent directory must exist."),
    ),
    repetitions: Flag.Int("repetitions").pipe(
      Flag.withSchema(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 10 }))),
      Flag.withDefault(5),
    ),
    live: Flag.Boolean("live").pipe(
      Flag.withDefault(false),
      Flag.withDescription("Call paid OpenAI and JEV APIs using environment credentials."),
    ),
  },
  benchmark,
);

NodeRuntime.runMain(
  Command.run(command, { version: "1.0.0" }).pipe(
    Effect.scoped,
    Effect.provide(NodeServices.layer),
  ),
);
