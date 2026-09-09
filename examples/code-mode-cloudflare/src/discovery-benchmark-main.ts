import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Console, Effect, Schema } from "effect";
import { Command } from "effect/unstable/cli";

import { command } from "./discovery-benchmark-command.ts";
import { BenchmarkError } from "./discovery-benchmark-contracts.ts";

Command.run(command, { version: "tool-discovery-v1" }).pipe(
  Effect.tapError((error) =>
    Console.error(
      Schema.is(BenchmarkError)(error)
        ? error.message
        : `Benchmark failed (${error._tag}); inspect the report or --help.`,
    ),
  ),
  Effect.scoped,
  Effect.provide(NodeServices.layer),
  (program) => NodeRuntime.runMain(program, { disableErrorReporting: true }),
);
