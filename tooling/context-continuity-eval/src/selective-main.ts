import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Effect } from "effect";
import { Command } from "effect/unstable/cli";

import { command } from "./selective-spike.ts";

NodeRuntime.runMain(
  Command.run(command, { version: "0.1.0" }).pipe(
    Effect.scoped,
    Effect.provide(NodeServices.layer),
  ),
);
