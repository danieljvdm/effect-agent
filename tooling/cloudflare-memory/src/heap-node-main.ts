import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Console, Effect, Schema } from "effect";
import { Argument, Command } from "effect/unstable/cli";

import { NodeSample } from "./heap-contracts.ts";
import { measureNode } from "./heap-node.ts";

const command = Command.make(
  "heap-node",
  {
    bundle: Argument.string("bundle").pipe(
      Argument.withDescription("Exact Wrangler JavaScript bundle to evaluate."),
    ),
  },
  ({ bundle }) =>
    measureNode(bundle).pipe(
      Effect.flatMap(Schema.encodeEffect(Schema.fromJsonString(NodeSample))),
      Effect.flatMap(Console.log),
    ),
);

NodeRuntime.runMain(
  Command.run(command, { version: "0.0.0" }).pipe(Effect.provide(NodeServices.layer)),
);
