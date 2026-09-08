import { NodeCrypto, NodeRuntime, NodeServices } from "@effect/platform-node";
import { Console, Effect, Layer, Schema } from "effect";
import { Command } from "effect/unstable/cli";

import { command } from "./command.ts";
import { EvaluationError } from "./contracts.ts";

const program = Command.run(command, { version: "1.0.0" }).pipe(
  Effect.tapError((error) =>
    Console.error(
      Schema.is(EvaluationError)(error)
        ? `${error.stage}: ${error.message}`
        : `Context evaluation failed (${error._tag}). Check --help and the required environment configuration.`,
    ),
  ),
  Effect.scoped,
  Effect.provide(Layer.merge(NodeServices.layer, NodeCrypto.layer)),
);

NodeRuntime.runMain(program, { disableErrorReporting: true });
