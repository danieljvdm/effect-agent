import { IdGenerator } from "@effect-agent/core";
import { ConversationHistoryError } from "@effect-agent/session/history";
import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Console, Effect, Layer, Schema } from "effect";
import { Command } from "effect/unstable/cli";

import { historyCommand } from "./history.ts";

const program = Command.run(historyCommand, { version: "0.0.0" }).pipe(
  Effect.provide(Layer.merge(NodeServices.layer, IdGenerator.layer)),
  Effect.tapError((error) =>
    Console.error(
      Schema.is(ConversationHistoryError)(error) && error.reason === "not-found"
        ? "No saved history. Run seed with the same --database file first."
        : String(error),
    ),
  ),
);

NodeRuntime.runMain(program, { disableErrorReporting: true });
