import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Console, Effect } from "effect";
import { Command } from "effect/unstable/cli";

import { command } from "./command.ts";
import { CURRENT_RUNNER_VERSION } from "./contracts.ts";

const program = Command.run(command, { version: CURRENT_RUNNER_VERSION }).pipe(
  Effect.tapError((error) => Console.error(String(error))),
  Effect.scoped,
  Effect.provide(NodeServices.layer),
);

NodeRuntime.runMain(program, { disableErrorReporting: true });
