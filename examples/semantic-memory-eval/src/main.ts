import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Cause, Console, Effect } from "effect";
import { Command } from "effect/unstable/cli";

import { command } from "./command.ts";

const program = Command.run(command, { version: "0.0.0" }).pipe(
  Effect.tapCause((cause) => Console.error(Cause.pretty(cause))),
  Effect.scoped,
  Effect.provide(NodeServices.layer),
);

NodeRuntime.runMain(program, { disableErrorReporting: true });
