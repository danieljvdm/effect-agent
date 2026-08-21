import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Cause, Console, Effect, Layer } from "effect";
import { Command as CliCommand } from "effect/unstable/cli";
import { FetchHttpClient } from "effect/unstable/http";

import { command } from "./command.ts";

// ---------------------------------------------------------------------------
// The executable boundary owns platform provisioning, Scope, signal handling,
// process completion, and concise diagnostics for unhandled failures.
// ---------------------------------------------------------------------------

const program = CliCommand.run(command, { version: "0.0.0" }).pipe(
  Effect.scoped,
  Effect.provide(Layer.merge(NodeServices.layer, FetchHttpClient.layer)),
  Effect.tapCause((cause) =>
    Cause.hasInterruptsOnly(cause)
      ? Effect.void
      : Console.error(
          Cause.prettyErrors(cause)
            .map((error) => error.message)
            .join("\n"),
        ),
  ),
);

NodeRuntime.runMain(program, { disableErrorReporting: true });
