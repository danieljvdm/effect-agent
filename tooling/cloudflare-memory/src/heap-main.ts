import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Effect } from "effect";
import { Command } from "effect/unstable/cli";
import { FetchHttpClient } from "effect/unstable/http";
import { Socket } from "effect/unstable/socket";

import { command } from "./heap.ts";

NodeRuntime.runMain(
  Command.run(command, { version: "0.0.0" }).pipe(
    Effect.scoped,
    Effect.provide([
      NodeServices.layer,
      FetchHttpClient.layer,
      Socket.layerWebSocketConstructorGlobal,
    ]),
  ),
);
