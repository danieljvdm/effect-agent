import { NodeDurableHost } from "@effect-agent/platform-node";
import { NodeRuntime } from "@effect/platform-node";
import { Effect } from "effect";

import { HostLive } from "./node-host.ts";

NodeRuntime.runMain(NodeDurableHost.run.pipe(Effect.provide(HostLive)));
