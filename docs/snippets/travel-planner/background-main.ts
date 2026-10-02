import { NodeRuntime } from "@effect/platform-node";
import { NodeDurableHost } from "@yielded/agent-platform-node";
import { Effect } from "effect";

import { HostLive } from "./background-host.ts";

NodeRuntime.runMain(NodeDurableHost.run.pipe(Effect.provide(HostLive)));
