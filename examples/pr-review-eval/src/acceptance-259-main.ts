import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Effect } from "effect";

import { acceptance259 } from "./acceptance-259.ts";

NodeRuntime.runMain(acceptance259.pipe(Effect.scoped, Effect.provide(NodeServices.layer)));
