import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Effect } from "effect";

import { benchmark } from "./workflow.ts";

NodeRuntime.runMain(benchmark.pipe(Effect.provide(NodeServices.layer)));
