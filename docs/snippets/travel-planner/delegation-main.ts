import { NodeRuntime } from "@effect/platform-node";
import { Console, Effect } from "effect";

import { program } from "./delegation-live.ts";

NodeRuntime.runMain(program.pipe(Effect.tap(({ output }) => Console.log(output))));
