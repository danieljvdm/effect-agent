import { WorkerEnvironment } from "alchemy/Cloudflare/Workers/WorkerRuntime";
import { Effect } from "effect";

// Website.Vite owns deployment resources; these public bridges own only the runtime graph.
export const runtimeStack = { name: "effect-agent-travel-planner", stage: "runtime" };

/** The native bindings declared by alchemy.run.ts, supplied by Alchemy's event runtime. */
export const plannerEnvironment = Effect.map(WorkerEnvironment, (env) => env as Cloudflare.Env);
