import { WorkerEnvironment as AlchemyEnvironment } from "alchemy/Cloudflare/Workers/WorkerRuntime";
import { Effect, Layer } from "effect";
import { WorkerEnvironment } from "effect-cf";

// Website.Vite owns deployment resources; these public bridges own only the runtime graph.
export const runtimeStack = { name: "effect-agent-travel-planner", stage: "runtime" };

/** Existing R2, Workflow, and Sandbox adapters consume the same native Worker bindings. */
export const plannerEnvironment = Layer.effect(WorkerEnvironment)(
  Effect.map(AlchemyEnvironment, (env) => env as Cloudflare.Env),
);
