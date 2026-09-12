import { SubagentRuntime } from "@effect-agent/capabilities/Subagent";
import { SubagentReservationsMemoryLive } from "@effect-agent/capabilities/SubagentReservations";
import { IdGenerator } from "@effect-agent/core/IdGenerator";
import { NodeDurableHost } from "@effect-agent/platform-node";
import { Layer } from "effect";

import { Coordinator } from "./coordinator.ts";
import { Research, ResearchFailed } from "./delegation.ts";
import { definitions, ModelLive, OpenAiLive } from "./node-agent.ts";
import { TravelToolsLive } from "./tools.ts";

const ResearchLive = SubagentRuntime.layer(Research, ModelLive, {
  mapChildFailure: (error) => ResearchFailed.make({ reason: error._tag }),
}).pipe(Layer.provide(TravelToolsLive));

export const HostLive = NodeDurableHost.layer(
  [
    { agent: Coordinator, model: ModelLive, definitions },
    { agent: Research.target, model: ModelLive, definitions },
  ],
  {
    filename: "./agents.sqlite",
    deploymentId: "attached-research",
    producerId: "worker-start-001",
    workerConcurrency: 4,
  },
).pipe(
  Layer.provide(ResearchLive),
  Layer.provide(SubagentReservationsMemoryLive),
  Layer.provide(IdGenerator.layer),
  Layer.provide(TravelToolsLive),
  Layer.provide(OpenAiLive),
);
