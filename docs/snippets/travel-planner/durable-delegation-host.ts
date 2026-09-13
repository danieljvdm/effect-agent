import { NodeDurableHost } from "@effect-agent/platform-node";
import { Layer } from "effect";
import { Subagent } from "effect-agent";
import { SubagentReservationsMemoryLive } from "effect-agent/subagent-reservations";

import { Coordinator } from "./coordinator.ts";
import { Research } from "./delegation.ts";
import { definitions, ModelLive, OpenAiLive } from "./node-agent.ts";
import { TravelToolsLive } from "./tools.ts";

const ResearchLive = Subagent.layer(Research, ModelLive).pipe(Layer.provide(TravelToolsLive));

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

  Layer.provide(TravelToolsLive),
  Layer.provide(OpenAiLive),
);
