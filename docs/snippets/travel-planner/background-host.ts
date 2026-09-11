import { NodeDurableHost } from "@effect-agent/platform-node";
import { Layer } from "effect";

import { WorkerAccessLive } from "./background-access.ts";
import { BackgroundCoordinator, ResearchBackground } from "./background-coordinator.ts";
import { Research } from "./delegation.ts";
import { definitions, ModelLive, OpenAiLive } from "./node-agent.ts";
import { TravelToolsLive } from "./tools.ts";

export const HostLive = NodeDurableHost.layer(
  [
    {
      agent: BackgroundCoordinator,
      model: ModelLive,
      definitions,
    },
    { agent: Research.target, model: ModelLive, definitions },
  ],
  {
    filename: "./agents.sqlite",
    deploymentId: "background-research",
    producerId: "worker-start-001",
    workerConcurrency: 4,
  },
).pipe(
  Layer.provide(ResearchBackground.layer),
  Layer.provide(TravelToolsLive),
  Layer.provide(WorkerAccessLive),
  Layer.provide(OpenAiLive),
);
