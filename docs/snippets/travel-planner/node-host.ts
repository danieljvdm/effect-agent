import { NodeDurableHost } from "@effect-agent/platform-node";
import { Layer } from "effect";

import { definitions, ModelLive, OpenAiLive, planner } from "./node-agent.ts";

export const HostLive = NodeDurableHost.layer([{ agent: planner, model: ModelLive, definitions }], {
  filename: "./agents.sqlite",
  deploymentId: "travel-planner",
  producerId: "worker-start-001",
  workerConcurrency: 4,
}).pipe(Layer.provide(OpenAiLive));
