import * as NodeHost from "@effect-agent/platform-node/NodeDurableHost";
import { Effect, Layer } from "effect";

import { authority, handlers, registrations, type Models } from "./agents.ts";
import { demonstration } from "./application.ts";
import { openAiModels } from "./openai.ts";

/** Reopen this SQLite file to resume outstanding builders, reports and peer messages. */
export const nodeHostWithModels = <R>(filename: string, models: Models<R>, modelVersion: string) =>
  NodeHost.layer(registrations(models, modelVersion), {
    filename,
    deploymentId: "durable-orchestration-v1",
    producerId: "node-orchestration",
    workerConcurrency: 3,
    wakeScanInterval: 25,
    settlementPollInterval: 25,
  }).pipe(Layer.provide([authority, handlers(models)]));

export const nodeHost = (filename: string) =>
  Layer.unwrap(
    openAiModels.pipe(
      Effect.map(({ models, modelVersion }) => nodeHostWithModels(filename, models, modelVersion)),
    ),
  );

/** Observe worker failures while interacting, so a stopped pool cannot leave the caller waiting. */
export const nodeDemonstration = Effect.raceFirst(
  demonstration,
  NodeHost.run.pipe(Effect.andThen(Effect.never)),
);
