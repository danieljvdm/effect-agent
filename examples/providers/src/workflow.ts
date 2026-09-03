import { NodeDurableRuntime } from "@effect-agent/platform-node";
import {
  NodeWorkflowRepairTrigger,
  SqlWorkflowDispatchStore,
} from "@effect-agent/platform-node/workflow";
import type { AgentRegistration } from "@effect-agent/thread";
import { WorkflowDurableHost } from "@effect-agent/workflow";
import { NodeCrypto } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { Layer } from "effect";
import { ClusterWorkflowEngine, SingleRunner } from "effect/unstable/cluster";
import type { WorkflowEngine } from "effect/unstable/workflow";

/** Single-process runner bookkeeping; native messages and replies still require SQL storage. */
export const SqlWorkflowEngine = ClusterWorkflowEngine.layer.pipe(
  Layer.provide(SingleRunner.layer({ runnerStorage: "memory" })),
);

/** Inject the engine without changing the Agent registrations or durable driver. */
export const workflowHost = <
  const Entries extends ReadonlyArray<AgentRegistration>,
  EngineError,
  EngineRequirements,
>(
  registrations: Entries,
  engine: Layer.Layer<WorkflowEngine.WorkflowEngine, EngineError, EngineRequirements>,
  options: {
    readonly agentDatabase: string;
    readonly workflowDatabase: string;
    readonly deploymentId: string;
    readonly producerId: string;
  },
) => {
  const workflowDatabase = SqliteClient.layer({ filename: options.workflowDatabase });

  const infrastructure = Layer.mergeAll(engine, SqlWorkflowDispatchStore.layer).pipe(
    Layer.provide(workflowDatabase),
  );

  return WorkflowDurableHost.layerRegistered(registrations, {
    deploymentId: options.deploymentId,
    executionConcurrency: 4,
    repairBatchSize: 32,
    dispatchTimeoutMillis: 10_000,
  }).pipe(
    Layer.provide(
      NodeDurableRuntime.layer({
        filename: options.agentDatabase,
        deploymentId: options.deploymentId,
        producerId: options.producerId,
      }),
    ),
    Layer.provide(infrastructure),
    Layer.provide(NodeWorkflowRepairTrigger.layer({ interval: "1 second" })),
    Layer.provide(NodeCrypto.layer),
  );
};
