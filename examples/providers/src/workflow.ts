import { Agent } from "@effect-agent/core";
import { NodeDurableAgentRuntime } from "@effect-agent/platform-node";
import {
  NodeWorkflowRepairTrigger,
  SqlWorkflowDispatchStore,
} from "@effect-agent/platform-node/workflow";
import type { AgentRegistration } from "@effect-agent/thread";
import { AgentWorkflow, WorkflowAgentHost } from "@effect-agent/workflow";
import { NodeCrypto } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { Layer, Schema } from "effect";
import { Toolkit } from "effect/unstable/ai";
import { ClusterWorkflowEngine, SingleRunner } from "effect/unstable/cluster";
import { Workflow, type WorkflowEngine } from "effect/unstable/workflow";

export const triage = Agent.make("triage", {
  input: Schema.String,
  output: Schema.Struct({
    severity: Schema.Literals(["low", "high", "critical"]),
    summary: Schema.String,
  }),
  instructions: "Triage the bug report and explain its severity.",
  toolkit: Toolkit.empty,
});

export const Review = Workflow.make("Review", {
  payload: { issueId: Schema.String, report: Schema.String },
  success: triage.output,
  error: AgentWorkflow.Error,
  idempotencyKey: ({ issueId }) => issueId,
});

export const ReviewLive = Review.toLayer(({ report }) =>
  AgentWorkflow.execute(triage, report, { name: "triage" }),
);

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
    readonly principal: string;
  },
) => {
  const workflowDatabase = SqliteClient.layer({ filename: options.workflowDatabase });

  const infrastructure = Layer.mergeAll(engine, SqlWorkflowDispatchStore.layer).pipe(
    Layer.provide(workflowDatabase),
  );

  return WorkflowAgentHost.layer({
    deploymentId: options.deploymentId,
    principal: options.principal,
    executionConcurrency: 4,
    repairBatchSize: 32,
    dispatchTimeoutMillis: 10_000,
  }).pipe(
    Layer.provide(
      NodeDurableAgentRuntime.layerRegistered(registrations, {
        filename: options.agentDatabase,
        deploymentId: options.deploymentId,
        producerId: options.producerId,
      }),
    ),
    Layer.provideMerge(infrastructure),
    Layer.provide(NodeWorkflowRepairTrigger.layer({ interval: "1 second" })),
    Layer.provide(NodeCrypto.layer),
  );
};
