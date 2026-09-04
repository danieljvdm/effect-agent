import * as Agent from "@effect-agent/core/Agent";
import { AgentPolicy } from "@effect-agent/core/AgentPolicy";
import { ThreadId } from "@effect-agent/core/Identifiers";
import {
  NodeDurableAgentRuntime,
  type NodeDurableAgentRuntimeOptions,
} from "@effect-agent/platform-node/NodeDurableAgentRuntime";
import {
  NodeWorkflowRepairTrigger,
  SqlWorkflowDispatchStore,
} from "@effect-agent/platform-node/NodeWorkflow";
import { type AgentRegistration } from "@effect-agent/thread/AgentRegistration";
import { digestDefinitions } from "@effect-agent/thread/Digest";
import {
  DefinitionDigestInput,
  DeploymentId,
  type DefinitionDigests,
} from "@effect-agent/thread/Records";
import { IdempotencyKey, Principal } from "@effect-agent/thread/SubmissionLedger";
import { ThreadRead, ThreadStore } from "@effect-agent/thread/ThreadStore";
import { WorkflowAgentHost } from "@effect-agent/workflow/WorkflowAgentHost";
import {
  WorkflowDispatchScan,
  WorkflowDispatchStore,
} from "@effect-agent/workflow/WorkflowDispatch";
import { NodeCrypto } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { Effect, FileSystem, Layer, Ref, Schema, Stream } from "effect";
import { LanguageModel, Model, Toolkit, type Prompt, type Response } from "effect/unstable/ai";
import { ClusterWorkflowEngine, SingleRunner } from "effect/unstable/cluster";
import { WorkflowEngine } from "effect/unstable/workflow";

export const deploymentId = Schema.decodeSync(DeploymentId)("workflow-certification");
export const workflowPrefix = "effect-agent/certification/v1";
export const workflowName = `${workflowPrefix}/deployment/${deploymentId.length}:${deploymentId}`;
export const principal = Schema.decodeSync(Principal)("workflow-principal");
export const usage = { inputTokens: {}, outputTokens: {} };

export const finalParts = (answer = "done"): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: JSON.stringify({ answer }) },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage },
];

export const planner = Agent.make("workflow-planner", {
  input: Schema.Struct({ question: Schema.String }),
  output: Schema.Struct({ answer: Schema.String }),
  instructions: "Answer as JSON.",
  toolkit: Toolkit.empty,
  policy: AgentPolicy.make({
    maxTurns: 4,
    maxToolCalls: 4,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  }),
});

export const definitionsFor = (id: string) =>
  DefinitionDigestInput.make({
    agent: { id, version: 1 },
    model: { provider: "scripted", name: "workflow-test", version: 1 },
    tools: { version: 1 },
  });

export const makeModel = Effect.fn("WorkflowTest.makeModel")(function* (
  script: (call: number, prompt: Prompt.Prompt) => Stream.Stream<Response.StreamPartEncoded>,
) {
  const calls = yield* Ref.make(0);

  const model = Model.make(
    "scripted",
    "workflow-test",
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: ({ prompt }) =>
          Stream.unwrap(
            Ref.getAndUpdate(calls, (n) => n + 1).pipe(Effect.map((call) => script(call, prompt))),
          ),
      }),
    ),
  );

  return { model, calls };
});

export const makePlanner = Effect.fn("WorkflowTest.makePlanner")(function* (
  script: (call: number, prompt: Prompt.Prompt) => Stream.Stream<Response.StreamPartEncoded> = () =>
    Stream.fromIterable(finalParts()),
) {
  const scripted = yield* makeModel(script);
  const agent = Agent.withModel(planner, scripted.model);
  const definitions = definitionsFor(planner.id);
  const digests = yield* digestDefinitions(definitions);

  return { calls: scripted.calls, agent, definitions, digests };
});

export const submitOptions = (
  definitions: DefinitionDigests,
  thread = "workflow-thread",
  key = thread,
) => ({
  threadId: Schema.decodeSync(ThreadId)(thread),
  idempotencyKey: Schema.decodeSync(IdempotencyKey)(key),
  principal,
  definitions,
});

/** Native message storage shares the dispatch connection; the agent journal is a separate database. */
export const workflowInfrastructure = (directory: string) => {
  const sql = SqliteClient.layer({ filename: `${directory}/workflow.sqlite` });

  const engine = ClusterWorkflowEngine.layer.pipe(
    Layer.provide(
      SingleRunner.layer({
        runnerStorage: "memory",
        shardingConfig: {
          shardsPerGroup: 1,
          entityMessagePollInterval: "10 millis",
          entityReplyPollInterval: "10 millis",
          entityTerminationTimeout: "100 millis",
        },
      }),
    ),
  );

  return Layer.mergeAll(engine, SqlWorkflowDispatchStore.layer).pipe(
    Layer.provideMerge(sql),
    Layer.provide(NodeCrypto.layer),
  );
};

export const hostLayer = <const Entries extends ReadonlyArray<AgentRegistration>>(
  directory: string,
  registrations: Entries,
  options: Partial<NodeDurableAgentRuntimeOptions> = {},
  memoryEngine = false,
  repairBatchSize = 2,
) =>
  WorkflowAgentHost.layer({
    deploymentId,
    principal,
    workflowName: workflowPrefix,
    executionConcurrency: 1,
    repairBatchSize,
  }).pipe(
    Layer.provideMerge(
      NodeDurableAgentRuntime.layerRegistered(registrations, {
        filename: `${directory}/agent.sqlite`,
        deploymentId,
        producerId: "workflow-owner",
        ownershipLeaseDuration: 200,
        leaseRenewalInterval: 50,
        settlementPollInterval: 10,
        abortPollInterval: 10,
        ...options,
      }),
    ),
    Layer.provideMerge(
      memoryEngine
        ? Layer.mergeAll(WorkflowEngine.layerMemory, SqlWorkflowDispatchStore.layer).pipe(
            Layer.provideMerge(SqliteClient.layer({ filename: `${directory}/workflow.sqlite` })),
          )
        : workflowInfrastructure(directory),
    ),
    Layer.provide(NodeWorkflowRepairTrigger.layer({ interval: "10 millis" })),
    Layer.provide(NodeCrypto.layer),
  );

export const temporaryDirectory = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;

  return yield* fs.makeTempDirectoryScoped({ prefix: "effect-agent-workflow-" });
});

export const readLog = Effect.fn("WorkflowTest.readLog")(function* (thread: ThreadId) {
  const store = yield* ThreadStore;

  return yield* Stream.runCollect(store.read(new ThreadRead({ threadId: thread, limit: 1024 })));
});

export const pendingIntents = Effect.gen(function* () {
  const store = yield* WorkflowDispatchStore;

  return yield* store.scan(new WorkflowDispatchScan({ deploymentId, workflowName, limit: 1000 }));
});

export const until = Effect.fn("WorkflowTest.until")(function* <A, E, R>(
  effect: Effect.Effect<A, E, R>,
  predicate: (value: A) => boolean,
) {
  while (true) {
    const value = yield* effect;

    if (predicate(value)) return value;
    yield* Effect.sleep("5 millis");
  }
}, Effect.timeout("10 seconds"));
