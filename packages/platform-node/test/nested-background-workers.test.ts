import * as Subagent from "@effect-agent/capabilities/Subagent";
import { SubagentReservationsMemoryLive } from "@effect-agent/capabilities/SubagentReservations";
import * as Agent from "@effect-agent/core/Agent";
import { ThreadId } from "@effect-agent/core/Identifiers";
import { IdGenerator } from "@effect-agent/core/IdGenerator";
import { SubagentGrant } from "@effect-agent/core/SubagentContract";
import { WorkerError } from "@effect-agent/core/Worker";
import { SubagentHost } from "@effect-agent/engine/SubagentHost";
import * as NodeHost from "@effect-agent/platform-node/NodeDurableHost";
import { DurableAgentRuntime } from "@effect-agent/thread/DurableAgentRuntime";
import { DefinitionDigestInput } from "@effect-agent/thread/Records";
import { IdempotencyKey, Principal } from "@effect-agent/thread/SubmissionLedger";
import { ThreadExportRequest, ThreadStore } from "@effect-agent/thread/ThreadStore";
import { WorkerHostAuthorizer } from "@effect-agent/thread/WorkerHost";
import { NodeFileSystem } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Context, Deferred, Effect, FileSystem, Layer, Ref, Schema, Stream } from "effect";
import { LanguageModel, Model, Toolkit, type Prompt, type Response } from "effect/unstable/ai";

const principal = Schema.decodeSync(Principal)("nested-owner");
const rootThreadId = Schema.decodeSync(ThreadId)("nested-root");
const key = Schema.decodeSync(IdempotencyKey);
const input = Schema.Struct({ question: Schema.String });
const output = Schema.Struct({ answer: Schema.String });
const usage = { inputTokens: {}, outputTokens: {} };

const finalParts = (answer: string): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: JSON.stringify({ answer }) },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage },
];

const toolParts = (name: string): ReadonlyArray<Response.StreamPartEncoded> => [
  {
    type: "tool-call",
    id: `${name}-call`,
    name,
    params: { question: name },
    providerExecuted: false,
  },
  { type: "finish", reason: "tool-calls", usage },
];

const makeModel = Effect.fn("NestedWorkers.makeModel")(function* (
  name: string,
  script: (call: number, prompt: Prompt.Prompt) => Stream.Stream<Response.StreamPartEncoded>,
) {
  const calls = yield* Ref.make(0);
  const visibleTools = yield* Ref.make<ReadonlyArray<ReadonlyArray<string>>>([]);
  const prompts = yield* Ref.make<ReadonlyArray<Prompt.Prompt>>([]);

  const model = Model.make(
    "scripted",
    name,
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: (options) =>
          Stream.unwrap(
            Effect.gen(function* () {
              const call = yield* Ref.getAndUpdate(calls, (count) => count + 1);

              yield* Ref.update(visibleTools, (previous) => [
                ...previous,
                options.tools.map((tool) => tool.name),
              ]);
              yield* Ref.update(prompts, (previous) => [...previous, options.prompt]);

              return script(call, options.prompt);
            }),
          ),
      }),
    ),
  );

  return { model, calls, visibleTools, prompts };
});

it.live(
  "settles the root independently while its background builder attaches a depth-two scout on one worker",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "nested-workers-" });
        const scoutEntered = yield* Deferred.make<void>();
        const releaseScout = yield* Deferred.make<void>();

        const scoutModel = yield* makeModel("scout", () =>
          Stream.fromEffect(
            Deferred.succeed(scoutEntered, undefined).pipe(
              Effect.andThen(Deferred.await(releaseScout)),
            ),
          ).pipe(Stream.flatMap(() => Stream.fromIterable(finalParts("scout finding")))),
        );

        const scout = Agent.withModel(
          Agent.make("nested-scout", {
            input,
            output,
            instructions: "Answer as JSON.",
            toolkit: Toolkit.empty,
            policy: {
              maxTurns: 1,
              maxToolCalls: 1,
              maxDuration: "20 seconds",
              toolConcurrency: 1,
              toolResultBounds: { maxBytes: 1024 },
            },
          }),
          scoutModel.model,
        );

        const scoutDeclaration = Subagent.make("scout", {
          target: scout.definition,
          success: output,
          projectResult: (result) => Effect.succeed(result),
          grant: SubagentGrant.make({
            allowedToolNames: [],
            maxDepth: 2,
            childLifetimes: ["attached"],
          }),
          policy: Subagent.SubagentPolicy.make({
            maxChildren: 1,
            maxConcurrency: 1,
            maxTurns: 1,
            maxToolCalls: 1,
            maxDuration: "20 seconds",
            maxResultBytes: 1024,
          }),
        });

        const scoutBackground = Subagent.background(scoutDeclaration, { start: true });

        const builderModel = yield* makeModel("builder", (call) =>
          Stream.fromIterable(call === 0 ? toolParts("scout") : finalParts("builder complete")),
        );

        const builder = Agent.withModel(
          Agent.make("nested-builder", {
            input,
            output,
            instructions: "Attach one scout, then answer as JSON.",
            toolkit: Toolkit.make(scoutDeclaration.tool, scoutBackground.tools.scout_start),
            policy: {
              maxTurns: 2,
              maxToolCalls: 1,
              maxDuration: "30 seconds",
              toolConcurrency: 1,
              toolResultBounds: { maxBytes: 1024 },
            },
          }),
          builderModel.model,
        );

        const build = Subagent.make("build", {
          target: builder.definition,
          success: output,
          projectResult: (result) => Effect.succeed(result),
          grant: SubagentGrant.make({
            allowedToolNames: ["scout", "scout_start"],
            maxDepth: 2,
            childLifetimes: ["attached"],
          }),
          policy: Subagent.SubagentPolicy.make({
            maxChildren: 2,
            maxConcurrency: 2,
            maxTurns: 4,
            maxToolCalls: 2,
            maxDuration: "1 minute",
            maxResultBytes: 2048,
            descendantInvocations: 1,
          }),
        });

        const background = Subagent.background(build, { start: true });

        const rootModel = yield* makeModel("root", (call) =>
          Stream.fromIterable(call === 0 ? toolParts("build_start") : finalParts("root complete")),
        );

        const root = Agent.withModel(
          Agent.make("nested-root-agent", {
            input,
            output,
            instructions: "Start one builder and finish without waiting for it.",
            toolkit: background.toolkit,
            policy: {
              maxTurns: 8,
              maxToolCalls: 4,
              maxDuration: "2 minutes",
              toolConcurrency: 2,
            },
          }),
          rootModel.model,
        );

        const authority = Layer.succeed(WorkerHostAuthorizer)({
          authorize: (request) =>
            request.principal === principal
              ? Effect.succeed(principal)
              : WorkerError.make({ operation: request.operation, reason: "denied" }),
        });

        const handlers = Layer.mergeAll(
          background.layer,
          scoutBackground.layer,
          Subagent.SubagentRuntime.layer(scoutDeclaration, scout),
        ).pipe(Layer.provide([SubagentReservationsMemoryLive, IdGenerator.layer]));

        const context = yield* Layer.build(
          NodeHost.layer(
            [root, builder, scout].map((agent) => ({
              agent,
              definitions: DefinitionDigestInput.make({
                agent: agent.definition.id,
                model: "nested-v1",
                tools: Object.keys(agent.definition.toolkit.tools),
              }),
            })),
            {
              filename: `${directory}/runtime.sqlite`,
              deploymentId: "nested-v1",
              producerId: "nested-node",
              workerConcurrency: 1,
              wakeScanInterval: 10,
              settlementPollInterval: 10,
            },
          ).pipe(Layer.provide([authority, handlers])),
        );

        const runtime = Context.get(context, DurableAgentRuntime);
        const store = Context.get(context, ThreadStore);

        const readLog = (threadId: ThreadId) =>
          store.export(ThreadExportRequest.make({ threadId }));

        const rootReceipt = yield* runtime.submitRegistered(
          root,
          { question: "build" },
          { threadId: rootThreadId, principal, idempotencyKey: key("root-input") },
        );

        const rootSettlement = yield* runtime.awaitSettlement(rootReceipt);

        expect(rootSettlement).toMatchObject({ outcome: "completed" });
        yield* Deferred.await(scoutEntered);
        const owner = yield* runtime.workerHost({ sourceThreadId: rootThreadId, principal });

        const workers = yield* Subagent.list(build).pipe(
          Effect.provideService(SubagentHost, owner),
        );

        expect(workers.items).toHaveLength(1);
        const summary = workers.items[0];

        if (summary === undefined || summary.latestReceipt === null)
          return yield* Effect.die("Expected the builder's accepted Receipt");
        const worker = yield* Schema.decodeEffect(Subagent.Worker(build))(summary.worker);
        const receipt = summary.latestReceipt;

        expect(
          yield* Subagent.inspect(build, worker, receipt).pipe(
            Effect.provideService(SubagentHost, owner),
          ),
        ).toEqual({ _tag: "Pending", receipt });
        expect((yield* runtime.submissionStatus(rootReceipt))._tag).toBe("settled");
        expect(yield* Ref.get(rootModel.calls)).toBe(2);
        expect(yield* Ref.get(builderModel.calls)).toBe(1);
        expect(yield* Ref.get(scoutModel.calls)).toBe(1);
        expect(yield* Ref.get(rootModel.visibleTools)).toEqual([["build_start"], ["build_start"]]);
        expect(yield* Ref.get(builderModel.visibleTools)).toEqual([["scout"]]);
        expect(yield* Ref.get(scoutModel.visibleTools)).toEqual([[]]);

        const builderOwner = yield* runtime.workerHost({
          sourceThreadId: worker.threadId,
          principal,
        });

        expect(
          yield* Subagent.start(
            scoutDeclaration,
            { question: "forbidden background" },
            {
              idempotencyKey: key("forbidden-background"),
            },
          ).pipe(Effect.provideService(SubagentHost, builderOwner), Effect.result),
        ).toMatchObject({
          _tag: "Failure",
          failure: { _tag: "SubagentPrestartDenied", reason: "grant-violation" },
        });

        const excessSubtree = Subagent.start(
          build,
          { question: "excess subtree" },
          {
            idempotencyKey: key("excess-subtree"),
          },
        ).pipe(Effect.provideService(SubagentHost, owner), Effect.result);

        expect(yield* excessSubtree).toMatchObject({
          _tag: "Failure",
          failure: { _tag: "WorkerError", reason: "capacity" },
        });

        const rootLog = yield* readLog(rootThreadId);

        const rootRequests = rootLog.records.flatMap(({ record }) =>
          record.payload._tag === "WorkerInputRequested" ? [record.payload] : [],
        );

        expect(rootRequests).toHaveLength(1);
        expect(rootRequests[0]?.admission.sourceSubmissionId).toBe(rootReceipt.submissionId);
        expect(rootRequests[0]?.admission.origin).toMatchObject({
          worker,
          source: {
            _tag: "tool",
            threadId: rootThreadId,
            agentId: root.definition.id,
            toolCallId: "build_start-call",
          },
          depth: 1,
          policy: { maxTurns: 2, maxToolCalls: 1, toolResultBounds: { maxBytes: 1024 } },
          budget: {
            descendantInvocations: 1,
            allocation: { turns: 4, toolCalls: 2, durationMillis: 60_000, resultBytes: 2048 },
          },
          grant: { maxDepth: 2, childLifetimes: ["attached"] },
        });
        expect(
          rootLog.records.filter(({ record }) => record.payload._tag === "SubagentRequested"),
        ).toHaveLength(0);
        expect(
          rootLog.records.flatMap(({ record }) =>
            record.payload._tag === "ToolCallPrepared" ? [record.payload] : [],
          ),
        ).toMatchObject([{ toolName: "build_start", executionKind: "orchestration" }]);
        const builderLog = yield* readLog(worker.threadId);

        const attached = builderLog.records.flatMap(({ record }) =>
          record.payload._tag === "SubagentRequested" ? [record.payload] : [],
        );

        expect(attached).toHaveLength(1);
        expect(
          builderLog.records.flatMap(({ record }) =>
            record.payload._tag === "WorkerOriginRecorded" ? [record.payload.origin] : [],
          ),
        ).toEqual([rootRequests[0]?.admission.origin]);
        expect(attached[0]).toMatchObject({
          targetAgentId: scout.definition.id,
          depth: 2,
          grant: { allowedToolNames: [], maxDepth: 2, childLifetimes: ["attached"] },
          budget: {
            allocation: { turns: 1, toolCalls: 1, durationMillis: 20_000, resultBytes: 1024 },
          },
        });
        expect(
          builderLog.records.filter(({ record }) => record.payload._tag === "WorkerInputRequested"),
        ).toHaveLength(0);
        expect(
          builderLog.records.flatMap(({ record }) =>
            record.payload._tag === "SubtreeBudgetReserved" ? [record.payload] : [],
          ),
        ).toMatchObject([
          { lifetime: "attached", depth: 2, sourceSubmissionId: receipt.submissionId },
        ]);
        const requested = attached[0];

        if (requested === undefined)
          return yield* Effect.die("Expected the attached scout request");
        const scoutLog = yield* readLog(requested.childThreadId);

        expect(
          scoutLog.records.flatMap(({ record }) =>
            record.payload._tag === "SubagentLineageRecorded" ? [record.payload] : [],
          ),
        ).toMatchObject([
          {
            parentLink: { parentThreadId: worker.threadId, depth: 2 },
            budget: requested.budget,
            grant: requested.grant,
          },
        ]);
        yield* Deferred.succeed(releaseScout, undefined);
        expect(
          yield* Subagent.await(build, worker, receipt).pipe(
            Effect.provideService(SubagentHost, owner),
          ),
        ).toMatchObject({ outcome: "completed", result: { answer: "builder complete" } });
        expect(yield* Ref.get(builderModel.calls)).toBe(2);
        expect(yield* Ref.get(scoutModel.calls)).toBe(1);
        expect(yield* Ref.get(builderModel.visibleTools)).toEqual([["scout"], ["scout"]]);
        expect(JSON.stringify((yield* Ref.get(builderModel.prompts))[1])).toContain(
          "scout finding",
        );
        const completedBuilder = yield* readLog(worker.threadId);

        expect(
          completedBuilder.records.filter(({ record }) => record.payload._tag === "SubagentJoined"),
        ).toHaveLength(1);
        expect(
          completedBuilder.records.filter(
            ({ record }) => record.payload._tag === "ToolCallSettled",
          ),
        ).toHaveLength(1);
        // Settlement releases active capacity, but the already-reserved subtree is never refunded.
        expect(yield* excessSubtree).toMatchObject({
          _tag: "Failure",
          failure: { _tag: "WorkerError", reason: "capacity" },
        });
      }),
    ).pipe(Effect.provide(NodeFileSystem.layer)),
  15_000,
);
