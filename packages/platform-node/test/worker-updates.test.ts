import * as Subagent from "@effect-agent/capabilities/Subagent";
import * as Agent from "@effect-agent/core/Agent";
import * as AgentUpdates from "@effect-agent/core/AgentUpdates";
import { ThreadId } from "@effect-agent/core/Identifiers";
import { WorkerCompletion, WorkerError, WorkerUpdate } from "@effect-agent/core/Worker";
import { SubagentHost } from "@effect-agent/engine/SubagentHost";
import * as NodeHost from "@effect-agent/platform-node/NodeDurableHost";
import { DurableAgentRuntime } from "@effect-agent/thread/DurableAgentRuntime";
import { MessageDeliveryStore } from "@effect-agent/thread/MessageDelivery";
import { DefinitionDigestInput } from "@effect-agent/thread/Records";
import { AbortCommand, IdempotencyKey, Principal } from "@effect-agent/thread/SubmissionLedger";
import { ThreadExportRequest, ThreadStore } from "@effect-agent/thread/ThreadStore";
import { WorkerHostAuthorizer } from "@effect-agent/thread/WorkerHost";
import { NodeFileSystem } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import {
  Context,
  Deferred,
  Effect,
  Exit,
  Fiber,
  FileSystem,
  Layer,
  Schema,
  Scope,
  Stream,
} from "effect";
import { LanguageModel, Model, Toolkit, type Response } from "effect/unstable/ai";

const input = Schema.Struct({ question: Schema.String });
const output = Schema.Struct({ answer: Schema.String });

const areaConcern = Schema.Struct({
  _tag: Schema.Literal("AreaConcern"),
  area: Schema.String,
  recommendation: Schema.String,
});

const concern = {
  _tag: "AreaConcern" as const,
  area: "Johannesburg CBD",
  recommendation: "Rosebank",
};

const principal = Schema.decodeSync(Principal)("update-owner");
const threadId = Schema.decodeSync(ThreadId)("johannesburg-parent");
const key = Schema.decodeSync(IdempotencyKey);
const usage = { inputTokens: {}, outputTokens: {} };

const finish = (answer: string): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: JSON.stringify({ answer }) },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage },
];

const calls = (
  ...tools: ReadonlyArray<{ name: string; params: unknown }>
): ReadonlyArray<Response.StreamPartEncoded> => [
  ...tools.map(({ name, params }) => ({
    type: "tool-call" as const,
    id: `${name}-call`,
    name,
    params,
    providerExecuted: false,
  })),
  { type: "finish", reason: "tool-calls", usage },
];

const model = (name: string, streamText: Parameters<typeof LanguageModel.make>[0]["streamText"]) =>
  Model.make(
    "scripted",
    name,
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({ generateText: () => Effect.succeed([]), streamText }),
    ),
  );

const authority = Layer.succeed(WorkerHostAuthorizer, {
  authorize: (request) =>
    request.principal === principal
      ? Effect.succeed(principal)
      : WorkerError.make({ operation: request.operation, reason: "denied" }),
});

it.live(
  "discusses a Johannesburg area concern and redirects the same hotel and activities workers before either finishes",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "worker-updates-" });
        const parentEntered = yield* Deferred.make<void>();
        const releaseParent = yield* Deferred.make<void>();
        const hotelEntered = yield* Deferred.make<void>();
        const activitiesEntered = yield* Deferred.make<void>();
        const releaseChildren = yield* Deferred.make<void>();

        const policy = {
          maxTurns: 10,
          maxToolCalls: 10,
          maxDuration: "30 seconds",
          toolConcurrency: 2,
        } as const;

        const hotel = Agent.make("hotel", {
          input,
          output,
          updates: areaConcern,
          instructions: "Research Johannesburg hotels and surface area concerns early.",
          toolkit: Toolkit.empty,
          policy,
        });

        const activities = Agent.make("activities", {
          input,
          output,
          instructions: "Research activities near the selected hotel area.",
          toolkit: Toolkit.empty,
          policy,
        });

        const hotels = Subagent.background(hotel, {
          start: true,
          followUp: true,
          reportToParent: true,
        });

        const outings = Subagent.background(activities, {
          start: true,
          followUp: true,
          reportToParent: true,
        });

        const parent = Agent.make("travel-planner", {
          input,
          output,
          instructions: "Discuss intermediate findings with the user and steer both specialists.",
          toolkit: Toolkit.merge(hotels.toolkit, outings.toolkit),
          policy: { maxTurns: 40, maxToolCalls: 40, maxDuration: "2 minutes", toolConcurrency: 4 },
        });

        let hotelCalls = 0;
        let activityCalls = 0;
        let parentCalls = 0;
        const hotelPrompts: Array<string> = [];
        const activityPrompts: Array<string> = [];
        const parentPrompts: Array<string> = [];
        const workers = yield* Deferred.make<ReadonlyArray<WorkerUpdate["worker"]>>();

        const hotelBinding = Agent.withModel(
          hotel,
          model("hotel", ({ prompt }) =>
            Stream.unwrap(
              Effect.gen(function* () {
                hotelPrompts.push(JSON.stringify(prompt));
                const call = hotelCalls++;

                if (call === 0)
                  return Stream.fromIterable(
                    calls({ name: "emit_update", params: { value: concern } }),
                  );
                if (call === 1) {
                  yield* Deferred.succeed(hotelEntered, undefined);
                  yield* Deferred.await(releaseChildren);
                }

                return Stream.fromIterable(finish("Rosebank hotel shortlist"));
              }),
            ),
          ),
        );

        const activityBinding = Agent.withModel(
          activities,
          model("activities", ({ prompt }) =>
            Stream.unwrap(
              Effect.gen(function* () {
                activityPrompts.push(JSON.stringify(prompt));
                if (activityCalls++ === 0) {
                  yield* Deferred.succeed(activitiesEntered, undefined);
                  yield* Deferred.await(releaseChildren);
                }

                return Stream.fromIterable(finish("Rosebank activities"));
              }),
            ),
          ),
        );

        const parentBinding = Agent.withModel(
          parent,
          model("parent", ({ prompt }) =>
            Stream.unwrap(
              Effect.gen(function* () {
                parentPrompts.push(JSON.stringify(prompt));
                const call = parentCalls++;

                if (call === 0)
                  return Stream.fromIterable(
                    calls(
                      { name: "hotel_start", params: { question: "Johannesburg hotels" } },
                      { name: "activities_start", params: { question: "Johannesburg activities" } },
                    ),
                  );
                if (call === 1) {
                  yield* Deferred.succeed(parentEntered, undefined);
                  yield* Deferred.await(releaseParent);

                  return Stream.fromIterable(finish("The specialists are researching."));
                }
                if (call === 2) {
                  expect(JSON.stringify(prompt)).toContain("WorkerUpdate");
                  expect(JSON.stringify(prompt)).toContain("AreaConcern");

                  return Stream.fromIterable(
                    finish(
                      "The hotel specialist raised a CBD area concern. Shall we use Rosebank?",
                    ),
                  );
                }
                if (call === 3) {
                  expect(JSON.stringify(prompt)).toContain("Yes, use Rosebank");

                  return Stream.fromIterable(
                    calls(
                      ...(yield* Deferred.await(workers)).map((worker) => ({
                        name: `${worker.targetAgentId}_follow_up`,
                        params: { worker, parameters: { question: "Focus on Rosebank" } },
                      })),
                    ),
                  );
                }

                return Stream.fromIterable(finish("Both specialists are now focused on Rosebank."));
              }),
            ),
          ),
        );

        const context = yield* Layer.build(
          NodeHost.NodeDurableHost.layerRegistered(
            [parentBinding, hotelBinding, activityBinding].map((agent) => ({
              agent,
              definitions: DefinitionDigestInput.make({
                agent: agent.definition.id,
                model: "v1",
                tools: Object.keys(agent.definition.toolkit.tools),
              }),
            })),
            {
              filename: `${directory}/runtime.sqlite`,
              deploymentId: "updates-v1",
              producerId: "updates-node",
              workerConcurrency: 1,
              wakeScanInterval: 10,
              settlementPollInterval: 10,
            },
          ).pipe(Layer.provide([authority, hotels.layer, outings.layer])),
        );

        const runtime = Context.get(context, DurableAgentRuntime);
        const store = Context.get(context, ThreadStore);
        const deliveries = Context.get(context, MessageDeliveryStore);
        const read = (id: ThreadId) => store.export(ThreadExportRequest.make({ threadId: id }));

        yield* Context.get(context, NodeHost.NodeDurableHost)
          .runWorkers(Effect.never)
          .pipe(Effect.forkChild);

        const receipt = yield* runtime.submitRegistered(
          parentBinding,
          { question: "Plan Johannesburg" },
          { threadId, principal, idempotencyKey: key("plan") },
        );

        const parentFiber = yield* runtime.processThreadResolved(threadId).pipe(Effect.forkChild);

        yield* Deferred.await(parentEntered);

        const launched = (yield* read(threadId)).records.flatMap(({ record }) =>
          record.payload._tag === "WorkerInputRequested"
            ? [record.payload.admission.origin.worker]
            : [],
        );

        expect(launched).toHaveLength(2);
        yield* Deferred.succeed(workers, launched);
        const hotelWorker = launched.find((worker) => worker.targetAgentId === hotel.id);
        const activitiesWorker = launched.find((worker) => worker.targetAgentId === activities.id);

        if (hotelWorker === undefined || activitiesWorker === undefined)
          return yield* Effect.die("Missing actual worker handles");

        const childFibers = yield* Effect.forEach(launched, (worker) =>
          runtime.processThreadResolved(worker.threadId).pipe(Effect.forkChild),
        );

        yield* Deferred.await(hotelEntered);
        yield* Deferred.await(activitiesEntered);
        yield* Effect.gen(function* () {
          for (;;) {
            const rows = yield* deliveries.list({
              ownerThreadId: hotelWorker.threadId,
              limit: 100,
            });

            if (rows.items.some((row) => row.receipt !== null)) return;
            yield* Effect.sleep(10);
          }
        }).pipe(Effect.timeout("3 seconds"));
        yield* Deferred.succeed(releaseParent, undefined);
        yield* Fiber.join(parentFiber);
        expect((yield* runtime.awaitSettlement(receipt)).outcome).toBe("completed");
        expect(parentPrompts.at(-1)).toContain("AreaConcern");

        const steering = yield* runtime.submitRegistered(
          parentBinding,
          { question: "Yes, use Rosebank" },
          { threadId, principal, idempotencyKey: key("rosebank") },
        );

        yield* runtime.processThreadResolved(threadId);
        expect((yield* runtime.awaitSettlement(steering)).outcome).toBe("completed");
        const parentBeforeCompletion = yield* read(threadId);

        const admissions = parentBeforeCompletion.records.flatMap(({ record }) =>
          record.payload._tag === "UserInputRecorded" ? [record.payload] : [],
        );

        const update = yield* Schema.decodeUnknownEffect(WorkerUpdate)(
          admissions[1]?.messageAdmission,
        );

        expect(update.worker).toEqual(hotelWorker);
        expect(yield* AgentUpdates.decode(hotel, update.update)).toEqual(concern);
        expect(admissions[1]?.runId).toBe(admissions[0]?.runId);
        expect(admissions[1]?.input).toEqual({ question: "Plan Johannesburg" });

        const requests = parentBeforeCompletion.records.flatMap(({ record }) =>
          record.payload._tag === "WorkerInputRequested" ? [record.payload.admission] : [],
        );

        expect(requests).toHaveLength(4);
        expect(requests.map((request) => request.origin.worker.threadId).sort()).toEqual(
          [...launched, ...launched].map((worker) => worker.threadId).sort(),
        );
        for (const worker of launched) {
          const log = yield* read(worker.threadId);

          expect(
            log.records.filter(({ record }) => record.payload._tag === "RunCompleted"),
          ).toHaveLength(0);
          expect(
            log.records.filter(({ record }) => record.payload._tag === "WorkerReportPrepared"),
          ).toHaveLength(0);
        }
        expect(
          admissions.filter((admission) => Schema.is(WorkerCompletion)(admission.messageAdmission)),
        ).toHaveLength(0);
        yield* Deferred.succeed(releaseChildren, undefined);
        yield* Effect.forEach(childFibers, Fiber.join);
        expect(hotelPrompts.at(-1)).toContain("Focus on Rosebank");
        expect(activityPrompts.at(-1)).toContain("Focus on Rosebank");
        yield* Effect.gen(function* () {
          for (;;) {
            const pages = yield* Effect.forEach(launched, (worker) =>
              deliveries.list({ ownerThreadId: worker.threadId, limit: 100 }),
            );

            if (
              pages.flatMap((page) => page.items).filter((row) => row.receipt !== null).length === 3
            )
              return;
            yield* Effect.sleep(10);
          }
        }).pipe(Effect.timeout("3 seconds"));
        yield* runtime.processThreadResolved(threadId);
        const finalLog = yield* read(threadId);

        const completions = finalLog.records.flatMap(({ record }) =>
          record.payload._tag === "UserInputRecorded" &&
          Schema.is(WorkerCompletion)(record.payload.messageAdmission)
            ? [Schema.decodeUnknownSync(WorkerCompletion)(record.payload.messageAdmission)]
            : [],
        );

        expect(completions).toHaveLength(2);
        expect(completions.map((message) => message.report.worker.threadId).sort()).toEqual(
          launched.map((worker) => worker.threadId).sort(),
        );
        expect(completions.every((message) => message.report.outcome === "completed")).toBe(true);
      }),
    ).pipe(Effect.provide(NodeFileSystem.layer)),
  15_000,
);

for (const [parentState, failpoint] of [
  ["completed", "update:after-canonical-append"],
  ["aborted", "update:after-delivery-insert"],
] as const) {
  it.live(
    `retains the update after lost acknowledgement at ${failpoint}, a Node restart, and a ${parentState} parent`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const directory = yield* fs.makeTempDirectoryScoped({ prefix: "update-restart-" });
          const lostAck = yield* Deferred.make<void>();
          let childModelCalls = 0;

          const child = Agent.make("restart-hotel", {
            input,
            output,
            updates: areaConcern,
            instructions: "Research hotels and report concerns.",
            toolkit: Toolkit.empty,
            policy: { maxTurns: 2, maxToolCalls: 1, maxDuration: "30 seconds" },
          });

          const declaration = Subagent.make("restart-hotel", { target: child });
          const background = Subagent.background(child, { start: true, reportToParent: true });

          const source = Agent.withModel(
            Agent.make("restart-parent", {
              input,
              output,
              instructions: "Discuss findings.",
              toolkit: background.toolkit,
              policy: { maxTurns: 10, maxToolCalls: 10, maxDuration: "1 minute" },
            }),
            model("restart-parent", () => Stream.fromIterable(finish("I have the finding."))),
          );

          const childBinding = Agent.withModel(
            child,
            model("restart-child", () => {
              childModelCalls++;

              return Stream.fromIterable(
                calls({ name: "emit_update", params: { value: concern } }),
              );
            }),
          );

          const registrations = [source, childBinding].map((agent) => ({
            agent,
            definitions: DefinitionDigestInput.make({
              agent: agent.definition.id,
              model: "v1",
              tools: Object.keys(agent.definition.toolkit.tools),
            }),
          }));

          const options = {
            filename: `${directory}/runtime.sqlite`,
            deploymentId: "restart-updates",
            producerId: "node",
            workerConcurrency: 1,
            wakeScanInterval: 10,
            settlementPollInterval: 10,
          };

          const firstScope = yield* Scope.make();

          yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void));

          const first = yield* Layer.build(
            NodeHost.NodeDurableHost.layerRegistered(registrations, {
              ...options,
              runtimeFailpoint: (location) =>
                location === failpoint
                  ? Deferred.succeed(lostAck, undefined).pipe(Effect.andThen(Effect.never))
                  : Effect.void,
            }).pipe(Layer.provide([authority, background.layer])),
          ).pipe(Scope.provide(firstScope));

          const runtime = Context.get(first, DurableAgentRuntime);

          const sourceReceipt = yield* runtime.submitRegistered(
            source,
            { question: "Johannesburg" },
            { threadId, principal, idempotencyKey: key("source") },
          );

          if (parentState === "completed") yield* runtime.processThreadResolved(threadId);

          const owner = yield* runtime.workerHost({
            sourceThreadId: threadId,
            principal,
            sourceSubmissionId: sourceReceipt.submissionId,
          });

          const started = yield* Subagent.start(
            declaration,
            { question: "Check the CBD" },
            { idempotencyKey: key("hotel") },
          ).pipe(Effect.provideService(SubagentHost, owner));

          if (parentState === "aborted") {
            yield* runtime.abort(
              AbortCommand.make({
                submissionId: sourceReceipt.submissionId,
                author: principal,
                reason: "cancel parent",
              }),
            );
            yield* runtime.processThreadResolved(threadId);
          }
          expect((yield* runtime.awaitSettlement(sourceReceipt)).outcome).toBe(parentState);
          yield* runtime
            .processThreadResolved(started.worker.threadId)
            .pipe(Effect.forkIn(firstScope));
          yield* Deferred.await(lostAck);

          const before = yield* Context.get(first, ThreadStore).export(
            ThreadExportRequest.make({ threadId: started.worker.threadId }),
          );

          const accepted = before.records.flatMap(({ record }) =>
            record.payload._tag === "AgentUpdateEmitted" ? [record.payload.update] : [],
          );

          expect(accepted).toHaveLength(1);
          expect(
            before.records.filter(({ record }) => record.payload._tag === "ToolCallSettled"),
          ).toHaveLength(0);
          yield* Scope.close(firstScope, Exit.void);

          const second = yield* Layer.build(
            NodeHost.layer(registrations, options).pipe(
              Layer.provide([authority, background.layer]),
            ),
          );

          const reopened = Context.get(second, DurableAgentRuntime);
          const store = Context.get(second, ThreadStore);
          const deliveries = Context.get(second, MessageDeliveryStore);
          const read = (id: ThreadId) => store.export(ThreadExportRequest.make({ threadId: id }));

          yield* Effect.gen(function* () {
            for (;;) {
              const rows = yield* deliveries.list({
                ownerThreadId: started.worker.threadId,
                limit: 100,
              });

              if (
                rows.items.length === 1 &&
                rows.items[0]?.receipt !== null &&
                rows.items[0]?.receipt !== undefined
              ) {
                yield* reopened.awaitSettlement(rows.items[0].receipt);

                return;
              }
              yield* Effect.sleep(10);
            }
          }).pipe(Effect.timeout("5 seconds"));
          const parentLog = yield* read(threadId);

          const messages = parentLog.records.flatMap(({ record }) =>
            record.payload._tag === "UserInputRecorded" &&
            Schema.is(WorkerUpdate)(record.payload.messageAdmission)
              ? [record.payload.messageAdmission]
              : [],
          );

          expect(messages).toHaveLength(1);
          expect(messages[0]).toMatchObject({ worker: started.worker, update: accepted[0] });
          const blocked = yield* read(started.worker.threadId);

          expect(
            blocked.records.filter(({ record }) => record.payload._tag === "AgentUpdateEmitted"),
          ).toHaveLength(1);
          expect(
            blocked.records.filter(({ record }) => record.payload._tag === "RunCompleted"),
          ).toHaveLength(0);
          expect(childModelCalls).toBe(1);
          expect(
            blocked.records.filter(({ record }) => record.payload._tag === "ToolCallUnknown"),
          ).toHaveLength(1);
          yield* reopened.abort(
            AbortCommand.make({
              submissionId: started.receipt.submissionId,
              author: principal,
              reason: "resolve interrupted worker",
            }),
          );
          expect((yield* reopened.awaitSettlement(started.receipt)).outcome).toBe("aborted");
          yield* Effect.gen(function* () {
            for (;;) {
              const rows = yield* deliveries.list({
                ownerThreadId: started.worker.threadId,
                limit: 100,
              });

              if (rows.items.length === 2 && rows.items.every((row) => row.receipt !== null)) {
                for (const row of rows.items)
                  if (row.receipt !== null) yield* reopened.awaitSettlement(row.receipt);

                return;
              }
              yield* Effect.sleep(10);
            }
          }).pipe(Effect.timeout("5 seconds"));
          const finalParent = yield* read(threadId);

          const completion = finalParent.records.flatMap(({ record }) =>
            record.payload._tag === "UserInputRecorded" &&
            Schema.is(WorkerCompletion)(record.payload.messageAdmission)
              ? [record.payload.messageAdmission]
              : [],
          );

          expect(completion).toHaveLength(1);
          expect(completion[0]?.report.outcome).toBe("aborted");
          const finalChild = yield* read(started.worker.threadId);

          expect(
            finalChild.records.flatMap(({ record }) =>
              record.payload._tag === "AgentUpdateEmitted" ? [record.payload.update] : [],
            ),
          ).toEqual(accepted);
          expect((yield* reopened.awaitSettlement(sourceReceipt)).outcome).toBe(parentState);
          expect(childModelCalls).toBe(1);
        }),
      ).pipe(Effect.provide(NodeFileSystem.layer)),
    15_000,
  );
}
