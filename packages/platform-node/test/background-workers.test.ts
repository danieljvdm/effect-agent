import * as NodeHost from "@effect-agent/platform-node/node-durable-host";
import { NodeFileSystem } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import {
  Clock,
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
import * as Agent from "effect-agent/agent";
import { DurableAgentRuntime } from "effect-agent/durable-agent-runtime";
import { DurableRuntimeFailpointError } from "effect-agent/durable-failpoint";
import { ThreadId, ToolCallId } from "effect-agent/identifiers";
import { MessageDeliveryFailpoint, MessageDeliveryStore } from "effect-agent/message-delivery";
import type { Receipt } from "effect-agent/receipt";
import { DefinitionDigestInput } from "effect-agent/records";
import { RunToolAuthorization } from "effect-agent/run-options";
import * as Subagent from "effect-agent/subagent";
import { SubagentHost } from "effect-agent/subagent-host";
import { ApprovalDecisionCommand, IdempotencyKey, Principal } from "effect-agent/submission-ledger";
import { readOutstanding, ThreadExportRequest, ThreadStore } from "effect-agent/thread-store";
import { AssignmentDisposition, WorkerError, type WorkerSummary } from "effect-agent/worker";
import { WorkerHostAuthorizer } from "effect-agent/worker-host";
import { TestClock } from "effect/testing";
import { LanguageModel, Model, Tool, Toolkit, type Response } from "effect/unstable/ai";

const principal = Schema.decodeSync(Principal)("worker-owner");
const sourceThreadId = Schema.decodeSync(ThreadId)("background-source");
const key = Schema.decodeSync(IdempotencyKey);
const definitions = DefinitionDigestInput.make({ agent: "background-v1", model: "v1", tools: [] });

const parts: ReadonlyArray<Response.StreamPartEncoded> = [
  { type: "text-start", id: "reply" },
  { type: "text-delta", id: "reply", delta: '{"answer":"done"}' },
  { type: "text-end", id: "reply" },
  { type: "finish", reason: "stop", usage: { inputTokens: {}, outputTokens: {} } },
];

const agent = (id: string, beforeReply: Effect.Effect<void> = Effect.void, disposition?: string) =>
  Agent.withModel(
    Agent.make(id, {
      input: Schema.Struct({ question: Schema.String }),
      output: Schema.Struct({ answer: Schema.String }),
      instructions: "Answer as JSON.",
      runDisposition: { schema: Schema.String, fromOutput: () => disposition },
      toolkit: Toolkit.empty,
      policy: { maxTurns: 20, maxToolCalls: 20, maxDuration: "1 minute", toolConcurrency: 2 },
    }),
    Model.make(
      "scripted",
      id,
      Layer.effect(
        LanguageModel.LanguageModel,
        LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: () => Stream.unwrap(beforeReply.pipe(Effect.as(Stream.fromIterable(parts)))),
        }),
      ),
    ),
  );

const source = agent("background-source-agent");
const target = agent("background-target-agent");

const declaration = Subagent.make("research", {
  target: target.definition,
  success: target.definition.output,
  projectResult: (output) => Effect.succeed(output),
  policy: Subagent.SubagentPolicy.make({
    maxChildren: 8,
    maxConcurrency: 2,
    maxTurns: 1,
    maxToolCalls: 1,
    maxDuration: "1 second",
  }),
});

const authority = Layer.succeed(WorkerHostAuthorizer)({
  authorize: (request) =>
    request.principal === principal && request.sourceThreadId === sourceThreadId
      ? Effect.succeed(principal)
      : WorkerError.make({ operation: request.operation, reason: "denied" }),
});

const withFacet = <A, E>(
  facet: SubagentHost["Service"],
  effect: Effect.Effect<A, E, SubagentHost>,
) => effect.pipe(Effect.provideService(SubagentHost, facet));

// Regression: https://github.com/danieljvdm/effect-agent/blob/4c417d98e8cc790c42ab4200a54a0548fe32e6e3/packages/effect-agent/src/durable/internal/worker-host.ts#L1645-L1691
for (const completion of ["interrupted"] as const) {
  it.effect(
    `tracks one retained delivery through acceptance and completion after its ${completion} claim`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const directory = yield* fs.makeTempDirectoryScoped({ prefix: "worker-admission-" });
          const claimed = yield* Deferred.make<void>();
          const release = yield* Deferred.make<void>();
          const accepted = yield* Deferred.make<void>();
          const processed = yield* Deferred.make<void>();
          let claims = 0;
          let admissions = 0;
          let modelCalls = 0;
          let finalized = 0;

          const child = agent(
            "admission-target",
            Effect.sync(() => {
              modelCalls++;
            }),
          );

          const research = Subagent.make("research", {
            target: child.definition,
            policy: declaration.policy,
          });

          const context = yield* Layer.build(
            NodeHost.NodeDurableHost.layerRegistered(
              [
                { agent: source, definitions },
                { agent: child, definitions },
              ],
              {
                filename: `${directory}/runtime.sqlite`,
                deploymentId: "admission-v1",
                producerId: "admission-node",
                workerConcurrency: 1,
                runtimeFailpoint: (location) =>
                  Effect.sync(() => {
                    if (location === "submit:after-admit") admissions++;
                  }),
              },
            ).pipe(
              Layer.provide(authority),
              Layer.provide(
                Layer.succeed(MessageDeliveryFailpoint, {
                  hit: (point) =>
                    Effect.gen(function* () {
                      if (point === "message-delivery:accept:after")
                        yield* Deferred.succeed(accepted, undefined);
                      if (point === "message-delivery:process:after")
                        yield* Deferred.succeed(processed, undefined);
                      if (point !== "message-delivery:claim:after") return;
                      claims++;
                      if (claims !== 1) return;
                      yield* Deferred.succeed(claimed, undefined).pipe(
                        Effect.andThen(Deferred.await(release)),
                        Effect.ensuring(
                          Effect.sync(() => {
                            finalized++;
                          }),
                        ),
                      );
                    }),
                }),
              ),
            ),
          );

          const runtime = Context.get(context, DurableAgentRuntime);
          const host = Context.get(context, NodeHost.NodeDurableHost);
          const store = Context.get(context, MessageDeliveryStore);
          const history = Context.get(context, ThreadStore);

          const sourceReceipt = yield* runtime.submitRegistered(
            source,
            { question: "prepare" },
            { threadId: sourceThreadId, principal, idempotencyKey: key("source") },
          );

          yield* runtime.processThreadResolved(sourceThreadId);
          expect((yield* runtime.awaitSettlement(sourceReceipt)).outcome).toBe("completed");
          admissions = 0;
          const facet = yield* runtime.workerHost({ sourceThreadId, principal });

          const start = withFacet(
            facet,
            Subagent.start(research, { question: "once" }, { idempotencyKey: key("held-claim") }),
          );

          const first = yield* start.pipe(Effect.forkChild);

          yield* Deferred.await(claimed);
          const clockStart = yield* Clock.currentTimeMillis;
          const competing = yield* Effect.all([start, start, start], { concurrency: 3 });
          const retained = competing[0]!;

          expect(competing).toEqual([retained, retained, retained]);
          expect(retained.delivery).toEqual({
            message: { ownerThreadId: sourceThreadId, messageId: expect.any(String) },
            status: "pending",
            receipt: null,
            settlement: null,
            reason: null,
          });

          const inspect = withFacet(
            facet,
            Subagent.inspect(research, retained.worker, retained.delivery.message),
          );

          expect(yield* inspect).toEqual(retained.delivery);
          expect(yield* Clock.currentTimeMillis).toBe(clockStart);
          expect(claims).toBe(1);
          expect(admissions).toBe(0);
          expect(modelCalls).toBe(0);
          expect(
            (yield* store.list({ ownerThreadId: sourceThreadId, limit: 10 })).items,
          ).toHaveLength(1);
          yield* Fiber.interrupt(first);
          expect(Exit.hasInterrupts(yield* Fiber.await(first))).toBe(true);
          yield* TestClock.adjust("31 seconds");

          // The normal host pump recovers the retained operation; no business command is resent.
          const pump = yield* host.runWorkers(Effect.never).pipe(Effect.forkChild);

          yield* Deferred.await(accepted);
          const admitted = yield* inspect;

          expect(admitted).toMatchObject({
            message: retained.delivery.message,
            status: "accepted",
            settlement: null,
          });
          if (admitted.status !== "accepted") return yield* Effect.die("Expected acceptance");
          expect((yield* start).delivery).toEqual(admitted);
          expect(admissions).toBe(1);
          expect(modelCalls).toBe(0);
          expect(finalized).toBe(1);
          yield* runtime.processThreadResolved(retained.worker.threadId);

          const settled = yield* withFacet(
            facet,
            Subagent.await(research, retained.worker, admitted.receipt),
          );

          expect(settled).toMatchObject({ _tag: "Settled", outcome: "completed" });
          if (settled._tag !== "Settled") return yield* Effect.die("Expected settlement");
          yield* TestClock.adjust("5 seconds");
          yield* Deferred.await(processed);
          expect(yield* inspect).toEqual({
            message: retained.delivery.message,
            status: "processed",
            receipt: admitted.receipt,
            settlement: { settlementId: settled.settlementId, outcome: "completed" },
            reason: null,
          });
          expect(modelCalls).toBe(1);
          yield* Fiber.interrupt(pump);

          const sourceLog = yield* history.export(
            ThreadExportRequest.make({ threadId: sourceThreadId }),
          );

          const childLog = yield* history.export(
            ThreadExportRequest.make({ threadId: retained.worker.threadId }),
          );

          expect(
            sourceLog.records.filter(
              ({ record }) => record.payload._tag === "WorkerInputRequested",
            ),
          ).toHaveLength(1);
          expect(
            childLog.records.filter(({ record }) => record.payload._tag === "UserInputRecorded"),
          ).toHaveLength(1);
        }),
      ).pipe(Effect.provide(NodeFileSystem.layer)),
    15_000,
  );
}

it.effect(
  "stops an active worker and queued steering, preserves external-action evidence and replays after a real SQLite owner restart",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "worker-stop-" });
        const entered = yield* Deferred.make<void>();
        const pay = yield* Deferred.make<void>();
        let payments = 0;
        let finalized = 0;
        let loseAck = false;
        const output = Schema.Struct({ answer: Schema.String });

        const toolkit = Toolkit.make(
          Tool.make("pay", { parameters: Schema.Struct({}), success: output }),
        );

        const child = Agent.withModel(
          Agent.make("payment-worker", {
            input: target.definition.input,
            output,
            instructions: "Complete the purchase",
            runDisposition: {
              workerLifecycle: "assignment",
              schema: AssignmentDisposition,
              fromOutput: () => "completed",
            },
            toolkit,
            completion: { tool: "pay", required: true, project: ({ result }) => result },
            policy: { maxTurns: 4, maxToolCalls: 4, maxDuration: "1 minute" },
          }),
          Model.make(
            "scripted",
            "payment",
            Layer.effect(
              LanguageModel.LanguageModel,
              LanguageModel.make({
                generateText: () => Effect.succeed([]),
                streamText: () =>
                  Stream.fromIterable([
                    { type: "tool-call", id: "payment", name: "pay", params: {} },
                    {
                      type: "finish",
                      reason: "tool-calls",
                      usage: { inputTokens: {}, outputTokens: {} },
                    },
                  ]),
              }),
            ),
          ),
        );

        const handlers = toolkit.toLayer({
          pay: () =>
            Effect.gen(function* () {
              yield* Deferred.succeed(entered, undefined);
              yield* Deferred.await(pay);
              payments++;

              return { answer: "paid" };
            }).pipe(
              Effect.ensuring(
                Effect.sync(() => {
                  finalized++;
                }),
              ),
            ),
        });

        const research = Subagent.make("research", {
          target: child.definition,
          policy: declaration.policy,
        });

        const registrations = [
          { agent: source, definitions },
          { agent: child, definitions: { ...definitions, tools: [{ name: "pay", version: "1" }] } },
        ];

        const options = {
          filename: `${directory}/runtime.sqlite`,
          deploymentId: "stop-v1",
          producerId: "stop-node",
          settlementPollInterval: 1,
          abortPollInterval: 1,
        };

        const firstScope = yield* Scope.make();

        yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void));

        const first = yield* Layer.build(
          NodeHost.NodeDurableHost.layerRegistered(registrations, {
            ...options,
            runtimeFailpoint: (location) =>
              loseAck && location === "worker:after-stop-seal"
                ? DurableRuntimeFailpointError.make({ location })
                : Effect.void,
          }).pipe(Layer.provide([authority, handlers])),
        ).pipe(Scope.provide(firstScope));

        const runtime = Context.get(first, DurableAgentRuntime);

        yield* runtime.submitRegistered(
          source,
          { question: "buy coffee" },
          { threadId: sourceThreadId, principal, idempotencyKey: key("source") },
        );
        const owner = yield* runtime.workerHost({ sourceThreadId, principal });

        const start = yield* withFacet(
          owner,
          Subagent.start(research, { question: "coffee" }, { idempotencyKey: key("purchase") }),
        );

        const running = yield* runtime
          .processThreadResolved(start.worker.threadId)
          .pipe(Effect.forkChild);

        yield* Deferred.await(entered);

        const steering = yield* withFacet(
          owner,
          Subagent.followUp(
            research,
            start.worker,
            { question: "latest email" },
            { idempotencyKey: key("email") },
          ),
        );

        const before = yield* withFacet(owner, Subagent.inspect(research, start.worker));

        expect(before.acceptedInput?.messageId).toBe(steering.message.messageId);
        expect(before.appliedInput?.messageId).toBe(start.delivery.message.messageId);
        expect(before.run?.hostReceipt).toEqual(start.delivery.receipt);
        expect(before.run?.outcome).toBeNull();
        loseAck = true;
        expect(
          yield* withFacet(
            owner,
            Subagent.stop(research, start.worker, { idempotencyKey: key("stop") }),
          ).pipe(Effect.flip),
        ).toMatchObject({ reason: "storage" });
        loseAck = false;
        yield* TestClock.adjust(10);
        yield* Fiber.join(running);

        const stopped = yield* withFacet(
          owner,
          Subagent.stop(research, start.worker, { idempotencyKey: key("stop") }),
        );

        expect(stopped).toEqual({ worker: start.worker, idempotencyKey: key("stop") });
        expect(finalized).toBe(1);
        yield* Deferred.succeed(pay, undefined);
        expect(payments).toBe(0);

        const outstanding = yield* readOutstanding({
          threadId: start.worker.threadId,
          limit: 10,
        }).pipe(Effect.provide(first));

        expect(outstanding.operations).toHaveLength(1);
        yield* Scope.close(firstScope, Exit.void);

        const second = yield* Layer.build(
          NodeHost.NodeDurableHost.layerRegistered(registrations, options).pipe(
            Layer.provide([authority, handlers]),
          ),
        );

        const reopened = Context.get(second, DurableAgentRuntime);
        const nextOwner = yield* reopened.workerHost({ sourceThreadId, principal });

        expect(
          yield* withFacet(
            nextOwner,
            Subagent.stop(research, start.worker, { idempotencyKey: key("stop") }),
          ),
        ).toEqual(stopped);
        yield* reopened.runRecovery({ threadId: start.worker.threadId });
        yield* reopened.processThreadResolved(start.worker.threadId);
        const after = yield* withFacet(nextOwner, Subagent.inspect(research, start.worker));

        expect(after.state).toBe("stopped");
        expect(after.run?.outcome).toBe("aborted");
        expect(after.appliedInput?.messageId).toBe(start.delivery.message.messageId);
        expect(
          yield* withFacet(
            nextOwner,
            Subagent.followUp(
              research,
              start.worker,
              { question: "automatic continuation" },
              { idempotencyKey: key("continue") },
            ),
          ),
        ).toMatchObject({ status: "refused", reason: "worker-stopped" });
        expect(payments).toBe(0);
        expect(
          (yield* readOutstanding({ threadId: start.worker.threadId, limit: 10 }).pipe(
            Effect.provide(second),
          )).operations,
        ).toHaveLength(1);

        // The Main lane remains independently usable.
        const main = yield* reopened.submitRegistered(
          source,
          { question: "hello" },
          { threadId: sourceThreadId, principal, idempotencyKey: key("main-next") },
        );

        yield* reopened.processThreadResolved(sourceThreadId);
        expect((yield* reopened.awaitSettlement(main)).outcome).toBe("completed");
      }),
    ).pipe(Effect.provide(NodeFileSystem.layer)),
  15_000,
);

for (const stopPoint of ["worker:after-stop-append", "worker:after-stop-seal"] as const)
  it.effect(
    `stops a retained start racing admission and replays after ${stopPoint} and restart`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;

          const directory = yield* fs.makeTempDirectoryScoped({
            prefix: "unadmitted-worker-stop-",
          });

          const retained = yield* Deferred.make<void>();
          const admit = yield* Deferred.make<void>();
          let hold = true;
          let fault = true;
          let calls = 0;

          const child = agent(
            "unadmitted-worker",
            Effect.sync(() => {
              calls++;
            }),
          );

          const research = Subagent.make("research", {
            target: child.definition,
            policy: declaration.policy,
          });

          const registrations = [
            { agent: source, definitions },
            { agent: child, definitions },
          ];

          const options = {
            filename: `${directory}/runtime.sqlite`,
            deploymentId: "stop-v1",
            producerId: "stop-node",
            settlementPollInterval: 1,
          };

          const scope = yield* Scope.make();

          yield* Effect.addFinalizer(() => Scope.close(scope, Exit.void));

          const first = yield* Layer.build(
            NodeHost.NodeDurableHost.layerRegistered(registrations, {
              ...options,
              runtimeFailpoint: (location) =>
                hold && location === "worker:before-source-append"
                  ? Deferred.succeed(retained, undefined).pipe(
                      Effect.andThen(Deferred.await(admit)),
                    )
                  : fault && location === stopPoint
                    ? DurableRuntimeFailpointError.make({ location })
                    : Effect.void,
            }).pipe(Layer.provide(authority)),
          ).pipe(Scope.provide(scope));

          const runtime = Context.get(first, DurableAgentRuntime);

          yield* runtime.submitRegistered(
            source,
            { question: "launch" },
            { threadId: sourceThreadId, principal, idempotencyKey: key("source") },
          );
          const owner = yield* runtime.workerHost({ sourceThreadId, principal });

          const launching = yield* withFacet(
            owner,
            Subagent.start(
              research,
              { question: "never execute" },
              { idempotencyKey: key("pending") },
            ),
          ).pipe(Effect.forkChild);

          yield* Deferred.await(retained);
          const inventory = yield* withFacet(owner, Subagent.list(research, { limit: 1 }));

          expect(inventory.items).toHaveLength(1);
          const pending = inventory.items[0]!;

          expect(pending.acceptedInput).toBeNull();
          expect(pending.appliedInput).toBeNull();
          expect(pending.pendingDelivery).toMatchObject({ status: "pending", receipt: null });

          expect(
            yield* withFacet(
              owner,
              Subagent.stop(research, pending.worker, { idempotencyKey: key("stop") }),
            ).pipe(Effect.flip),
          ).toMatchObject({ reason: "storage" });
          fault = false;

          const stopped = yield* withFacet(
            owner,
            Subagent.stop(research, pending.worker, { idempotencyKey: key("stop") }),
          );

          hold = false;
          yield* Deferred.succeed(admit, undefined);
          expect((yield* Fiber.join(launching)).delivery).toMatchObject({
            status: "refused",
            reason: "worker-stopped",
          });

          const other = yield* withFacet(
            owner,
            Subagent.start(
              research,
              { question: "other assignment" },
              { idempotencyKey: key("other") },
            ),
          );

          expect(
            yield* withFacet(
              owner,
              Subagent.stop(research, other.worker, { idempotencyKey: key("stop") }),
            ).pipe(Effect.flip),
          ).toMatchObject({ reason: "idempotency-conflict" });
          yield* Scope.close(scope, Exit.void);

          const second = yield* Layer.build(
            NodeHost.NodeDurableHost.layerRegistered(registrations, options).pipe(
              Layer.provide(authority),
            ),
          );

          const reopened = Context.get(second, DurableAgentRuntime);
          const next = yield* reopened.workerHost({ sourceThreadId, principal });

          expect(
            yield* withFacet(
              next,
              Subagent.stop(research, pending.worker, { idempotencyKey: key("stop") }),
            ),
          ).toEqual(stopped);
          expect(
            (yield* withFacet(
              next,
              Subagent.start(
                research,
                { question: "never execute" },
                { idempotencyKey: key("pending") },
              ),
            )).delivery.status,
          ).toBe("refused");
          yield* reopened.processThreadResolved(pending.worker.threadId);
          expect(calls).toBe(0);
        }),
      ).pipe(Effect.provide(NodeFileSystem.layer)),
    15_000,
  );

it.effect(
  "drains approval-held worker corrections before the next model request and fences newer input",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "worker-approval-input-" });
        const nextModel = yield* Deferred.make<void>();
        const releaseModel = yield* Deferred.make<void>();
        const prompts: Array<ReadonlyArray<string>> = [];
        const consentCalls: Array<string> = [];
        let dispatches = 0;
        let latest: Effect.Effect<WorkerSummary> = Effect.die("Worker not started");
        const corrections = ["clarification", "first email", "corrected email"];
        const consentId = Schema.decodeSync(ToolCallId)("consent-1");

        const toolkit = Toolkit.make(
          Tool.make("consent", {
            parameters: Schema.Struct({ purpose: Schema.String }),
            success: Schema.String,
            needsApproval: true,
          }),
          Tool.make("navigate", { parameters: Schema.Struct({}), success: Schema.String }),
        );

        const child = Agent.withModel(
          Agent.make("approval-input-worker", {
            input: target.definition.input,
            inputPrompt: ({ question }) => Effect.succeed(question),
            output: target.definition.output,
            instructions: "Request consent, then navigate using the latest instructions.",
            toolkit,
            policy: { maxTurns: 5, maxToolCalls: 3, maxDuration: "1 minute", toolConcurrency: 1 },
          }),
          Model.make(
            "scripted",
            "approval-input",
            Layer.effect(
              LanguageModel.LanguageModel,
              LanguageModel.make({
                generateText: () => Effect.succeed([]),
                streamText: ({ prompt }) =>
                  Stream.unwrap(
                    Effect.gen(function* () {
                      const turn = prompts.length;

                      prompts.push(
                        prompt.content.flatMap((message) =>
                          message.role === "user"
                            ? message.content.flatMap((part) =>
                                part.type === "text" ? [part.text] : [],
                              )
                            : [],
                        ),
                      );
                      if (turn === 1) {
                        yield* Deferred.succeed(nextModel, undefined);
                        yield* Deferred.await(releaseModel);
                      }

                      return Stream.fromIterable<Response.StreamPartEncoded>(
                        turn < 2
                          ? [
                              {
                                type: "tool-call",
                                id: turn === 0 ? consentId : "navigate-1",
                                name: turn === 0 ? "consent" : "navigate",
                                params: turn === 0 ? { purpose: "original request" } : {},
                                providerExecuted: false,
                              },
                              {
                                type: "finish",
                                reason: "tool-calls",
                                usage: { inputTokens: {}, outputTokens: {} },
                              },
                            ]
                          : parts,
                      );
                    }),
                  ),
              }),
            ),
          ),
        );

        const handlers = toolkit.toLayer({
          consent: ({ purpose }) =>
            Effect.sync(() => {
              consentCalls.push(purpose);

              return "approved";
            }),
          navigate: () =>
            Effect.sync(() => {
              dispatches++;

              return "navigated";
            }),
        });

        const research = Subagent.make("research", {
          target: child.definition,
          policy: Subagent.SubagentPolicy.make({
            maxChildren: 5,
            maxConcurrency: 2,
            maxTurns: 3,
            maxToolCalls: 3,
            maxDuration: "10 seconds",
          }),
        });

        const context = yield* Layer.build(
          NodeHost.NodeDurableHost.layerRegistered(
            [
              { agent: source, definitions },
              { agent: child, definitions },
            ],
            {
              filename: `${directory}/runtime.sqlite`,
              deploymentId: "approval-input-v1",
              producerId: "approval-input-node",
              toolAuthorization: Layer.succeed(RunToolAuthorization, {
                authorize: ({ call }) =>
                  Effect.gen(function* () {
                    if (call.toolName === "consent") return { _tag: "allowed" as const };
                    const snapshot = yield* latest;

                    return snapshot.acceptedInput?.messageId === snapshot.appliedInput?.messageId
                      ? { _tag: "allowed" as const }
                      : { _tag: "denied" as const, reason: "Newer worker input is pending" };
                  }),
              }),
            },
          ).pipe(Layer.provide([authority, handlers])),
        );

        const runtime = Context.get(context, DurableAgentRuntime);
        const history = Context.get(context, ThreadStore);

        yield* runtime.submitRegistered(
          source,
          { question: "launch" },
          { threadId: sourceThreadId, principal, idempotencyKey: key("source") },
        );
        yield* runtime.processThreadResolved(sourceThreadId);
        const owner = yield* runtime.workerHost({ sourceThreadId, principal });

        const start = yield* withFacet(
          owner,
          Subagent.start(research, { question: "original" }, { idempotencyKey: key("first") }),
        );

        latest = withFacet(owner, Subagent.inspect(research, start.worker)).pipe(Effect.orDie);
        const receipt = start.delivery.receipt;

        if (receipt === null) return yield* Effect.die("Expected an admitted worker");
        yield* runtime.processThreadResolved(start.worker.threadId);
        expect(prompts).toEqual([["original"]]);
        expect(consentCalls).toEqual([]);

        const followUp = (question: string) =>
          withFacet(
            owner,
            Subagent.followUp(
              research,
              start.worker,
              { question },
              { idempotencyKey: key(question) },
            ),
          );

        const updates = yield* Effect.forEach(corrections, followUp);
        const held = yield* latest;

        expect(updates.every((update) => update.receipt !== null)).toBe(true);
        expect(held.acceptedInput?.messageId).toBe(updates[2]?.message.messageId);
        expect(held.appliedInput?.messageId).toBe(start.delivery.message.messageId);
        yield* runtime.resolveApproval(
          ApprovalDecisionCommand.make({
            submissionId: receipt.submissionId,
            toolCallId: consentId,
            decision: "approved",
            resolver: "operator",
            reason: "Approve the original consent request",
          }),
        );

        const resumed = yield* runtime
          .processThreadResolved(start.worker.threadId)
          .pipe(Effect.forkChild);

        yield* Deferred.await(nextModel);
        expect(prompts).toEqual([["original"], ["original", ...corrections]]);
        expect(consentCalls).toEqual(["original request"]);
        const drained = yield* latest;

        expect(drained.appliedInput?.messageId).toBe(updates[2]?.message.messageId);
        expect(drained.run?.runId).toBe(held.run?.runId);
        const newer = yield* followUp("arrived after drain");
        const pending = yield* latest;

        expect(pending.acceptedInput?.messageId).toBe(newer.message.messageId);
        expect(pending.appliedInput).toEqual(drained.appliedInput);
        yield* Deferred.succeed(releaseModel, undefined);
        yield* Fiber.join(resumed);
        expect(dispatches).toBe(0);
        expect(consentCalls).toEqual(["original request"]);

        const log = yield* history.export(
          ThreadExportRequest.make({ threadId: start.worker.threadId }),
        );

        const payloads = log.records.map(({ record }) => record.payload);
        const inputs = payloads.filter((payload) => payload._tag === "UserInputRecorded");

        expect(inputs).toHaveLength(5);
        expect(inputs.slice(0, 4).map((input) => input.submissionId)).toEqual([
          receipt.submissionId,
          ...updates.map((update) => update.receipt?.submissionId),
        ]);
        expect(inputs.slice(0, 4).every((input) => input.runId === held.run?.runId)).toBe(true);
        expect(payloads.filter((payload) => payload._tag === "ToolApprovalRequested")).toHaveLength(
          1,
        );
        expect(payloads.filter((payload) => payload._tag === "ToolApprovalDecided")).toMatchObject([
          { toolCallId: consentId, decision: "approved" },
        ]);
        expect(
          payloads.findIndex((payload) => payload._tag === "ToolApprovalDecided"),
        ).toBeLessThan(
          payloads.findIndex(
            (payload) =>
              payload._tag === "UserInputRecorded" &&
              payload.submissionId === updates[0]?.receipt?.submissionId,
          ),
        );
        expect(
          payloads.filter(
            (payload) => payload._tag === "ToolCallPrepared" && payload.toolCallId === consentId,
          ),
        ).toHaveLength(1);
        expect(payloads.filter((payload) => payload._tag === "RunStarted")).toHaveLength(2);

        const sourceLog = yield* history.export(
          ThreadExportRequest.make({ threadId: sourceThreadId }),
        );

        expect(
          sourceLog.records.flatMap(({ record }) =>
            record.payload._tag === "UserInputRecorded" ? [record.payload.input] : [],
          ),
        ).toEqual([{ question: "launch" }]);
      }),
    ).pipe(Effect.provide(NodeFileSystem.layer)),
  15_000,
);

it.effect.each(["same", "revoked"] as const)(
  "retains the first public start capture when preparation races (%s)",
  (replay) =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "worker-capture-race-" });
        const preparing = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        let preparations = 0;
        let denied = false;

        const currentAuthority = Layer.succeed(WorkerHostAuthorizer)({
          authorize: (request) =>
            !denied && request.principal === principal && request.sourceThreadId === sourceThreadId
              ? Effect.succeed(principal)
              : WorkerError.make({ operation: request.operation, reason: "denied" }),
        });

        const research = Subagent.make("research", {
          target: target.definition,
          policy: declaration.policy,
          prepareInput: ({ question }) =>
            Effect.gen(function* () {
              const capture = ++preparations;

              if (capture === 1) {
                yield* Deferred.succeed(preparing, undefined);
                yield* Deferred.await(release);
              }

              return { question: `${question}:${capture}` };
            }),
        });

        const context = yield* Layer.build(
          NodeHost.NodeDurableHost.layerRegistered(
            [
              { agent: source, definitions },
              { agent: target, definitions },
            ],
            {
              filename: `${directory}/runtime.sqlite`,
              deploymentId: "capture-v1",
              producerId: "capture-node",
            },
          ).pipe(Layer.provide(currentAuthority)),
        );

        const runtime = Context.get(context, DurableAgentRuntime);

        yield* runtime.submitRegistered(
          source,
          { question: "launch" },
          {
            threadId: sourceThreadId,
            principal,
            idempotencyKey: key("source"),
          },
        );
        const owner = yield* runtime.workerHost({ sourceThreadId, principal });

        const start = (question: string) =>
          withFacet(
            owner,
            Subagent.start(research, { question }, { idempotencyKey: key("same-command") }),
          );

        const first = yield* start("brief").pipe(Effect.result, Effect.forkChild);

        yield* Deferred.await(preparing);
        const winner = yield* start("brief");

        denied = replay === "revoked";
        yield* Deferred.succeed(release, undefined);
        const loser = yield* Fiber.join(first);

        expect(loser).toMatchObject(
          replay !== "same"
            ? {
                _tag: "Failure",
                failure: { reason: "denied" },
              }
            : { _tag: "Success", success: winner },
        );
        denied = false;
        expect(yield* start("brief")).toEqual(winner);
        expect(preparations).toBe(2);

        const retained = yield* Context.get(context, MessageDeliveryStore).list({
          ownerThreadId: sourceThreadId,
          limit: 10,
        });

        expect(retained.items).toHaveLength(1);
        expect(retained.items[0]?.envelope.input).toEqual({ question: "brief:2" });
        expect(retained.items[0]?.envelope.workerAdmission?.parameters).toEqual({
          question: "brief",
        });
      }),
    ).pipe(Effect.provide(NodeFileSystem.layer)),
  15_000,
);

const assignment = (reply: () => Stream.Stream<Response.StreamPartEncoded>) =>
  Agent.withModel(
    Agent.make("assignment-worker", {
      input: target.definition.input,
      output: target.definition.output,
      instructions: "Answer as JSON.",
      toolkit: Toolkit.empty,
      runDisposition: {
        workerLifecycle: "assignment",
        schema: AssignmentDisposition,
        fromOutput: (output) => (output.answer === "question" ? "waiting" : "completed"),
      },
      policy: { maxTurns: 8, maxToolCalls: 8, maxDuration: "1 minute" },
    }),
    Model.make(
      "scripted",
      "assignment",
      Layer.effect(
        LanguageModel.LanguageModel,
        LanguageModel.make({ generateText: () => Effect.succeed([]), streamText: reply }),
      ),
    ),
  );

for (const point of ["terminalize:after-canonical-append", "failed-with-queue"] as const) {
  it.effect(
    `retains assignment settlement after ${point} and SQLite restart`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const directory = yield* fs.makeTempDirectoryScoped({ prefix: "assignment-restart-" });
          let calls = 0;

          const child = assignment(() => {
            calls++;

            return Stream.fromIterable(
              point === "failed-with-queue"
                ? parts.map((part) =>
                    part.type === "text-delta" ? { ...part, delta: '{"answer":123}' } : part,
                  )
                : parts,
            );
          });

          const research = Subagent.make("assignment", {
            target: child.definition,
            policy: declaration.policy,
          });

          const registrations = [
            { agent: source, definitions },
            { agent: child, definitions },
          ];

          const options = {
            filename: `${directory}/runtime.sqlite`,
            deploymentId: "assignment-v1",
            producerId: "assignment-node",
          };

          let armed = false;
          const firstScope = yield* Scope.make();

          yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void));

          const first = yield* Layer.build(
            NodeHost.NodeDurableHost.layerRegistered(registrations, {
              ...options,
              runtimeFailpoint: (location) =>
                armed &&
                (location === point ||
                  (point === "failed-with-queue" && location === "terminalize:after-reserve"))
                  ? DurableRuntimeFailpointError.make({ location })
                  : Effect.void,
            }).pipe(Layer.provide(authority)),
          ).pipe(Scope.provide(firstScope));

          const runtime = Context.get(first, DurableAgentRuntime);

          yield* runtime.submitRegistered(
            source,
            { question: "launch" },
            { threadId: sourceThreadId, principal, idempotencyKey: key("source") },
          );
          const owner = yield* runtime.workerHost({ sourceThreadId, principal });

          const startCommand = Subagent.start(
            research,
            { question: "task" },
            { idempotencyKey: key("task") },
          );

          const start = yield* withFacet(owner, startCommand);

          armed = true;
          expect(
            Exit.isFailure(
              yield* runtime.processThreadResolved(start.worker.threadId).pipe(Effect.exit),
            ),
          ).toBe(true);
          let queued: Receipt | undefined;

          if (point === "failed-with-queue") {
            queued =
              (yield* withFacet(
                owner,
                Subagent.followUp(
                  research,
                  start.worker,
                  { question: "queued" },
                  { idempotencyKey: key("queued") },
                ),
              )).receipt ?? undefined;
            expect(queued).toBeDefined();
          }
          yield* Scope.close(firstScope, Exit.void);

          const second = yield* Layer.build(
            NodeHost.NodeDurableHost.layerRegistered(registrations, options).pipe(
              Layer.provide(authority),
            ),
          );

          const reopened = Context.get(second, DurableAgentRuntime);
          const nextOwner = yield* reopened.workerHost({ sourceThreadId, principal });

          yield* reopened.runRecovery({ threadId: start.worker.threadId });
          yield* reopened.processThreadResolved(start.worker.threadId);
          const summary = yield* withFacet(nextOwner, Subagent.inspect(research, start.worker));

          expect(summary.state).toBe(point === "failed-with-queue" ? "failed" : "completed");
          expect(summary.run?.outcome).toBe(point === "failed-with-queue" ? "failed" : "completed");
          expect(calls).toBe(1);
          expect(yield* withFacet(nextOwner, startCommand)).toEqual(start);
          expect(
            yield* withFacet(
              nextOwner,
              Subagent.followUp(
                research,
                start.worker,
                { question: "reopen" },
                { idempotencyKey: key("new") },
              ),
            ),
          ).toMatchObject({ status: "refused", reason: "worker-stopped" });
          // Direct destination admission cannot bypass the seal either.
          expect(
            yield* reopened
              .submitRegistered(
                child,
                { question: "new start" },
                { threadId: start.worker.threadId, principal, idempotencyKey: key("direct") },
              )
              .pipe(Effect.flip),
          ).toMatchObject({ _tag: "AdmissionPolicyError", code: "worker-stopped" });
          if (queued !== undefined)
            expect(yield* reopened.submissionStatus(queued)).toMatchObject({
              _tag: "settled",
              settlement: { outcome: "aborted" },
            });

          const original = yield* withFacet(
            nextOwner,
            Subagent.inspect(research, start.worker, start.delivery.receipt!),
          );

          expect(original).toMatchObject({
            outcome: point === "failed-with-queue" ? "failed" : "completed",
          });
        }),
      ).pipe(Effect.provide(NodeFileSystem.layer)),
    15_000,
  );
}

it.effect(
  "keeps waiting assignments steerable and vetoes a completion when newer input is accepted before finalization",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "assignment-correction-" });
        let calls = 0;
        let finalized = 0;

        const child = assignment(() =>
          Stream.fromIterable(
            parts.map((part) =>
              part.type === "text-delta" && calls === 0
                ? { ...part, delta: '{"answer":"question"}' }
                : part,
            ),
          ).pipe(
            Stream.ensuring(
              Effect.sync(() => {
                calls++;
                finalized++;
              }),
            ),
          ),
        );

        const research = Subagent.make("assignment", {
          target: child.definition,
          policy: declaration.policy,
        });

        const registrations = [
          { agent: source, definitions },
          { agent: child, definitions },
        ];

        const options = {
          filename: `${directory}/runtime.sqlite`,
          deploymentId: "assignment-v1",
          producerId: "assignment-node",
        };

        let armed = false;
        const firstScope = yield* Scope.make();

        yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void));

        const first = yield* Layer.build(
          NodeHost.NodeDurableHost.layerRegistered(registrations, {
            ...options,
            runtimeFailpoint: (location) =>
              armed && location === "terminalize:after-reserve"
                ? DurableRuntimeFailpointError.make({ location })
                : Effect.void,
          }).pipe(Layer.provide(authority)),
        ).pipe(Scope.provide(firstScope));

        const runtime = Context.get(first, DurableAgentRuntime);

        yield* runtime.submitRegistered(
          source,
          { question: "launch" },
          { threadId: sourceThreadId, principal, idempotencyKey: key("source") },
        );
        const owner = yield* runtime.workerHost({ sourceThreadId, principal });

        const start = yield* withFacet(
          owner,
          Subagent.start(research, { question: "task" }, { idempotencyKey: key("task") }),
        );

        yield* runtime.processThreadResolved(start.worker.threadId);
        expect(yield* withFacet(owner, Subagent.inspect(research, start.worker))).toMatchObject({
          state: "idle",
          run: { disposition: "waiting" },
        });
        yield* withFacet(
          owner,
          Subagent.followUp(
            research,
            start.worker,
            { question: "answer" },
            { idempotencyKey: key("answer") },
          ),
        );
        armed = true;
        expect(
          Exit.isFailure(
            yield* runtime.processThreadResolved(start.worker.threadId).pipe(Effect.exit),
          ),
        ).toBe(true);

        const correction = yield* withFacet(
          owner,
          Subagent.followUp(
            research,
            start.worker,
            { question: "correction" },
            { idempotencyKey: key("correction") },
          ),
        );

        expect(correction.receipt).not.toBeNull();
        yield* Scope.close(firstScope, Exit.void);

        const second = yield* Layer.build(
          NodeHost.NodeDurableHost.layerRegistered(registrations, options).pipe(
            Layer.provide(authority),
          ),
        );

        const reopened = Context.get(second, DurableAgentRuntime);
        const nextOwner = yield* reopened.workerHost({ sourceThreadId, principal });

        yield* reopened.runRecovery({ threadId: start.worker.threadId });
        const pending = yield* withFacet(nextOwner, Subagent.inspect(research, start.worker));

        expect(pending.state).toBe("active");
        expect(pending.acceptedInput?.messageId).toBe(correction.message.messageId);
        expect(pending.appliedInput?.messageId).not.toBe(correction.message.messageId);
        yield* reopened.processThreadResolved(start.worker.threadId);
        expect(yield* withFacet(nextOwner, Subagent.inspect(research, start.worker))).toMatchObject(
          { state: "completed", appliedInput: { messageId: correction.message.messageId } },
        );
        expect(calls).toBe(3);
        expect(finalized).toBe(3);
      }),
    ).pipe(Effect.provide(NodeFileSystem.layer)),
  15_000,
);

// Regression: https://github.com/danieljvdm/effect-agent/blob/c721a292205e06185f0136c3ed05dcf53e29ca66/packages/effect-agent/src/durable/internal/worker-host.ts#L2189-L2210
for (const point of ["worker:after-source-append", "worker:after-origin-append"] as const) {
  it.effect(
    `recovers a successor after interruption at ${point}`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const directory = yield* fs.makeTempDirectoryScoped({ prefix: "successor-restart-" });
          let calls = 0;

          const child = assignment(() => {
            calls++;

            return Stream.fromIterable(parts);
          });

          const research = Subagent.make("assignment", {
            target: child.definition,
            policy: declaration.policy,
          });

          const registrations = [
            { agent: source, definitions },
            { agent: child, definitions },
          ];

          const options = {
            filename: `${directory}/runtime.sqlite`,
            deploymentId: "successor-restart",
            producerId: "successor-node",
          };

          const entered = yield* Deferred.make<void>();
          let armed = false;
          let finalized = 0;
          let revoked = false;

          const guarded = Layer.succeed(WorkerHostAuthorizer)({
            authorize: (request) =>
              revoked && request.operation === "start"
                ? WorkerError.make({ operation: request.operation, reason: "denied" })
                : Effect.succeed(principal),
          });

          const firstScope = yield* Scope.make();

          yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void));

          const first = yield* Layer.build(
            NodeHost.NodeDurableHost.layerRegistered(registrations, {
              ...options,
              runtimeFailpoint: (location) =>
                armed && location === point
                  ? Deferred.succeed(entered, undefined).pipe(
                      Effect.andThen(Effect.never),
                      Effect.ensuring(
                        Effect.sync(() => {
                          finalized++;
                        }),
                      ),
                    )
                  : Effect.void,
            }).pipe(Layer.provide(guarded)),
          ).pipe(Scope.provide(firstScope));

          const runtime = Context.get(first, DurableAgentRuntime);

          yield* runtime.submitRegistered(
            source,
            { question: "launch" },
            { threadId: sourceThreadId, principal, idempotencyKey: key("source") },
          );
          const owner = yield* runtime.workerHost({ sourceThreadId, principal });

          const original = yield* withFacet(
            owner,
            Subagent.start(research, { question: "original" }, { idempotencyKey: key("original") }),
          );

          yield* runtime.processThreadResolved(original.worker.threadId);
          const sealed = yield* withFacet(owner, Subagent.inspect(research, original.worker));

          expect(sealed.state).toBe("completed");

          const command = Subagent.start(
            research,
            { question: "correction" },
            { idempotencyKey: key("successor"), continuationOf: original.worker },
          );

          armed = true;
          const interrupted = yield* withFacet(owner, command).pipe(Effect.forkChild);

          yield* Deferred.await(entered);
          yield* Fiber.interrupt(interrupted);
          expect(Exit.hasInterrupts(yield* Fiber.await(interrupted))).toBe(true);
          expect(finalized).toBe(1);
          expect(calls).toBe(1);

          const retained = yield* Context.get(first, MessageDeliveryStore).list({
            ownerThreadId: sourceThreadId,
            limit: 10,
          });

          expect(retained.items).toHaveLength(2);

          const saved = retained.items.find(
            (row) => row.envelope.workerAdmission?.origin.continuationOf !== undefined,
          );

          expect(saved?.envelope.workerAdmission?.origin.continuationOf?.receipt).toEqual(
            sealed.run?.hostReceipt,
          );
          yield* Scope.close(firstScope, Exit.void);

          const second = yield* Layer.build(
            NodeHost.NodeDurableHost.layerRegistered(registrations, options).pipe(
              Layer.provide(guarded),
            ),
          );

          const reopened = Context.get(second, DurableAgentRuntime);
          const next = yield* reopened.workerHost({ sourceThreadId, principal });

          yield* TestClock.adjust("31 seconds");
          revoked = true;
          expect(yield* withFacet(next, command).pipe(Effect.flip)).toMatchObject({
            reason: "denied",
          });
          revoked = false;
          const successor = yield* withFacet(next, command);

          expect(successor.worker).toEqual(saved?.envelope.workerAdmission?.origin.worker);
          expect(successor.delivery.status).toBe("accepted");
          expect(
            (yield* Context.get(second, MessageDeliveryStore).list({
              ownerThreadId: sourceThreadId,
              limit: 10,
            })).items,
          ).toHaveLength(2);
          yield* reopened.processThreadResolved(successor.worker.threadId);
          expect(calls).toBe(2);
          expect((yield* withFacet(next, Subagent.inspect(research, successor.worker))).state).toBe(
            "completed",
          );
          expect(yield* withFacet(next, Subagent.inspect(research, original.worker))).toEqual(
            sealed,
          );
        }),
      ).pipe(Effect.provide(NodeFileSystem.layer)),
    15_000,
  );
}
