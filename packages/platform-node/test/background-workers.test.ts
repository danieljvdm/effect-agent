import * as Subagent from "@effect-agent/capabilities/Subagent";
import * as Agent from "@effect-agent/core/Agent";
import { ThreadId } from "@effect-agent/core/Identifiers";
import type { Receipt } from "@effect-agent/core/Receipt";
import { WorkerError } from "@effect-agent/core/Worker";
import { SubagentHost } from "@effect-agent/engine/SubagentHost";
import * as NodeHost from "@effect-agent/platform-node/NodeDurableHost";
import { DurableAgentRuntime } from "@effect-agent/thread/DurableAgentRuntime";
import {
  MessageDeliveryFailpoint,
  MessageDeliveryFailpointError,
  MessageDeliveryStore,
} from "@effect-agent/thread/MessageDelivery";
import { DefinitionDigestInput } from "@effect-agent/thread/Records";
import { IdempotencyKey, Principal } from "@effect-agent/thread/SubmissionLedger";
import { ThreadExportRequest, ThreadStore } from "@effect-agent/thread/ThreadStore";
import { WorkerConcurrencyResolver, WorkerHostAuthorizer } from "@effect-agent/thread/WorkerHost";
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
  Option,
  Result,
  Schema,
  Scope,
  Stream,
  Tracer,
} from "effect";
import { TestClock } from "effect/testing";
import { LanguageModel, Model, Toolkit, type Response } from "effect/unstable/ai";

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

const agent = (id: string, beforeReply: Effect.Effect<void> = Effect.void) =>
  Agent.withModel(
    Agent.make(id, {
      input: Schema.Struct({ question: Schema.String }),
      output: Schema.Struct({ answer: Schema.String }),
      instructions: "Answer as JSON.",
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

const untilSettled = (
  facet: SubagentHost["Service"],
  started: { readonly worker: Subagent.Worker<"research">; readonly receipt: Receipt },
) =>
  Effect.gen(function* () {
    for (let attempt = 0; attempt < 200; attempt++) {
      const result = yield* withFacet(
        facet,
        Subagent.inspect(declaration, started.worker, started.receipt),
      );

      if (result._tag === "Settled") return result;
      yield* TestClock.adjust(10);
      yield* Effect.yieldNow;
    }

    return yield* Effect.die("Worker receipt did not settle");
  });

// Regression: https://github.com/danieljvdm/effect-agent/blob/4c417d98e8cc790c42ab4200a54a0548fe32e6e3/packages/thread/src/internal/worker-host.ts#L1645-L1691
for (const completion of ["released", "interrupted"] as const) {
  it.effect(
    `reports retained worker delivery while another admission owns its ${completion} claim`,
    () =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const directory = yield* fs.makeTempDirectoryScoped({ prefix: "worker-admission-" });
          const claimed = yield* Deferred.make<void>();
          const release = yield* Deferred.make<void>();
          let claims = 0;
          let admissions = 0;
          let modelCalls = 0;
          let finalized = 0;
          let starts = 0;
          let driverPasses = 0;

          const tracer = Tracer.make({
            span: (options) => {
              if (options.name === "WorkerHost.start") starts++;
              if (options.name === "MessageDelivery.process") driverPasses++;

              return new Tracer.NativeSpan(options);
            },
          });

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
                      if (point !== "message-delivery:claim:after") return;
                      claims++;
                      if (claims !== 1) return;
                      yield* Deferred.succeed(claimed, undefined);
                      yield* Deferred.await(release).pipe(
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
          const store = Context.get(context, MessageDeliveryStore);
          const history = Context.get(context, ThreadStore);

          const sourceReceipt = yield* runtime.submitRegistered(
            source,
            { question: "prepare" },
            {
              threadId: sourceThreadId,
              principal,
              idempotencyKey: key("source"),
            },
          );

          yield* runtime.processThreadResolved(sourceThreadId);
          expect((yield* runtime.awaitSettlement(sourceReceipt)).outcome).toBe("completed");
          admissions = 0;
          const facet = yield* runtime.workerHost({ sourceThreadId, principal });

          const start = withFacet(
            facet,
            Subagent.start(
              research,
              { question: "once" },
              {
                idempotencyKey: key("held-claim"),
              },
            ),
          ).pipe(Effect.withTracer(tracer), Effect.withTracerEnabled(true));

          const first = yield* start.pipe(Effect.forkChild);

          yield* Deferred.await(claimed);
          const retained = (yield* store.list({ ownerThreadId: sourceThreadId, limit: 10 })).items;

          expect(retained).toHaveLength(1);
          expect(retained[0]).toMatchObject({ status: "pending", receipt: null });
          expect(retained[0]!.leaseUntilMillis).toBeGreaterThan(yield* Clock.currentTimeMillis);
          const clockStart = yield* Clock.currentTimeMillis;
          const wallStart = performance.now();

          const competing = yield* Effect.all([start, start, start].map(Effect.result), {
            concurrency: 3,
          });

          const wallMillis = performance.now() - wallStart;
          const clockMillis = (yield* Clock.currentTimeMillis) - clockStart;

          expect(claims).toBe(1);
          expect(admissions).toBe(0);
          expect(modelCalls).toBe(0);
          expect(clockMillis).toBe(0);
          if (completion === "interrupted") {
            yield* Fiber.interrupt(first);
            expect(Exit.hasInterrupts(yield* Fiber.await(first))).toBe(true);
            yield* TestClock.adjust("31 seconds");
          } else {
            yield* Deferred.succeed(release, undefined);
          }
          const started = yield* completion === "interrupted" ? start : Fiber.join(first);
          const replay = yield* start;

          expect(replay).toEqual(started);
          expect(admissions).toBe(1);
          expect(claims).toBe(completion === "interrupted" ? 2 : 1);
          expect(starts).toBe(completion === "interrupted" ? 6 : 5);
          expect(driverPasses).toBe(completion === "interrupted" ? 5 : 4);
          expect(finalized).toBe(1);
          yield* runtime.processThreadResolved(started.worker.threadId);
          expect((yield* runtime.awaitSettlement(started.receipt)).outcome).toBe("completed");
          expect(modelCalls).toBe(1);

          const sourceLog = yield* history.export(
            ThreadExportRequest.make({ threadId: sourceThreadId }),
          );

          const childLog = yield* history.export(
            ThreadExportRequest.make({ threadId: started.worker.threadId }),
          );

          expect(
            sourceLog.records.filter(
              ({ record }) => record.payload._tag === "WorkerInputRequested",
            ),
          ).toHaveLength(1);
          expect(
            childLog.records.filter(({ record }) => record.payload._tag === "UserInputRecorded"),
          ).toHaveLength(1);

          const outcomes = competing.map((result) =>
            Result.isFailure(result)
              ? result.failure._tag === "WorkerError"
                ? result.failure.reason
                : result.failure._tag
              : "started",
          );

          console.info(
            "Pending worker delivery admission",
            JSON.stringify({
              outcomes,
              completion,
              wallMillis,
              clockMillis,
              claims,
              admissions,
              modelCalls,
              finalized,
              starts,
              driverPasses,
            }),
          );
          expect(outcomes).toEqual(["delivery-pending", "delivery-pending", "delivery-pending"]);
        }),
      ).pipe(Effect.provide(NodeFileSystem.layer)),
    15_000,
  );
}

// The same delivery path must retain refusal and actual failure diagnostics.
it.effect(
  "keeps refusal, storage failure, defect and timeout distinct from retained delivery",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "worker-admission-errors-" });
        let mode: "retry" | "refuse" | "storage" | "defect" | "timeout" = "retry";
        let claims = 0;
        let timeoutsFinalized = 0;
        const timeoutEntered = yield* Deferred.make<void>();

        const context = yield* Layer.build(
          NodeHost.NodeDurableHost.layerRegistered(
            [
              { agent: source, definitions },
              { agent: target, definitions },
            ],
            {
              filename: `${directory}/runtime.sqlite`,
              deploymentId: "errors-v1",
              producerId: "errors-node",
            },
          ).pipe(
            Layer.provide(authority),
            Layer.provide(
              Layer.succeed(WorkerConcurrencyResolver, {
                resolve: () =>
                  Effect.suspend(() => {
                    if (mode === "retry")
                      return WorkerError.make({ operation: "start", reason: "unavailable" });
                    if (mode === "timeout")
                      return Deferred.succeed(timeoutEntered, undefined).pipe(
                        Effect.andThen(Effect.never),
                        Effect.ensuring(
                          Effect.sync(() => {
                            timeoutsFinalized++;
                          }),
                        ),
                      );

                    return Effect.succeed(Option.some({ maxActiveWorkersPerSource: 0 }));
                  }),
              }),
            ),
            Layer.provide(
              Layer.succeed(MessageDeliveryFailpoint, {
                hit: (point) =>
                  Effect.suspend(() => {
                    if (point !== "message-delivery:claim:before") return Effect.void;
                    claims++;
                    if (mode === "storage") return MessageDeliveryFailpointError.make({ point });
                    if (mode === "defect") return Effect.die("injected delivery defect");

                    return Effect.void;
                  }),
              }),
            ),
          ),
        );

        const runtime = Context.get(context, DurableAgentRuntime);
        const deliveries = Context.get(context, MessageDeliveryStore);

        yield* runtime.submitRegistered(
          source,
          { question: "prepare" },
          {
            threadId: sourceThreadId,
            principal,
            idempotencyKey: key("source"),
          },
        );
        yield* runtime.processThreadResolved(sourceThreadId);
        const facet = yield* runtime.workerHost({ sourceThreadId, principal });

        const start = (id: string) =>
          withFacet(
            facet,
            Subagent.start(declaration, { question: id }, { idempotencyKey: key(id) }),
          );

        expect(yield* start("retry").pipe(Effect.flip)).toMatchObject({
          _tag: "WorkerError",
          reason: "storage",
        });
        expect(yield* start("retry").pipe(Effect.flip)).toMatchObject({
          _tag: "WorkerError",
          reason: "storage",
        });
        expect(claims).toBe(1);
        expect(
          (yield* deliveries.list({ ownerThreadId: sourceThreadId, limit: 10 })).items[0],
        ).toMatchObject({
          status: "pending",
          receipt: null,
          retry: { lastFailure: "storage" },
        });
        mode = "refuse";
        yield* TestClock.adjust("1 second");
        expect(yield* start("retry").pipe(Effect.flip)).toMatchObject({
          _tag: "WorkerError",
          reason: "capacity",
        });
        expect(yield* start("retry").pipe(Effect.flip)).toMatchObject({
          _tag: "WorkerError",
          reason: "capacity",
        });
        expect(claims).toBe(2);
        expect(
          (yield* deliveries.list({ ownerThreadId: sourceThreadId, limit: 10 })).items[0],
        ).toMatchObject({
          status: "refused",
          receipt: null,
          refusal: "worker-capacity",
        });
        mode = "storage";
        expect(yield* start("storage").pipe(Effect.flip)).toMatchObject({
          _tag: "WorkerError",
          reason: "storage",
        });
        mode = "defect";
        expect(Exit.hasDies(yield* start("defect").pipe(Effect.exit))).toBe(true);
        mode = "timeout";
        const waiting = yield* start("timeout").pipe(Effect.result, Effect.forkChild);

        yield* Deferred.await(timeoutEntered);
        yield* TestClock.adjust("30 seconds");
        expect(yield* Fiber.join(waiting)).toMatchObject({
          _tag: "Failure",
          failure: { reason: "storage" },
        });
        expect(timeoutsFinalized).toBe(1);

        const retained = (yield* deliveries.list({ ownerThreadId: sourceThreadId, limit: 10 }))
          .items;

        expect(retained.find((row) => row.retry.lastFailure === "timeout")).toMatchObject({
          status: "pending",
          receipt: null,
        });

        const sourceLog = yield* Context.get(context, ThreadStore).export(
          ThreadExportRequest.make({ threadId: sourceThreadId }),
        );

        expect(
          sourceLog.records.filter(({ record }) => record.payload._tag === "WorkerInputRequested"),
        ).toHaveLength(0);
      }),
    ).pipe(Effect.provide(NodeFileSystem.layer)),
  15_000,
);

it.effect(
  "retains worker ownership across a Node restart and accepts another input after both agents settle",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "background-worker-" });

        const options = {
          filename: `${directory}/runtime.sqlite`,
          deploymentId: "background-v1",
          producerId: "background-node",
          workerConcurrency: 1,
          wakeScanInterval: 10,
          settlementPollInterval: 10,
        };

        const registrations = [
          { agent: source, definitions },
          { agent: target, definitions },
        ];

        const firstScope = yield* Scope.make();

        yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void));

        const first = yield* Layer.build(
          NodeHost.NodeDurableHost.layerRegistered(registrations, options).pipe(
            Layer.provide(authority),
          ),
        ).pipe(Scope.provide(firstScope));

        const runtime = Context.get(first, DurableAgentRuntime);

        const sourceReceipt = yield* runtime.submitRegistered(
          source,
          { question: "prepare workspace" },
          {
            threadId: sourceThreadId,
            principal,
            idempotencyKey: key("source"),
          },
        );

        yield* runtime.processThreadResolved(sourceThreadId);
        expect((yield* runtime.awaitSettlement(sourceReceipt)).outcome).toBe("completed");
        const facet = yield* runtime.workerHost({ sourceThreadId, principal });

        const started = yield* withFacet(
          facet,
          Subagent.start(declaration, { question: "first" }, { idempotencyKey: key("first") }),
        );

        expect(
          yield* withFacet(
            facet,
            Subagent.start(declaration, { question: "first" }, { idempotencyKey: key("first") }),
          ),
        ).toEqual(started);
        expect(
          yield* withFacet(
            facet,
            Subagent.start(declaration, { question: "changed" }, { idempotencyKey: key("first") }),
          ).pipe(Effect.result),
        ).toMatchObject({ _tag: "Failure", failure: { reason: "idempotency-conflict" } });
        expect(
          yield* withFacet(facet, Subagent.inspect(declaration, started.worker, started.receipt)),
        ).toEqual({ _tag: "Pending", receipt: started.receipt });

        const waiter = yield* withFacet(
          facet,
          Subagent.await(declaration, started.worker, started.receipt),
        ).pipe(Effect.forkChild);

        yield* Fiber.interrupt(waiter);
        expect(
          yield* withFacet(facet, Subagent.inspect(declaration, started.worker, started.receipt)),
        ).toEqual({ _tag: "Pending", receipt: started.receipt });
        yield* Scope.close(firstScope, Exit.void);

        const second = yield* Layer.build(
          NodeHost.layer(registrations, options).pipe(Layer.provide(authority)),
        );

        const reopened = Context.get(second, DurableAgentRuntime);
        const owner = yield* reopened.workerHost({ sourceThreadId, principal });
        const firstResult = yield* untilSettled(owner, started);

        expect(firstResult).toMatchObject({ outcome: "completed", result: { answer: "done" } });

        const nextReceipt = yield* withFacet(
          owner,
          Subagent.followUp(
            declaration,
            started.worker,
            { question: "second" },
            { idempotencyKey: key("second") },
          ),
        );

        expect(nextReceipt.threadId).toBe(started.worker.threadId);
        expect(nextReceipt.submissionId).not.toBe(started.receipt.submissionId);

        const secondResult = yield* untilSettled(owner, {
          worker: started.worker,
          receipt: nextReceipt,
        });

        expect(secondResult).toMatchObject({ outcome: "completed", result: { answer: "done" } });
        expect(secondResult.runId).not.toBe(firstResult.runId);
        expect(yield* withFacet(owner, Subagent.list(declaration))).toEqual({
          items: [{ worker: started.worker, latestReceipt: nextReceipt, state: "idle" }],
          next: null,
        });

        const sourceLog = yield* Context.get(second, ThreadStore).export(
          ThreadExportRequest.make({ threadId: sourceThreadId }),
        );

        expect(
          sourceLog.records.filter(({ record }) => record.payload._tag === "WorkerInputCompleted"),
        ).toHaveLength(2);

        const childLog = yield* Context.get(second, ThreadStore).export(
          ThreadExportRequest.make({ threadId: started.worker.threadId }),
        );

        expect(
          childLog.records.filter(({ record }) => record.payload._tag === "WorkerOriginRecorded"),
        ).toHaveLength(1);
        expect(
          yield* reopened
            .submitRegistered(
              target,
              { question: "bypass" },
              { threadId: started.worker.threadId, principal, idempotencyKey: key("bypass") },
            )
            .pipe(Effect.result),
        ).toMatchObject({ _tag: "Failure" });
        expect(
          yield* reopened
            .workerHost({ sourceThreadId, principal: Schema.decodeSync(Principal)("stranger") })
            .pipe(Effect.result),
        ).toMatchObject({ _tag: "Failure", failure: { reason: "denied" } });
      }),
    ).pipe(Effect.provide(NodeFileSystem.layer)),
  15_000,
);
