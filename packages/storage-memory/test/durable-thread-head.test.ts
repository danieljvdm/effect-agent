import * as Agent from "@effect-agent/core/Agent";
import { AgentPolicy } from "@effect-agent/core/AgentPolicy";
import { ReceiptId, ThreadId } from "@effect-agent/core/Identifiers";
import { CompactionError, ContextCompactor } from "@effect-agent/engine/ContextCompactor";
import { ModelCallContext } from "@effect-agent/engine/ContextWindow";
import { DurableStep, ToolExecutionClass } from "@effect-agent/engine/DurableStep";
import { RunContextPreparation, RunToolAuthorization } from "@effect-agent/engine/RunOptions";
import { MemorySubmissionLedgerLive } from "@effect-agent/storage-memory/MemorySubmissionLedger";
import { MemoryThreadStoreLive } from "@effect-agent/storage-memory/MemoryThreadStore";
import { DurableWorkerBinding, type ResolvedBinding } from "@effect-agent/thread/AgentRegistration";
import {
  DurableAgentRuntime,
  DurableRuntimeConfig,
  Receipt,
} from "@effect-agent/thread/DurableAgentRuntime";
import { DurableRuntimeFailpointError } from "@effect-agent/thread/DurableFailpoint";
import { OperationAuthorizer, OperationDenied } from "@effect-agent/thread/OperationAuthorizer";
import { DefinitionDigests, DeploymentId, Digest, ProducerId } from "@effect-agent/thread/Records";
import {
  AbortCommand,
  IdempotencyKey,
  LedgerError,
  OwnershipRenewal,
  OwnershipToken,
  Principal,
  QueueSequence,
  RecoverySnapshotRequest,
  ReleaseOwnershipRequest,
  SubmissionLedger,
} from "@effect-agent/thread/SubmissionLedger";
import { DurableRuntimeFailpointTestControl } from "@effect-agent/thread/testing/DurableFailpointTestControl";
import {
  ThreadExportRequest,
  ThreadRead,
  ThreadStore,
  ThreadStoreError,
} from "@effect-agent/thread/ThreadStore";
import { ToolReconciler } from "@effect-agent/thread/ToolReconciler";
import { WakeScheduler } from "@effect-agent/thread/WakeScheduler";
import { NodeCrypto } from "@effect/platform-node";
import { expect, layer } from "@effect/vitest";
import {
  Deferred,
  Duration,
  Effect,
  Exit,
  Fiber,
  Layer,
  Option,
  Ref,
  Schema,
  Stream,
} from "effect";
import { TestClock } from "effect/testing";
import {
  type Prompt,
  LanguageModel,
  Model,
  Tool,
  Toolkit,
  type Response,
} from "effect/unstable/ai";

const digest = Schema.decodeSync(Digest)("a".repeat(64));
const digests = DefinitionDigests.make({ agent: digest, model: digest, tools: digest });

const policy = AgentPolicy.make({
  maxTurns: 2,
  maxToolCalls: 2,
  maxDuration: "30 seconds",
  toolConcurrency: 1,
});

const definition = Agent.make("bounded-head", {
  input: Schema.String,
  output: Schema.String,
  instructions: "Answer as JSON.",
  toolkit: Toolkit.empty,
  policy,
});

const options = (thread: string, key: string) => ({
  threadId: Schema.decodeSync(ThreadId)(thread),
  principal: Schema.decodeSync(Principal)("head-test"),
  idempotencyKey: Schema.decodeSync(IdempotencyKey)(key),
  definitions: digests,
});

const finalParts: ReadonlyArray<Response.StreamPartEncoded> = [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: '"done"' },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage: { inputTokens: {}, outputTokens: {} } },
];

const makeModel = (
  response: Stream.Stream<Response.StreamPartEncoded>,
  close: Effect.Effect<void> = Effect.void,
) =>
  Model.make(
    "scripted",
    "bounded-head",
    Layer.effect(
      LanguageModel.LanguageModel,
      Effect.gen(function* () {
        yield* Effect.addFinalizer(() => close);

        return yield* LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: () => response,
        });
      }),
    ),
  );

const baseLayer = Layer.mergeAll(
  MemorySubmissionLedgerLive,
  MemoryThreadStoreLive,
  WakeScheduler.layerNoop,
  DurableRuntimeFailpointTestControl.layer,
  ToolReconciler.uncertain,
  DurableRuntimeConfig.layer({
    deploymentId: Schema.decodeSync(DeploymentId)("head-test"),
    producerId: Schema.decodeSync(ProducerId)("head-test"),
    leaseRenewalInterval: Duration.seconds(5),
    settlementPollInterval: Duration.millis(100),
  }),
).pipe(Layer.provideMerge(NodeCrypto.layer));

const makeRuntime = (bindings: ReadonlyArray<ResolvedBinding> = []) =>
  DurableAgentRuntime.pipe(
    Effect.provide(
      DurableAgentRuntime.layerWithBindings(bindings).pipe(
        Layer.provide(RunToolAuthorization.allowAll),
      ),
    ),
  );

const snapshot = Effect.fn(function* (receipt: Receipt) {
  const ledger = yield* SubmissionLedger;

  return yield* ledger.loadRecoverySnapshot(
    RecoverySnapshotRequest.make({ submissionId: receipt.submissionId }),
  );
});

layer(baseLayer)("bounded durable Thread processing", (it) => {
  // Regression seam: https://linear.app/reve/issue/KOM-125
  it.effect("resets completed Run context below capacity and recovers the canonical reset", () =>
    Effect.gen(function* () {
      const store = yield* ThreadStore;
      const failpoints = yield* DurableRuntimeFailpointTestControl;

      for (const restart of [false, true]) {
        const requests: Array<Prompt.Prompt> = [];

        const model = Model.make(
          "scripted",
          "fresh-request",
          Layer.effect(
            LanguageModel.LanguageModel,
            LanguageModel.make({
              generateText: () => Effect.succeed([]),
              streamText: (request) => {
                requests.push(request.prompt);

                return Stream.fromIterable<Response.StreamPartEncoded>([
                  { type: "text-start", id: "answer" },
                  {
                    type: "text-delta",
                    id: "answer",
                    delta: JSON.stringify(requests.length === 1 ? "OBSOLETE COMPLETION" : "done"),
                  },
                  ...finalParts.slice(2),
                ]);
              },
            }),
          ),
        );

        const agent = Agent.withModel(
          Agent.make("fresh-request-reset", {
            input: Schema.String,
            output: Schema.String,
            instructions: "Preserve the current request and return a JSON string.",
            toolkit: Toolkit.empty,
            policy: AgentPolicy.make({ ...policy, contextTokenLimit: 20_000 }),
          }),
          model,
        );

        const freshRuntime = Effect.gen(function* () {
          const binding = yield* DurableWorkerBinding.make(agent, digests);

          return yield* makeRuntime([binding]).pipe(
            Effect.provideService(
              RunContextPreparation,
              RunContextPreparation.of({
                hook: {
                  prepare: (request) =>
                    Effect.succeed({
                      prompt: request.source,
                      ...(request.turn === 1 ? { rollover: {} } : {}),
                    }),
                },
              }),
            ),
            Effect.provide(ContextCompactor.layerRollover),
          );
        });

        const runtime = yield* freshRuntime;

        const first = yield* runtime.submit(
          agent,
          "OBSOLETE REQUEST",
          options(`fresh-reset-${restart}`, "first"),
        );

        expect(Option.isSome(yield* runtime.processThreadHead(first.threadId))).toBe(true);

        const original = yield* store.export(
          ThreadExportRequest.make({ threadId: first.threadId }),
        );

        expect(
          original.records.some((entry) => entry.record.payload._tag === "CompactionCreated"),
        ).toBe(false);

        const second = yield* runtime.submit(
          agent,
          "CURRENT REQUEST: explain the new result",
          options(`fresh-reset-${restart}`, "second"),
        );

        if (restart) {
          yield* failpoints.setHandler((location) =>
            location === "compaction:after-canonical-append"
              ? DurableRuntimeFailpointError.make({ location })
              : Effect.void,
          );
          expect(
            Exit.isFailure(yield* Effect.exit(runtime.processThreadHead(second.threadId))),
          ).toBe(true);
          expect(requests).toHaveLength(1);
          expect((yield* snapshot(second)).ownership).toBeUndefined();

          const interrupted = yield* store.export(
            ThreadExportRequest.make({ threadId: second.threadId }),
          );

          expect(
            interrupted.records.filter(
              (entry) => entry.record.payload._tag === "CompactionCreated",
            ),
          ).toHaveLength(1);
          yield* failpoints.clear;
        }

        const resumed = restart ? yield* freshRuntime : runtime;

        expect(Option.isSome(yield* resumed.processThreadHead(second.threadId))).toBe(true);
        expect(requests).toHaveLength(2);
        const outgoing = JSON.stringify(requests[1]);

        expect(outgoing).toContain("CURRENT REQUEST: explain the new result");
        expect(outgoing).toContain("Preserve the current request and return a JSON string.");
        expect(outgoing).not.toContain("OBSOLETE REQUEST");
        expect(outgoing).not.toContain("OBSOLETE COMPLETION");
        const final = yield* store.export(ThreadExportRequest.make({ threadId: second.threadId }));

        expect(final.records.slice(0, original.records.length)).toEqual(original.records);
        expect(
          final.records.filter((entry) => entry.record.payload._tag === "RunStarted"),
        ).toHaveLength(2);

        const resets = final.records.filter(
          (entry) => entry.record.payload._tag === "CompactionCreated",
        );

        expect(resets).toHaveLength(1);
        expect(resets[0]?.record.payload).toMatchObject({ kind: "rollover", turn: 1 });
        expect(JSON.stringify(final.records)).toContain("OBSOLETE COMPLETION");
      }
    }),
  );

  // Regression seam: https://linear.app/reve/issue/KOM-125
  it.effect.each([
    "live",
    "append-failure",
    "append-interruption",
    "after-append-failure",
    "failed-tool",
  ] as const)("prepares from canonical profile evidence after %s", (scenario) =>
    Effect.gen(function* () {
      const restart = scenario !== "live" && scenario !== "failed-tool";

      const profileReceipt = Schema.Struct({
        profile: Schema.Literal("small"),
        evidence: Schema.String,
      });

      const selectModel = Tool.make("select_model", {
        parameters: Schema.Struct({}),
        success: profileReceipt,
        failure: Schema.Struct({ message: Schema.String }),
        failureMode: "return",
        dependencies: [DurableStep],
      }).annotate(ToolExecutionClass, "idempotent");

      const tools = Toolkit.make(selectModel);

      const routedDefinition = Agent.make("recover-routed-context", {
        input: Schema.String,
        output: Schema.String,
        instructions: "Preserve the current request and return a JSON string.",
        toolkit: tools,
        policy: AgentPolicy.make({
          ...policy,
          tokenBudget: 5_000,
          completionReserveTokens: 500,
        }),
      });

      const store = yield* ThreadStore;
      const requests: Array<{ model: string; prompt: Prompt.Prompt }> = [];
      const preparations: Array<string> = [];
      let handlerCalls = 0;
      let appendFault = scenario === "append-failure" || scenario === "append-interruption";

      const faultingStore = ThreadStore.of({
        ...store,
        append: (request) =>
          Effect.suspend(() => {
            if (
              appendFault &&
              request.batch.records.some((record) => record.payload._tag === "ToolCallSettled")
            ) {
              appendFault = false;

              return scenario === "append-interruption"
                ? Effect.interrupt
                : ThreadStoreError.make({
                    operation: "append",
                    message: "Profile result append failed",
                  });
            }

            return store.append(request);
          }),
      });

      const nativeModel = (name: "large" | "small") =>
        Model.make(
          "scripted",
          name,
          Layer.effect(
            LanguageModel.LanguageModel,
            LanguageModel.make({
              generateText: () => Effect.succeed([]),
              streamText: (request) => {
                requests.push({ model: name, prompt: request.prompt });

                return Stream.fromIterable<Response.StreamPartEncoded>(
                  requests.length === 1
                    ? [
                        {
                          type: "tool-call",
                          id: "select-small",
                          name: "select_model",
                          params: {},
                          providerExecuted: false,
                        },
                        {
                          type: "finish",
                          reason: "tool-calls",
                          usage: { inputTokens: { total: 100 }, outputTokens: { total: 10 } },
                        },
                      ]
                    : [
                        ...finalParts.slice(0, -1),
                        {
                          type: "finish",
                          reason: "stop",
                          usage: { inputTokens: { total: 75 }, outputTokens: { total: 5 } },
                        },
                      ],
                );
              },
            }),
          ),
        );

      const agent = Agent.withModel(routedDefinition, makeModel(Stream.empty));

      const freshRuntime = Effect.gen(function* () {
        // Each runtime has fresh host services. The only route state crosses the restart
        // in a successful canonical Tool result, not an incarnation-local Ref.
        const preparation = RunContextPreparation.of({
          hook: {
            prepare: (request) =>
              Effect.gen(function* () {
                const records = yield* store
                  .read(ThreadRead.make({ threadId: request.threadId, limit: 128 }))
                  .pipe(
                    Stream.runCollect,
                    Effect.mapError((cause) =>
                      CompactionError.make({ message: "Profile receipt is unavailable", cause }),
                    ),
                  );

                const receipt = records.findLast(
                  (entry) =>
                    entry.record.payload._tag === "ToolCallSettled" &&
                    entry.record.payload.runId === request.runId &&
                    entry.record.payload.toolName === "select_model" &&
                    !entry.record.payload.isFailure,
                )?.record.payload;

                const profile =
                  receipt?._tag === "ToolCallSettled"
                    ? (yield* Schema.decodeUnknownEffect(profileReceipt)(receipt.result).pipe(
                        Effect.mapError((cause) =>
                          CompactionError.make({ message: "Invalid profile receipt", cause }),
                        ),
                      )).profile
                    : "large";

                preparations.push(profile);

                return {
                  prompt: request.source,
                  modelCall: {
                    model: nativeModel(profile),
                    context: ModelCallContext.make({
                      contextCapacity: profile === "large" ? 12_000 : 4_000,
                      maxInputTokens: profile === "large" ? 9_000 : 2_000,
                      outputReserveTokens: 400,
                      uncountedOverheadTokens: 100,
                    }),
                  },
                };
              }),
          },
        });

        const binding = yield* DurableWorkerBinding.make(agent, digests).pipe(
          Effect.provide(
            tools.toLayer({
              select_model: () =>
                Effect.gen(function* () {
                  const steps = yield* DurableStep;

                  const selected = yield* steps.do(
                    "select-profile",
                    profileReceipt,
                    Effect.sync(() => {
                      handlerCalls += 1;

                      return {
                        profile: "small" as const,
                        evidence: "completed action evidence ".repeat(600),
                      };
                    }),
                  );

                  if (scenario === "failed-tool") {
                    return yield* Effect.fail({ message: "Profile selection was rejected" });
                  }

                  return selected;
                }),
            }),
          ),
        );

        return yield* makeRuntime([binding]).pipe(
          Effect.provideService(RunContextPreparation, preparation),
          Effect.provideService(ThreadStore, faultingStore),
          Effect.provide(ContextCompactor.layerRollover),
        );
      });

      const first = yield* freshRuntime;

      const receipt = yield* first.submit(
        agent,
        "CURRENT REQUEST: investigate the connection pool",
        options(`routed-${scenario}`, "first"),
      );

      const failpoints = yield* DurableRuntimeFailpointTestControl;

      if (restart) {
        yield* failpoints.setHandler((location) =>
          scenario === "after-append-failure" && location === "turn:after-results-append"
            ? DurableRuntimeFailpointError.make({ location })
            : Effect.void,
        );
        const interrupted = yield* Effect.exit(first.processThreadHead(receipt.threadId));

        expect(Exit.isFailure(interrupted)).toBe(true);
        expect(preparations).toEqual(["large"]);
        expect(requests.map((request) => request.model)).toEqual(["large"]);
        expect((yield* snapshot(receipt)).ownership).toBeUndefined();
      }
      const before = yield* store.export(ThreadExportRequest.make({ threadId: receipt.threadId }));

      const startedBefore = before.records.filter(
        (entry) => entry.record.payload._tag === "RunStarted",
      );

      expect(startedBefore).toHaveLength(restart ? 1 : 0);

      yield* failpoints.clear;
      if (restart) yield* TestClock.adjust("5 seconds");
      const resumed = restart ? yield* freshRuntime : first;
      const settled = yield* resumed.processThreadHead(receipt.threadId);

      expect(Option.isSome(settled)).toBe(true);
      const selected = scenario === "failed-tool" ? "large" : "small";

      expect(preparations).toEqual(["large", selected]);
      expect(requests.map((request) => request.model)).toEqual(["large", selected]);
      expect(handlerCalls).toBe(1);
      const second = requests[1];

      if (second === undefined) throw new Error("Expected the resumed smaller-model call");
      const text = JSON.stringify(second.prompt);

      expect(text).toContain("CURRENT REQUEST: investigate the connection pool");
      if (selected === "small") {
        expect(text).toContain("A fresh context window has started.");
        expect(text).toContain("turn 2/2");
        expect(text).toContain("tool-calls 1/2");
        expect(text).toContain("tokens 110/5000");
        expect(text).toContain(`elapsed ${restart ? 5 : 0}s/30s`);
      }
      const after = yield* store.export(ThreadExportRequest.make({ threadId: receipt.threadId }));

      const startedAfter = after.records.filter(
        (entry) => entry.record.payload._tag === "RunStarted",
      );

      expect(startedAfter).toHaveLength(1);
      if (restart) expect(startedAfter).toEqual(startedBefore);
      expect(
        after.records.filter((entry) => entry.record.payload._tag === "ToolStepSettled"),
      ).toHaveLength(1);

      const profileResults = after.records.filter(
        (entry) => entry.record.payload._tag === "ToolCallSettled",
      );

      expect(profileResults).toHaveLength(1);
      expect(profileResults[0]?.record.payload).toMatchObject({
        toolName: "select_model",
        isFailure: scenario === "failed-tool",
      });

      const rollovers = after.records.filter(
        (entry) => entry.record.payload._tag === "CompactionCreated",
      );

      expect(rollovers).toHaveLength(selected === "small" ? 1 : 0);
      if (selected === "small") {
        expect(rollovers[0]?.record.payload).toMatchObject({ kind: "rollover", turn: 2 });
      }

      const terminal = after.records.find(
        (entry) => entry.record.payload._tag === "SubmissionSettled",
      )?.record.payload;

      expect(terminal).toMatchObject({
        outcome: "completed",
        usageSummary: {
          modelCalls: 2,
          inputTokens: { total: 175 },
          outputTokens: { total: 15 },
          byModel:
            selected === "small"
              ? [
                  { model: "large", modelCalls: 1 },
                  { model: "small", modelCalls: 1 },
                ]
              : [{ model: "large", modelCalls: 2 }],
        },
      });
    }),
  );

  it.effect("settles only the FIFO head and closes its provider before returning", () =>
    Effect.gen(function* () {
      const closed = yield* Ref.make(0);

      const model = makeModel(
        Stream.fromIterable(finalParts),
        Ref.update(closed, (n) => n + 1),
      );

      const agent = Agent.withModel(definition, model);
      const binding = yield* DurableWorkerBinding.make(agent, digests);
      const bindings = [binding];
      const runtime = yield* makeRuntime(bindings);

      bindings.length = 0;
      const first = yield* runtime.submit(agent, "first", options("bounded", "first"));
      const reserved = yield* Deferred.make<void>();
      const finish = yield* Deferred.make<void>();
      const control = yield* DurableRuntimeFailpointTestControl;

      yield* control.setHandler((location) =>
        location === "terminalize:after-reserve"
          ? Deferred.succeed(reserved, undefined).pipe(Effect.andThen(Deferred.await(finish)))
          : Effect.void,
      );

      expect((yield* runtime.submissionStatus(first))._tag).toBe("pending");

      const worker = yield* runtime.processThreadHead(first.threadId).pipe(Effect.forkChild);

      yield* Deferred.await(reserved);
      // Admission after the final Turn cannot join that Run and must remain FIFO work.
      const second = yield* runtime.submit(agent, "second", options("bounded", "second"));

      yield* control.clear;
      yield* Deferred.succeed(finish, undefined);
      const result = yield* Fiber.join(worker);

      expect(Option.isSome(result)).toBe(true);
      if (Option.isSome(result)) expect(result.value.submissionId).toBe(first.submissionId);
      expect((yield* runtime.submissionStatus(first))._tag).toBe("settled");
      expect((yield* runtime.submissionStatus(second))._tag).toBe("pending");
      expect((yield* snapshot(second)).ownership).toBeUndefined();
      expect((yield* snapshot(second)).submission.state).toBe("ready");
      expect(yield* Ref.get(closed)).toBe(1);

      const remainder = yield* runtime.processThreadResolved(first.threadId);

      expect(remainder.map((settlement) => settlement.submissionId)).toEqual([second.submissionId]);
      expect(yield* Ref.get(closed)).toBe(2);
    }),
  );

  it.effect.each(["failpoint", "lookup", "defect"] as const)(
    "releases a claim after an early %s failure",
    (failure) =>
      Effect.gen(function* () {
        const ledger = yield* SubmissionLedger;
        const armed = yield* Ref.make(false);

        const runtime = yield* makeRuntime().pipe(
          Effect.provideService(SubmissionLedger, {
            ...ledger,
            lookup: (request) =>
              Ref.get(armed).pipe(
                Effect.flatMap((enabled) => {
                  if (!enabled || failure === "failpoint") return ledger.lookup(request);

                  return failure === "lookup"
                    ? Effect.fail(LedgerError.make({ operation: "lookup", message: "unavailable" }))
                    : Effect.die("lookup defect");
                }),
              ),
          }),
        );

        const receipt = yield* runtime.submit(
          { definition },
          "first",
          options(`early-${failure}`, "first"),
        );

        const control = yield* DurableRuntimeFailpointTestControl;

        yield* Ref.set(armed, true);
        if (failure === "failpoint") {
          yield* control.setHandler((location) =>
            location === "claim:after-claim"
              ? Effect.fail(DurableRuntimeFailpointError.make({ location }))
              : Effect.void,
          );
        }
        const result = yield* Effect.exit(runtime.processThreadHead(receipt.threadId));

        expect(Exit.isFailure(result)).toBe(true);
        expect((yield* snapshot(receipt)).ownership).toBeUndefined();
        yield* control.clear;
        yield* Ref.set(armed, false);
        expect((yield* runtime.submissionStatus(receipt))._tag).toBe("pending");
      }),
  );

  it.effect("registers claim cleanup even when interrupted during the acquisition handoff", () =>
    Effect.gen(function* () {
      const ledger = yield* SubmissionLedger;
      const acquired = yield* Deferred.make<void>();
      const handoff = yield* Deferred.make<void>();

      const runtime = yield* makeRuntime().pipe(
        Effect.provideService(SubmissionLedger, {
          ...ledger,
          claim: (request) =>
            ledger.claim(request).pipe(
              Effect.tap(() => Deferred.succeed(acquired, undefined)),
              Effect.tap(() => Deferred.await(handoff)),
            ),
        }),
      );

      const receipt = yield* runtime.submit({ definition }, "first", options("handoff", "first"));

      const worker = yield* runtime.processThreadHead(receipt.threadId).pipe(Effect.forkChild);

      yield* Deferred.await(acquired);
      const interrupt = yield* Fiber.interrupt(worker).pipe(Effect.forkChild);

      yield* Effect.yieldNow;
      yield* Deferred.succeed(handoff, undefined);
      yield* Fiber.join(interrupt);
      expect((yield* snapshot(receipt)).ownership).toBeUndefined();
      expect((yield* runtime.submissionStatus(receipt))._tag).toBe("pending");
    }),
  );

  it.effect(
    "detaches a waiter and releases the latest renewed token when its Attempt is interrupted",
    () =>
      Effect.gen(function* () {
        const ledger = yield* SubmissionLedger;
        const started = yield* Deferred.make<void>();
        const renewalReturned = yield* Deferred.make<void>();
        const renewalHandoff = yield* Deferred.make<void>();
        const renewedToken = Schema.decodeSync(OwnershipToken)("renewed-token");
        const originalToken = yield* Ref.make<Option.Option<OwnershipToken>>(Option.none());
        const releasedTokens = yield* Ref.make<ReadonlyArray<OwnershipToken>>([]);
        const closed = yield* Ref.make(false);

        const agent = Agent.withModel(
          definition,
          makeModel(
            Stream.fromEffect(Deferred.succeed(started, undefined)).pipe(
              Stream.drain,
              Stream.concat(Stream.never),
            ),
            Ref.set(closed, true),
          ),
        );

        const binding = yield* DurableWorkerBinding.make(agent, digests);

        const runtime = yield* makeRuntime([binding]).pipe(
          Effect.provideService(SubmissionLedger, {
            ...ledger,
            renewOwnership: (request) =>
              ledger.renewOwnership(request).pipe(
                Effect.tap(() => Deferred.await(started)),
                Effect.tap((renewed) =>
                  Ref.set(originalToken, Option.some(renewed.ownershipToken)),
                ),
                Effect.map((renewed) =>
                  OwnershipRenewal.make({ ...renewed, ownershipToken: renewedToken }),
                ),
                Effect.tap(() => Deferred.succeed(renewalReturned, undefined)),
                Effect.tap(() => Deferred.await(renewalHandoff)),
              ),
            releaseOwnership: (request) =>
              Effect.gen(function* () {
                yield* Ref.update(releasedTokens, (tokens) => [...tokens, request.ownershipToken]);
                const original = yield* Ref.get(originalToken);

                return yield* ledger.releaseOwnership(
                  ReleaseOwnershipRequest.make({
                    ...request,
                    ownershipToken:
                      request.ownershipToken === renewedToken && Option.isSome(original)
                        ? original.value
                        : request.ownershipToken,
                  }),
                );
              }),
          }),
        );

        const receipt = yield* runtime.submit(agent, "first", options("renewed", "first"));

        const worker = yield* runtime.processThreadHead(receipt.threadId).pipe(Effect.forkChild);

        yield* Deferred.await(started);
        yield* Deferred.await(renewalReturned);
        const waiter = yield* runtime.awaitSettlement(receipt).pipe(Effect.forkChild);

        yield* Fiber.interrupt(waiter);
        expect((yield* snapshot(receipt)).ownership).toBeDefined();
        expect(yield* Ref.get(closed)).toBe(false);
        const interrupt = yield* Fiber.interrupt(worker).pipe(Effect.forkChild);

        yield* Effect.yieldNow;
        yield* Deferred.succeed(renewalHandoff, undefined);
        yield* Fiber.join(interrupt);
        expect(yield* Ref.get(releasedTokens)).toEqual([renewedToken]);
        expect((yield* snapshot(receipt)).ownership).toBeUndefined();
        expect(yield* Ref.get(closed)).toBe(true);
        expect((yield* runtime.submissionStatus(receipt))._tag).toBe("pending");
      }),
  );

  it.effect("keeps approval suspension pending without touching a queued follower", () =>
    Effect.gen(function* () {
      const approvalTools = Toolkit.make(
        Tool.make("approve", {
          parameters: Schema.Struct({}),
          success: Schema.String,
          needsApproval: true,
        }),
      );

      const agent = Agent.withModel(
        Agent.make("approval-head", {
          input: Schema.String,
          output: Schema.String,
          instructions: "Call approve.",
          toolkit: approvalTools,
          policy,
        }),
        makeModel(
          Stream.fromIterable<Response.StreamPartEncoded>([
            {
              type: "tool-call",
              id: "approval-1",
              name: "approve",
              params: {},
              providerExecuted: false,
            },
            { type: "finish", reason: "tool-calls", usage: { inputTokens: {}, outputTokens: {} } },
          ]),
        ),
      );

      const binding = yield* DurableWorkerBinding.make(agent, digests).pipe(
        Effect.provide(approvalTools.toLayer({ approve: () => Effect.die("unapproved handler") })),
      );

      const runtime = yield* makeRuntime([binding]);

      const first = yield* runtime.submit(agent, "first", options("suspended", "first"));
      const second = yield* runtime.submit(agent, "second", options("suspended", "second"));

      expect(Option.isNone(yield* runtime.processThreadHead(first.threadId))).toBe(true);
      expect((yield* snapshot(first)).submission.state).toBe("suspended");
      expect((yield* runtime.submissionStatus(first))._tag).toBe("pending");
      expect(Option.isNone(yield* runtime.processThreadHead(first.threadId))).toBe(true);
      expect((yield* snapshot(second)).ownership).toBeUndefined();
      expect((yield* runtime.submissionStatus(second))._tag).toBe("pending");
    }),
  );

  it.effect("recovers a queued abort without claiming or releasing the head", () =>
    Effect.gen(function* () {
      const ledger = yield* SubmissionLedger;
      const claims = yield* Ref.make(0);

      const runtime = yield* makeRuntime().pipe(
        Effect.provideService(SubmissionLedger, {
          ...ledger,
          claim: (request) =>
            Ref.update(claims, (n) => n + 1).pipe(Effect.andThen(ledger.claim(request))),
        }),
      );

      const first = yield* runtime.submit(
        { definition },
        "first",
        options("queued-abort", "first"),
      );

      const second = yield* runtime.submit(
        { definition },
        "second",
        options("queued-abort", "second"),
      );

      yield* runtime.abort(
        AbortCommand.make({
          submissionId: second.submissionId,
          author: "test",
          reason: "cancel queued work",
        }),
      );
      yield* runtime.recoverSubmission(second.submissionId);
      expect(yield* Ref.get(claims)).toBe(0);
      expect((yield* snapshot(first)).ownership).toBeUndefined();
      const status = yield* runtime.submissionStatus(second);

      expect(status._tag).toBe("settled");
      if (status._tag === "settled") expect(status.settlement.outcome).toBe("aborted");
    }),
  );

  it.effect(
    "releases recovery ownership when settlement reservation fails before its canonical append",
    () =>
      Effect.gen(function* () {
        const runtime = yield* makeRuntime();
        const control = yield* DurableRuntimeFailpointTestControl;

        const receipt = yield* runtime.submit(
          { definition },
          "first",
          options("recovery-failure", "first"),
        );

        yield* runtime.abort(
          AbortCommand.make({
            submissionId: receipt.submissionId,
            author: "test",
            reason: "cancel",
          }),
        );
        yield* control.setHandler((location) =>
          location === "terminalize:after-reserve"
            ? Effect.fail(DurableRuntimeFailpointError.make({ location }))
            : Effect.void,
        );

        expect(
          (yield* runtime.recoverSubmission(receipt.submissionId).pipe(Effect.flip))._tag,
        ).toBe("DurableRuntimeFailpointError");
        expect((yield* snapshot(receipt)).ownership).toBeUndefined();
        expect((yield* runtime.submissionStatus(receipt))._tag).toBe("pending");
        yield* control.clear;
        yield* runtime.recoverSubmission(receipt.submissionId);
        expect((yield* runtime.submissionStatus(receipt))._tag).toBe("settled");
      }),
  );

  it.effect("reports invalid public admission constraints as typed failures", () =>
    Effect.gen(function* () {
      const runtime = yield* makeRuntime();

      expect(
        (yield* runtime
          .submit({ definition }, "input", {
            ...options("invalid-constraints", "group"),
            admissionGroup: "",
          })
          .pipe(Effect.flip))._tag,
      ).toBe("LedgerError");
      expect(
        (yield* runtime
          .submit({ definition }, "input", {
            ...options("invalid-constraints", "fence"),
            admissionFence: { policyId: "host", key: "entity", revision: "" },
          })
          .pipe(Effect.flip))._tag,
      ).toBe("LedgerError");
    }),
  );

  for (const location of [
    "terminalize:after-reserve",
    "terminalize:after-canonical-append",
  ] as const) {
    it.effect(`holds admission group through ${location} until canonical repair finalizes`, () =>
      Effect.gen(function* () {
        const runtime = yield* makeRuntime();
        const control = yield* DurableRuntimeFailpointTestControl;
        const original = { ...options(`group-${location}`, "first"), admissionGroup: "entity" };
        const receipt = yield* runtime.submit({ definition }, "first", original);

        yield* runtime.abort(
          AbortCommand.make({
            submissionId: receipt.submissionId,
            author: "test",
            reason: "cancel",
          }),
        );
        yield* control.setHandler((point) =>
          point === location ? DurableRuntimeFailpointError.make({ location }) : Effect.void,
        );
        expect(
          (yield* runtime.recoverSubmission(receipt.submissionId).pipe(Effect.flip))._tag,
        ).toBe("DurableRuntimeFailpointError");
        expect((yield* runtime.submissionStatus(receipt))._tag).toBe("pending");
        expect(
          yield* runtime
            .submit({ definition }, "second", {
              ...original,
              idempotencyKey: Schema.decodeSync(IdempotencyKey)("second"),
            })
            .pipe(Effect.flip),
        ).toMatchObject({ reason: "occupied" });
        expect((yield* runtime.submit({ definition }, "first", original)).receiptId).toBe(
          receipt.receiptId,
        );
        yield* control.clear;
        yield* runtime.recoverSubmission(receipt.submissionId);
        expect((yield* runtime.submissionStatus(receipt))._tag).toBe("settled");
        expect(
          (yield* runtime.submit({ definition }, "second", {
            ...original,
            idempotencyKey: Schema.decodeSync(IdempotencyKey)("second"),
          })).submissionId,
        ).not.toBe(receipt.submissionId);
      }),
    );
  }

  it.effect("authorizes status before ledger reads and rejects a mismatched receipt Thread", () =>
    Effect.gen(function* () {
      const runtime = yield* makeRuntime();

      const receipt = yield* runtime.submit(
        { definition },
        "first",
        options("authorized", "first"),
      );

      const ledger = yield* SubmissionLedger;
      const reads = yield* Ref.make(0);

      const agent = Agent.withModel(definition, makeModel(Stream.fromIterable(finalParts)));
      const binding = yield* DurableWorkerBinding.make(agent, digests);

      const denied = yield* makeRuntime([binding]).pipe(
        Effect.provideService(OperationAuthorizer, {
          authorize: (request) =>
            Effect.fail(OperationDenied.make({ operation: request.operation, reason: "denied" })),
        }),
        Effect.provideService(SubmissionLedger, {
          ...ledger,
          lookup: (request) =>
            Ref.update(reads, (n) => n + 1).pipe(Effect.andThen(ledger.lookup(request))),
        }),
      );

      expect((yield* denied.submissionStatus(receipt).pipe(Effect.flip))._tag).toBe(
        "OperationDenied",
      );
      expect(yield* Ref.get(reads)).toBe(0);
      expect((yield* denied.inspectSubmissionStatus(receipt))._tag).toBe("pending");

      expect(Option.isSome(yield* denied.processThreadHead(receipt.threadId))).toBe(true);
      expect((yield* denied.inspectSubmissionStatus(receipt))._tag).toBe("settled");

      const mismatched = Receipt.make({
        ...receipt,
        threadId: Schema.decodeSync(ThreadId)("other"),
      });

      expect((yield* runtime.submissionStatus(mismatched).pipe(Effect.flip))._tag).toBe(
        "OperationDenied",
      );
      for (const altered of [
        Receipt.make({ ...receipt, receiptId: Schema.decodeSync(ReceiptId)("wrong-receipt") }),
        Receipt.make({ ...receipt, queueSequence: Schema.decodeSync(QueueSequence)(999) }),
      ])
        expect((yield* runtime.submissionStatus(altered).pipe(Effect.flip))._tag).toBe(
          "OperationDenied",
        );
    }),
  );
});
