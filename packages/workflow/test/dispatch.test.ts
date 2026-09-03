import { Agent, AgentPolicy, ReceiptId, SettlementId, ThreadId } from "@effect-agent/core";
import { RunToolAuthorization } from "@effect-agent/engine";
import { MemorySubmissionLedgerLive, MemoryThreadStoreLive } from "@effect-agent/storage-memory";
import {
  AdmissionRequest,
  DefinitionDigestInput,
  DeploymentId,
  DurableAgentRuntime,
  DurableRuntimeConfig,
  DurableRuntimeFailpoint,
  IdempotencyKey,
  LedgerError,
  Principal,
  ProducerId,
  QueueSequence,
  Receipt,
  RecoverySnapshotRequest,
  SubmissionLedger,
  ThreadRead,
  ThreadStore,
  ToolReconciler,
  WakeScheduler,
  digestDefinitions,
  digestJson,
} from "@effect-agent/thread";
import { NodeCrypto } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import {
  Context,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Option,
  Ref,
  Result,
  Schema,
  Stream,
} from "effect";
import { TestClock } from "effect/testing";
import { LanguageModel, Model, Tool, Toolkit, type Response } from "effect/unstable/ai";
import { Workflow, WorkflowEngine } from "effect/unstable/workflow";

import {
  WorkflowDispatchError,
  WorkflowDispatchIntent,
  WorkflowDispatchStore,
  WorkflowAgentHost,
  WorkflowRepairTrigger,
  WorkflowSettlementReference,
  WorkflowSubmission,
} from "../src/index.ts";

const deploymentId = Schema.decodeSync(DeploymentId)("dispatch-tests");
const definitions = DefinitionDigestInput.make({ agent: "v1", model: "v1", tools: "v1" });

const definition = Agent.make("dispatch-test-agent", {
  input: Schema.String,
  output: Schema.Struct({ answer: Schema.String }),
  instructions: "Return an answer as JSON.",
  toolkit: Toolkit.empty,
  policy: AgentPolicy.make({
    maxTurns: 1,
    maxToolCalls: 1,
    maxDuration: "1 minute",
    toolConcurrency: 1,
  }),
});

const model = Model.make(
  "scripted",
  "dispatch-tests",
  Layer.effect(
    LanguageModel.LanguageModel,
    LanguageModel.make({
      generateText: () => Effect.succeed([]),
      streamText: () =>
        Stream.make(
          { type: "text-start", id: "answer" },
          { type: "text-delta", id: "answer", delta: '{"answer":"done"}' },
          { type: "text-end", id: "answer" },
          { type: "finish", reason: "stop", usage: { inputTokens: {}, outputTokens: {} } },
        ),
    }),
  ),
);

const Hold = Tool.make("hold", { parameters: Schema.Struct({}), success: Schema.String });
const holdTools = Toolkit.make(Hold);

const toolDefinition = Agent.make("dispatch-ordinary-tool", {
  input: Schema.String,
  output: Schema.Struct({ answer: Schema.String }),
  instructions: "Call hold once, then answer as JSON.",
  toolkit: holdTools,
  policy: AgentPolicy.make({
    maxTurns: 3,
    maxToolCalls: 1,
    maxDuration: "1 minute",
    toolConcurrency: 1,
  }),
});

const baseLayer = Layer.mergeAll(
  MemorySubmissionLedgerLive,
  MemoryThreadStoreLive,
  WorkflowEngine.layerMemory,
  RunToolAuthorization.allowAll,
  ToolReconciler.uncertain,
  WakeScheduler.layerNoop,
  DurableRuntimeFailpoint.layer,
  DurableRuntimeConfig.layer({
    deploymentId,
    producerId: Schema.decodeSync(ProducerId)("dispatch-worker"),
  }),
).pipe(Layer.provideMerge(NodeCrypto.layer));

const sameIntent = Schema.toEquivalence(WorkflowDispatchIntent);

type ScanMode = "normal" | "empty" | "fail" | "block";

const makeBlockedOperation = Effect.gen(function* () {
  const entered = yield* Deferred.make<void>();
  const released = yield* Ref.make(0);

  const run = Deferred.succeed(entered, undefined).pipe(
    Effect.andThen(Effect.never),
    Effect.ensuring(Ref.update(released, (count) => count + 1)),
  );

  return { entered, released, run };
});

// Every service is real except this local outbox and the selected fault decorators.
// The outbox implements identity conflicts, filtering, ordering, and exact removal.
const makeFixture = Effect.fn("dispatch-test.makeFixture")(function* (ordinaryTool = false) {
  const base = yield* Layer.build(baseLayer);
  const realLedger = Context.get(base, SubmissionLedger);
  const native = Context.get(base, WorkflowEngine.WorkflowEngine);
  const rows = new Map<string, WorkflowDispatchIntent>();
  const ledgerScanMode = yield* Ref.make<ScanMode>("normal");
  const intentScanMode = yield* Ref.make<ScanMode>("normal");
  const ledgerBlock = yield* makeBlockedOperation;
  const intentBlock = yield* makeBlockedOperation;
  const resumeBlock = yield* makeBlockedOperation;
  const blockedResume = yield* Ref.make<Option.Option<string>>(Option.none());
  const launched = yield* Ref.make<ReadonlyArray<string>>([]);
  const claims = yield* Ref.make(0);
  const nativeName = yield* Ref.make("");
  const pollOverride = yield* Ref.make<Option.Option<WorkflowSettlementReference>>(Option.none());
  const parkNextRun = yield* Ref.make(false);
  const parking = yield* Deferred.make<void>();
  const allowSuspension = yield* Deferred.make<void>();
  const toolEntered = yield* Deferred.make<void>();
  const releaseTool = yield* Deferred.make<void>();
  const toolInvocations = yield* Ref.make(0);
  const modelCalls = yield* Ref.make(0);

  const blockingModel = Model.make(
    "scripted",
    "dispatch-ordinary-tool",
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: () =>
          Stream.unwrap(
            Ref.getAndUpdate(modelCalls, (count) => count + 1).pipe(
              Effect.map((call): Stream.Stream<Response.StreamPartEncoded> =>
                call === 0
                  ? Stream.make(
                      {
                        type: "tool-call",
                        id: "hold-once",
                        name: "hold",
                        params: {},
                        providerExecuted: false,
                      },
                      {
                        type: "finish",
                        reason: "tool-calls",
                        usage: { inputTokens: {}, outputTokens: {} },
                      },
                    )
                  : Stream.make(
                      { type: "text-start", id: "answer" },
                      { type: "text-delta", id: "answer", delta: '{"answer":"done"}' },
                      { type: "text-end", id: "answer" },
                      {
                        type: "finish",
                        reason: "stop",
                        usage: { inputTokens: {}, outputTokens: {} },
                      },
                    ),
              ),
            ),
          ),
      }),
    ),
  );

  const toolContext = yield* Layer.build(
    holdTools.toLayer({
      hold: () =>
        Effect.gen(function* () {
          yield* Ref.update(toolInvocations, (count) => count + 1);
          yield* Deferred.succeed(toolEntered, undefined);
          yield* Deferred.await(releaseTool);

          return "released";
        }),
    }),
  );

  const selectedDefinition = ordinaryTool ? toolDefinition : definition;

  const ledger = SubmissionLedger.of({
    ...realLedger,
    claim: (request) =>
      Ref.update(claims, (count) => count + 1).pipe(Effect.andThen(realLedger.claim(request))),
    scanNonterminal: Stream.unwrap(
      Effect.gen(function* () {
        switch (yield* Ref.get(ledgerScanMode)) {
          case "normal":
            return realLedger.scanNonterminal;
          case "empty":
            return Stream.empty;
          case "fail":
            return Stream.fail(
              LedgerError.make({
                operation: "test-discovery",
                message: "Discovery failed independently of identity lookup",
              }),
            );
          case "block":
            return Stream.fromEffect(ledgerBlock.run);
        }
      }),
    ),
  });

  const engine = WorkflowEngine.WorkflowEngine.of({
    ...native,
    register: (workflow, execute) =>
      Ref.set(nativeName, workflow._tag).pipe(
        Effect.andThen(
          native.register(workflow, (payload, executionId) =>
            Effect.gen(function* () {
              if (yield* Ref.getAndSet(parkNextRun, false)) {
                yield* Deferred.succeed(parking, undefined);
                yield* Deferred.await(allowSuspension);

                return yield* Workflow.suspend(yield* WorkflowEngine.WorkflowInstance);
              }

              return yield* execute(payload, executionId);
            }),
          ),
        ),
      ),
    execute: (workflow, options) =>
      Ref.update(launched, (ids) => [...ids, options.executionId]).pipe(
        Effect.andThen(native.execute(workflow, options)),
      ),
    resume: (workflow, executionId) =>
      Effect.gen(function* () {
        const blocked = yield* Ref.get(blockedResume);

        if (Option.isSome(blocked) && blocked.value === executionId) return yield* resumeBlock.run;
        yield* native.resume(workflow, executionId);
      }),
    poll: (workflow, executionId) =>
      Effect.gen(function* () {
        const override = yield* Ref.get(pollOverride);

        if (Option.isNone(override)) return yield* native.poll(workflow, executionId);

        const value = yield* Schema.decodeUnknownEffect(workflow.successSchema)(
          override.value,
        ).pipe(Effect.orDie);

        return Option.some(new Workflow.Complete({ exit: Exit.succeed(value) }));
      }),
  });

  const dispatch = WorkflowDispatchStore.of({
    put: (intent) =>
      Effect.gen(function* () {
        const previous = rows.get(intent.executionId);

        if (previous !== undefined && !sameIntent(previous, intent)) {
          return yield* WorkflowDispatchError.make({
            operation: "put",
            message: "Intent conflict",
          });
        }
        rows.set(intent.executionId, intent);

        return intent;
      }),
    scan: (request) =>
      Effect.gen(function* () {
        switch (yield* Ref.get(intentScanMode)) {
          case "empty":
            return [];
          case "fail":
            return yield* WorkflowDispatchError.make({
              operation: "test-scan",
              message: "Scan failed",
            });
          case "block":
            return yield* intentBlock.run;
          case "normal":
            return [...rows.values()]
              .filter(
                (intent) =>
                  intent.deploymentId === request.deploymentId &&
                  intent.workflowName === request.workflowName &&
                  (request.after === undefined || intent.executionId > request.after),
              )
              .sort((left, right) =>
                left.executionId < right.executionId
                  ? -1
                  : left.executionId > right.executionId
                    ? 1
                    : 0,
              )
              .slice(0, request.limit);
        }
      }),
    remove: (intent) =>
      Effect.gen(function* () {
        const previous = rows.get(intent.executionId);

        if (previous !== undefined && !sameIntent(previous, intent)) {
          return yield* WorkflowDispatchError.make({
            operation: "remove",
            message: "Intent conflict",
          });
        }
        rows.delete(intent.executionId);
      }),
  });

  const services = base.pipe(
    Context.merge(toolContext),
    Context.add(SubmissionLedger, ledger),
    Context.add(WorkflowEngine.WorkflowEngine, engine),
    Context.add(WorkflowDispatchStore, dispatch),
    Context.add(WorkflowRepairTrigger, { register: () => Effect.void }),
  );

  const runtimeContext = yield* Layer.build(
    DurableAgentRuntime.layerRegistered([
      { agent: selectedDefinition, model: ordinaryTool ? blockingModel : model, definitions },
    ]),
  ).pipe(Effect.provideContext(services));

  const runtime = Context.get(runtimeContext, DurableAgentRuntime);

  const hostContext = yield* Layer.build(
    WorkflowAgentHost.layer({
      deploymentId,
      principal: "test-caller",
      dispatchTimeoutMillis: 100,
      repairBatchSize: 16,
      executionConcurrency: 2,
    }),
  ).pipe(Effect.provideContext(Context.merge(services, runtimeContext)));

  const host = Context.get(hostContext, WorkflowAgentHost);
  const digests = yield* digestDefinitions(definitions).pipe(Effect.provideContext(base));

  const admit = Effect.fn("dispatch-test.admit")((name: string, key = name) =>
    runtime.submit({ definition: selectedDefinition }, "question", {
      threadId: Schema.decodeSync(ThreadId)(name),
      principal: Schema.decodeSync(Principal)("test-caller"),
      idempotencyKey: Schema.decodeSync(IdempotencyKey)(key),
      definitions: digests,
    }),
  );

  const intentFor = Effect.fn("dispatch-test.intentFor")(function* (receipt: Receipt) {
    return WorkflowDispatchIntent.make({
      version: 1,
      deploymentId,
      receipt,
      workflowName: yield* Ref.get(nativeName),
      executionId: yield* host.executionId(receipt),
    });
  });

  const client = (intent: WorkflowDispatchIntent) =>
    Workflow.make(intent.workflowName, {
      payload: WorkflowSubmission,
      success: WorkflowSettlementReference,
      idempotencyKey: () => "unused-by-explicit-id-client",
    });

  const complete = Effect.fn("dispatch-test.complete")(function* (receipt: Receipt) {
    const intent = yield* intentFor(receipt);

    yield* dispatch.put(intent);

    const reference = yield* native.execute(client(intent), {
      executionId: intent.executionId,
      payload: WorkflowSubmission.make(intent),
    });

    return { intent, reference };
  });

  const waitNative = Effect.fn("dispatch-test.waitNative")(function* (
    intent: WorkflowDispatchIntent,
    tag: "Suspended" | "Complete",
  ) {
    while (true) {
      const status = yield* native.poll(client(intent), intent.executionId);

      if (Option.isSome(status) && status.value._tag === tag) return status.value;
      yield* Effect.yieldNow;
    }
  });

  return {
    host,
    runtime,
    realLedger,
    dispatch,
    rows,
    admit,
    intentFor,
    complete,
    waitNative,
    ledgerScanMode,
    intentScanMode,
    ledgerBlock,
    intentBlock,
    resumeBlock,
    blockedResume,
    launched,
    claims,
    pollOverride,
    parkNextRun,
    parking,
    allowSuspension,
    toolEntered,
    releaseTool,
    toolInvocations,
    store: Context.get(base, ThreadStore),
  };
});

it.effect(
  "repairs retained terminal intents despite discovery failure and checks canonical completion",
  () =>
    Effect.gen(function* () {
      const fixture = yield* makeFixture();
      const { intent, reference } = yield* fixture.complete(yield* fixture.admit("terminal"));

      yield* Ref.set(
        fixture.pollOverride,
        Option.some(
          WorkflowSettlementReference.make({
            ...reference,
            settlementId: Schema.decodeSync(SettlementId)("wrong-settlement"),
          }),
        ),
      );
      const mismatch = yield* Effect.result(fixture.host.repair);

      expect(
        Result.isFailure(mismatch) &&
          mismatch.failure._tag === "WorkflowDispatchError" &&
          mismatch.failure.operation === "completion",
      ).toBe(true);
      expect(fixture.rows.has(intent.executionId)).toBe(true);

      yield* Ref.set(fixture.pollOverride, Option.none());
      yield* Ref.set(fixture.ledgerScanMode, "fail");
      const repaired = yield* Effect.result(fixture.host.repair);

      expect(Result.isFailure(repaired) && repaired.failure._tag === "LedgerError").toBe(true);
      expect(fixture.rows.size).toBe(0);
      expect((yield* fixture.runtime.inspectSubmissionStatus(intent.receipt))._tag).toBe("settled");
    }).pipe(Effect.scoped),
);

it.effect("times out a stuck dispatch, finalizes it, and repairs the remaining intent", () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture();
    const first = yield* fixture.complete(yield* fixture.admit("stuck-a"));
    const second = yield* fixture.complete(yield* fixture.admit("stuck-b"));

    const [blocked, later] = [first.intent, second.intent].sort((left, right) =>
      left.executionId < right.executionId ? -1 : 1,
    );

    if (blocked === undefined || later === undefined)
      return yield* Effect.die("Missing test intents");
    yield* Ref.set(fixture.blockedResume, Option.some(blocked.executionId));
    const repair = yield* Effect.forkChild(Effect.result(fixture.host.repair));

    yield* Deferred.await(fixture.resumeBlock.entered);
    yield* TestClock.adjust(100);
    const result = yield* Fiber.join(repair);

    expect(
      Result.isFailure(result) &&
        result.failure._tag === "WorkflowDispatchError" &&
        result.failure.operation === "dispatch",
    ).toBe(true);
    expect(yield* Ref.get(fixture.resumeBlock.released)).toBe(1);
    expect(fixture.rows.has(blocked.executionId)).toBe(true);
    expect(fixture.rows.has(later.executionId)).toBe(false);

    yield* Ref.set(fixture.blockedResume, Option.none());
    yield* fixture.host.repair;
    expect(fixture.rows.size).toBe(0);
  }).pipe(Effect.scoped),
);

it.effect.each(["ledger", "intents"] as const)(
  "bounds the %s scan without blocking the other repair queue",
  (queue) =>
    Effect.gen(function* () {
      const fixture = yield* makeFixture();
      const receipt = yield* fixture.admit(`scan-${queue}`);
      const intent = yield* fixture.intentFor(receipt);

      if (queue === "ledger") yield* fixture.complete(receipt);
      const mode = queue === "ledger" ? fixture.ledgerScanMode : fixture.intentScanMode;
      const block = queue === "ledger" ? fixture.ledgerBlock : fixture.intentBlock;

      yield* Ref.set(mode, "block");
      const repair = yield* Effect.forkChild(Effect.result(fixture.host.repair));

      yield* Deferred.await(block.entered);
      yield* TestClock.adjust(100);
      const result = yield* Fiber.join(repair);

      expect(
        Result.isFailure(result) &&
          result.failure._tag === "WorkflowDispatchError" &&
          result.failure.operation === (queue === "ledger" ? "scanSubmissions" : "scanIntents"),
      ).toBe(true);
      expect(yield* Ref.get(block.released)).toBe(1);
      expect(yield* Ref.get(fixture.launched)).toContain(intent.executionId);
      yield* fixture.waitNative(intent, "Complete");
      yield* Ref.set(mode, "normal");
      yield* fixture.host.repair;
      expect(fixture.rows.size).toBe(0);
    }).pipe(Effect.scoped),
);

it.effect("rejects corrupted and cross-deployment outbox identities before launch or claim", () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture();
    const receipt = yield* fixture.admit("identity");
    const valid = yield* fixture.intentFor(receipt);

    const foreignAdmission = yield* fixture.realLedger.admit(
      AdmissionRequest.make({
        threadId: Schema.decodeSync(ThreadId)("foreign"),
        principal: Schema.decodeSync(Principal)("foreign"),
        idempotencyKey: Schema.decodeSync(IdempotencyKey)("foreign"),
        agentId: definition.id,
        agentDigests: yield* digestDefinitions(definitions).pipe(Effect.provide(NodeCrypto.layer)),
        deploymentId: Schema.decodeSync(DeploymentId)("another-deployment"),
        inputPayload: "question",
        inputDigest: yield* digestJson("question").pipe(Effect.provide(NodeCrypto.layer)),
      }),
    );

    const foreign = Receipt.make({
      ...foreignAdmission,
      threadId: Schema.decodeSync(ThreadId)("foreign"),
    });

    const corruptions = [
      WorkflowDispatchIntent.make({ ...valid, executionId: "wrong-execution-id" }),
      WorkflowDispatchIntent.make({
        ...valid,
        receipt: Receipt.make({
          ...receipt,
          receiptId: Schema.decodeSync(ReceiptId)("wrong-receipt"),
        }),
      }),
      WorkflowDispatchIntent.make({
        ...valid,
        receipt: Receipt.make({
          ...receipt,
          queueSequence: Schema.decodeSync(QueueSequence)(receipt.queueSequence + 1),
        }),
      }),
      yield* fixture.intentFor(foreign),
    ];

    yield* Ref.set(fixture.ledgerScanMode, "empty");
    for (const intent of corruptions) {
      yield* fixture.dispatch.put(intent);
      const result = yield* Effect.result(fixture.host.repair);

      expect(Result.isFailure(result) && result.failure._tag === "WorkflowDispatchError").toBe(
        true,
      );
      expect(fixture.rows.has(intent.executionId)).toBe(true);
      yield* fixture.dispatch.remove(intent);
    }
    expect(yield* Ref.get(fixture.launched)).toEqual([]);
    expect(yield* Ref.get(fixture.claims)).toBe(0);
  }).pipe(Effect.scoped),
);

it.effect("retains an early wake until suspension and rejects premature native completion", () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture();
    const receipt = yield* fixture.admit("early-wake");
    const intent = yield* fixture.intentFor(receipt);

    yield* Ref.set(fixture.parkNextRun, true);
    yield* Ref.set(
      fixture.pollOverride,
      Option.some(
        WorkflowSettlementReference.make({
          version: 1,
          submissionId: receipt.submissionId,
          threadId: receipt.threadId,
          settlementId: Schema.decodeSync(SettlementId)("premature"),
        }),
      ),
    );
    const early = yield* Effect.result(fixture.host.repair);

    yield* Deferred.await(fixture.parking);
    expect(
      Result.isFailure(early) &&
        early.failure._tag === "WorkflowDispatchError" &&
        early.failure.operation === "completion",
    ).toBe(true);
    expect((yield* fixture.runtime.inspectSubmissionStatus(receipt))._tag).toBe("pending");
    expect(fixture.rows.has(intent.executionId)).toBe(true);
    yield* Ref.set(fixture.pollOverride, Option.none());
    yield* Deferred.succeed(fixture.allowSuspension, undefined);
    yield* fixture.waitNative(intent, "Suspended");

    yield* fixture.host.repair;
    yield* fixture.waitNative(intent, "Complete");
    yield* fixture.host.repair;
    expect((yield* fixture.runtime.inspectSubmissionStatus(receipt))._tag).toBe("settled");
    expect(fixture.rows.size).toBe(0);
  }).pipe(Effect.scoped),
);

it.effect("serializes one Thread before taking permits and lets another Thread finish", () =>
  Effect.gen(function* () {
    const fixture = yield* makeFixture(true);
    const head = yield* fixture.admit("owned-thread", "head");

    yield* fixture.host.repair;
    yield* Deferred.await(fixture.toolEntered);

    const before = yield* fixture.realLedger.loadRecoverySnapshot(
      RecoverySnapshotRequest.make({ submissionId: head.submissionId }),
    );

    expect(before.ownership).toBeDefined();
    const follower = yield* fixture.admit("owned-thread", "follower");
    const independent = yield* fixture.admit("independent-thread");

    yield* fixture.host.repair;
    yield* fixture.waitNative(yield* fixture.intentFor(independent), "Complete");

    const whileHeld = yield* fixture.realLedger.loadRecoverySnapshot(
      RecoverySnapshotRequest.make({ submissionId: head.submissionId }),
    );

    const records = yield* Stream.runCollect(
      fixture.store.read(
        ThreadRead.make({
          threadId: head.threadId,
          limit: 1024,
        }),
      ),
    );

    expect(whileHeld.ownership).toEqual(before.ownership);
    expect(records.some((record) => record.record.payload._tag === "ToolCallUnknown")).toBe(false);
    expect(yield* Ref.get(fixture.toolInvocations)).toBe(1);
    expect((yield* fixture.runtime.inspectSubmissionStatus(independent))._tag).toBe("settled");

    yield* Deferred.succeed(fixture.releaseTool, undefined);
    yield* fixture.waitNative(yield* fixture.intentFor(head), "Complete");
    yield* fixture.waitNative(yield* fixture.intentFor(follower), "Complete");
    yield* fixture.host.repair;
    expect((yield* fixture.runtime.inspectSubmissionStatus(follower))._tag).toBe("settled");
    expect(yield* Ref.get(fixture.toolInvocations)).toBe(1);
    expect(fixture.rows.size).toBe(0);
  }).pipe(Effect.scoped),
);
