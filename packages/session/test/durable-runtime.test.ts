import { NodeCrypto } from "@effect/platform-node";
import { describe, expect, layer } from "@effect/vitest";
import {
  Cause,
  DateTime,
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
  Agent,
  AgentPolicy,
  ConversationId,
  IdGenerator,
  ReceiptId,
  SubmissionId,
  ToolCallId,
} from "@effect-agent/core";
import { LanguageModel, Model, Prompt, Tool, Toolkit, type Response } from "effect/unstable/ai";
import {
  MemoryConversationStoreLive,
  MemorySubmissionLedgerLive,
} from "@effect-agent/storage-memory";

import {
  AbortCommand,
  AdmissionConflict,
  BatchId,
  CanonicalRecordEnvelope,
  CanonicalSequence,
  ConversationRead,
  ConversationStore,
  DefinitionDigests,
  DeploymentId,
  Digest,
  DurableAgentRuntime,
  DurableRuntimeConfig,
  DurableRuntimeFailpoint,
  DurableRuntimeFailpointError,
  DurableRuntimeFailpointTestControl,
  FenceRejected,
  IdempotencyKey,
  ObservationOffset,
  Principal,
  ProducerId,
  QueueSequence,
  Receipt,
  RecoverySnapshotRequest,
  Settlement,
  SettlementConflict,
  SubmissionLedger,
  SubmissionLookupById,
  SubmissionLookupByKey,
  WakeScheduler,
  modelResponseRecordId,
  projectRunJournal,
  promptFromCanonicalRecords,
  recoveryRepairRecordId,
  runIdForSubmission,
  submissionInputRecordId,
  submissionSettlementRecordId,
  toolCallSettledRecordId,
  turnCanonicalBatch,
  turnIdForRun,
  type DurableRuntimeFailpointLocation,
  type DurableSubmitOptions,
} from "../src/index.ts";

const SHA_A = Schema.decodeSync(Digest)("a".repeat(64));
const PRINCIPAL = Schema.decodeSync(Principal)("principal-durable");
const DIGESTS = DefinitionDigests.make({ agent: SHA_A, model: SHA_A, tools: SHA_A });
const decodeConversationId = Schema.decodeSync(ConversationId);
const decodeIdempotencyKey = Schema.decodeSync(IdempotencyKey);

const submitOptions = (conversationId: string, idempotencyKey: string): DurableSubmitOptions => ({
  conversationId: decodeConversationId(conversationId),
  principal: PRINCIPAL,
  idempotencyKey: decodeIdempotencyKey(idempotencyKey),
  definitions: DIGESTS,
});

const usage = { inputTokens: {}, outputTokens: {} };

const finalParts = (text: string): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: text },
  { type: "text-end", id: "answer" },
  { type: "finish", reason: "stop", usage },
];

const toolCallParts: ReadonlyArray<Response.StreamPartEncoded> = [
  {
    type: "tool-call",
    id: "search-1",
    name: "search",
    params: { query: "sea" },
    providerExecuted: false,
  },
  { type: "finish", reason: "tool-calls", usage },
];

/**
 * Scripted model whose call counter and captured request prompts live OUTSIDE the Model Layer,
 * so they survive Layer rebuilds across Attempts (each Attempt provides the Model afresh).
 */
const makeScriptedModel = (script: (call: number) => ReadonlyArray<Response.StreamPartEncoded>) =>
  Effect.gen(function* () {
    const calls = yield* Ref.make(0);
    const prompts: Array<Prompt.Prompt> = [];
    const model = Model.make(
      "scripted",
      "durable-test",
      Layer.effect(
        LanguageModel.LanguageModel,
        LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: (request) =>
            Stream.unwrap(
              Ref.getAndUpdate(calls, (call) => call + 1).pipe(
                Effect.map((call) => {
                  prompts.push(request.prompt);
                  return Stream.fromIterable(script(call));
                }),
              ),
            ),
        }),
      ),
    );
    return { model, prompts };
  });

const plannerDefinition = Agent.define("durable-planner", {
  input: Schema.Struct({ question: Schema.String }),
  output: Schema.Struct({ answer: Schema.String }),
  instructions: ({ question }) => `Answer ${question} as JSON.`,
  toolkit: Toolkit.empty,
  policy: AgentPolicy.make({
    maxTurns: 3,
    maxToolCalls: 2,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  }),
});

const Search = Tool.make("search", {
  parameters: Schema.Struct({ query: Schema.String }),
  success: Schema.Struct({ available: Schema.Boolean }),
});
const searchTools = Toolkit.make(Search);
const searchDefinition = Agent.define("durable-search", {
  input: Schema.Struct({ question: Schema.String }),
  output: Schema.Struct({ answer: Schema.String }),
  instructions: "Search before answering.",
  toolkit: searchTools,
  policy: AgentPolicy.make({
    maxTurns: 3,
    maxToolCalls: 2,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  }),
});
const searchToolLayer = searchTools.toLayer({
  search: () => Effect.succeed({ available: true }),
});

const configLayer = DurableRuntimeConfig.layer({
  deploymentId: Schema.decodeSync(DeploymentId)("deployment-durable"),
  producerId: Schema.decodeSync(ProducerId)("producer-durable"),
  settlementPollInterval: Duration.millis(100),
  leaseRenewalInterval: Duration.seconds(5),
  abortPollInterval: Duration.millis(100),
});

const baseLayer = Layer.mergeAll(
  MemorySubmissionLedgerLive,
  MemoryConversationStoreLive,
  WakeScheduler.layerNoop,
  DurableRuntimeFailpoint.layerTest,
  configLayer,
).pipe(Layer.provideMerge(NodeCrypto.layer));

const testLayer = DurableAgentRuntime.layer.pipe(Layer.provideMerge(baseLayer));

const readLog = (conversationId: string) =>
  Effect.gen(function* () {
    const store = yield* ConversationStore;
    return yield* Stream.runCollect(
      store.read(
        ConversationRead.make({
          conversationId: decodeConversationId(conversationId),
          limit: 1_024,
        }),
      ),
    );
  });

const logTags = (records: ReadonlyArray<CanonicalRecordEnvelope>): ReadonlyArray<string> =>
  records.map((envelope) => envelope.record.payload._tag);

const lookupState = (submissionId: SubmissionId) =>
  Effect.gen(function* () {
    const ledger = yield* SubmissionLedger;
    const snapshot = yield* ledger.lookup(SubmissionLookupById.make({ submissionId }));
    expect(Option.isSome(snapshot)).toBe(true);
    if (Option.isNone(snapshot)) throw new Error("Expected the Submission to exist");
    return snapshot.value.state;
  });

const armFailpoint = (location: DurableRuntimeFailpointLocation) =>
  Effect.gen(function* () {
    const control = yield* DurableRuntimeFailpointTestControl;
    yield* control.setHandler((hitLocation) =>
      hitLocation === location
        ? Effect.fail(DurableRuntimeFailpointError.make({ location: hitLocation }))
        : Effect.void,
    );
  });

const clearFailpoint = Effect.gen(function* () {
  const control = yield* DurableRuntimeFailpointTestControl;
  yield* control.clear;
});

const failureTag = <A, E>(exit: Exit.Exit<A, E>): string => {
  expect(Exit.isFailure(exit)).toBe(true);
  if (Exit.isSuccess(exit)) throw new Error("Expected the Effect to fail");
  const failure = Cause.findErrorOption(exit.cause);
  expect(Option.isSome(failure)).toBe(true);
  if (Option.isNone(failure)) throw new Error("Expected a typed failure");
  const error: unknown = failure.value;
  return typeof error === "object" && error !== null && "_tag" in error
    ? String(error._tag)
    : "unknown";
};

layer(testLayer)("DUR P4 DurableAgentRuntime", (it) => {
  it.effect("submit returns a durable Receipt once admission and readiness commit", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const scripted = yield* makeScriptedModel(() => finalParts('{"answer":"ok"}'));
      const agent = Agent.withModel(plannerDefinition, scripted.model);

      const receipt = yield* runtime.submit(
        agent,
        { question: "receipt?" },
        submitOptions("conversation-submit", "submit-1"),
      );

      expect(receipt.conversationId).toBe("conversation-submit");
      expect(receipt.queueSequence).toBe(1);
      expect(yield* lookupState(receipt.submissionId)).toBe("ready");
      const records = yield* readLog("conversation-submit");
      expect(logTags(records)).toEqual(["ConversationCreated"]);
      expect(records[0]?.record.recordId).toBe("conversation-created:conversation-submit");
    }),
  );

  it.effect("same-key resubmission returns the original Receipt; different content conflicts", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const scripted = yield* makeScriptedModel(() => finalParts('{"answer":"ok"}'));
      const agent = Agent.withModel(plannerDefinition, scripted.model);
      const options = submitOptions("conversation-idempotent", "submit-1");

      const first = yield* runtime.submit(agent, { question: "same?" }, options);
      const second = yield* runtime.submit(agent, { question: "same?" }, options);
      expect(second).toEqual(first);

      const conflict = yield* Effect.exit(
        runtime.submit(agent, { question: "different!" }, options),
      );
      expect(failureTag(conflict)).toBe("AdmissionConflict");
    }),
  );

  it.effect("runs accepted work to settlement with an ordered canonical log", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const scripted = yield* makeScriptedModel((call) =>
        call === 0 ? toolCallParts : finalParts('{"answer":"Found."}'),
      );
      const agent = Agent.withModel(searchDefinition, scripted.model);
      const conversation = "conversation-full-run";

      const receipt = yield* runtime.submit(
        agent,
        { question: "Is a flight available?" },
        submitOptions(conversation, "run-1"),
      );
      const settlements = yield* runtime
        .processConversation(agent, decodeConversationId(conversation))
        .pipe(Effect.provide(searchToolLayer));

      expect(settlements).toHaveLength(1);
      expect(settlements[0]?.outcome).toBe("completed");
      expect(settlements[0]?.receiptId).toBe(receipt.receiptId);

      const submissionId = receipt.submissionId;
      const runId = runIdForSubmission(submissionId);
      const records = yield* readLog(conversation);
      expect(logTags(records)).toEqual([
        "ConversationCreated",
        "UserInputRecorded",
        "ModelResponseRecorded",
        "ToolCallSettled",
        "ModelResponseRecorded",
        "SubmissionSettled",
      ]);
      expect(records.map((envelope) => envelope.record.recordId)).toEqual([
        `conversation-created:${conversation}`,
        submissionInputRecordId(submissionId),
        modelResponseRecordId(runId, 1),
        `tool-settled:${runId}:1:search-1`,
        modelResponseRecordId(runId, 2),
        submissionSettlementRecordId(submissionId),
      ]);

      const settled = records.at(-1)?.record.payload;
      expect(settled?._tag).toBe("SubmissionSettled");
      if (settled?._tag === "SubmissionSettled") {
        expect(settled.outcome).toBe("completed");
        expect(settled.runId).toBe(runId);
        expect(settled.result).toEqual({ answer: "Found." });
      }

      // The canonical journal alone rebuilds the exact model-visible prompt.
      const prompt = yield* promptFromCanonicalRecords(records);
      expect(prompt.content.map((message) => message.role)).toEqual([
        "system",
        "user",
        "assistant",
        "tool",
        "assistant",
      ]);

      const settlement = yield* runtime.awaitSettlement(receipt);
      expect(settlement).toEqual(settlements[0]);
    }),
  );

  it.effect("awaitSettlement waits on the poll fallback until the lane settles", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const scripted = yield* makeScriptedModel(() => finalParts('{"answer":"waited"}'));
      const agent = Agent.withModel(plannerDefinition, scripted.model);
      const conversation = "conversation-await";

      const receipt = yield* runtime.submit(
        agent,
        { question: "when?" },
        submitOptions(conversation, "await-1"),
      );
      const waiter = yield* Effect.forkChild(runtime.awaitSettlement(receipt));
      yield* runtime.processConversation(agent, decodeConversationId(conversation));
      yield* TestClock.adjust(Duration.millis(150));
      const settlement = yield* Fiber.join(waiter);
      expect(settlement.outcome).toBe("completed");
      expect(settlement.receiptId).toBe(receipt.receiptId);
    }),
  );

  it.effect("keeps one Conversation lane FIFO by admitted queue sequence", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const scripted = yield* makeScriptedModel(() => finalParts('{"answer":"next"}'));
      const agent = Agent.withModel(plannerDefinition, scripted.model);
      const conversation = "conversation-fifo";

      const first = yield* runtime.submit(
        agent,
        { question: "first" },
        submitOptions(conversation, "fifo-1"),
      );
      const second = yield* runtime.submit(
        agent,
        { question: "second" },
        submitOptions(conversation, "fifo-2"),
      );
      expect(first.queueSequence).toBe(1);
      expect(second.queueSequence).toBe(2);

      const settlements = yield* runtime.processConversation(
        agent,
        decodeConversationId(conversation),
      );
      expect(settlements.map((settlement) => settlement.submissionId)).toEqual([
        first.submissionId,
        second.submissionId,
      ]);

      const records = yield* readLog(conversation);
      const settledIds = records
        .filter((envelope) => envelope.record.payload._tag === "SubmissionSettled")
        .map((envelope) => envelope.record.recordId);
      expect(settledIds).toEqual([
        submissionSettlementRecordId(first.submissionId),
        submissionSettlementRecordId(second.submissionId),
      ]);
      const firstInputSequence = records.find(
        (envelope) => envelope.record.recordId === submissionInputRecordId(first.submissionId),
      )?.sequence;
      const secondInputSequence = records.find(
        (envelope) => envelope.record.recordId === submissionInputRecordId(second.submissionId),
      )?.sequence;
      expect(firstInputSequence).toBeDefined();
      expect(secondInputSequence).toBeDefined();
      expect(Number(firstInputSequence)).toBeLessThan(Number(secondInputSequence));
    }),
  );

  it.effect("aborts ready, unclaimed work through the recovery pass without an Attempt", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const scripted = yield* makeScriptedModel(() => finalParts('{"answer":"never"}'));
      const agent = Agent.withModel(plannerDefinition, scripted.model);
      const conversation = "conversation-abort-ready";

      const receipt = yield* runtime.submit(
        agent,
        { question: "abort me" },
        submitOptions(conversation, "abort-ready-1"),
      );
      const intent = yield* runtime.abort(
        AbortCommand.make({
          submissionId: receipt.submissionId,
          author: "operator",
          reason: "user cancelled",
        }),
      );
      expect(intent.submissionId).toBe(receipt.submissionId);

      const reports = yield* runtime.runRecovery;
      const report = reports.find((entry) => entry.submissionId === receipt.submissionId);
      expect(report?.decision._tag).toBe("SettleAborted");
      expect(report?.disposition).toBe("repaired");

      const settlement = yield* runtime.awaitSettlement(receipt);
      expect(settlement.outcome).toBe("aborted");

      const records = yield* readLog(conversation);
      const tags = logTags(records);
      expect(tags).toContain("AbortRequested");
      expect(tags.indexOf("AbortRequested")).toBeLessThan(tags.indexOf("SubmissionSettled"));
      // No model ran and no input became canonical for the aborted, never-claimed head.
      expect(tags).not.toContain("ModelResponseRecorded");
      // DUR-013: the executed decision left a deterministic audit record.
      expect(records.map((envelope) => envelope.record.recordId)).toContain(
        recoveryRepairRecordId(receipt.submissionId, "SettleAborted"),
      );

      // Repeating the same abort command is idempotent only until settlement (DUR-012).
      const conflict = yield* Effect.exit(
        runtime.abort(
          AbortCommand.make({
            submissionId: receipt.submissionId,
            author: "operator",
            reason: "user cancelled",
          }),
        ),
      );
      expect(failureTag(conflict)).toBe("SettlementConflict");
    }),
  );

  it.effect("aborts a running Submission canonically before interrupting its Run fiber", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const started = yield* Deferred.make<void>();
      const latch = yield* Deferred.make<void>();
      const model = Model.make(
        "scripted",
        "durable-blocked",
        Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: () =>
              Stream.fromEffect(
                Deferred.succeed(started, void 0).pipe(Effect.andThen(Deferred.await(latch))),
              ).pipe(Stream.flatMap(() => Stream.fromIterable(finalParts('{"answer":"never"}')))),
          }),
        ),
      );
      const agent = Agent.withModel(plannerDefinition, model);
      const conversation = "conversation-abort-running";

      const receipt = yield* runtime.submit(
        agent,
        { question: "block" },
        submitOptions(conversation, "abort-running-1"),
      );
      const worker = yield* Effect.forkChild(
        runtime.processConversation(agent, decodeConversationId(conversation)),
      );
      yield* Deferred.await(started);

      yield* runtime.abort(
        AbortCommand.make({
          submissionId: receipt.submissionId,
          author: "operator",
          reason: "stop the run",
        }),
      );
      yield* TestClock.adjust(Duration.millis(150));

      const settlements = yield* Fiber.join(worker);
      expect(settlements).toHaveLength(1);
      expect(settlements[0]?.outcome).toBe("aborted");
      expect(yield* Deferred.isDone(latch)).toBe(false);

      const records = yield* readLog(conversation);
      const tags = logTags(records);
      expect(tags.indexOf("AbortRequested")).toBeGreaterThanOrEqual(0);
      expect(tags.indexOf("AbortRequested")).toBeLessThan(tags.indexOf("SubmissionSettled"));
      const settled = records.at(-1)?.record.payload;
      if (settled?._tag === "SubmissionSettled") {
        expect(settled.outcome).toBe("aborted");
      }
    }),
  );

  it.effect("fences a superseded Attempt out of canonical history", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const started = yield* Deferred.make<void>();
      const latch = yield* Deferred.make<void>();
      const calls = yield* Ref.make(0);
      const model = Model.make(
        "scripted",
        "durable-fencing",
        Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: () =>
              Stream.unwrap(
                Ref.getAndUpdate(calls, (call) => call + 1).pipe(
                  Effect.map((call) =>
                    call === 0
                      ? Stream.fromEffect(
                          Deferred.succeed(started, void 0).pipe(
                            Effect.andThen(Deferred.await(latch)),
                          ),
                        ).pipe(
                          Stream.flatMap(() =>
                            Stream.fromIterable(finalParts('{"answer":"stale"}')),
                          ),
                        )
                      : Stream.fromIterable(finalParts('{"answer":"fresh"}')),
                  ),
                ),
              ),
          }),
        ),
      );
      const agent = Agent.withModel(plannerDefinition, model);
      const conversation = "conversation-fencing";

      const receipt = yield* runtime.submit(
        agent,
        { question: "who owns the lane?" },
        submitOptions(conversation, "fencing-1"),
      );

      const staleWorker = yield* Effect.forkChild(
        runtime.processConversation(agent, decodeConversationId(conversation)),
      );
      yield* Deferred.await(started);

      // A second Attempt (same producer restarting) supersedes the first: higher epoch.
      const settlements = yield* runtime.processConversation(
        agent,
        decodeConversationId(conversation),
      );
      expect(settlements).toHaveLength(1);
      expect(settlements[0]?.outcome).toBe("completed");

      // Unblock the stale Attempt: its canonical append must be fenced, not committed.
      yield* Deferred.succeed(latch, void 0);
      const staleExit = yield* Fiber.await(staleWorker);
      expect(failureTag(staleExit)).toBe("FenceRejected");

      const records = yield* readLog(conversation);
      const runId = runIdForSubmission(receipt.submissionId);
      const turnRecords = records.filter(
        (envelope) => envelope.record.recordId === modelResponseRecordId(runId, 1),
      );
      expect(turnRecords).toHaveLength(1);
      const firstTurn = turnRecords[0]?.record.payload;
      if (firstTurn?._tag === "ModelResponseRecorded") {
        const messages = yield* Schema.decodeUnknownEffect(Prompt.Prompt)(firstTurn.messages);
        const text = messages.content
          .filter((message) => message.role === "assistant")
          .flatMap((message) => message.content)
          .filter((part) => part.type === "text")
          .map((part) => part.text)
          .join("");
        expect(text).toBe('{"answer":"fresh"}');
      }
      const settledRecords = logTags(records).filter((tag) => tag === "SubmissionSettled");
      expect(settledRecords).toHaveLength(1);
    }),
  );

  it.effect("a failpoint-interrupted submit resumes to the same Receipt", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const ledger = yield* SubmissionLedger;
      const scripted = yield* makeScriptedModel(() => finalParts('{"answer":"resume"}'));
      const agent = Agent.withModel(plannerDefinition, scripted.model);
      const conversation = "conversation-submit-resume";
      const options = submitOptions(conversation, "resume-1");

      yield* armFailpoint("submit:after-admit");
      const interrupted = yield* Effect.exit(runtime.submit(agent, { question: "kill" }, options));
      expect(failureTag(interrupted)).toBe("DurableRuntimeFailpointError");
      yield* clearFailpoint;

      const admitted = yield* ledger.lookup(
        SubmissionLookupByKey.make({
          conversationId: options.conversationId,
          principal: options.principal,
          idempotencyKey: options.idempotencyKey,
        }),
      );
      expect(Option.isSome(admitted)).toBe(true);
      const resumed = yield* runtime.submit(agent, { question: "kill" }, options);
      if (Option.isSome(admitted)) {
        expect(admitted.value.state).toBe("admitted");
        expect(resumed.submissionId).toBe(admitted.value.submissionId);
        expect(resumed.receiptId).toBe(admitted.value.receiptId);
      }
      expect(yield* lookupState(resumed.submissionId)).toBe("ready");

      // Same idempotency key resumes to the same Receipt after the materialize-boundary kill too.
      yield* armFailpoint("submit:after-materialize");
      const options2 = submitOptions(conversation, "resume-2");
      const killed = yield* Effect.exit(runtime.submit(agent, { question: "kill" }, options2));
      expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
      yield* clearFailpoint;
      const receipt2 = yield* runtime.submit(agent, { question: "kill" }, options2);
      const resubmitted = yield* runtime.submit(agent, { question: "kill" }, options2);
      expect(resubmitted).toEqual(receipt2);
      expect(yield* lookupState(receipt2.submissionId)).toBe("ready");
    }),
  );

  it.effect(
    "recovery completes materialization and repairs readiness (kill submit boundaries)",
    () =>
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const scripted = yield* makeScriptedModel(() => finalParts('{"answer":"ok"}'));
        const agent = Agent.withModel(plannerDefinition, scripted.model);

        // Kill after admission: nothing durable beyond the ledger row.
        yield* armFailpoint("submit:after-admit");
        const killedAdmit = yield* Effect.exit(
          runtime.submit(
            agent,
            { question: "recover me" },
            submitOptions("conversation-recover-admit", "recover-1"),
          ),
        );
        expect(failureTag(killedAdmit)).toBe("DurableRuntimeFailpointError");
        yield* clearFailpoint;

        const reports = yield* runtime.runRecovery;
        const admitReport = reports.find(
          (entry) => entry.conversationId === "conversation-recover-admit",
        );
        expect(admitReport?.decision._tag).toBe("CompleteMaterialization");
        expect(admitReport?.disposition).toBe("repaired");
        if (admitReport !== undefined) {
          expect(yield* lookupState(admitReport.submissionId)).toBe("ready");
        }
        expect(logTags(yield* readLog("conversation-recover-admit"))).toContain(
          "ConversationCreated",
        );

        // Kill after materialization: only the readiness marker is missing.
        yield* armFailpoint("submit:after-materialize");
        const killedReady = yield* Effect.exit(
          runtime.submit(
            agent,
            { question: "recover me" },
            submitOptions("conversation-recover-ready", "recover-2"),
          ),
        );
        expect(failureTag(killedReady)).toBe("DurableRuntimeFailpointError");
        yield* clearFailpoint;

        const readinessReports = yield* runtime.runRecovery;
        const repaired = readinessReports.find(
          (report) => report.conversationId === "conversation-recover-ready",
        );
        expect(repaired?.decision._tag).toBe("RepairReadiness");
        expect(repaired?.disposition).toBe("repaired");
        if (repaired !== undefined) {
          expect(yield* lookupState(repaired.submissionId)).toBe("ready");
        }
      }),
  );

  it.effect("recovery re-applies input after a claim-boundary kill (exactly one record)", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const ledger = yield* SubmissionLedger;
      const scripted = yield* makeScriptedModel(() => finalParts('{"answer":"applied"}'));
      const agent = Agent.withModel(plannerDefinition, scripted.model);
      const conversation = "conversation-recover-claim";

      const receipt = yield* runtime.submit(
        agent,
        { question: "apply once" },
        submitOptions(conversation, "claim-kill-1"),
      );
      yield* armFailpoint("claim:after-claim");
      const killed = yield* Effect.exit(
        runtime.processConversation(agent, decodeConversationId(conversation)),
      );
      expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
      yield* clearFailpoint;

      const reports = yield* runtime.runRecovery;
      const report = reports.find((entry) => entry.submissionId === receipt.submissionId);
      expect(report?.decision._tag).toBe("ApplyInput");
      expect(report?.disposition).toBe("repaired");

      const snapshot = yield* ledger.loadRecoverySnapshot(
        RecoverySnapshotRequest.make({ submissionId: receipt.submissionId }),
      );
      expect(snapshot.inputApplied?.recordId).toBe(submissionInputRecordId(receipt.submissionId));

      const settlements = yield* runtime.processConversation(
        agent,
        decodeConversationId(conversation),
      );
      expect(settlements[0]?.outcome).toBe("completed");
      const inputRecords = (yield* readLog(conversation)).filter(
        (envelope) => envelope.record.recordId === submissionInputRecordId(receipt.submissionId),
      );
      expect(inputRecords).toHaveLength(1);
    }),
  );

  it.effect("recovery repairs a lost input marker from canonical history (DUR-015)", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const scripted = yield* makeScriptedModel(() => finalParts('{"answer":"repaired"}'));
      const agent = Agent.withModel(plannerDefinition, scripted.model);
      const conversation = "conversation-recover-marker";

      const receipt = yield* runtime.submit(
        agent,
        { question: "mark me" },
        submitOptions(conversation, "marker-1"),
      );
      yield* armFailpoint("input:after-canonical-append");
      const killed = yield* Effect.exit(
        runtime.processConversation(agent, decodeConversationId(conversation)),
      );
      expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
      yield* clearFailpoint;

      const reports = yield* runtime.runRecovery;
      const report = reports.find((entry) => entry.submissionId === receipt.submissionId);
      expect(report?.decision._tag).toBe("RepairInputMarker");
      expect(report?.disposition).toBe("repaired");

      const settlements = yield* runtime.processConversation(
        agent,
        decodeConversationId(conversation),
      );
      expect(settlements[0]?.outcome).toBe("completed");
      const inputRecords = (yield* readLog(conversation)).filter(
        (envelope) => envelope.record.recordId === submissionInputRecordId(receipt.submissionId),
      );
      expect(inputRecords).toHaveLength(1);
    }),
  );

  it.effect("recovery appends a reserved-but-unappended settlement exactly as reserved", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const scripted = yield* makeScriptedModel(() => finalParts('{"answer":"reserved"}'));
      const agent = Agent.withModel(plannerDefinition, scripted.model);
      const conversation = "conversation-recover-reserved";

      const receipt = yield* runtime.submit(
        agent,
        { question: "reserve" },
        submitOptions(conversation, "reserved-1"),
      );
      yield* armFailpoint("terminalize:after-reserve");
      const killed = yield* Effect.exit(
        runtime.processConversation(agent, decodeConversationId(conversation)),
      );
      expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
      yield* clearFailpoint;

      const reports = yield* runtime.runRecovery;
      const report = reports.find((entry) => entry.submissionId === receipt.submissionId);
      expect(report?.decision._tag).toBe("AppendReservedSettlement");
      expect(report?.disposition).toBe("repaired");

      const settlement = yield* runtime.awaitSettlement(receipt);
      expect(settlement.outcome).toBe("completed");
      const records = yield* readLog(conversation);
      const settledRecords = records.filter(
        (envelope) => envelope.record.payload._tag === "SubmissionSettled",
      );
      expect(settledRecords).toHaveLength(1);
      expect(records.map((envelope) => envelope.record.recordId)).toContain(
        recoveryRepairRecordId(receipt.submissionId, "AppendReservedSettlement"),
      );
    }),
  );

  it.effect("recovery finalizes the ledger from history without rewriting the record", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const scripted = yield* makeScriptedModel(() => finalParts('{"answer":"finalized"}'));
      const agent = Agent.withModel(plannerDefinition, scripted.model);
      const conversation = "conversation-recover-finalize";

      const receipt = yield* runtime.submit(
        agent,
        { question: "finalize" },
        submitOptions(conversation, "finalize-1"),
      );
      yield* armFailpoint("terminalize:after-canonical-append");
      const killed = yield* Effect.exit(
        runtime.processConversation(agent, decodeConversationId(conversation)),
      );
      expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
      yield* clearFailpoint;

      // The canonical settlement exists while the ledger row is still nonterminal.
      const beforeRecords = yield* readLog(conversation);
      expect(logTags(beforeRecords)).toContain("SubmissionSettled");
      expect(yield* lookupState(receipt.submissionId)).not.toBe("settled");

      const reports = yield* runtime.runRecovery;
      const report = reports.find((entry) => entry.submissionId === receipt.submissionId);
      expect(report?.decision._tag).toBe("FinalizeLedgerFromHistory");
      expect(report?.disposition).toBe("repaired");

      expect(yield* lookupState(receipt.submissionId)).toBe("settled");
      const afterRecords = yield* readLog(conversation);
      const settledRecords = afterRecords.filter(
        (envelope) => envelope.record.payload._tag === "SubmissionSettled",
      );
      expect(settledRecords).toHaveLength(1);
      expect(settledRecords[0]).toEqual(
        beforeRecords.find((envelope) => envelope.record.payload._tag === "SubmissionSettled"),
      );
    }),
  );

  it.effect("a new Attempt resumes from the last committed Turn boundary", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const scripted = yield* makeScriptedModel((call) =>
        call === 0 ? toolCallParts : finalParts('{"answer":"Resumed."}'),
      );
      const agent = Agent.withModel(searchDefinition, scripted.model);
      const conversation = "conversation-resume-turn";

      const receipt = yield* runtime.submit(
        agent,
        { question: "resume?" },
        submitOptions(conversation, "resume-turn-1"),
      );
      yield* armFailpoint("turn:after-canonical-append");
      const killed = yield* Effect.exit(
        runtime
          .processConversation(agent, decodeConversationId(conversation))
          .pipe(Effect.provide(searchToolLayer)),
      );
      expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
      yield* clearFailpoint;

      // Turn 1 (model response + settled Tool batch) is canonical; the run is not settled.
      const runId = runIdForSubmission(receipt.submissionId);
      const committed = yield* readLog(conversation);
      expect(logTags(committed)).toEqual([
        "ConversationCreated",
        "UserInputRecorded",
        "ModelResponseRecorded",
        "ToolCallSettled",
      ]);

      const settlements = yield* runtime
        .processConversation(agent, decodeConversationId(conversation))
        .pipe(Effect.provide(searchToolLayer));
      expect(settlements[0]?.outcome).toBe("completed");

      // The resumed Attempt's model request saw the canonical prompt, tool result included,
      // without re-appended instructions or input.
      expect(scripted.prompts).toHaveLength(2);
      const resumedPrompt = scripted.prompts[1];
      expect(resumedPrompt?.content.map((message) => message.role)).toEqual([
        "system",
        "user",
        "assistant",
        "tool",
      ]);

      const records = yield* readLog(conversation);
      expect(
        records.filter((envelope) => envelope.record.recordId === modelResponseRecordId(runId, 1)),
      ).toHaveLength(1);
      expect(records.map((envelope) => envelope.record.recordId)).toContain(
        modelResponseRecordId(runId, 2),
      );
      const settled = records.at(-1)?.record.payload;
      if (settled?._tag === "SubmissionSettled") {
        expect(settled.result).toEqual({ answer: "Resumed." });
      }
    }),
  );

  const typeProofReceipt = Receipt.make({
    receiptId: Schema.decodeSync(ReceiptId)("receipt-types"),
    submissionId: Schema.decodeSync(SubmissionId)("submission-types"),
    conversationId: decodeConversationId("conversation-types"),
    queueSequence: Schema.decodeSync(QueueSequence)(1),
  });

  it.effect("keeps failure and requirement channels typed (E/R proofs)", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const scripted = yield* makeScriptedModel(() => finalParts('{"answer":"typed"}'));
      const agent = Agent.withModel(plannerDefinition, scripted.model);

      const submitProgram = runtime.submit(
        agent,
        { question: "typed" },
        submitOptions("conversation-types", "types-1"),
      );
      type SubmitError = Effect.Error<typeof submitProgram>;
      const submitHasAdmissionConflict: AdmissionConflict extends SubmitError ? true : false = true;
      const submitHasFailpoint: DurableRuntimeFailpointError extends SubmitError ? true : false =
        true;

      const awaitProgram = runtime.awaitSettlement(typeProofReceipt);
      type AwaitError = Effect.Error<typeof awaitProgram>;
      type AwaitSuccess = Effect.Success<typeof awaitProgram>;
      const awaitHasConflict: SettlementConflict extends AwaitError ? true : false = true;
      const awaitReturnsSettlement: AwaitSuccess extends Settlement ? true : false = true;

      const workerProgram = runtime.processConversation(
        Agent.withModel(searchDefinition, scripted.model),
        decodeConversationId("conversation-types"),
      );
      type WorkerError = Effect.Error<typeof workerProgram>;
      type WorkerServices = Effect.Services<typeof workerProgram>;
      const workerHasFence: FenceRejected extends WorkerError ? true : false = true;
      const workerProvidesIdGenerator: IdGenerator extends WorkerServices ? true : false = false;

      expect(submitHasAdmissionConflict).toBe(true);
      expect(submitHasFailpoint).toBe(true);
      expect(awaitHasConflict).toBe(true);
      expect(awaitReturnsSettlement).toBe(true);
      expect(workerHasFence).toBe(true);
      expect(workerProvidesIdGenerator).toBe(false);
    }),
  );

  describe("run journal projections", () => {
    const runId = runIdForSubmission(Schema.decodeSync(SubmissionId)("submission-journal"));
    const otherRunId = runIdForSubmission(Schema.decodeSync(SubmissionId)("submission-other"));
    const callOneId = Schema.decodeSync(ToolCallId)("call-1");
    const producerId = Schema.decodeSync(ProducerId)("producer-journal");
    const deploymentId = Schema.decodeSync(DeploymentId)("deployment-journal");
    const createdAt = DateTime.toUtc(DateTime.makeUnsafe(1_000));

    const turnOneAppended: ReadonlyArray<Prompt.Message> = [
      Prompt.makeMessage("system", { content: "Answer as JSON." }),
      Prompt.makeMessage("user", {
        content: [Prompt.makePart("text", { text: '{"question":"resume?"}' })],
      }),
      Prompt.makeMessage("assistant", {
        content: [
          Prompt.makePart("tool-call", {
            id: "call-1",
            name: "search",
            params: { query: "sea" },
            providerExecuted: false,
          }),
        ],
      }),
      Prompt.makeMessage("tool", {
        content: [
          Prompt.makePart("tool-result", {
            id: "call-1",
            name: "search",
            result: { available: true },
            isFailure: false,
          }),
        ],
      }),
    ];
    const turnTwoAppended: ReadonlyArray<Prompt.Message> = [
      Prompt.makeMessage("assistant", {
        content: [Prompt.makePart("text", { text: '{"answer":"Found."}' })],
      }),
    ];

    const envelopeAt = (sequence: number, record: CanonicalRecordEnvelope["record"]) =>
      CanonicalRecordEnvelope.make({
        conversationId: decodeConversationId("conversation-journal"),
        batchId: Schema.decodeSync(BatchId)(`batch-journal-${sequence}`),
        sequence: Schema.decodeSync(CanonicalSequence)(sequence),
        offset: Schema.decodeSync(ObservationOffset)(`memory:${sequence}`),
        record,
      });

    it.effect("folds Turn seams into batches and replays them to the same prompt", () =>
      Effect.gen(function* () {
        const batchOne = yield* turnCanonicalBatch({
          runId,
          turn: 1,
          turnId: turnIdForRun(runId, 1),
          appended: turnOneAppended,
          producerId,
          deploymentId,
          createdAt,
        });
        expect(batchOne.records.map((record) => record.recordId)).toEqual([
          modelResponseRecordId(runId, 1),
          `tool-settled:${runId}:1:call-1`,
        ]);
        expect(batchOne.records[0]?.payload._tag).toBe("ModelResponseRecorded");
        expect(batchOne.records[1]?.payload._tag).toBe("ToolCallSettled");

        const batchTwo = yield* turnCanonicalBatch({
          runId,
          turn: 2,
          turnId: turnIdForRun(runId, 2),
          appended: turnTwoAppended,
          producerId,
          deploymentId,
          createdAt,
        });

        const journalRecords = [
          ...batchOne.records.map((record, index) => envelopeAt(index + 1, record)),
          ...batchTwo.records.map((record, index) =>
            envelopeAt(batchOne.records.length + index + 1, record),
          ),
        ];

        const prompt = yield* promptFromCanonicalRecords(journalRecords);
        expect(prompt.content.map((message) => message.role)).toEqual([
          "system",
          "user",
          "assistant",
          "tool",
          "assistant",
        ]);
        const toolMessage = prompt.content.find((message) => message.role === "tool");
        expect(
          toolMessage?.content.filter((part) => part.type === "tool-result").map((part) => part.id),
        ).toEqual(["call-1"]);

        const projection = yield* projectRunJournal(journalRecords, runId);
        expect(projection.committedTurns).toBe(2);
        expect(projection.historyBefore.content).toHaveLength(0);

        // Replay equivalence: projecting the same canonical records is deterministic.
        const replayed = yield* projectRunJournal(journalRecords, runId);
        const encodedFirst = yield* Schema.encodeEffect(Prompt.Prompt)(projection.prompt);
        const encodedReplayed = yield* Schema.encodeEffect(Prompt.Prompt)(replayed.prompt);
        expect(encodedReplayed).toEqual(encodedFirst);

        // Another Run's projection treats these records as prior history.
        const other = yield* projectRunJournal(journalRecords, otherRunId);
        expect(other.committedTurns).toBe(0);
        expect(other.historyBefore.content).toHaveLength(prompt.content.length);
      }),
    );

    it.effect("rejects a Turn with no model-visible messages", () =>
      Effect.gen(function* () {
        const exit = yield* Effect.exit(
          turnCanonicalBatch({
            runId,
            turn: 1,
            turnId: turnIdForRun(runId, 1),
            appended: [],
            producerId,
            deploymentId,
            createdAt,
          }),
        );
        expect(failureTag(exit)).toBe("RunJournalError");
      }),
    );

    it.effect("keeps tool-settled record identities per (run, turn, toolCallId)", () =>
      Effect.gen(function* () {
        const batch = yield* turnCanonicalBatch({
          runId,
          turn: 3,
          turnId: turnIdForRun(runId, 3),
          appended: turnOneAppended,
          producerId,
          deploymentId,
          createdAt,
        });
        const toolRecord = batch.records[1];
        expect(toolRecord?.recordId).toBe(toolCallSettledRecordId(runId, 3, callOneId));
        if (toolRecord?.payload._tag === "ToolCallSettled") {
          expect(toolRecord.payload.result).toEqual({ available: true });
          expect(toolRecord.payload.isFailure).toBe(false);
        }
      }),
    );
  });
});
