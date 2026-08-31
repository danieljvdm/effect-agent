import { Agent, AgentPolicy, ThreadId, type SubmissionId } from "@effect-agent/core";
import { MemoryThreadStoreLive, MemorySubmissionLedgerLive } from "@effect-agent/storage-memory";
import {
  AbortCommand,
  ThreadRead,
  ThreadStore,
  DefinitionDigests,
  DeploymentId,
  Digest,
  DurableAgentRuntime,
  DurableRuntimeConfig,
  DurableRuntimeFailpointError,
  IdempotencyKey,
  JoinedToHost,
  Principal,
  ProducerId,
  SubmissionLedger,
  SubmissionLookupById,
  ToolReconciler,
  WakeScheduler,
  runIdForSubmission,
  type DurableRuntimeFailpointLocation,
  type DurableSubmitOptions,
  type CanonicalRecordEnvelope,
} from "@effect-agent/thread";
import { DurableRuntimeFailpointTestControl } from "@effect-agent/thread/testing";
import { NodeCrypto } from "@effect/platform-node";
import { expect, layer } from "@effect/vitest";
import { Cause, Duration, Effect, Exit, Layer, Option, Ref, Schema, Stream } from "effect";
import { LanguageModel, Model, Toolkit, type Prompt, type Response } from "effect/unstable/ai";

const SHA_A = Schema.decodeSync(Digest)("a".repeat(64));
const PRINCIPAL = Schema.decodeSync(Principal)("principal-durable-join");
const DIGESTS = DefinitionDigests.make({ agent: SHA_A, model: SHA_A, tools: SHA_A });
const decodeThreadId = Schema.decodeSync(ThreadId);
const decodeIdempotencyKey = Schema.decodeSync(IdempotencyKey);

const submitOptions = (threadId: string, idempotencyKey: string): DurableSubmitOptions => ({
  threadId: decodeThreadId(threadId),
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
      "durable-join-test",
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

/** No-tool Q&A agent: the join seams under test are pure Turn seams. */
const joinDefinition = Agent.make("durable-join", {
  input: Schema.Struct({ question: Schema.String }),
  output: Schema.Struct({ answer: Schema.String }),
  instructions: "Answer every question as JSON.",
  toolkit: Toolkit.empty,
  policy: AgentPolicy.make({
    maxTurns: 4,
    maxToolCalls: 2,
    maxDuration: "30 seconds",
    toolConcurrency: 1,
  }),
});

const PRODUCER_ID = Schema.decodeSync(ProducerId)("producer-durable-join");

const configLayer = DurableRuntimeConfig.layer({
  deploymentId: Schema.decodeSync(DeploymentId)("deployment-durable-join"),
  producerId: PRODUCER_ID,
  settlementPollInterval: Duration.millis(100),
  leaseRenewalInterval: Duration.seconds(5),
  abortPollInterval: Duration.millis(100),
});

const baseLayer = Layer.mergeAll(
  MemorySubmissionLedgerLive,
  MemoryThreadStoreLive,
  WakeScheduler.layerNoop,
  DurableRuntimeFailpointTestControl.layer,
  ToolReconciler.uncertain,
  configLayer,
).pipe(Layer.provideMerge(NodeCrypto.layer));

const testLayer = DurableAgentRuntime.layer.pipe(Layer.provideMerge(baseLayer));

const readLog = (threadId: string) =>
  Effect.gen(function* () {
    const store = yield* ThreadStore;
    return yield* Stream.runCollect(
      store.read(
        ThreadRead.make({
          threadId: decodeThreadId(threadId),
          limit: 1_024,
        }),
      ),
    );
  });

const logTags = (records: ReadonlyArray<CanonicalRecordEnvelope>): ReadonlyArray<string> =>
  records.map((envelope) => envelope.record.payload._tag);

const recordsById = (records: ReadonlyArray<CanonicalRecordEnvelope>) =>
  new Map(records.map((envelope) => [envelope.record.recordId as string, envelope]));

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

/** Arm one failpoint to fire only on its N-th hit (1-based) within this handler's lifetime. */
const armFailpointAt = (location: DurableRuntimeFailpointLocation, occurrence: number) =>
  Effect.gen(function* () {
    const control = yield* DurableRuntimeFailpointTestControl;
    const seen = { count: 0 };
    yield* control.setHandler((hitLocation) => {
      if (hitLocation !== location) return Effect.void;
      seen.count += 1;
      return seen.count === occurrence
        ? Effect.fail(DurableRuntimeFailpointError.make({ location: hitLocation }))
        : Effect.void;
    });
  });

const clearFailpoint = Effect.gen(function* () {
  const control = yield* DurableRuntimeFailpointTestControl;
  yield* control.clear;
});

const failureOf = <A, E>(exit: Exit.Exit<A, E>): unknown => {
  expect(Exit.isFailure(exit)).toBe(true);
  if (Exit.isSuccess(exit)) throw new Error("Expected the Effect to fail");
  const failure = Cause.findErrorOption(exit.cause);
  expect(Option.isSome(failure)).toBe(true);
  if (Option.isNone(failure)) throw new Error("Expected a typed failure");
  return failure.value;
};

const failureTag = <A, E>(exit: Exit.Exit<A, E>): string => {
  const error = failureOf(exit);
  return typeof error === "object" && error !== null && "_tag" in error
    ? String(error._tag)
    : "unknown";
};

const promptOccurrences = (prompt: Prompt.Prompt, needle: string): number =>
  JSON.stringify(prompt).split(needle).length - 1;

layer(testLayer)("DUR P5 joining/joined queued input (plan §2.5)", (it) => {
  it.effect("a queued Submission joins at the Turn seam and settles with the host", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const scripted = yield* makeScriptedModel(() => finalParts('{"answer":"host answer"}'));
      const agent = Agent.withModel(joinDefinition, scripted.model);
      const thread = "thread-join-basic";

      const host = yield* runtime.submit(
        agent,
        { question: "host question" },
        submitOptions(thread, "join-basic-host"),
      );
      const joined = yield* runtime.submit(
        agent,
        { question: "queued question" },
        submitOptions(thread, "join-basic-2"),
      );

      const settlements = yield* runtime.processThread(agent, decodeThreadId(thread));
      // The lane produced ONE head settlement; the joined Submission settled with it
      // (DUR-002: each accepted Submission still gets its own settlement record).
      expect(settlements).toHaveLength(1);
      expect(settlements[0]?.outcome).toBe("completed");
      expect(yield* lookupState(host.submissionId)).toBe("settled");
      expect(yield* lookupState(joined.submissionId)).toBe("settled");

      const settled = yield* runtime.awaitSettlement(joined);
      expect(settled.outcome).toBe("completed");

      const hostRunId = runIdForSubmission(host.submissionId);
      const records = yield* readLog(thread);
      expect(logTags(records)).toEqual([
        "ThreadCreated",
        "UserInputRecorded",
        "RunStarted",
        "UserInputRecorded",
        "ModelResponseRecorded",
        "RunCompleted",
        "SubmissionSettled",
        "SubmissionSettled",
      ]);
      const byId = recordsById(records);
      const joinedInput = byId.get(`input:${joined.submissionId}`);
      if (joinedInput?.record.payload._tag === "UserInputRecorded") {
        expect(joinedInput.record.payload.kind).toBe("steering");
        expect(joinedInput.record.payload.runId).toBe(hostRunId);
      } else {
        throw new Error("Expected a canonical joined UserInputRecorded record");
      }
      const joinedSettlement = byId.get(`settlement:${joined.submissionId}`);
      if (joinedSettlement?.record.payload._tag === "SubmissionSettled") {
        expect(joinedSettlement.record.payload.outcome).toBe("completed");
        // The joined Submission settles WITH the host Run.
        expect(joinedSettlement.record.payload.runId).toBe(hostRunId);
      } else {
        throw new Error("Expected a canonical joined SubmissionSettled record");
      }
      // The joined input entered the model prompt exactly once.
      expect(scripted.prompts).toHaveLength(1);
      const first = scripted.prompts[0];
      expect(first === undefined ? 0 : promptOccurrences(first, "queued question")).toBe(1);
    }),
  );

  it.effect("joined input becomes canonical before the next model request", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const scripted = yield* makeScriptedModel(() => finalParts('{"answer":"covered"}'));
      const agent = Agent.withModel(joinDefinition, scripted.model);
      const thread = "thread-join-canonical-first";

      const host = yield* runtime.submit(
        agent,
        { question: "host question" },
        submitOptions(thread, "canonical-host"),
      );
      const joined = yield* runtime.submit(
        agent,
        { question: "queued question" },
        submitOptions(thread, "canonical-2"),
      );
      yield* runtime.processThread(agent, decodeThreadId(thread));

      const hostRunId = runIdForSubmission(host.submissionId);
      const byId = recordsById(yield* readLog(thread));
      const input = byId.get(`input:${joined.submissionId}`);
      const response = byId.get(`model-response:${hostRunId}:1`);
      expect(input).toBeDefined();
      expect(response).toBeDefined();
      if (input === undefined || response === undefined) return;
      // Canonical order is the coverage rule's foundation: the joined `input:{sid}` record
      // committed BEFORE the model response that consumed it, and the response's committed
      // messages carry the joined text (prompt-covered).
      expect(input.sequence).toBeLessThan(response.sequence);
      if (response.record.payload._tag === "ModelResponseRecorded") {
        expect(JSON.stringify(response.record.payload.messages)).toContain("queued question");
      }
      const first = scripted.prompts[0];
      expect(first === undefined ? 0 : promptOccurrences(first, "queued question")).toBe(1);
    }),
  );

  it.effect("drain policy one claims exactly one queued Submission per seam", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const scripted = yield* makeScriptedModel((call) =>
        call === 0 ? finalParts('{"answer":"turn one"}') : finalParts('{"answer":"turn two"}'),
      );
      const agent = Agent.withModel(joinDefinition, scripted.model);
      const thread = "thread-join-policy-one";

      const host = yield* runtime.submit(
        agent,
        { question: "host question" },
        submitOptions(thread, "policy-host"),
      );
      const second = yield* runtime.submit(
        agent,
        { question: "second request" },
        submitOptions(thread, "policy-2"),
      );
      const third = yield* runtime.submit(
        agent,
        { question: "third request" },
        submitOptions(thread, "policy-3"),
      );
      const settlements = yield* runtime.processThread(agent, decodeThreadId(thread));
      expect(settlements).toHaveLength(1);
      expect(yield* lookupState(second.submissionId)).toBe("settled");
      expect(yield* lookupState(third.submissionId)).toBe("settled");

      // One claim per seam: the second request joined at the pre-Turn seam, the third only at
      // the seam AFTER Turn 1 — so Turn 1's request carries the second but not the third.
      expect(scripted.prompts).toHaveLength(2);
      const [first, secondPrompt] = scripted.prompts;
      expect(first === undefined ? 0 : promptOccurrences(first, "second request")).toBe(1);
      expect(first === undefined ? 0 : promptOccurrences(first, "third request")).toBe(0);
      expect(
        secondPrompt === undefined ? 0 : promptOccurrences(secondPrompt, "third request"),
      ).toBe(1);

      const hostRunId = runIdForSubmission(host.submissionId);
      const byId = recordsById(yield* readLog(thread));
      const inputSecond = byId.get(`input:${second.submissionId}`);
      const inputThird = byId.get(`input:${third.submissionId}`);
      const responseOne = byId.get(`model-response:${hostRunId}:1`);
      const responseTwo = byId.get(`model-response:${hostRunId}:2`);
      expect(inputSecond).toBeDefined();
      expect(inputThird).toBeDefined();
      expect(responseOne).toBeDefined();
      expect(responseTwo).toBeDefined();
      if (
        inputSecond === undefined ||
        inputThird === undefined ||
        responseOne === undefined ||
        responseTwo === undefined
      ) {
        return;
      }
      // Every joined input committed before the model response that covers it.
      expect(inputSecond.sequence).toBeLessThan(responseOne.sequence);
      expect(inputThird.sequence).toBeLessThan(responseOne.sequence);
      expect(responseOne.sequence).toBeLessThan(responseTwo.sequence);
    }),
  );

  it.effect(
    "a kill at join:after-claim reverts the claim and delivers the input exactly once",
    () =>
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const scripted = yield* makeScriptedModel(() => finalParts('{"answer":"resumed"}'));
        const agent = Agent.withModel(joinDefinition, scripted.model);
        const thread = "thread-join-fp-claim";

        yield* runtime.submit(
          agent,
          { question: "host question" },
          submitOptions(thread, "fp-claim-host"),
        );
        const joined = yield* runtime.submit(
          agent,
          { question: "queued question" },
          submitOptions(thread, "fp-claim-2"),
        );
        yield* armFailpoint("join:after-claim");
        const killed = yield* Effect.exit(runtime.processThread(agent, decodeThreadId(thread)));
        expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
        yield* clearFailpoint;

        // The claim is durable but the canonical input never committed: pre-append `joining`
        // reverts to ready (DUR-016) and no `input:{sid}` record exists.
        expect(yield* lookupState(joined.submissionId)).toBe("joining");
        expect(recordsById(yield* readLog(thread)).has(`input:${joined.submissionId}`)).toBe(false);
        const reports = yield* runtime.runRecovery;
        const report = reports.find((entry) => entry.submissionId === joined.submissionId);
        expect(report?.decision._tag).toBe("RevertJoining");
        expect(report?.disposition).toBe("repaired");
        expect(yield* lookupState(joined.submissionId)).toBe("ready");

        const settlements = yield* runtime.processThread(agent, decodeThreadId(thread));
        expect(settlements[0]?.outcome).toBe("completed");
        const settled = yield* runtime.awaitSettlement(joined);
        expect(settled.outcome).toBe("completed");

        // Exactly one canonical `input:{sid}` record ever, delivered exactly once.
        const records = yield* readLog(thread);
        expect(
          records.filter((envelope) => envelope.record.recordId === `input:${joined.submissionId}`),
        ).toHaveLength(1);
        const first = scripted.prompts[0];
        expect(first === undefined ? 0 : promptOccurrences(first, "queued question")).toBe(1);
      }),
  );

  it.effect(
    "a kill at join:after-canonical-append repairs the marker and reattaches without duplication",
    () =>
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const scripted = yield* makeScriptedModel(() => finalParts('{"answer":"reattached"}'));
        const agent = Agent.withModel(joinDefinition, scripted.model);
        const thread = "thread-join-fp-append";

        yield* runtime.submit(
          agent,
          { question: "host question" },
          submitOptions(thread, "fp-append-host"),
        );
        const joined = yield* runtime.submit(
          agent,
          { question: "queued question" },
          submitOptions(thread, "fp-append-2"),
        );
        yield* armFailpoint("join:after-canonical-append");
        const killed = yield* Effect.exit(runtime.processThread(agent, decodeThreadId(thread)));
        expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
        yield* clearFailpoint;

        // The input is canonical but the joined marker was lost.
        expect(yield* lookupState(joined.submissionId)).toBe("joining");
        expect(recordsById(yield* readLog(thread)).has(`input:${joined.submissionId}`)).toBe(true);
        const reports = yield* runtime.runRecovery;
        const report = reports.find((entry) => entry.submissionId === joined.submissionId);
        expect(report?.decision._tag).toBe("RepairJoinMarker");
        expect(report?.disposition).toBe("repaired");
        expect(yield* lookupState(joined.submissionId)).toBe("joined");

        const settlements = yield* runtime.processThread(agent, decodeThreadId(thread));
        expect(settlements[0]?.outcome).toBe("completed");
        const settled = yield* runtime.awaitSettlement(joined);
        expect(settled.outcome).toBe("completed");

        // Reattached, never duplicated: one canonical record, one prompt delivery.
        const records = yield* readLog(thread);
        expect(
          records.filter((envelope) => envelope.record.recordId === `input:${joined.submissionId}`),
        ).toHaveLength(1);
        const first = scripted.prompts[0];
        expect(first === undefined ? 0 : promptOccurrences(first, "queued question")).toBe(1);
      }),
  );

  it.effect("a lost join marker is repaired by the resuming host Attempt without recovery", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const scripted = yield* makeScriptedModel(() => finalParts('{"answer":"inline"}'));
      const agent = Agent.withModel(joinDefinition, scripted.model);
      const thread = "thread-join-inline-repair";

      yield* runtime.submit(
        agent,
        { question: "host question" },
        submitOptions(thread, "inline-host"),
      );
      const joined = yield* runtime.submit(
        agent,
        { question: "queued question" },
        submitOptions(thread, "inline-2"),
      );
      yield* armFailpoint("join:after-canonical-append");
      const killed = yield* Effect.exit(runtime.processThread(agent, decodeThreadId(thread)));
      expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
      yield* clearFailpoint;
      expect(yield* lookupState(joined.submissionId)).toBe("joining");

      // No recovery pass: the resuming host Attempt repairs the marker from history at its
      // first drain seam (DUR-015) and re-delivers the uncovered input.
      const settlements = yield* runtime.processThread(agent, decodeThreadId(thread));
      expect(settlements[0]?.outcome).toBe("completed");
      expect(yield* lookupState(joined.submissionId)).toBe("settled");
      const records = yield* readLog(thread);
      expect(
        records.filter((envelope) => envelope.record.recordId === `input:${joined.submissionId}`),
      ).toHaveLength(1);
      const first = scripted.prompts[0];
      expect(first === undefined ? 0 : promptOccurrences(first, "queued question")).toBe(1);
    }),
  );

  it.effect("a joined Submission's awaitSettlement resolves with the host outcome", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      // The host Run fails (the final output does not decode): the joined Submission settles
      // `failed` WITH the host — never with a fabricated outcome of its own.
      const scripted = yield* makeScriptedModel(() => finalParts("not json"));
      const agent = Agent.withModel(joinDefinition, scripted.model);
      const thread = "thread-join-host-outcome";

      const host = yield* runtime.submit(
        agent,
        { question: "host question" },
        submitOptions(thread, "outcome-host"),
      );
      const joined = yield* runtime.submit(
        agent,
        { question: "queued question" },
        submitOptions(thread, "outcome-2"),
      );
      const settlements = yield* runtime.processThread(agent, decodeThreadId(thread));
      expect(settlements[0]?.outcome).toBe("failed");
      const settled = yield* runtime.awaitSettlement(joined);
      expect(settled.outcome).toBe("failed");

      const byId = recordsById(yield* readLog(thread));
      const hostSettlement = byId.get(`settlement:${host.submissionId}`);
      const joinedSettlement = byId.get(`settlement:${joined.submissionId}`);
      if (
        hostSettlement?.record.payload._tag === "SubmissionSettled" &&
        joinedSettlement?.record.payload._tag === "SubmissionSettled"
      ) {
        expect(hostSettlement.record.payload.outcome).toBe("failed");
        expect(hostSettlement.record.payload.result).toEqual({
          errorTag: "AgentOutputError",
          message: expect.any(String),
        });
        expect(joinedSettlement.record.payload.outcome).toBe("failed");
        expect(joinedSettlement.record.payload.runId).toBe(runIdForSubmission(host.submissionId));
        // Joined fanout carries the host's already-bounded diagnostic exactly — never a raw Cause.
        expect(joinedSettlement.record.payload.result).toEqual(
          hostSettlement.record.payload.result,
        );
        expect(settlements[0]?.failure).toEqual(hostSettlement.record.payload.result);
        expect(settled.failure).toEqual(hostSettlement.record.payload.result);
      } else {
        throw new Error("Expected canonical host and joined SubmissionSettled records");
      }
    }),
  );

  it.effect("abort of a joined Submission fails with the host-linkage conflict", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const scripted = yield* makeScriptedModel(() => finalParts('{"answer":"reserved"}'));
      const agent = Agent.withModel(joinDefinition, scripted.model);
      const thread = "thread-join-abort-joined";

      const host = yield* runtime.submit(
        agent,
        { question: "host question" },
        submitOptions(thread, "abort-joined-host"),
      );
      const joined = yield* runtime.submit(
        agent,
        { question: "queued question" },
        submitOptions(thread, "abort-joined-2"),
      );
      // Kill after the HOST's settlement reservation: the joined Submission is durably
      // `joined` while the host is not yet settled.
      yield* armFailpointAt("terminalize:after-reserve", 1);
      const killed = yield* Effect.exit(runtime.processThread(agent, decodeThreadId(thread)));
      expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
      yield* clearFailpoint;
      expect(yield* lookupState(joined.submissionId)).toBe("joined");

      const aborted = yield* Effect.exit(
        runtime.abort(
          AbortCommand.make({
            submissionId: joined.submissionId,
            author: "operator",
            reason: "stop the joined request",
          }),
        ),
      );
      // A joined Submission settles with its host: the abort target is the host, carried in
      // the typed conflict (plan §2.5).
      const error = failureOf(aborted);
      expect(error instanceof JoinedToHost).toBe(true);
      if (error instanceof JoinedToHost) {
        expect(error.submissionId).toBe(joined.submissionId);
        expect(error.hostSubmissionId).toBe(host.submissionId);
      }

      // Recovery completes the reserved host settlement AND the joined prefix.
      const reports = yield* runtime.runRecovery;
      const hostReport = reports.find((entry) => entry.submissionId === host.submissionId);
      expect(hostReport?.decision._tag).toBe("AppendReservedSettlement");
      expect(hostReport?.disposition).toBe("repaired");
      const settled = yield* runtime.awaitSettlement(joined);
      expect(settled.outcome).toBe("completed");
    }),
  );

  it.effect("abort of a joining Submission reverts and settles aborted before consumption", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const scripted = yield* makeScriptedModel(() => finalParts('{"answer":"alone"}'));
      const agent = Agent.withModel(joinDefinition, scripted.model);
      const thread = "thread-join-abort-joining";

      yield* runtime.submit(
        agent,
        { question: "host question" },
        submitOptions(thread, "abort-joining-host"),
      );
      const joining = yield* runtime.submit(
        agent,
        { question: "queued question" },
        submitOptions(thread, "abort-joining-2"),
      );
      yield* armFailpoint("join:after-claim");
      const killed = yield* Effect.exit(runtime.processThread(agent, decodeThreadId(thread)));
      expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
      yield* clearFailpoint;
      expect(yield* lookupState(joining.submissionId)).toBe("joining");

      // Abort of a `joining` Submission records the intent normally (revert-then-abort).
      const intent = yield* runtime.abort(
        AbortCommand.make({
          submissionId: joining.submissionId,
          author: "operator",
          reason: "withdraw the queued request",
        }),
      );
      expect(intent.submissionId).toBe(joining.submissionId);

      const reports = yield* runtime.runRecovery;
      const report = reports.find((entry) => entry.submissionId === joining.submissionId);
      expect(report?.decision._tag).toBe("RevertJoining");
      expect(report?.disposition).toBe("repaired");

      // The resuming host honors the pre-consumption intent: the re-claimed row reverts
      // instead of joining, the host completes alone, and the abort settles the Submission.
      const settlements = yield* runtime.processThread(agent, decodeThreadId(thread));
      expect(settlements.map((settlement) => settlement.outcome)).toEqual(["completed", "aborted"]);
      const settled = yield* runtime.awaitSettlement(joining);
      expect(settled.outcome).toBe("aborted");

      // The input was never consumed: no canonical `input:{sid}` record, no prompt delivery.
      const records = yield* readLog(thread);
      expect(recordsById(records).has(`input:${joining.submissionId}`)).toBe(false);
      expect(recordsById(records).has(`abort:${joining.submissionId}`)).toBe(true);
      for (const prompt of scripted.prompts) {
        expect(promptOccurrences(prompt, "queued question")).toBe(0);
      }
    }),
  );

  it.effect(
    "a kill between host settlement append and joined settlement settles from history",
    () =>
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        // The host fails output decoding, then recovery must copy the canonical diagnostic into
        // the joined settlement instead of reconstructing it from a live Cause.
        const scripted = yield* makeScriptedModel(() => finalParts("not json"));
        const agent = Agent.withModel(joinDefinition, scripted.model);
        const thread = "thread-join-settle-history";

        const host = yield* runtime.submit(
          agent,
          { question: "host question" },
          submitOptions(thread, "history-host"),
        );
        const joined = yield* runtime.submit(
          agent,
          { question: "queued question" },
          submitOptions(thread, "history-2"),
        );
        // Kill after the HOST's canonical settlement append: the host outcome is canonical, the
        // ledger is not finalized, and the joined settlement never started.
        yield* armFailpointAt("terminalize:after-canonical-append", 1);
        const killed = yield* Effect.exit(runtime.processThread(agent, decodeThreadId(thread)));
        expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
        yield* clearFailpoint;
        expect(yield* lookupState(joined.submissionId)).toBe("joined");

        const reports = yield* runtime.runRecovery;
        const hostReport = reports.find((entry) => entry.submissionId === host.submissionId);
        const joinedReport = reports.find((entry) => entry.submissionId === joined.submissionId);
        expect(hostReport?.decision._tag).toBe("FinalizeLedgerFromHistory");
        expect(hostReport?.disposition).toBe("repaired");
        // The canonical host settlement authorizes the joined settlement (DUR-015).
        expect(joinedReport?.decision._tag).toBe("SettleJoinedWithHost");
        expect(joinedReport?.disposition).toBe("repaired");

        const settledHost = yield* runtime.awaitSettlement(host);
        const settledJoined = yield* runtime.awaitSettlement(joined);
        expect(settledHost.outcome).toBe("failed");
        expect(settledJoined.outcome).toBe("failed");
        const byId = recordsById(yield* readLog(thread));
        const hostRecord = byId.get(`settlement:${host.submissionId}`);
        const joinedRecord = byId.get(`settlement:${joined.submissionId}`);
        if (
          hostRecord?.record.payload._tag === "SubmissionSettled" &&
          joinedRecord?.record.payload._tag === "SubmissionSettled"
        ) {
          expect(hostRecord.record.payload.outcome).toBe("failed");
          expect(joinedRecord.record.payload.outcome).toBe("failed");
          expect(joinedRecord.record.payload.runId).toBe(runIdForSubmission(host.submissionId));
          expect(joinedRecord.record.payload.result).toEqual(hostRecord.record.payload.result);
          expect(settledHost.failure).toEqual(hostRecord.record.payload.result);
          expect(settledJoined.failure).toEqual(hostRecord.record.payload.result);
        } else {
          throw new Error("Expected canonical host and joined SubmissionSettled records");
        }
      }),
  );

  it.effect("a kill inside the joined-settlement loop converges through the reservation", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const scripted = yield* makeScriptedModel(() => finalParts('{"answer":"loop"}'));
      const agent = Agent.withModel(joinDefinition, scripted.model);
      const thread = "thread-join-settle-loop";

      const host = yield* runtime.submit(
        agent,
        { question: "host question" },
        submitOptions(thread, "loop-host"),
      );
      const joined = yield* runtime.submit(
        agent,
        { question: "queued question" },
        submitOptions(thread, "loop-2"),
      );
      // First reserve is the host's, the second is the JOINED Submission's: kill right after
      // the joined reservation commits, before its canonical append.
      yield* armFailpointAt("terminalize:after-reserve", 2);
      const killed = yield* Effect.exit(runtime.processThread(agent, decodeThreadId(thread)));
      expect(failureTag(killed)).toBe("DurableRuntimeFailpointError");
      yield* clearFailpoint;
      expect(yield* lookupState(host.submissionId)).toBe("settled");
      expect(yield* lookupState(joined.submissionId)).toBe("terminalizing");

      const reports = yield* runtime.runRecovery;
      const report = reports.find((entry) => entry.submissionId === joined.submissionId);
      expect(report?.decision._tag).toBe("AppendReservedSettlement");
      expect(report?.disposition).toBe("repaired");
      const settled = yield* runtime.awaitSettlement(joined);
      expect(settled.outcome).toBe("completed");
      const records = yield* readLog(thread);
      expect(
        records.filter(
          (envelope) => envelope.record.recordId === `settlement:${joined.submissionId}`,
        ),
      ).toHaveLength(1);
    }),
  );

  it.effect(
    "an admitted-gap Submission breaks the joining prefix and converges in FIFO order",
    () =>
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const ledger = yield* SubmissionLedger;
        const scripted = yield* makeScriptedModel((call) =>
          call === 0
            ? finalParts('{"answer":"host answer"}')
            : finalParts('{"answer":"gap answer"}'),
        );
        const agent = Agent.withModel(joinDefinition, scripted.model);
        const thread = "thread-join-fifo-gap";

        const host = yield* runtime.submit(
          agent,
          { question: "host question" },
          submitOptions(thread, "fifo-host"),
        );
        const joined = yield* runtime.submit(
          agent,
          { question: "queued question" },
          submitOptions(thread, "fifo-2"),
        );
        // The gap: admitted but never marked ready (killed between admission and readiness).
        yield* armFailpoint("submit:after-admit");
        const gapExit = yield* Effect.exit(
          runtime.submit(agent, { question: "gap question" }, submitOptions(thread, "fifo-3")),
        );
        expect(failureTag(gapExit)).toBe("DurableRuntimeFailpointError");
        yield* clearFailpoint;
        const gapSnapshot = yield* ledger.lookup(
          SubmissionLookupById.make({ submissionId: host.submissionId }),
        );
        expect(Option.isSome(gapSnapshot)).toBe(true);

        const settlements = yield* runtime.processThread(agent, decodeThreadId(thread));
        // Two head settlements: the host (with the joined Submission settling alongside) and the
        // gap Submission as its OWN later Run — never skipped, never joined past the gap.
        expect(settlements.map((settlement) => settlement.outcome)).toEqual([
          "completed",
          "completed",
        ]);
        expect(yield* lookupState(joined.submissionId)).toBe("settled");

        const hostRunId = runIdForSubmission(host.submissionId);
        const records = yield* readLog(thread);
        const byId = recordsById(records);
        const joinedSettlement = byId.get(`settlement:${joined.submissionId}`);
        if (joinedSettlement?.record.payload._tag === "SubmissionSettled") {
          expect(joinedSettlement.record.payload.runId).toBe(hostRunId);
        } else {
          throw new Error("Expected the joined Submission to settle with the host Run");
        }
        // The gap Submission ran as its own Run.
        const gapInput = records.find(
          (envelope) =>
            envelope.record.payload._tag === "UserInputRecorded" &&
            envelope.record.payload.kind === "user" &&
            JSON.stringify(envelope.record.payload.input).includes("gap question"),
        );
        expect(gapInput).toBeDefined();
        if (gapInput?.record.payload._tag === "UserInputRecorded") {
          expect(gapInput.record.payload.runId).not.toBe(hostRunId);
        }
        // The host's Turn saw the joined text but never the gap text; the gap ran afterwards.
        expect(scripted.prompts).toHaveLength(2);
        const [hostPrompt, gapPrompt] = scripted.prompts;
        expect(
          hostPrompt === undefined ? 0 : promptOccurrences(hostPrompt, "queued question"),
        ).toBe(1);
        expect(hostPrompt === undefined ? 0 : promptOccurrences(hostPrompt, "gap question")).toBe(
          0,
        );
        expect(gapPrompt === undefined ? 0 : promptOccurrences(gapPrompt, "gap question")).toBe(1);
      }),
  );
});
