import { MemorySubmissionLedgerLive } from "@effect-agent/storage-memory/memory-submission-ledger";
import { MemoryThreadStoreLive } from "@effect-agent/storage-memory/memory-thread-store";
import { SqliteStorageFailpoint } from "@effect-agent/storage-sqlite/sqlite-storage-failpoint";
import { submissionLedgerLayer } from "@effect-agent/storage-sqlite/sqlite-submission-ledger";
import {
  storageConfigLayer,
  threadStoreLayer,
} from "@effect-agent/storage-sqlite/sqlite-thread-store";
import { NodeCrypto, NodeFileSystem } from "@effect/platform-node";
import { SqliteClient } from "@effect/sql-sqlite-node";
import { expect, layer } from "@effect/vitest";
import {
  Cause,
  Clock,
  Deferred,
  Duration,
  Effect,
  Exit,
  Fiber,
  FileSystem,
  Layer,
  Option,
  Ref,
  Schema,
  SchemaGetter,
  Stream,
} from "effect";
import * as Agent from "effect-agent/agent";
import { AgentPolicy } from "effect-agent/agent-policy";
import {
  DurableAgentRuntime,
  DurableRuntimeConfig,
  type DurableSubmitOptions,
} from "effect-agent/durable-agent-runtime";
import {
  DurableRuntimeFailpointError,
  type DurableRuntimeFailpointLocation,
} from "effect-agent/durable-failpoint";
import { ThreadId, type SubmissionId } from "effect-agent/identifiers";
import {
  drainLifecyclePublications,
  LifecyclePublicationError,
  LifecyclePublicationHandler,
  lifecyclePublicationLayer,
} from "effect-agent/lifecycle-publication";
import {
  DefinitionDigests,
  DeploymentId,
  Digest,
  ProducerId,
  type CanonicalRecordEnvelope,
} from "effect-agent/records";
import { runIdForSubmission } from "effect-agent/run-journal";
import {
  AbortCommand,
  IdempotencyKey,
  Principal,
  SubmissionLedger,
  SubmissionLookupById,
} from "effect-agent/submission-ledger";
import { DurableRuntimeFailpointTestControl } from "effect-agent/testing/durable-failpoint-test-control";
import { ThreadRead, ThreadStore } from "effect-agent/thread-store";
import { ToolReconciler } from "effect-agent/tool-reconciler";
import { WakeScheduler } from "effect-agent/wake-scheduler";
import {
  LanguageModel,
  Model,
  Tool,
  Toolkit,
  type Prompt,
  type Response,
} from "effect/unstable/ai";

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
const makeScriptedModel = (
  script: (
    call: number,
  ) =>
    | ReadonlyArray<Response.StreamPartEncoded>
    | Effect.Effect<ReadonlyArray<Response.StreamPartEncoded>>,
) =>
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
                Effect.flatMap((call) => {
                  prompts.push(request.prompt);
                  const parts = script(call);

                  return Effect.isEffect(parts)
                    ? Effect.map(parts, Stream.fromIterable)
                    : Effect.succeed(Stream.fromIterable(parts));
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

const publicationStorageLayer = Layer.unwrap(
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const directory = yield* fs.makeTempDirectoryScoped({ prefix: "lifecycle-join-" });
    const filename = `${directory}/native.sqlite`;

    return Layer.mergeAll(threadStoreLayer, submissionLedgerLayer).pipe(
      Layer.provide(lifecyclePublicationLayer),
      Layer.provide([
        SqliteClient.layer({ filename }),
        storageConfigLayer({ filename }),
        SqliteStorageFailpoint.layer,
      ]),
      Layer.provide(NodeCrypto.layer),
    );
  }),
).pipe(Layer.provide(NodeFileSystem.layer));

// Regression: c68edc7a paused live joins and later model turns behind publication.
// Real SQLite keeps debt pending while the controlled model accepts a joining input.
layer(Layer.mergeAll(baseLayer, publicationStorageLayer))("asynchronous lifecycle joins", (it) => {
  it.effect("completes a joined input before publishing its lifecycle batch", () =>
    Effect.gen(function* () {
      const store = yield* ThreadStore;
      const publications = store.lifecyclePublications;

      if (publications === undefined) return yield* Effect.fail("Missing lifecycle storage");
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const prompts: Array<Prompt.Prompt> = [];

      const model = Model.make(
        "scripted",
        "publication-join",
        Layer.effect(
          LanguageModel.LanguageModel,
          LanguageModel.make({
            generateText: () => Effect.succeed([]),
            streamText: (request) =>
              Stream.unwrap(
                Effect.gen(function* () {
                  prompts.push(request.prompt);
                  if (prompts.length === 1) {
                    yield* Deferred.succeed(entered, undefined);
                    yield* Deferred.await(release);
                  }

                  return Stream.fromIterable(
                    finalParts(prompts.length === 1 ? "continue" : '{"answer":"done"}'),
                  );
                }),
              ),
          }),
        ),
      );

      const agent = Agent.withModel(joinDefinition, model);

      yield* Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;

        // Regression: 50ec5514 let an unavailable owner abort the entire delivery wave.
        const blocked = yield* runtime.submit(
          agent,
          { question: "unavailable destination" },
          submitOptions("a-unavailable-publication", "blocked"),
        );

        const host = yield* runtime.submit(
          agent,
          { question: "host question" },
          submitOptions("publication-join", "host"),
        );

        const worker = yield* Effect.forkChild(runtime.processThread(agent, host.threadId));

        yield* Deferred.await(entered);

        const joined = yield* runtime.submit(
          agent,
          { question: "queued question" },
          submitOptions("publication-join", "joined"),
        );

        yield* Deferred.succeed(release, undefined);
        const settled = yield* Fiber.join(worker);

        expect(settled[0]?.outcome).toBe("completed");
        expect(prompts).toHaveLength(2);
        expect(promptOccurrences(prompts[1]!, "queued question")).toBe(1);
        expect(
          (yield* readLog("publication-join")).filter(
            ({ record }) => record.recordId === `input:${joined.submissionId}`,
          ),
        ).toHaveLength(1);
        const pending = yield* publications.pending(yield* Clock.currentTimeMillis, 2);

        expect(pending.map((batch) => batch[0].ownerThreadId)).toEqual([
          blocked.threadId,
          host.threadId,
        ]);
        const delivered: Array<string> = [];

        const result = yield* drainLifecyclePublications(publications).pipe(
          Effect.provideService(LifecyclePublicationHandler, {
            publish: (batch) =>
              batch[0].ownerThreadId === blocked.threadId
                ? Effect.fail(LifecyclePublicationError.make({ reason: "unavailable" }))
                : Effect.sync(() => {
                    delivered.push(...batch.map((fact) => fact.id));
                  }),
          }),
          Effect.exit,
        );

        expect(delivered).toEqual(pending[1]?.map((fact) => fact.id));
        expect(failureTag(result)).toBe("LifecyclePublicationError");
        expect(yield* publications.pending(Number.MAX_SAFE_INTEGER, 2)).toEqual([pending[0]]);
      }).pipe(Effect.provide(DurableAgentRuntime.layer));
    }),
  );
});
layer(testLayer)("DUR P5 joining/joined queued input (plan §2.5)", (it) => {
  // Regression: f0c51b92 still drains an unserviceable burst after a final text response.
  it.effect("leaves a final-turn text response's follow-ups ready for independent Runs", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const thread = "last-text-turn-join";
      const followUps: Array<Effect.Success<ReturnType<typeof runtime.submit>>> = [];

      const scripted = yield* makeScriptedModel(
        (call): Effect.Effect<ReadonlyArray<Response.StreamPartEncoded>> =>
          Effect.gen(function* () {
            if (call === 0) {
              for (const question of ["second", "third"]) {
                followUps.push(
                  yield* runtime
                    .submit(agent, { question }, submitOptions(thread, question))
                    .pipe(Effect.orDie),
                );
              }
            }

            return finalParts('"answered"');
          }),
      );

      const agent = Agent.withModel(
        Agent.make("last-text-turn-join", {
          input: Schema.Struct({ question: Schema.String }),
          output: Schema.String,
          instructions: "Answer as JSON.",
          toolkit: Toolkit.empty,
          policy: { maxTurns: 1, onExhaustion: "fail" },
        }),
        scripted.model,
      );

      const host = yield* runtime.submit(
        agent,
        { question: "initial" },
        submitOptions(thread, "host"),
      );

      const settlements = yield* runtime.processThread(agent, host.threadId);

      expect(settlements.map((settled) => settled.outcome)).toEqual(["completed", "completed"]);
      for (const receipt of [host, ...followUps]) {
        expect((yield* runtime.awaitSettlement(receipt)).outcome).toBe("completed");
      }
      expect(scripted.prompts).toHaveLength(2);
    }),
  );

  // Regression: f0c51b92 treats the exact tool-call cap as blocking a text-only turn.
  it.effect("joins at the tool-call cap when an optional completion can continue in text", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;

      const toolkit = Toolkit.make(
        Tool.make("answer", { parameters: Schema.Struct({}), success: Schema.String }),
      );

      const scripted = yield* makeScriptedModel((call) =>
        call === 0
          ? [
              { type: "tool-call", id: "answer-0", name: "answer", params: {} },
              { type: "finish", reason: "tool-calls", usage },
            ]
          : finalParts('"covered"'),
      );

      const agent = Agent.withModel(
        Agent.make("exact-call-cap-join", {
          input: Schema.Struct({ question: Schema.String }),
          output: Schema.String,
          instructions: "Answer the question.",
          toolkit,
          completion: { tool: "answer", project: ({ result }) => result },
          policy: { maxTurns: 3, maxToolCalls: 1, onExhaustion: "fail" },
        }),
        scripted.model,
      );

      const thread = "exact-call-cap-join";

      const host = yield* runtime.submit(
        agent,
        { question: "initial" },
        submitOptions(thread, "host"),
      );

      let followUp: typeof host | undefined;

      const settlements = yield* runtime.processThread(agent, host.threadId).pipe(
        Effect.provide(
          toolkit.toLayer({
            answer: () =>
              Effect.gen(function* () {
                followUp = yield* runtime
                  .submit(agent, { question: "follow-up" }, submitOptions(thread, "later"))
                  .pipe(Effect.orDie);

                return "answered";
              }),
          }),
        ),
      );

      expect(settlements).toHaveLength(1);
      expect((yield* runtime.awaitSettlement(followUp!)).outcome).toBe("completed");
      expect(scripted.prompts).toHaveLength(2);
      expect(promptOccurrences(scripted.prompts[1]!, "follow-up")).toBe(1);
    }),
  );

  // Regression: 625c5595 claims follow-ups even when the completing Run has no turns left.
  it.effect("leaves a last-turn follow-up ready for its own successful Run", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;

      const toolkit = Toolkit.make(
        Tool.make("answer", { parameters: Schema.Struct({}), success: Schema.String }),
      );

      const scripted = yield* makeScriptedModel((call) => [
        { type: "tool-call", id: `answer-${call}`, name: "answer", params: {} },
        { type: "finish", reason: "tool-calls", usage },
      ]);

      const agent = Agent.withModel(
        Agent.make("last-turn-join", {
          input: Schema.Struct({ question: Schema.String }),
          output: Schema.String,
          instructions: "Answer the question.",
          toolkit,
          completion: { tool: "answer", required: true, project: ({ result }) => result },
          policy: { maxTurns: 1, maxToolCalls: 1, onExhaustion: "fail" },
        }),
        scripted.model,
      );

      const thread = "last-turn-join";

      const host = yield* runtime.submit(
        agent,
        { question: "initial" },
        submitOptions(thread, "host"),
      );

      let followUp: typeof host | undefined;

      const settlements = yield* runtime.processThread(agent, host.threadId).pipe(
        Effect.provide(
          toolkit.toLayer({
            answer: () =>
              Effect.gen(function* () {
                if (followUp === undefined) {
                  followUp = yield* runtime
                    .submit(agent, { question: "later" }, submitOptions(thread, "later"))
                    .pipe(Effect.orDie);
                }

                return "answered";
              }),
          }),
        ),
      );

      expect(settlements.map((settled) => settled.outcome)).toEqual(["completed", "completed"]);
      expect((yield* runtime.awaitSettlement(host)).outcome).toBe("completed");
      expect((yield* runtime.awaitSettlement(followUp!)).outcome).toBe("completed");
      expect(scripted.prompts).toHaveLength(2);
      const records = recordsById(yield* readLog(thread));
      const later = records.get(`settlement:${followUp!.submissionId}`)?.record.payload;

      expect(later?._tag === "SubmissionSettled" ? later.runId : undefined).toBe(
        runIdForSubmission(followUp!.submissionId),
      );
    }),
  );

  // Regression: 625c5595 joins a whole batch before rendering a potentially invalid prompt.
  it.effect("keeps a valid queued input runnable after a rejected joining prompt", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const scripted = yield* makeScriptedModel(() => finalParts('"answered"'));

      const agent = Agent.withModel(
        Agent.make("rejected-join", {
          input: Schema.Struct({ question: Schema.String }),
          inputPrompt: ({ question }) =>
            question === "invalid" ? Effect.fail("Rejected input") : Effect.succeed(question),
          output: Schema.String,
          instructions: "Answer the question as JSON.",
          toolkit: Toolkit.empty,
          policy: { maxTurns: 4, maxToolCalls: 2 },
        }),
        scripted.model,
      );

      const thread = "rejected-join";

      const host = yield* runtime.submit(
        agent,
        { question: "initial" },
        submitOptions(thread, "host"),
      );

      const invalid = yield* runtime.submit(
        agent,
        { question: "invalid" },
        submitOptions(thread, "invalid"),
      );

      const valid = yield* runtime.submit(
        agent,
        { question: "valid later input" },
        submitOptions(thread, "valid"),
      );

      yield* runtime.processThread(agent, host.threadId);
      expect((yield* runtime.awaitSettlement(valid)).outcome).toBe("completed");
      expect((yield* runtime.awaitSettlement(invalid)).outcome).toBe("failed");
      expect(
        scripted.prompts.some((prompt) => promptOccurrences(prompt, "valid later input") === 1),
      ).toBe(true);
    }),
  );

  // Regression: d2473211 only retains returned results if cancellation happens inside drain.
  it.effect("retains a returned completion tool when disposition encoding is interrupted", () =>
    Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      const encoding = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();

      const disposition = Schema.String.pipe(
        Schema.decodeTo(Schema.String, {
          decode: SchemaGetter.passthrough(),
          encode: SchemaGetter.transformEffect((value) =>
            Deferred.succeed(encoding, undefined).pipe(
              Effect.andThen(Deferred.await(release)),
              Effect.as(value),
            ),
          ),
        }),
      );

      const toolkit = Toolkit.make(
        Tool.make("answer", { parameters: Schema.Struct({}), success: Schema.String }),
      );

      const scripted = yield* makeScriptedModel((call) =>
        call === 0
          ? [
              { type: "tool-call", id: "answer-0", name: "answer", params: {} },
              { type: "finish", reason: "tool-calls", usage },
            ]
          : finalParts('"answered"'),
      );

      const agent = Agent.withModel(
        Agent.make("interrupted-completion", {
          input: Schema.Struct({ question: Schema.String }),
          output: Schema.String,
          instructions: "Answer the question.",
          toolkit,
          completion: { tool: "answer", project: ({ result }) => result },
          runDisposition: { schema: disposition, fromOutput: () => "completed" },
          policy: { maxTurns: 4, maxToolCalls: 4 },
        }),
        scripted.model,
      );

      const thread = "interrupted-completion";

      const host = yield* runtime.submit(
        agent,
        { question: "initial" },
        submitOptions(thread, "host"),
      );

      let calls = 0;

      yield* Effect.gen(function* () {
        const worker = yield* Effect.forkChild(runtime.processThread(agent, host.threadId));

        yield* Deferred.await(encoding);
        yield* Fiber.interrupt(worker);
        const records = yield* readLog(thread);

        expect(
          records.filter(({ record }) => record.payload._tag === "ToolCallSettled"),
        ).toHaveLength(1);
        expect(calls).toBe(1);
        yield* Deferred.succeed(release, undefined);
        yield* runtime.processThread(agent, host.threadId);
        expect((yield* runtime.awaitSettlement(host)).outcome).toBe("completed");
        expect(calls).toBe(1);
      }).pipe(
        Effect.provide(
          toolkit.toLayer({
            answer: () =>
              Effect.sync(() => {
                calls++;

                return "answered";
              }),
          }),
        ),
      );
    }),
  );

  // https://github.com/danieljvdm/effect-agent/blob/e40e0574/packages/effect-agent/src/engine/internal/agent-runtime.ts
  // A live provider cannot deterministically admit input inside the completion Tool boundary.
  it.effect(
    "covers a queued burst and a follow-up arriving inside a completion tool in one Run",
    () =>
      Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;

        const toolkit = Toolkit.make(
          Tool.make("handoff", { parameters: Schema.Struct({}), success: Schema.String }),
          Tool.make("answer", { parameters: Schema.Struct({}), success: Schema.String }),
        );

        const scripted = yield* makeScriptedModel((call) => [
          {
            type: "tool-call",
            id: `call-${call}`,
            name: call === 0 ? "handoff" : "answer",
            params: {},
          },
          { type: "finish", reason: "tool-calls", usage },
        ]);

        const agent = Agent.withModel(
          Agent.make("completion-join", {
            input: Schema.Struct({ question: Schema.String }),
            output: Schema.String,
            instructions: "Hand off, then acknowledge any new instructions.",
            toolkit,
            completion: { tool: "answer", required: true, project: ({ result }) => result },
            completionFromTools: [
              { tool: "handoff", project: ({ result }) => Option.some(result) },
            ],
            policy: { maxTurns: 4, maxToolCalls: 4, maxDuration: "30 seconds" },
          }),
          scripted.model,
        );

        const thread = "completion-join-burst";

        const host = yield* runtime.submit(
          agent,
          { question: "initial" },
          submitOptions(thread, "host"),
        );

        const second = yield* runtime.submit(
          agent,
          { question: "second" },
          submitOptions(thread, "second"),
        );

        const third = yield* runtime.submit(
          agent,
          { question: "third" },
          submitOptions(thread, "third"),
        );

        let followUp: typeof host | undefined;
        let handoffs = 0;

        const settlements = yield* runtime.processThread(agent, host.threadId).pipe(
          Effect.provide(
            toolkit.toLayer({
              handoff: () =>
                Effect.gen(function* () {
                  handoffs++;
                  followUp = yield* runtime
                    .submit(
                      agent,
                      { question: "during-handoff" },
                      submitOptions(thread, "follow-up"),
                    )
                    .pipe(Effect.orDie);

                  return "delegated";
                }),
              answer: () => Effect.succeed("all covered"),
            }),
          ),
        );

        expect(settlements).toHaveLength(1);
        expect(scripted.prompts).toHaveLength(2);
        expect(promptOccurrences(scripted.prompts[0]!, "second")).toBe(1);
        expect(promptOccurrences(scripted.prompts[0]!, "third")).toBe(1);
        expect(promptOccurrences(scripted.prompts[1]!, "during-handoff")).toBe(1);
        expect(handoffs).toBe(1);
        expect(followUp).toBeDefined();
        const records = recordsById(yield* readLog(thread));

        for (const receipt of [host, second, third, followUp!]) {
          const settled = yield* runtime.awaitSettlement(receipt);

          expect(settled.outcome).toBe("completed");
          const record = records.get(`settlement:${receipt.submissionId}`)?.record.payload;

          expect(record?._tag === "SubmissionSettled" ? record.runId : undefined).toBe(
            runIdForSubmission(host.submissionId),
          );
        }
        const hostResult = records.get(`settlement:${host.submissionId}`)?.record.payload;

        expect(hostResult?._tag === "SubmissionSettled" ? hostResult.result : undefined).toBe(
          "all covered",
        );
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

      const reports = (yield* runtime.runRecovery()).reports;
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

      const reports = (yield* runtime.runRecovery()).reports;
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
