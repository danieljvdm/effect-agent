import { MemorySubmissionLedgerLive } from "@effect-agent/storage-memory/memory-submission-ledger";
import { MemoryThreadStoreLive } from "@effect-agent/storage-memory/memory-thread-store";
import { NodeCrypto } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Cause, Effect, Exit, Layer, Option, Schema, Stream } from "effect";
import * as Agent from "effect-agent/agent";
import { DurableWorkerBinding } from "effect-agent/agent-registration";
import * as DecisionTurn from "effect-agent/decision-turn";
import { DurableAgentRuntime, DurableRuntimeConfig } from "effect-agent/durable-agent-runtime";
import { DurableRuntimeFailpointError } from "effect-agent/durable-failpoint";
import { ThreadId } from "effect-agent/identifiers";
import { DefinitionDigests, DeploymentId, Digest, ProducerId } from "effect-agent/records";
import { RunToolAuthorization } from "effect-agent/run-options";
import { IdempotencyKey, Principal } from "effect-agent/submission-ledger";
import { DurableRuntimeFailpointTestControl } from "effect-agent/testing/durable-failpoint-test-control";
import { ThreadRead, ThreadStore } from "effect-agent/thread-store";
import { ToolReconciler } from "effect-agent/tool-reconciler";
import { WakeScheduler } from "effect-agent/wake-scheduler";
import {
  Decision,
  DecisionModel,
  LanguageModel,
  Model,
  type Response,
  Tool,
  Toolkit,
} from "effect/unstable/ai";

const base = Layer.mergeAll(
  MemoryThreadStoreLive,
  MemorySubmissionLedgerLive,
  WakeScheduler.layerNoop,
  ToolReconciler.uncertain,
  RunToolAuthorization.allowAll,
  DurableRuntimeFailpointTestControl.layer,
  DurableRuntimeConfig.layer({
    deploymentId: DeploymentId.make("decision-turn"),
    producerId: ProducerId.make("decision-turn"),
  }),
).pipe(Layer.provideMerge(NodeCrypto.layer));

const classification = Decision.make({
  input: Schema.String,
  decisions: {
    route: Decision.classify({
      instructions: "Choose whether to look up the answer.",
      criteria: { lookup: "Look up the answer", stop: "No lookup needed" },
    }),
  },
});

const languageModel = (parts: ReadonlyArray<Response.StreamPartEncoded>) =>
  Model.make(
    "test",
    "generator",
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({
        generateText: () => Effect.succeed([]),
        streamText: () => Stream.fromIterable(parts),
      }),
    ),
  );

const decisionModel = (onCall: () => void, inputTokens: number) =>
  Model.make(
    "test",
    "classifier",
    Layer.effect(
      DecisionModel.DecisionModel,
      DecisionModel.make({
        decide: () =>
          Effect.sync(() => {
            onCall();

            return {
              answers: {
                route: {
                  _tag: "Classify" as const,
                  label: "lookup",
                  probabilities: { lookup: 1, stop: 0 },
                },
              },
              usage: { inputTokens, outputTokens: 1 },
            };
          }),
      }),
    ),
  );

const definitionsFor = (agent: Digest) => {
  const digest = Digest.make("a".repeat(64));

  return DefinitionDigests.make({ agent, model: digest, tools: digest });
};

const submitOptions = (threadId: ThreadId, definitions: DefinitionDigests) => ({
  threadId,
  principal: Principal.make("test"),
  idempotencyKey: IdempotencyKey.make("one"),
  definitions,
});

// Rejected Decisions were dropped before the next canonical response in this commit:
// https://github.com/danieljvdm/effect-agent/commit/39828502be1328fef6c89e2baa68d1caa79812ad
// Both windows matter: a response-only fix could execute the rejected Tool on recovery.
for (const location of ["turn:after-response-append", "turn:after-canonical-append"] as const) {
  it.effect(`recovers a budget-rejected Decision after ${location}`, () =>
    Effect.gen(function* () {
      let decisions = 0;
      let executions = 0;
      const lookup = Tool.make("lookup", { parameters: Tool.EmptyParams, success: Schema.String });
      const toolkit = Toolkit.make(lookup);

      const definition = Agent.make("decision-budget", {
        input: Schema.String,
        output: Schema.String,
        instructions: "Look up, then answer.",
        toolkit,
        policy: {
          maxTurns: 4,
          maxToolCalls: 4,
          maxDuration: "30 seconds",
          tokenBudget: 10_000,
          completionReserveTokens: 0,
          onExhaustion: "final-answer",
        },
      });

      const decision = DecisionTurn.make(definition, {
        version: 1,
        decision: classification,
        model: decisionModel(() => decisions++, 11_000),
        tool: lookup,
        prepare: () => Effect.succeed(Option.some("lookup")),
        project: () => Option.some({ text: "Selected lookup.", parameters: {} }),
      });

      const agent = Agent.withModel(
        definition,
        languageModel([
          { type: "text-start", id: "answer" },
          { type: "text-delta", id: "answer", delta: '"done"' },
          { type: "text-end", id: "answer" },
          {
            type: "finish",
            reason: "stop",
            usage: { inputTokens: { total: 1 }, outputTokens: { total: 1 } },
          },
        ]),
      );

      const definitions = definitionsFor(yield* decision.contract);

      const binding = yield* DurableWorkerBinding.make(agent, definitions, {
        decisionTurn: decision,
      }).pipe(
        Effect.provide(
          toolkit.toLayer({ lookup: () => Effect.sync(() => `execution-${++executions}`) }),
        ),
      );

      const runtime = yield* DurableAgentRuntime.pipe(
        Effect.provide(DurableAgentRuntime.layerWithBindings([binding])),
      );

      const control = yield* DurableRuntimeFailpointTestControl;
      const threadId = ThreadId.make(location);

      yield* runtime.submit(agent, "go", submitOptions(threadId, definitions));
      yield* control.setHandler((point) =>
        point === location ? DurableRuntimeFailpointError.make({ location }) : Effect.void,
      );

      const interrupted = yield* runtime
        .processThreadResolved(threadId)
        .pipe(Effect.exit, Effect.ensuring(control.clear));

      expect(Exit.isFailure(interrupted)).toBe(true);
      if (Exit.isSuccess(interrupted)) throw new Error("Expected interruption after append");
      const failure = Cause.findErrorOption(interrupted.cause);

      expect(Option.isSome(failure) && Schema.is(DurableRuntimeFailpointError)(failure.value)).toBe(
        true,
      );
      yield* runtime.runRecovery();
      const settled = yield* runtime.processThreadResolved(threadId);

      expect(settled).toMatchObject([
        {
          outcome: "completed",
          usageSummary: {
            modelCalls: 2,
            inputTokens: { total: 11_001 },
            outputTokens: { total: 2 },
          },
        },
      ]);
      expect(decisions).toBe(1);
      expect(executions).toBe(0);
      const store = yield* ThreadStore;

      const records = yield* store
        .read(ThreadRead.make({ threadId, limit: 128 }))
        .pipe(Stream.runCollect);

      const payloads = records.map(({ record }) => record.payload);

      expect(payloads.filter((payload) => payload._tag === "DecisionTurnRecorded")).toMatchObject([
        { turn: 1, decision: { projection: "tool" } },
      ]);
      expect(payloads.find((payload) => payload._tag === "RunCompleted")).toMatchObject({
        output: "done",
      });
    }).pipe(Effect.provide(base)),
  );
}

// The same commit allowed Decision preparation to displace a forced completion request.
it.effect("skips the Decision on a required-completion final Turn", () =>
  Effect.gen(function* () {
    let decisions = 0;
    const lookup = Tool.make("lookup", { parameters: Tool.EmptyParams, success: Schema.String });

    const complete = Tool.make("complete", {
      parameters: Schema.Struct({ answer: Schema.String }),
      success: Schema.String,
    });

    const toolkit = Toolkit.make(lookup, complete);

    const definition = Agent.make("decision-final-turn", {
      input: Schema.String,
      output: Schema.String,
      instructions: "Complete the request.",
      toolkit,
      policy: { maxTurns: 1, maxToolCalls: 3, maxDuration: "30 seconds", onExhaustion: "fail" },
      completion: { tool: "complete", required: true, project: ({ result }) => result },
    });

    const decision = DecisionTurn.make(definition, {
      version: 1,
      decision: classification,
      model: decisionModel(() => decisions++, 1),
      tool: lookup,
      prepare: () => Effect.succeed(Option.some("lookup")),
      project: () => Option.none(),
    });

    const agent = Agent.withModel(
      definition,
      languageModel([
        { type: "tool-call", id: "finish", name: "complete", params: { answer: "done" } },
        {
          type: "finish",
          reason: "tool-calls",
          usage: { inputTokens: { total: 1 }, outputTokens: { total: 1 } },
        },
      ]),
    );

    const definitions = definitionsFor(yield* decision.contract);

    const binding = yield* DurableWorkerBinding.make(agent, definitions, {
      decisionTurn: decision,
    }).pipe(
      Effect.provide(
        toolkit.toLayer({
          lookup: () => Effect.succeed("unused"),
          complete: ({ answer }) => Effect.succeed(answer),
        }),
      ),
    );

    const runtime = yield* DurableAgentRuntime.pipe(
      Effect.provide(DurableAgentRuntime.layerWithBindings([binding])),
    );

    const threadId = ThreadId.make("required-completion");

    yield* runtime.submit(agent, "go", submitOptions(threadId, definitions));
    expect(yield* runtime.processThreadResolved(threadId)).toMatchObject([
      { outcome: "completed" },
    ]);
    expect(decisions).toBe(0);
    const store = yield* ThreadStore;

    const records = yield* store
      .read(ThreadRead.make({ threadId, limit: 128 }))
      .pipe(Stream.runCollect);

    expect(
      records.find(({ record }) => record.payload._tag === "RunCompleted")?.record.payload,
    ).toMatchObject({
      output: "done",
    });
  }).pipe(Effect.provide(base)),
);
