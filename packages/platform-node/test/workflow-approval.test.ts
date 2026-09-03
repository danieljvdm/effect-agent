import * as Agent from "@effect-agent/core/Agent";
import { AgentPolicy } from "@effect-agent/core/AgentPolicy";
import { ToolCallId } from "@effect-agent/core/Identifiers";
import { digestDefinitions } from "@effect-agent/thread/Digest";
import { ApprovalDecisionCommand } from "@effect-agent/thread/SubmissionLedger";
import { WorkflowAgentHost } from "@effect-agent/workflow/WorkflowAgentHost";
import { NodeCrypto, NodeFileSystem } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Clock, Deferred, Effect, Layer, Ref, Schema, Stream } from "effect";
import { Tool, Toolkit, type Response } from "effect/unstable/ai";

import {
  definitionsFor,
  finalParts,
  hostLayer,
  makeModel,
  pendingIntents,
  planner,
  readLog,
  submitOptions,
  temporaryDirectory,
  until,
  usage,
} from "./workflow-fixtures.ts";

const platform = Layer.mergeAll(NodeCrypto.layer, NodeFileSystem.layer);
const callId = Schema.decodeSync(ToolCallId)("book-1");

const makeApproval = Effect.fn("WorkflowApprovalTest.make")(function* (maxTurns = 4) {
  const Book = Tool.make("book", {
    parameters: Schema.Struct({ ref: Schema.String }),
    success: Schema.Struct({ confirmation: Schema.String }),
    needsApproval: true,
  });

  const tools = Toolkit.make(Book);

  const definition = Agent.make("workflow-approval-limits", {
    input: planner.input,
    output: planner.output,
    instructions: "Book and answer.",
    toolkit: tools,
    policy: AgentPolicy.make({
      maxTurns,
      maxToolCalls: 2,
      maxDuration: "30 seconds",
      toolConcurrency: 1,
      onExhaustion: "final-answer",
    }),
  });

  const model = yield* makeModel((call) =>
    Stream.fromIterable<Response.StreamPartEncoded>(
      call === 0
        ? [
            {
              type: "tool-call",
              id: callId,
              name: "book",
              params: { ref: "x" },
              providerExecuted: false,
            },
            { type: "finish", reason: "tool-calls", usage },
          ]
        : finalParts(),
    ),
  );

  const agent = Agent.withModel(definition, model.model);
  const definitions = definitionsFor(definition.id);
  const digests = yield* digestDefinitions(definitions);
  const toolCalls = yield* Ref.make(0);

  const handlers = tools.toLayer({
    book: () => Ref.update(toolCalls, (n) => n + 1).pipe(Effect.as({ confirmation: "confirmed" })),
  });

  return { agent, definitions, digests, toolCalls, modelCalls: model.calls, handlers };
});

it.live("repairs an approval wake sent before native SQL Workflow suspension", () =>
  Effect.gen(function* () {
    const directory = yield* temporaryDirectory;
    const fixture = yield* makeApproval();
    const suspended = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();

    yield* Effect.gen(function* () {
      const host = yield* WorkflowAgentHost;

      const receipt = yield* host.submit(
        fixture.agent,
        { question: "race" },
        submitOptions(fixture.digests),
      );

      yield* Deferred.await(suspended);
      yield* host.resolveApproval(
        ApprovalDecisionCommand.make({
          submissionId: receipt.submissionId,
          toolCallId: callId,
          decision: "approved",
          resolver: "operator",
          reason: "approve before native suspension",
        }),
      );
      yield* host.repair;
      expect(yield* Ref.get(fixture.toolCalls)).toBe(0);
      expect((yield* host.submissionStatus(receipt))._tag).toBe("pending");
      expect(yield* pendingIntents).toHaveLength(1);
      yield* Deferred.succeed(release, undefined);
      expect((yield* host.awaitSettlement(receipt)).outcome).toBe("completed");
      yield* until(pendingIntents, (rows) => rows.length === 0);
      expect(yield* Ref.get(fixture.toolCalls)).toBe(1);
    }).pipe(
      Effect.provide(
        hostLayer(directory, [{ agent: fixture.agent, definitions: fixture.definitions }], {
          runtimeFailpoint: (point) =>
            point === "approval:after-suspend"
              ? Deferred.succeed(suspended, undefined).pipe(Effect.andThen(Deferred.await(release)))
              : Effect.void,
        }).pipe(Layer.provide(fixture.handlers)),
      ),
    );
  }).pipe(Effect.scoped, Effect.provide(platform)),
);

it.live.each(["duration", "turns"] as const)(
  "preserves the original %s budget through SQL approval restart",
  (limit) =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory;
      const fixture = yield* makeApproval(limit === "turns" ? 1 : 4);
      const liveClock = yield* Clock.Clock;
      let offset = 0;

      const clock: Clock.Clock = {
        sleep: (duration) => liveClock.sleep(duration),
        currentTimeMillisUnsafe: () => liveClock.currentTimeMillisUnsafe() + offset,
        currentTimeMillis: Effect.sync(() => liveClock.currentTimeMillisUnsafe() + offset),
        currentTimeNanosUnsafe: () =>
          liveClock.currentTimeNanosUnsafe() + BigInt(offset) * 1000000n,
        currentTimeNanos: Effect.sync(
          () => liveClock.currentTimeNanosUnsafe() + BigInt(offset) * 1000000n,
        ),
        monotonicTimeNanosUnsafe: () =>
          liveClock.monotonicTimeNanosUnsafe() + BigInt(offset) * 1000000n,
        monotonicTimeNanos: Effect.sync(
          () => liveClock.monotonicTimeNanosUnsafe() + BigInt(offset) * 1000000n,
        ),
      };

      const stack = hostLayer(directory, [
        { agent: fixture.agent, definitions: fixture.definitions },
      ]).pipe(
        Layer.provide(fixture.handlers),
        Layer.provide(Layer.succeedContext(Clock.Clock.context(clock))),
      );

      const before = yield* Effect.gen(function* () {
        const host = yield* WorkflowAgentHost;

        const receipt = yield* host.submit(
          fixture.agent,
          { question: "bounded" },
          submitOptions(fixture.digests),
        );

        const log = yield* until(readLog(receipt.threadId), (rows) =>
          rows.some((row) => row.record.payload._tag === "ToolApprovalRequested"),
        );

        expect(yield* Ref.get(fixture.modelCalls)).toBe(1);

        return { receipt, start: log.find((row) => row.record.payload._tag === "RunStarted") };
      }).pipe(Effect.provide(stack));

      if (limit === "duration") offset = 31000;

      yield* Effect.gen(function* () {
        const host = yield* WorkflowAgentHost;

        yield* host.resolveApproval(
          ApprovalDecisionCommand.make({
            submissionId: before.receipt.submissionId,
            toolCallId: callId,
            decision: "approved",
            resolver: "operator",
            reason: "resume original run budget",
          }),
        );
        const settlement = yield* host.awaitSettlement(before.receipt);

        expect(settlement.outcome).toBe(limit === "duration" ? "failed" : "completed");
        yield* until(pendingIntents, (rows) => rows.length === 0);
        const log = yield* readLog(before.receipt.threadId);

        expect(
          log.find((row) => row.record.payload._tag === "SubmissionSettled")?.record.payload,
        ).toMatchObject(
          limit === "duration"
            ? { policyLimit: "duration" }
            : { finishReason: "budget-exhausted", exhausted: "turns" },
        );
        expect(log.filter((row) => row.record.payload._tag === "RunStarted")).toEqual([
          before.start,
        ]);
        expect(yield* Ref.get(fixture.modelCalls)).toBe(limit === "duration" ? 1 : 2);
        expect(yield* Ref.get(fixture.toolCalls)).toBe(limit === "duration" ? 0 : 1);
      }).pipe(Effect.provide(stack));
    }).pipe(Effect.scoped, Effect.provide(platform)),
);
