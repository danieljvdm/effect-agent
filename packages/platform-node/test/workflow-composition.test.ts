import { AgentWorkflow } from "@effect-agent/workflow";
import { WorkflowAgentHost } from "@effect-agent/workflow/workflow-agent-host";
import { WorkflowDispatchFailpoint } from "@effect-agent/workflow/workflow-dispatch";
import { NodeCrypto, NodeFileSystem } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Deferred, Effect, Layer, Option, Ref, Schema, Stream } from "effect";
import { Agent } from "effect-agent";
import type { Receipt } from "effect-agent/durable-agent-runtime";
import { ToolCallId } from "effect-agent/identifiers";
import { AbortCommand, ApprovalDecisionCommand } from "effect-agent/submission-ledger";
import { Tool, Toolkit, type Response } from "effect/unstable/ai";
import { Workflow } from "effect/unstable/workflow";

import {
  definitionsFor,
  finalParts,
  hostLayer,
  makeModel,
  makePlanner,
  planner,
  readLog,
  temporaryDirectory,
  until,
  usage,
} from "./workflow-fixtures.ts";

const platform = Layer.mergeAll(NodeCrypto.layer, NodeFileSystem.layer);

const Review = Workflow.make("agent-composition/Review", {
  payload: { id: Schema.String },
  success: planner.output,
  error: AgentWorkflow.Error,
  idempotencyKey: ({ id }) => id,
});

it.live("suspends the parent for approval and resumes it after both SQL runtimes restart", () =>
  Effect.gen(function* () {
    const directory = yield* temporaryDirectory;
    const callId = ToolCallId.make("approve-1");

    const Book = Tool.make("book", {
      parameters: Schema.Struct({}),
      success: Schema.String,
      needsApproval: true,
    });

    const toolkit = Toolkit.make(Book);

    const definition = Agent.make("composed-booking", {
      input: planner.input,
      output: planner.output,
      instructions: "Book and answer as JSON.",
      policy: planner.policy,
      toolkit,
    });

    const model = yield* makeModel((call) =>
      Stream.fromIterable<Response.StreamPartEncoded>(
        call === 0
          ? [
              { type: "tool-call", id: callId, name: "book", params: {}, providerExecuted: false },
              { type: "finish", reason: "tool-calls", usage },
            ]
          : finalParts(),
      ),
    );

    const calls = yield* Ref.make(0);
    const receipt = yield* Deferred.make<Receipt>();
    const finalized = yield* Ref.make(0);

    const stack = Review.toLayer(
      Effect.fn(function* () {
        return yield* AgentWorkflow.execute(
          definition,
          { question: "book" },
          { name: "booking" },
        ).pipe(Effect.ensuring(Ref.update(finalized, (n) => n + 1)));
      }),
    ).pipe(
      Layer.provideMerge(
        hostLayer(directory, [
          { agent: definition, model: model.model, definitions: definitionsFor(definition.id) },
        ]),
      ),
      Layer.provide(
        toolkit.toLayer({ book: () => Ref.update(calls, (n) => n + 1).pipe(Effect.as("booked")) }),
      ),
    );

    const accepted = yield* Effect.gen(function* () {
      const id = yield* Review.execute({ id: "approval" }, { discard: true });
      const accepted = yield* Deferred.await(receipt);

      yield* until(readLog(accepted.threadId), (rows) =>
        rows.some((row) => row.record.payload._tag === "ToolApprovalRequested"),
      );
      yield* until(
        Review.poll(id),
        (result) => Option.isSome(result) && result.value._tag === "Suspended",
      );
      expect(yield* Ref.get(finalized)).toBeGreaterThan(0);
      expect(yield* Ref.get(calls)).toBe(0);

      return accepted;
    }).pipe(
      Effect.provide(stack),
      Effect.provideService(WorkflowDispatchFailpoint, {
        hit: (_point, intent) => Deferred.succeed(receipt, intent.receipt).pipe(Effect.asVoid),
      }),
    );

    yield* Effect.gen(function* () {
      const host = yield* WorkflowAgentHost;

      yield* host.resolveApproval(
        ApprovalDecisionCommand.make({
          submissionId: accepted.submissionId,
          toolCallId: callId,
          decision: "approved",
          resolver: "operator",
          reason: "approved after restart",
        }),
      );
      expect(yield* Review.execute({ id: "approval" })).toEqual({ answer: "done" });
      expect(yield* Ref.get(calls)).toBe(1);
      expect(yield* Ref.get(model.calls)).toBe(2);
    }).pipe(Effect.provide(stack));
  }).pipe(Effect.scoped, Effect.provide(platform)),
);

it.live("native parent interruption detaches; explicit agent abort stops its model", () =>
  Effect.gen(function* () {
    const directory = yield* temporaryDirectory;
    const started = yield* Deferred.make<void>();
    const receipt = yield* Deferred.make<Receipt>();
    const finalized = yield* Ref.make(0);

    const fixture = yield* makePlanner(() =>
      Stream.fromEffect(
        Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never)),
      ).pipe(Stream.ensuring(Ref.update(finalized, (n) => n + 1))),
    );

    const stack = Review.toLayer(() =>
      AgentWorkflow.execute(planner, { question: "wait" }, { name: "triage" }),
    ).pipe(Layer.provideMerge(hostLayer(directory, [fixture])));

    yield* Effect.gen(function* () {
      const id = yield* Review.execute({ id: "interrupt" }, { discard: true });

      yield* Deferred.await(started);
      const accepted = yield* Deferred.await(receipt);

      yield* until(
        Review.poll(id),
        (result) => Option.isSome(result) && result.value._tag === "Suspended",
      );
      yield* Review.interrupt(id);
      expect(yield* Ref.get(finalized)).toBe(0);
      const host = yield* WorkflowAgentHost;

      expect((yield* host.submissionStatus(accepted))._tag).toBe("pending");
      yield* host.abort(
        AbortCommand.make({
          submissionId: accepted.submissionId,
          author: "operator",
          reason: "stop agent too",
        }),
      );
      expect((yield* host.awaitSettlement(accepted)).outcome).toBe("aborted");
      yield* until(Ref.get(finalized), (n) => n === 1);
    }).pipe(
      Effect.provide(stack),
      Effect.provideService(WorkflowDispatchFailpoint, {
        hit: (_point, intent) => Deferred.succeed(receipt, intent.receipt).pipe(Effect.asVoid),
      }),
    );
  }).pipe(Effect.scoped, Effect.provide(platform)),
);
