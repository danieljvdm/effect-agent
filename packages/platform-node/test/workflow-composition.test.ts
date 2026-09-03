import { Agent } from "@effect-agent/core";
import { ToolCallId } from "@effect-agent/core/Identifiers";
import type { Receipt } from "@effect-agent/thread/DurableAgentRuntime";
import { OperationAuthorizer, OperationDenied } from "@effect-agent/thread/OperationAuthorizer";
import { AbortCommand, ApprovalDecisionCommand } from "@effect-agent/thread/SubmissionLedger";
import { AgentWorkflow } from "@effect-agent/workflow";
import { WorkflowAgentHost } from "@effect-agent/workflow/WorkflowAgentHost";
import { WorkflowDispatchFailpoint } from "@effect-agent/workflow/WorkflowDispatch";
import { NodeCrypto, NodeFileSystem } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Deferred, Effect, Fiber, Layer, Option, Ref, Schema, Stream } from "effect";
import { Tool, Toolkit, type Response } from "effect/unstable/ai";
import { DurableDeferred, Workflow } from "effect/unstable/workflow";

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

it.live.each([false, true])(
  "replays a native workflow step without rerunning its agent (memory=%s)",
  (memory) =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory;
      const fixture = yield* makePlanner();
      const reached = yield* Deferred.make<void>();
      const receipt = yield* Deferred.make<Receipt>();
      const runs = yield* Ref.make(0);
      const gate = DurableDeferred.make("after-agent");

      const live = Review.toLayer(
        Effect.fn(function* () {
          yield* Ref.update(runs, (n) => n + 1);

          const output = yield* AgentWorkflow.execute(
            planner,
            { question: "once" },
            { name: "triage" },
          );

          yield* Deferred.succeed(reached, undefined);
          yield* DurableDeferred.await(gate);

          return output;
        }),
      ).pipe(Layer.provideMerge(hostLayer(directory, [fixture], {}, memory)));

      yield* Effect.gen(function* () {
        const id = yield* Review.execute({ id: "replay" }, { discard: true });

        yield* Deferred.await(reached);
        yield* until(
          Review.poll(id),
          (result) => Option.isSome(result) && result.value._tag === "Suspended",
        );
        yield* DurableDeferred.succeed(gate, {
          token: DurableDeferred.tokenFromExecutionId(gate, { workflow: Review, executionId: id }),
          value: undefined,
        });
        expect(yield* Review.execute({ id: "replay" })).toEqual({ answer: "done" });
        expect(yield* Ref.get(fixture.calls)).toBe(1);
        expect(yield* Ref.get(runs)).toBeGreaterThan(1);
        const log = yield* readLog((yield* Deferred.await(receipt)).threadId);

        expect(log.filter((row) => row.record.payload._tag === "SubmissionSettled")).toHaveLength(
          1,
        );
      }).pipe(
        Effect.provide(live),
        Effect.provideService(WorkflowDispatchFailpoint, {
          hit: (_point, intent) => Deferred.succeed(receipt, intent.receipt).pipe(Effect.asVoid),
        }),
      );
    }).pipe(Effect.scoped, Effect.provide(platform)),
);

it.live("rejects changed input under the same workflow step identity", () =>
  Effect.gen(function* () {
    const directory = yield* temporaryDirectory;
    const fixture = yield* makePlanner();
    const reached = yield* Deferred.make<void>();
    const question = yield* Ref.make("original");
    const gate = DurableDeferred.make("change-input");

    const live = Review.toLayer(
      Effect.fn(function* () {
        const output = yield* AgentWorkflow.execute(
          planner,
          { question: yield* Ref.get(question) },
          { name: "triage" },
        );

        yield* Deferred.succeed(reached, undefined);
        yield* DurableDeferred.await(gate);

        return output;
      }),
    ).pipe(Layer.provideMerge(hostLayer(directory, [fixture])));

    yield* Effect.gen(function* () {
      const id = yield* Review.execute({ id: "conflict" }, { discard: true });

      yield* Deferred.await(reached);
      yield* until(
        Review.poll(id),
        (result) => Option.isSome(result) && result.value._tag === "Suspended",
      );
      yield* Ref.set(question, "changed");
      yield* DurableDeferred.succeed(gate, {
        token: DurableDeferred.tokenFromExecutionId(gate, { workflow: Review, executionId: id }),
        value: undefined,
      });
      expect(yield* Review.execute({ id: "conflict" }).pipe(Effect.result)).toMatchObject({
        _tag: "Failure",
        failure: { _tag: "AdmissionConflict" },
      });
      expect(yield* Ref.get(fixture.calls)).toBe(1);
    }).pipe(Effect.provide(live));
  }).pipe(Effect.scoped, Effect.provide(platform)),
);

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

it.live("distinct step names run independent agents in native structured concurrency", () =>
  Effect.gen(function* () {
    const directory = yield* temporaryDirectory;
    const fixture = yield* makePlanner();

    const Pair = Workflow.make("agent-composition/Pair", {
      payload: { id: Schema.String },
      success: Schema.Array(planner.output),
      error: AgentWorkflow.Error,
      idempotencyKey: ({ id }) => id,
    });

    const live = Pair.toLayer(() =>
      Effect.all(
        [
          AgentWorkflow.execute(planner, { question: "first" }, { name: "first" }),
          AgentWorkflow.execute(planner, { question: "second" }, { name: "second" }),
        ],
        { concurrency: 2 },
      ),
    ).pipe(Layer.provideMerge(hostLayer(directory, [fixture])));

    const outputs = yield* Pair.execute({ id: "pair" }).pipe(Effect.provide(live));

    expect(outputs).toEqual([{ answer: "done" }, { answer: "done" }]);
    expect(yield* Ref.get(fixture.calls)).toBe(2);
  }).pipe(Effect.scoped, Effect.provide(platform)),
);

it.live.each(["failure", "output", "authorization", "registration", "name"] as const)(
  "preserves a schema-encoded %s failure through Workflow.execute",
  (scenario) =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory;

      const fixture = yield* makePlanner(() =>
        scenario === "failure" ? Stream.empty : Stream.fromIterable(finalParts()),
      );

      const live = Review.toLayer(() =>
        scenario === "output"
          ? AgentWorkflow.execute(
              { ...planner, output: Schema.Struct({ count: Schema.Number }) },
              { question: "test" },
              { name: "step" },
            ).pipe(Effect.as({ answer: "unreachable" }))
          : AgentWorkflow.execute(
              planner,
              { question: "test" },
              { name: scenario === "name" ? "" : "step" },
            ),
      ).pipe(
        Layer.provideMerge(hostLayer(directory, scenario === "registration" ? [] : [fixture])),
      );

      const run = Review.execute({ id: scenario }).pipe(Effect.result, Effect.provide(live));

      const result = yield* scenario === "authorization"
        ? run.pipe(
            Effect.provideService(OperationAuthorizer, {
              authorize: (request) =>
                Effect.fail(
                  OperationDenied.make({ operation: request.operation, reason: "denied" }),
                ),
            }),
          )
        : run;

      expect(result).toMatchObject({
        _tag: "Failure",
        failure: {
          _tag:
            scenario === "output"
              ? "AgentOutputError"
              : scenario === "authorization"
                ? "OperationDenied"
                : scenario === "registration"
                  ? "BindingUnavailable"
                  : "WorkflowExecutionFailure",
        },
      });
      if (scenario === "registration" || scenario === "name")
        expect(yield* Ref.get(fixture.calls)).toBe(0);
    }).pipe(Effect.scoped, Effect.provide(platform)),
);

it.live("repairs a model defect while the parent remains durably suspended", () =>
  Effect.gen(function* () {
    const directory = yield* temporaryDirectory;
    const broken = yield* Ref.make(true);
    const defect = yield* Deferred.make<void>();

    const fixture = yield* makePlanner(() =>
      Stream.unwrap(
        Ref.get(broken).pipe(
          Effect.map((value) =>
            value
              ? Stream.fromEffect(
                  Deferred.succeed(defect, undefined).pipe(
                    Effect.andThen(Effect.die("model defect")),
                  ),
                )
              : Stream.fromIterable(finalParts()),
          ),
        ),
      ),
    );

    const stack = Review.toLayer(() =>
      AgentWorkflow.execute(planner, { question: "recover" }, { name: "triage" }),
    ).pipe(Layer.provideMerge(hostLayer(directory, [fixture])));

    yield* Effect.gen(function* () {
      const id = yield* Review.execute({ id: "defect" }, { discard: true });

      yield* Deferred.await(defect);
      yield* until(
        Review.poll(id),
        (result) => Option.isSome(result) && result.value._tag === "Suspended",
      );
      yield* Ref.set(broken, false);
      const host = yield* WorkflowAgentHost;

      yield* host.repair;
      expect(yield* Review.execute({ id: "defect" })).toEqual({ answer: "done" });
    }).pipe(Effect.provide(stack));
  }).pipe(Effect.scoped, Effect.provide(platform)),
);

it.live("a timed out parent waiter detaches and the parent observes explicit agent abort", () =>
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
      const waiter = yield* Review.execute({ id: "timeout" }).pipe(Effect.forkChild);

      yield* Deferred.await(started);
      expect(yield* Fiber.join(waiter).pipe(Effect.timeoutOption("1 millis"))).toEqual(
        Option.none(),
      );
      yield* Fiber.interrupt(waiter);
      expect(yield* Ref.get(finalized)).toBe(0);
      const host = yield* WorkflowAgentHost;
      const accepted = yield* Deferred.await(receipt);

      yield* host.abort(
        AbortCommand.make({
          submissionId: accepted.submissionId,
          author: "operator",
          reason: "stop",
        }),
      );
      expect(yield* Review.execute({ id: "timeout" }).pipe(Effect.result)).toMatchObject({
        _tag: "Failure",
        failure: { _tag: "WorkflowExecutionFailure", reason: "aborted", receipt: accepted },
      });
      expect(yield* Ref.get(finalized)).toBe(1);
    }).pipe(
      Effect.provide(stack),
      Effect.provideService(WorkflowDispatchFailpoint, {
        hit: (_point, intent) => Deferred.succeed(receipt, intent.receipt).pipe(Effect.asVoid),
      }),
    );
  }).pipe(Effect.scoped, Effect.provide(platform)),
);
