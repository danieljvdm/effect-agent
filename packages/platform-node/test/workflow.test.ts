import * as Agent from "@effect-agent/core/Agent";
import { ToolCallId } from "@effect-agent/core/Identifiers";
import { digestDefinitions } from "@effect-agent/thread/Digest";
import { type Receipt } from "@effect-agent/thread/DurableAgentRuntime";
import { DurableAgentRuntime } from "@effect-agent/thread/DurableAgentRuntime";
import { ApprovalDecisionCommand } from "@effect-agent/thread/SubmissionLedger";
import { WorkflowAgentHost } from "@effect-agent/workflow/WorkflowAgentHost";
import {
  WorkflowDispatchError,
  WorkflowDispatchFailpoint,
} from "@effect-agent/workflow/WorkflowDispatch";
import { NodeCrypto, NodeFileSystem } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Deferred, Effect, Exit, Fiber, Layer, Ref, Schema, Stream } from "effect";
import { Tool, Toolkit, type Response } from "effect/unstable/ai";

import {
  definitionsFor,
  finalParts,
  hostLayer,
  makeModel,
  makePlanner,
  pendingIntents,
  planner,
  readLog,
  submitOptions,
  temporaryDirectory,
  until,
  usage,
} from "./workflow-fixtures.ts";

const platform = Layer.mergeAll(NodeCrypto.layer, NodeFileSystem.layer);

it.live.each([false, true])(
  "settles through the injected engine and deduplicates admission (memory=%s)",
  (memory) =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory;
      const fixture = yield* makePlanner();

      yield* Effect.gen(function* () {
        const host = yield* WorkflowAgentHost;
        const options = submitOptions(fixture.digests);
        const receipt = yield* host.submit(fixture.agent, { question: "once" }, options);
        const duplicate = yield* host.submit(fixture.agent, { question: "once" }, options);

        expect(duplicate).toEqual(receipt);
        const settled = yield* host.awaitSettlement(receipt);

        expect(settled.outcome).toBe("completed");
        yield* until(pendingIntents, (rows) => rows.length === 0);
        expect(yield* Ref.get(fixture.calls)).toBe(1);
        const log = yield* readLog(receipt.threadId);

        expect(log.filter((row) => row.record.payload._tag === "SubmissionSettled")).toHaveLength(
          1,
        );
      }).pipe(Effect.provide(hostLayer(directory, [fixture], {}, memory)));
    }).pipe(Effect.scoped, Effect.provide(platform)),
);

it.live.each(["intent:before-persist", "launch:before"] as const)(
  "startup discovers admitted work after failure at %s",
  (location) =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory;
      const fixture = yield* makePlanner();
      const options = submitOptions(fixture.digests);

      yield* Effect.gen(function* () {
        const host = yield* WorkflowAgentHost;

        const result = yield* host
          .submit(fixture.agent, { question: "recover" }, options)
          .pipe(Effect.result);

        expect(result._tag).toBe("Failure");
        expect(yield* Ref.get(fixture.calls)).toBe(0);
      }).pipe(
        Effect.provide(hostLayer(directory, [fixture])),
        Effect.provideService(WorkflowDispatchFailpoint, {
          hit: (point) =>
            point === location
              ? Effect.fail(
                  new WorkflowDispatchError({
                    operation: point,
                    message: "injected launch outage",
                  }),
                )
              : Effect.void,
        }),
      );

      yield* Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const host = yield* WorkflowAgentHost;
        const receipt = yield* runtime.submit(fixture.agent, { question: "recover" }, options);

        expect((yield* host.awaitSettlement(receipt)).outcome).toBe("completed");
        yield* until(pendingIntents, (rows) => rows.length === 0);
        expect(yield* Ref.get(fixture.calls)).toBe(1);
      }).pipe(Effect.provide(hostLayer(directory, [fixture])));
    }).pipe(Effect.scoped, Effect.provide(platform)),
);

it.live(
  "repairs canonical settlement committed before native completion and retains completed intents until cleanup",
  () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory;
      const fixture = yield* makePlanner();
      const committed = yield* Deferred.make<void>();
      const submitted = yield* Deferred.make<Receipt>();

      const runtime = hostLayer(directory, [fixture], {
        runtimeFailpoint: (point) =>
          point === "terminalize:after-canonical-append"
            ? Deferred.succeed(committed, undefined).pipe(Effect.andThen(Effect.never))
            : Effect.void,
      });

      const first = yield* Effect.gen(function* () {
        const host = yield* WorkflowAgentHost;

        const receipt = yield* host.submit(
          fixture.agent,
          { question: "canonical" },
          submitOptions(fixture.digests),
        );

        yield* Deferred.succeed(submitted, receipt);

        return yield* Effect.never;
      }).pipe(Effect.provide(runtime), Effect.forkChild);

      const receipt = yield* Deferred.await(submitted);

      yield* Deferred.await(committed);
      yield* Fiber.interrupt(first);
      expect(yield* Ref.get(fixture.calls)).toBe(1);

      yield* Effect.gen(function* () {
        const host = yield* WorkflowAgentHost;

        expect((yield* host.awaitSettlement(receipt)).outcome).toBe("completed");
        yield* until(pendingIntents, (rows) => rows.length === 0);
        const log = yield* readLog(receipt.threadId);

        expect(log.filter((row) => row.record.payload._tag === "SubmissionSettled")).toHaveLength(
          1,
        );
        expect(yield* Ref.get(fixture.calls)).toBe(1);
      }).pipe(Effect.provide(hostLayer(directory, [fixture])));
    }).pipe(Effect.scoped, Effect.provide(platform)),
);

it.live(
  "approval and joined input survive restart at concurrency one; detached waiters do not abort work",
  () =>
    Effect.gen(function* () {
      const directory = yield* temporaryDirectory;

      const Book = Tool.make("book", {
        parameters: Schema.Struct({ ref: Schema.String }),
        success: Schema.Struct({ confirmation: Schema.String }),
        needsApproval: true,
      });

      const tools = Toolkit.make(Book);

      const definition = Agent.make("workflow-approval", {
        input: planner.input,
        output: planner.output,
        instructions: planner.instructions,
        policy: planner.policy,
        toolkit: tools,
      });

      const scripted = yield* makeModel((call) =>
        Stream.fromIterable<Response.StreamPartEncoded>(
          call === 0
            ? [
                {
                  type: "tool-call",
                  id: "book-1",
                  name: "book",
                  params: { ref: "reservation" },
                  providerExecuted: false,
                },
                { type: "finish", reason: "tool-calls", usage },
              ]
            : finalParts(),
        ),
      );

      const agent = Agent.withModel(definition, scripted.model);
      const definitions = definitionsFor(definition.id);
      const digests = yield* digestDefinitions(definitions);
      const calls = yield* Ref.make(0);

      const toolLayer = tools.toLayer({
        book: () => Ref.update(calls, (n) => n + 1).pipe(Effect.as({ confirmation: "confirmed" })),
      });

      const stack = hostLayer(directory, [{ agent, definitions }]).pipe(Layer.provide(toolLayer));

      const receipts = yield* Effect.gen(function* () {
        const runtime = yield* DurableAgentRuntime;
        const host = yield* WorkflowAgentHost;
        const first = yield* host.submit(agent, { question: "first" }, submitOptions(digests));

        yield* until(readLog(first.threadId), (rows) =>
          rows.some((row) => row.record.payload._tag === "ToolApprovalRequested"),
        );

        const second = yield* runtime.submit(
          agent,
          { question: "joined" },
          submitOptions(digests, "workflow-thread", "joined"),
        );

        const waiter = yield* host.awaitSettlement(first).pipe(Effect.forkChild);

        yield* Fiber.interrupt(waiter);
        expect(Exit.hasInterrupts(yield* Fiber.await(waiter))).toBe(true);
        expect(yield* Ref.get(calls)).toBe(0);

        return [first, second] as const;
      }).pipe(Effect.provide(stack));

      yield* Effect.gen(function* () {
        const host = yield* WorkflowAgentHost;

        yield* host.resolveApproval(
          new ApprovalDecisionCommand({
            submissionId: receipts[0].submissionId,
            toolCallId: Schema.decodeSync(ToolCallId)("book-1"),
            decision: "approved",
            resolver: "workflow-test",
            reason: "authorized",
          }),
        );
        for (const receipt of receipts)
          expect((yield* host.awaitSettlement(receipt)).outcome).toBe("completed");
        yield* until(pendingIntents, (rows) => rows.length === 0);
        expect(yield* Ref.get(calls)).toBe(1);
        const log = yield* readLog(receipts[0].threadId);

        expect(log.filter((row) => row.record.payload._tag === "SubmissionSettled")).toHaveLength(
          2,
        );
      }).pipe(Effect.provide(stack));
    }).pipe(Effect.scoped, Effect.provide(platform)),
);
