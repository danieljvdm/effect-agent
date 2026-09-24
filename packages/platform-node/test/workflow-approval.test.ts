import { WorkflowAgentHost } from "@effect-agent/workflow/workflow-agent-host";
import { NodeCrypto, NodeFileSystem } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Deferred, Effect, Layer, Ref, Schema, Stream } from "effect";
import * as Agent from "effect-agent/agent";
import { AgentPolicy } from "effect-agent/agent-policy";
import { digestDefinitions } from "effect-agent/digest";
import { ToolCallId } from "effect-agent/identifiers";
import { ApprovalDecisionCommand } from "effect-agent/submission-ledger";
import { Tool, Toolkit, type Response } from "effect/unstable/ai";

import {
  definitionsFor,
  finalParts,
  hostLayer,
  makeModel,
  pendingIntents,
  planner,
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
