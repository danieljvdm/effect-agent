import * as NodeHost from "@effect-agent/platform-node/node-durable-host";
import { NodeFileSystem } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import { Context, Effect, FileSystem, Layer, Schema, Stream } from "effect";
import * as Agent from "effect-agent/agent";
import { DurableAgentRuntime } from "effect-agent/durable-agent-runtime";
import { ThreadId } from "effect-agent/identifiers";
import { MessageDeliveryStore } from "effect-agent/message-delivery";
import { DefinitionDigestInput } from "effect-agent/records";
import * as Subagent from "effect-agent/subagent";
import { SubagentHost } from "effect-agent/subagent-host";
import { IdempotencyKey, Principal } from "effect-agent/submission-ledger";
import { WorkerHostAuthorizer } from "effect-agent/worker-host";
import { LanguageModel, Model, Toolkit, type Response } from "effect/unstable/ai";

const principal = Schema.decodeSync(Principal)("automatic-owner");
const threadId = Schema.decodeSync(ThreadId)("automatic-parent");
const key = Schema.decodeSync(IdempotencyKey);
const input = Schema.Struct({ question: Schema.String });
const output = Schema.Struct({ answer: Schema.String });

const finish = (answer: string): ReadonlyArray<Response.StreamPartEncoded> => [
  { type: "text-start", id: "answer" },
  { type: "text-delta", id: "answer", delta: JSON.stringify({ answer }) },
  { type: "text-end", id: "answer" },
  {
    type: "finish",
    reason: "stop",
    usage: { inputTokens: { total: 5 }, outputTokens: { total: 1 } },
  },
];

const model = (name: string, streamText: Parameters<typeof LanguageModel.make>[0]["streamText"]) =>
  Model.make(
    "scripted",
    name,
    Layer.effect(
      LanguageModel.LanguageModel,
      LanguageModel.make({ generateText: () => Effect.succeed([]), streamText }),
    ),
  );

// Regression: https://github.com/danieljvdm/effect-agent/blob/36a411fc3f468fa87d4dd91234988a716d4ebbc5/packages/effect-agent/src/durable/internal/worker-host.ts#L3058-L3075
it.effect(
  "authorizes successor completion reports as the current source principal",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const directory = yield* fs.makeTempDirectoryScoped({ prefix: "successor-report-" });
        const peer = Schema.decodeSync(Principal)("current-peer");

        const child = Agent.withModel(
          Agent.make("successor-report-child", {
            input,
            output,
            instructions: "Complete assignment",
            toolkit: Toolkit.empty,
            runDisposition: {
              workerLifecycle: "assignment",
              schema: Schema.Literals(["completed", "waiting"]),
              fromOutput: () => "completed" as const,
            },
            policy: { maxTurns: 2, maxToolCalls: 1, maxDuration: "10 seconds" },
          }),
          model("child", () => Stream.fromIterable(finish("done"))),
        );

        const declaration = Subagent.make("research", { target: child.definition });

        const background = Subagent.background(declaration, {
          start: true,
          followUp: true,
          reportToParent: true,
        });

        const parent = Agent.withModel(
          Agent.make("successor-report-parent", {
            input,
            output,
            instructions: "Coordinate",
            toolkit: background.toolkit,
            policy: {
              maxTurns: 10,
              maxToolCalls: 10,
              maxDuration: "30 seconds",
              toolConcurrency: 2,
            },
          }),
          model("parent", () => Stream.fromIterable(finish("acknowledged"))),
        );

        const context = yield* Layer.build(
          NodeHost.NodeDurableHost.layerRegistered(
            [
              {
                agent: parent,
                definitions: DefinitionDigestInput.make({
                  agent: "parent",
                  model: "v1",
                  tools: ["research_start", "research_follow_up"],
                }),
              },
              {
                agent: child,
                definitions: DefinitionDigestInput.make({ agent: "child", model: "v1", tools: [] }),
              },
            ],
            {
              filename: `${directory}/runtime.sqlite`,
              deploymentId: "successor-report",
              producerId: "node",
            },
          ).pipe(
            Layer.provide(background.layer),
            Layer.provide(
              Layer.succeed(WorkerHostAuthorizer)({
                authorize: (request) => {
                  // Worker execution keeps the original human; proven framework delivery keeps its destination principal.
                  return Effect.succeed(
                    request.access === "context" ||
                      request.access === "report" ||
                      request.access === "read"
                      ? request.principal
                      : principal,
                  );
                },
              }),
            ),
          ),
        );

        const runtime = Context.get(context, DurableAgentRuntime);

        const humanInput = yield* runtime.submitRegistered(
          parent,
          { question: "original human task" },
          { threadId, principal, idempotencyKey: key("human") },
        );

        const owner = yield* runtime.workerHost({
          sourceThreadId: threadId,
          sourceSubmissionId: humanInput.submissionId,
          principal,
        });

        const first = yield* Subagent.start(
          declaration,
          { question: "original" },
          { idempotencyKey: key("first") },
        ).pipe(Effect.provideService(SubagentHost, owner));

        yield* runtime.processThreadResolved(first.worker.threadId);

        const peerInput = yield* runtime.submitRegistered(
          parent,
          { question: "current peer correction" },
          { threadId, principal: peer, idempotencyKey: key("peer") },
        );

        const current = yield* runtime.workerHost({
          sourceThreadId: threadId,
          sourceSubmissionId: peerInput.submissionId,
          principal: peer,
        });

        const successor = yield* Subagent.start(
          declaration,
          { question: "apply correction" },
          { idempotencyKey: key("successor"), continuationOf: first.worker },
        ).pipe(Effect.provideService(SubagentHost, current));

        yield* runtime.processThreadResolved(successor.worker.threadId);

        const deliveries = yield* Context.get(context, MessageDeliveryStore).list({
          ownerThreadId: successor.worker.threadId,
          limit: 10,
        });

        expect(deliveries.items).toHaveLength(1);
        const report = deliveries.items[0]?.envelope;

        if (report === undefined || report.messageAdmission === undefined)
          return yield* Effect.die("Missing frozen completion report");
        expect(report.deliveryPrincipal).toBe(peer);
        expect(report.input).toEqual({ question: "current peer correction" });

        expect(
          yield* runtime
            .submitRegistered(
              parent,
              { question: "forged correction" },
              {
                threadId,
                principal: peer,
                idempotencyKey: report.admissionKey,
                messageAdmission: report.messageAdmission,
              },
            )
            .pipe(Effect.flip),
        ).toMatchObject({ _tag: "AdmissionPolicyError", reason: "refused" });

        const receipt = yield* runtime.submitRegistered(
          parent,
          { question: "current peer correction" },
          {
            threadId,
            principal: peer,
            idempotencyKey: report.admissionKey,
            messageAdmission: report.messageAdmission,
          },
        );

        expect(receipt.threadId).toBe(threadId);
      }),
    ).pipe(Effect.provide(NodeFileSystem.layer)),
  15_000,
);
