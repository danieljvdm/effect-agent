import * as Subagent from "@effect-agent/capabilities/Subagent";
import * as Agent from "@effect-agent/core/Agent";
import { ThreadId } from "@effect-agent/core/Identifiers";
import { WorkerCompletion, WorkerError } from "@effect-agent/core/Worker";
import { SubagentHost } from "@effect-agent/engine/SubagentHost";
import * as NodeHost from "@effect-agent/platform-node/NodeDurableHost";
import { DurableAgentRuntime } from "@effect-agent/thread/DurableAgentRuntime";
import { DurableRuntimeFailpointError } from "@effect-agent/thread/DurableFailpoint";
import { MessageDeliveryStore } from "@effect-agent/thread/MessageDelivery";
import { DefinitionDigestInput } from "@effect-agent/thread/Records";
import { IdempotencyKey, Principal } from "@effect-agent/thread/SubmissionLedger";
import { ThreadExportRequest, ThreadStore } from "@effect-agent/thread/ThreadStore";
import { WorkerHostAuthorizer } from "@effect-agent/thread/WorkerHost";
import { OpenAiClient, OpenAiLanguageModel, OpenAiTool } from "@effect/ai-openai";
import { NodeFileSystem } from "@effect/platform-node";
import { expect, it } from "@effect/vitest";
import {
  Context,
  Effect,
  Exit,
  FileSystem,
  Layer,
  Redacted,
  Ref,
  Schema,
  Scope,
  Stream,
} from "effect";
import { LanguageModel, Model, Tool, Toolkit, type Response } from "effect/unstable/ai";
import { HttpClient, HttpClientResponse } from "effect/unstable/http";

const principal = Schema.decodeSync(Principal)("report-owner");
const sourceThreadId = Schema.decodeSync(ThreadId)("report-source");
const key = Schema.decodeSync(IdempotencyKey);
const definitions = DefinitionDigestInput.make({ agent: "report-v1", model: "v1", tools: [] });
const input = Schema.Struct({ question: Schema.String });
const output = Schema.Struct({ answer: Schema.String });

const parts: ReadonlyArray<Response.StreamPartEncoded> = [
  { type: "text-start", id: "reply" },
  { type: "text-delta", id: "reply", delta: '{"answer":"done"}' },
  { type: "text-end", id: "reply" },
  { type: "finish", reason: "stop", usage: { inputTokens: {}, outputTokens: {} } },
];

const agent = <Tools extends Record<string, Tool.Any>>(
  id: string,
  toolkit: Toolkit.Toolkit<Tools>,
) =>
  Agent.withModel(
    Agent.make(id, {
      input,
      output,
      instructions: ({ question }) => `Answer as JSON for ${question}.`,
      inputPrompt: ({ question }) => `Application input: ${question}`,
      toolkit,
      policy: { maxTurns: 20, maxToolCalls: 20, maxDuration: "1 minute", toolConcurrency: 2 },
    }),
    Model.make(
      "scripted",
      id,
      Layer.effect(
        LanguageModel.LanguageModel,
        LanguageModel.make({
          generateText: () => Effect.succeed([]),
          streamText: () => Stream.fromIterable(parts),
        }),
      ),
    ),
  );

// Keep the real OpenAI encoder and SSE decoder; only the HTTP transport is synthetic.
const hostedChild = () => {
  let requests = 0;
  let completions = 0;

  const tools = Toolkit.make(
    OpenAiTool.WebSearch({}),
    Tool.make("finish_research", { parameters: output, success: output }),
  );

  const http = HttpClient.make((request) =>
    Effect.sync(() => {
      requests++;

      const search = {
        type: "web_search_call",
        id: "search-1",
        status: "completed",
        action: { type: "search", query: "research", sources: [] },
      };

      const completion = {
        type: "function_call",
        id: "function-1",
        call_id: "finish-1",
        name: "finish_research",
        arguments: JSON.stringify({ answer: "done" }),
        status: "completed",
      };

      const response = { id: "response-1", model: "gpt-5.6-sol", created_at: 1, output: [] };

      const events = [
        { type: "response.created", response },
        {
          type: "response.output_item.added",
          output_index: 0,
          item: { ...search, status: "in_progress" },
        },
        { type: "response.output_item.done", output_index: 0, item: search },
        { type: "response.output_item.added", output_index: 1, item: completion },
        {
          type: "response.function_call_arguments.done",
          output_index: 1,
          item_id: completion.id,
          arguments: completion.arguments,
        },
        { type: "response.output_item.done", output_index: 1, item: completion },
        {
          type: "response.completed",
          response: {
            ...response,
            output: [search, completion],
            usage: {
              input_tokens: 10,
              output_tokens: 5,
              total_tokens: 15,
              input_tokens_details: { cached_tokens: 0 },
              output_tokens_details: { reasoning_tokens: 0 },
            },
          },
        },
      ];

      return HttpClientResponse.fromWeb(
        request,
        new globalThis.Response(
          events
            .map(
              (event, sequence_number) =>
                `data: ${JSON.stringify({ ...event, sequence_number })}\n\n`,
            )
            .join(""),
          { headers: { "content-type": "text/event-stream" } },
        ),
      );
    }),
  );

  const model = OpenAiLanguageModel.model("gpt-5.6-sol", { store: false }).pipe(
    Layer.provide(
      OpenAiClient.layer({ apiKey: Redacted.make("fixture-key") }).pipe(
        Layer.provide(Layer.succeed(HttpClient.HttpClient, http)),
      ),
    ),
  );

  return {
    agent: Agent.withModel(
      Agent.make("report-child-agent", {
        input,
        output,
        instructions: "Research, then finish.",
        toolkit: tools,
        completion: { tool: "finish_research", required: true, project: ({ result }) => result },
        policy: { maxTurns: 2, maxToolCalls: 2, maxDuration: "1 minute" },
      }),
      model,
    ),
    handlers: tools.toLayer({
      finish_research: (result) =>
        Effect.sync(() => {
          completions++;

          return result;
        }),
    }),
    requests: () => requests,
    completions: () => completions,
  };
};

for (const mode of ["custom", "mapped", "standard"] as const)
  it.live.each([
    "turn:after-response-append",
    "turn:after-results-append",
    "worker:after-report-append",
  ] as const)(
    `${mode}: recovers hosted search completion and one report for joined child inputs after %s and a Node restart`,
    (failpoint) =>
      Effect.scoped(
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          const directory = yield* fs.makeTempDirectoryScoped({ prefix: "worker-report-" });
          const native = hostedChild();
          const child = native.agent;

          const projected = yield* Ref.make(0);

          const declaration = Subagent.make("research", {
            target: child.definition,
            success: output,
            projectResult: (value) =>
              Ref.update(projected, (count) => count + 1).pipe(Effect.as(value)),
            policy: Subagent.SubagentPolicy.make({
              maxChildren: 8,
              maxConcurrency: 2,
              maxTurns: 2,
              maxToolCalls: 2,
              maxDuration: "2 seconds",
            }),
          });

          const reporting = Subagent.reporting(declaration, {
            input,
            prepare: (report) =>
              Effect.succeed({
                question:
                  report.outcome === "completed"
                    ? `report:${report.runId}:${report.result.answer}`
                    : "report:failed",
              }),
          });

          const background = Subagent.background(declaration, {
            start: true,
            followUp: true,
            ...(mode === "standard"
              ? { reportToParent: true }
              : mode === "mapped"
                ? { reportToParent: reporting }
                : {}),
          });

          const source = agent("report-source-agent", background.toolkit);

          const registrations = [
            {
              agent: source,
              definitions,
              ...(mode === "custom" ? { reporting: [reporting] } : {}),
            },
            { agent: child, definitions },
          ];

          const authority = Layer.succeed(WorkerHostAuthorizer)({
            authorize: (request) =>
              request.principal === principal
                ? Effect.succeed(principal)
                : WorkerError.make({ operation: request.operation, reason: "denied" }),
          });

          const options = {
            filename: `${directory}/runtime.sqlite`,
            deploymentId: "reports-v1",
            producerId: "report-node",
            workerConcurrency: 1,
            wakeScanInterval: 10,
            settlementPollInterval: 10,
          };

          const firstScope = yield* Scope.make();

          yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void));

          const first = yield* Layer.build(
            NodeHost.NodeDurableHost.layerRegistered(registrations, {
              ...options,
              runtimeFailpoint: (location) =>
                location === failpoint
                  ? DurableRuntimeFailpointError.make({ location })
                  : Effect.void,
            }).pipe(
              Layer.provide(authority),
              Layer.provide(Layer.merge(native.handlers, background.layer)),
            ),
          ).pipe(Scope.provide(firstScope));

          const runtime = Context.get(first, DurableAgentRuntime);

          const sourceReceipt = yield* runtime.submitRegistered(
            source,
            { question: "launch complete" },
            {
              threadId: sourceThreadId,
              principal,
              idempotencyKey: key("source"),
            },
          );

          yield* runtime.processThreadResolved(sourceThreadId);
          const sourceSettlement = yield* runtime.awaitSettlement(sourceReceipt);

          expect(sourceSettlement.outcome).toBe("completed");

          const host = yield* runtime.workerHost({
            sourceThreadId,
            principal,
            sourceSubmissionId: sourceReceipt.submissionId,
          });

          const started = yield* Subagent.start(
            declaration,
            { question: "host input" },
            { idempotencyKey: key("first") },
          ).pipe(Effect.provideService(SubagentHost, host));

          const joined = yield* Subagent.followUp(
            declaration,
            started.worker,
            { question: "joined input" },
            { idempotencyKey: key("joined") },
          ).pipe(Effect.provideService(SubagentHost, host));

          const interrupted = yield* runtime
            .processThreadResolved(started.worker.threadId)
            .pipe(Effect.result);

          expect(interrupted).toMatchObject({
            _tag: "Failure",
            failure:
              failpoint === "worker:after-report-append"
                ? { _tag: "LedgerError", operation: "worker-completion" }
                : { _tag: "DurableRuntimeFailpointError", location: failpoint },
          });

          const firstLog = yield* Context.get(first, ThreadStore).export(
            ThreadExportRequest.make({ threadId: started.worker.threadId }),
          );

          const decisions = firstLog.records.flatMap(({ record }) =>
            record.payload._tag === "WorkerReportPrepared" ? [record.payload] : [],
          );

          expect(
            firstLog.records.flatMap(({ record }) =>
              record.payload._tag === "WorkerReportRefused" ? [record.payload] : [],
            ),
          ).toEqual([]);
          expect(decisions).toHaveLength(failpoint === "worker:after-report-append" ? 1 : 0);
          expect(yield* Ref.get(projected)).toBe(
            failpoint === "worker:after-report-append" ? 1 : 0,
          );
          expect(native.requests()).toBe(1);
          expect(native.completions()).toBe(failpoint === "turn:after-response-append" ? 0 : 1);
          expect(
            (yield* Context.get(first, MessageDeliveryStore).list({
              ownerThreadId: started.worker.threadId,
              limit: 100,
            })).items,
          ).toHaveLength(0);
          yield* Scope.close(firstScope, Exit.void);

          // No live source Run or retained wake fiber survives this complete host restart.
          const second = yield* Layer.build(
            NodeHost.layer(registrations, options).pipe(
              Layer.provide(authority),
              Layer.provide(Layer.merge(native.handlers, background.layer)),
            ),
          );

          const reopened = Context.get(second, DurableAgentRuntime);
          const store = Context.get(second, ThreadStore);
          const deliveries = Context.get(second, MessageDeliveryStore);

          yield* Effect.gen(function* () {
            for (;;) {
              const rows = yield* deliveries.list({
                ownerThreadId: started.worker.threadId,
                limit: 100,
              });

              if (rows.items.length === 1 && rows.items[0]?.status === "processed") return;
              yield* Effect.sleep("10 millis");
            }
          }).pipe(Effect.timeout("10 seconds"));
          const firstResult = yield* reopened.awaitSettlement(started.receipt);
          const joinedResult = yield* reopened.awaitSettlement(joined);

          expect(firstResult.outcome).toBe("completed");
          expect(joinedResult.outcome).toBe("completed");

          const childLog = yield* store.export(
            ThreadExportRequest.make({ threadId: started.worker.threadId }),
          );

          const childSettlements = childLog.records.flatMap(({ record }) =>
            record.payload._tag === "SubmissionSettled" ? [record.payload] : [],
          );

          const reports = childLog.records.flatMap(({ record }) =>
            record.payload._tag === "WorkerReportPrepared" ? [record.payload] : [],
          );

          const sourceLog = yield* store.export(
            ThreadExportRequest.make({ threadId: sourceThreadId }),
          );

          const inputs = sourceLog.records.flatMap(({ record }) =>
            record.payload._tag === "UserInputRecorded" ? [record.payload] : [],
          );

          expect(inputs).toHaveLength(2);
          if (mode !== "standard") {
            expect(inputs[1]?.input).toEqual({ question: `report:${reports[0]?.runId}:done` });
          } else {
            expect(inputs[1]?.input).toEqual({ question: "launch complete" });

            const message = yield* Schema.decodeUnknownEffect(WorkerCompletion)(
              inputs[1]?.messageAdmission,
            );

            expect(message).toMatchObject({
              _tag: "WorkerCompletion",
              budgetExhausted: false,
              report: {
                worker: started.worker,
                receipt: started.receipt,
                runId: reports[0]?.runId,
                outcome: "completed",
                result: { answer: "done" },
              },
            });
            expect(JSON.stringify(sourceLog)).toContain("WorkerCompletion");
          }
          expect(inputs[1]?.runId).not.toBe(inputs[0]?.runId);

          const settlements = sourceLog.records.flatMap(({ record }) =>
            record.payload._tag === "SubmissionSettled" ? [record.payload] : [],
          );

          expect(settlements).toHaveLength(2);
          expect(settlements.every((settlement) => settlement.outcome === "completed")).toBe(true);

          expect(reports).toHaveLength(1);
          expect(childSettlements).toHaveLength(2);
          expect(childSettlements.map((settlement) => settlement.runId)).toEqual([
            reports[0]?.runId,
            reports[0]?.runId,
          ]);
          if (decisions.length > 0) expect(reports).toEqual(decisions);
          expect(native.requests()).toBe(1);
          expect(native.completions()).toBe(1);
          expect(
            childLog.records.filter(({ record }) => record.payload._tag === "RunCompleted"),
          ).toHaveLength(1);
          expect(
            childLog.records.filter(
              ({ record }) => record.payload._tag === "ModelResponseRecorded",
            ),
          ).toHaveLength(1);
          expect(JSON.stringify(childLog)).toContain("OpenAiWebSearch");
          expect(yield* Ref.get(projected)).toBe(1);
          expect(
            (yield* deliveries.list({ ownerThreadId: started.worker.threadId, limit: 100 })).items,
          ).toHaveLength(1);
        }),
      ).pipe(Effect.provide(NodeFileSystem.layer)),
    15_000,
  );
