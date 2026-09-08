import * as Subagent from "@effect-agent/capabilities/Subagent";
import * as Agent from "@effect-agent/core/Agent";
import { WorkerError } from "@effect-agent/core/Worker";
import { DurableWorkerBinding } from "@effect-agent/thread/AgentRegistration";
import { WorkerHostAuthorizer } from "@effect-agent/thread/WorkerHost";
import { Effect, Layer, Schema, Stream } from "effect";
import { LanguageModel, Model, Tool, Toolkit, type Response } from "effect/unstable/ai";

import { TEST_DIGESTS, TEST_PRINCIPAL, finalParts } from "./fixtures.ts";

const definition = (name: string) =>
  Agent.make(name, {
    input: Schema.Struct({ question: Schema.String }),
    output: Schema.Struct({ answer: Schema.String }),
    instructions: "Answer as JSON.",
    toolkit: Toolkit.empty,
    policy: { maxTurns: 20, maxToolCalls: 20, maxDuration: "1 minute", toolConcurrency: 2 },
  });

export const backgroundSource = definition("cf-background-source");
export const backgroundReportSource = definition("cf-background-report-source");
export const backgroundTarget = definition("cf-background-target");

const model = Model.make(
  "scripted",
  "cf-background",
  Layer.effect(
    LanguageModel.LanguageModel,
    LanguageModel.make({
      generateText: () => Effect.succeed([]),
      streamText: () => Stream.fromIterable(finalParts('{"answer":"done"}')),
    }),
  ),
);

export const backgroundWorkers = Subagent.make("research", {
  target: backgroundTarget,
  success: backgroundTarget.output,
  projectResult: (output) => Effect.succeed(output),
  policy: Subagent.SubagentPolicy.make({
    maxChildren: 8,
    maxConcurrency: 2,
    maxTurns: 1,
    maxToolCalls: 1,
    maxDuration: "1 second",
  }),
});

const reportCheckpoint = Tool.make("report_checkpoint", {
  parameters: Schema.Struct({}),
  success: Schema.String,
});

const reportToolkit = Toolkit.make(reportCheckpoint);

const reportTarget = Agent.make("cf-report-target", {
  input: backgroundTarget.input,
  output: backgroundTarget.output,
  instructions: "Wait, accept any queued input, then answer as JSON.",
  toolkit: reportToolkit,
  policy: backgroundTarget.policy,
});

export const backgroundReportGates = new Set<string>();

const reportModel = Model.make(
  "scripted",
  "cf-report",
  Layer.effect(
    LanguageModel.LanguageModel,
    LanguageModel.make({
      generateText: () => Effect.succeed([]),
      streamText: ({ prompt }) => {
        const text = JSON.stringify(prompt);

        if (text.includes("report_checkpoint"))
          return Stream.fromIterable(finalParts('{"answer":"done"}'));
        const ref = /background-cf-report-[a-z0-9-]+/u.exec(text)?.[0] ?? "";

        const wait = Effect.gen(function* () {
          while (!backgroundReportGates.has(ref)) yield* Effect.sleep("10 millis");
        });

        const parts: ReadonlyArray<Response.StreamPartEncoded> = [
          {
            type: "tool-call",
            id: "checkpoint",
            name: "report_checkpoint",
            params: {},
            providerExecuted: false,
          },
          { type: "finish", reason: "tool-calls", usage: { inputTokens: {}, outputTokens: {} } },
        ];

        return Stream.fromEffectDrain(wait).pipe(Stream.concat(Stream.fromIterable(parts)));
      },
    }),
  ),
);

export const backgroundReportingWorkers = Subagent.make("reported_research", {
  target: reportTarget,
  success: reportTarget.output,
  projectResult: (value) => Effect.succeed(value),
  policy: Subagent.SubagentPolicy.make({
    maxChildren: 4,
    maxConcurrency: 2,
    maxTurns: 2,
    maxToolCalls: 1,
    maxDuration: "10 seconds",
  }),
});

export const backgroundWorkerBindings = Effect.all([
  DurableWorkerBinding.make(Agent.withModel(backgroundSource, model), TEST_DIGESTS),
  DurableWorkerBinding.make(Agent.withModel(backgroundTarget, model), TEST_DIGESTS),
  DurableWorkerBinding.make(Agent.withModel(backgroundReportSource, model), TEST_DIGESTS, [
    Subagent.reporting(backgroundReportingWorkers, {
      input: backgroundReportSource.input,
      prepare: (report) =>
        Effect.sync(() => {
          backgroundReportProjections.set(
            report.runId,
            (backgroundReportProjections.get(report.runId) ?? 0) + 1,
          );

          return {
            question:
              report.outcome === "completed"
                ? `report:${report.runId}:${report.result.answer}`
                : "report:failed",
          };
        }),
    }),
  ]),
  DurableWorkerBinding.make(Agent.withModel(reportTarget, reportModel), TEST_DIGESTS).pipe(
    Effect.provide(reportToolkit.toLayer({ report_checkpoint: () => Effect.succeed("ready") })),
  ),
]);

export const backgroundWorkerAuthority = Layer.succeed(WorkerHostAuthorizer)({
  authorize: (request) =>
    request.principal === TEST_PRINCIPAL && request.sourceThreadId.startsWith("background-cf-")
      ? Effect.succeed(TEST_PRINCIPAL)
      : WorkerError.make({ operation: request.operation, reason: "denied" }),
});

/** Explicitly suppress worker wake hints while the recovery test drives stored alarms. */
export const backgroundWakeDropPrefixes = new Set<string>();
/** External projection counter survives Object eviction; it is not runtime state. */
export const backgroundReportProjections = new Map<string, number>();
