import * as Subagent from "@effect-agent/capabilities/Subagent";
import { SubagentReservationsMemoryLive } from "@effect-agent/capabilities/SubagentReservations";
import * as Agent from "@effect-agent/core/Agent";
import { AgentPolicy } from "@effect-agent/core/AgentPolicy";
import type { ThreadId, SubmissionId } from "@effect-agent/core/Identifiers";
import { IdGenerator } from "@effect-agent/core/IdGenerator";
import { MessagingError } from "@effect-agent/core/Messaging";
import { SubagentGrant } from "@effect-agent/core/SubagentContract";
import { WorkerError } from "@effect-agent/core/Worker";
import { DurableWorkerBinding } from "@effect-agent/thread/AgentRegistration";
import { PeerAuthorizer, PeerRoutes } from "@effect-agent/thread/MessagingHost";
import {
  WorkerBudgetAuthorizer,
  WorkerConcurrencyResolver,
  WorkerHostAuthorizer,
  WorkerHostConfig,
  WorkerPolicyResolver,
} from "@effect-agent/thread/WorkerHost";
import { Duration, Effect, Layer, Option, Schema, Stream } from "effect";
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

const standardReports = Subagent.background(backgroundReportingWorkers, {
  start: true,
  followUp: true,
  reportToParent: true,
});

export const backgroundStandardReportSource = Agent.make("cf-background-standard-report-source", {
  input: backgroundReportSource.input,
  output: backgroundReportSource.output,
  instructions: ({ question }) => `Answer ${question} and consume WorkerCompletion messages.`,
  inputPrompt: ({ question }) => question,
  toolkit: standardReports.toolkit,
  policy: backgroundReportSource.policy,
});

export const independentBudgetSource = Agent.make("cf-independent-source", {
  input: backgroundSource.input,
  output: backgroundSource.output,
  instructions: "Answer as JSON.",
  toolkit: Toolkit.empty,
  policy: { maxTurns: 1, maxToolCalls: 1, maxDuration: "1 second", toolConcurrency: 1 },
});

const capturedInput = Schema.Struct({
  question: Schema.String,
  policy: Schema.toCodecJson(AgentPolicy),
});

export const capturedPolicySource = Agent.make("cf-captured-source", {
  input: capturedInput,
  output: backgroundSource.output,
  instructions: "Answer as JSON.",
  toolkit: Toolkit.empty,
  policy: { maxTurns: 1, maxToolCalls: 1, maxDuration: "1 second", toolConcurrency: 1 },
});

const independentScout = Agent.make("cf-independent-scout", {
  input: backgroundTarget.input,
  output: backgroundTarget.output,
  instructions: "Answer as JSON.",
  toolkit: Toolkit.empty,
  policy: {
    maxTurns: 1,
    maxToolCalls: 1,
    maxDuration: "10 seconds",
    toolConcurrency: 1,
    toolResultBounds: { maxBytes: 512 },
  },
});

const independentScoutDeclaration = Subagent.make("budget_scout", {
  target: independentScout,
  grant: SubagentGrant.make({ allowedToolNames: [], maxDepth: 2, childLifetimes: ["attached"] }),
  policy: Subagent.SubagentPolicy.make({
    maxChildren: 1,
    maxConcurrency: 1,
    maxTurns: 1,
    maxToolCalls: 1,
    maxDuration: "10 seconds",
    maxInputTokens: 20,
    maxOutputTokens: 30,
    maxCostMicrousd: 2,
    maxResultBytes: 512,
  }),
});

const independentPersona = Agent.make("cf-independent-persona", {
  input: Schema.Struct({
    question: Schema.String,
    policy: Schema.optionalKey(Schema.toCodecJson(AgentPolicy)),
  }),
  output: backgroundTarget.output,
  instructions: "Attach a scout once per task, then answer as JSON.",
  toolkit: Toolkit.make(independentScoutDeclaration.tool),
  policy: {
    maxTurns: 2,
    maxToolCalls: 1,
    maxDuration: "20 seconds",
    toolConcurrency: 1,
    toolResultBounds: { maxBytes: 1024 },
  },
});

export const independentBudgetWorkers = Subagent.make("independent_persona", {
  target: independentPersona,
  grant: SubagentGrant.make({
    allowedToolNames: ["budget_scout"],
    maxDepth: 2,
    childLifetimes: ["attached"],
  }),
  policy: Subagent.SubagentPolicy.make({
    maxChildren: 1,
    maxConcurrency: 1,
    descendantInvocations: 1,
    maxTurns: 3,
    maxToolCalls: 2,
    maxDuration: "40 seconds",
    maxResultBytes: 2048,
  }),
});

/** Construct author-owned reservations without constructing another registered Definition. */
export const capturedPolicyWorkers = (policy: AgentPolicy) =>
  Subagent.make("independent_persona", {
    target: independentPersona,
    grant: independentBudgetWorkers.grant,
    policy: Subagent.SubagentPolicy.make({
      maxChildren: 1,
      maxConcurrency: 1,
      descendantInvocations: 1,
      maxTurns: policy.maxTurns + 1,
      maxToolCalls: policy.maxToolCalls + 1,
      maxDuration: Duration.millis(Duration.toMillis(policy.maxDuration) + 20_000),
      maxResultBytes: policy.toolResultBounds.maxBytes + 1024,
    }),
  });

export const capturedPolicyOutages = new Set<string>();

export const capturedConcurrency = new Map<
  string,
  { readonly owner: SubmissionId; readonly limit: number }
>();

export const privateProgressRoutes = new Map<string, ThreadId>();
export const customRuntimeThreads = new Set<string>();

export const independentBudgetGrants = new Set<string>();
/** A source authorization succeeds before the destination's next admission loses its dependency. */
export const independentBudgetAdmissionOutages = new Set<string>();
export const independentBudgetAuthorityCalls = new Map<string, number>();
export const independentBudgetGates = new Set<string>();

const independentPersonaModel = Model.make(
  "scripted",
  "cf-independent-persona",
  Layer.effect(
    LanguageModel.LanguageModel,
    LanguageModel.make({
      generateText: () => Effect.succeed([]),
      streamText: ({ prompt }) => {
        const index = prompt.content.findLastIndex(
          (message) => message.role === "user" && JSON.stringify(message).includes(":task:"),
        );

        const task =
          /background-cf-independent-[a-z0-9-]+:task:[0-9]+/u.exec(
            JSON.stringify(prompt.content[index]),
          )?.[0] ?? "missing";

        const finished = prompt.content.slice(index + 1).some((message) => message.role === "tool");

        if (finished) return Stream.fromIterable(finalParts('{"answer":"done"}'));

        const wait = Effect.gen(function* () {
          while (!independentBudgetGates.has(task)) yield* Effect.sleep("10 millis");
        });

        const parts: ReadonlyArray<Response.StreamPartEncoded> = [
          {
            type: "tool-call",
            id: "scout-once",
            name: "budget_scout",
            params: { question: task },
            providerExecuted: false,
          },
          { type: "finish", reason: "tool-calls", usage: { inputTokens: {}, outputTokens: {} } },
        ];

        return Stream.fromEffectDrain(wait).pipe(Stream.concat(Stream.fromIterable(parts)));
      },
    }),
  ),
);

const independentScoutHandlers = Subagent.SubagentRuntime.layer(
  independentScoutDeclaration,
  Agent.withModel(independentScout, model),
).pipe(Layer.provide([SubagentReservationsMemoryLive, IdGenerator.layer]));

export const backgroundWorkerBindings = Effect.all([
  DurableWorkerBinding.make(
    Agent.withModel(backgroundStandardReportSource, model),
    TEST_DIGESTS,
  ).pipe(Effect.provide(standardReports.layer)),
  DurableWorkerBinding.make(Agent.withModel(capturedPolicySource, model), TEST_DIGESTS),
  DurableWorkerBinding.make(Agent.withModel(independentBudgetSource, model), TEST_DIGESTS),
  DurableWorkerBinding.make(Agent.withModel(independentScout, model), TEST_DIGESTS),
  DurableWorkerBinding.make(
    Agent.withModel(independentPersona, independentPersonaModel),
    TEST_DIGESTS,
  ).pipe(Effect.provide(independentScoutHandlers)),
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

export const backgroundWorkerAuthority = Layer.mergeAll(
  Layer.succeed(PeerRoutes)({
    resolve: (request) => {
      const destination = privateProgressRoutes.get(request.source.threadId);

      return destination === undefined
        ? MessagingError.make({ operation: "send", reason: "route-unavailable" })
        : Effect.succeed(destination);
    },
  }),
  Layer.succeed(PeerAuthorizer)({
    authorize: (request) =>
      request.principal === TEST_PRINCIPAL && privateProgressRoutes.has(request.source.threadId)
        ? Effect.succeed(TEST_PRINCIPAL)
        : MessagingError.make({ operation: request.operation, reason: "denied" }),
  }),
  Layer.succeed(WorkerConcurrencyResolver)({
    resolve: (request) => {
      const capture = capturedConcurrency.get(request.source.threadId);

      if (capture === undefined) return Effect.succeed(Option.none());

      return request.sourceSubmission?.submissionId === capture.owner
        ? Effect.succeed(Option.some({ maxActiveWorkersPerSource: capture.limit }))
        : WorkerError.make({ operation: "start", reason: "denied" });
    },
  }),
  Layer.succeed(WorkerPolicyResolver)({
    resolveSource: (request) =>
      Effect.gen(function* () {
        if (request.definition !== capturedPolicySource) return Option.none();
        if (request.submission === undefined)
          return yield* WorkerError.make({ operation: "start", reason: "unavailable" });

        const input = yield* Schema.decodeUnknownEffect(capturedInput)(
          request.submission.inputPayload,
        ).pipe(Effect.mapError(() => WorkerError.make({ operation: "start", reason: "denied" })));

        return Option.some(input.policy);
      }),
    resolveTarget: (request) =>
      Effect.gen(function* () {
        if (request.source.agentId !== capturedPolicySource.id) return Option.none();
        if (request.definition !== independentPersona)
          return yield* WorkerError.make({ operation: "start", reason: "denied" });
        if (capturedPolicyOutages.has(request.source.threadId))
          return yield* WorkerError.make({ operation: "start", reason: "unavailable" });
        if (request._tag === "RetainedWorker") return Option.some(request.origin.policy);
        if (request.sourceSubmission === undefined)
          return yield* WorkerError.make({ operation: "start", reason: "unavailable" });

        const input = yield* Schema.decodeUnknownEffect(capturedInput)(request.input).pipe(
          Effect.mapError(() => WorkerError.make({ operation: "start", reason: "denied" })),
        );

        return Option.some(input.policy);
      }),
  }),
  Layer.succeed(WorkerHostConfig)({
    maxWorkersPerSource: 32,
    maxActiveWorkersPerSource: 2,
    maxInputsPerWorker: 64,
    maxPendingInputsPerWorker: 8,
    lifetimeMillis: 86_400_000,
  }),
  Layer.succeed(WorkerBudgetAuthorizer)({
    authorize: (request) =>
      Effect.gen(function* () {
        const source = request.source.threadId;
        const calls = (independentBudgetAuthorityCalls.get(source) ?? 0) + 1;

        independentBudgetAuthorityCalls.set(source, calls);

        const declared =
          (request.worker.targetAgentId === independentPersona.id &&
            request.worker.delegationId === independentBudgetWorkers.delegationId) ||
          (request.worker.targetAgentId === backgroundTarget.id &&
            request.worker.delegationId === backgroundWorkers.delegationId);

        if (
          request.principal !== TEST_PRINCIPAL ||
          !independentBudgetGrants.has(source) ||
          !declared
        )
          return yield* WorkerError.make({ operation: "start", reason: "denied" });
        if (independentBudgetAdmissionOutages.has(source) && calls > 1)
          return yield* WorkerError.make({ operation: "start", reason: "unavailable" });
      }),
  }),
  Layer.succeed(WorkerHostAuthorizer)({
    authorize: (request) =>
      request.principal === TEST_PRINCIPAL && request.sourceThreadId.startsWith("background-cf-")
        ? Effect.succeed(TEST_PRINCIPAL)
        : WorkerError.make({ operation: request.operation, reason: "denied" }),
  }),
);

/** Explicitly suppress worker wake hints while the recovery test drives stored alarms. */
export const backgroundWakeDropPrefixes = new Set<string>();
/** External projection counter survives Object eviction; it is not runtime state. */
export const backgroundReportProjections = new Map<string, number>();
