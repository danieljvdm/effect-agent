import * as ContextTools from "@effect-agent/capabilities/ContextTools";
import * as MemoryNotes from "@effect-agent/capabilities/MemoryNotes";
import * as Agent from "@effect-agent/core/Agent";
import { AgentPolicy } from "@effect-agent/core/AgentPolicy";
import { ThreadId } from "@effect-agent/core/Identifiers";
import * as MemoryNamespace from "@effect-agent/core/MemoryNamespace";
import { MemoryKey, MemoryReader } from "@effect-agent/core/MemoryStore";
import { contextWindowId } from "@effect-agent/engine/Compaction";
import { ContextCompactor } from "@effect-agent/engine/ContextCompactor";
import { ContextHistoryPage } from "@effect-agent/engine/ContextHistory";
import { NodeDurableAgentRuntime } from "@effect-agent/platform-node/NodeDurableAgentRuntime";
import { digestDefinitions, digestDefinition } from "@effect-agent/thread/Digest";
import { DurableAgentRuntime } from "@effect-agent/thread/DurableAgentRuntime";
import { DurableRuntimeFailpointError } from "@effect-agent/thread/DurableFailpoint";
import {
  CanonicalRecordEnvelope,
  CanonicalSequence,
  DefinitionDigestInput,
} from "@effect-agent/thread/Records";
import { runIdForSubmission } from "@effect-agent/thread/RunJournal";
import { memoryStoreLayer } from "@effect-agent/thread/SqlMemoryStore";
import { IdempotencyKey, Principal } from "@effect-agent/thread/SubmissionLedger";
import * as ThreadContextHistory from "@effect-agent/thread/ThreadContextHistory";
import { project } from "@effect-agent/thread/ThreadContextHistoryProjection";
import { ThreadRead, ThreadStore, ThreadTailRequest } from "@effect-agent/thread/ThreadStore";
import { OpenAiClient, OpenAiLanguageModel } from "@effect/ai-openai";
import {
  Clock,
  Console,
  DateTime,
  Effect,
  Exit,
  FileSystem,
  Layer,
  Option,
  Path,
  Ref,
  Schema,
  Stream,
} from "effect";
import { IdGenerator, Toolkit } from "effect/unstable/ai";

import {
  check,
  EvaluationError,
  EvaluationReport,
  ProjectStatus,
  type Check,
  type PhaseResult,
  type RestartEvidence,
} from "./contracts.ts";
import { hasSearchPathToRead } from "./evidence.ts";
import {
  makeLiveClient,
  MAX_INPUT_TOKENS,
  MAX_MODEL_CALLS,
  MAX_OUTPUT_TOKENS,
  type ModelId,
  RequestAudit,
} from "./live-model.ts";
import {
  gradeStatus,
  instructions,
  makeScenario,
  REQUIRED_ROLLOVERS,
  RESTARTS,
  SCENARIO_VERSION,
  type ScenarioPhase,
} from "./scenario.ts";

export const CONTEXT_TOKEN_LIMIT = 16_000;

const notesNamespace = MemoryNamespace.define({
  name: "example/context-continuity-notes",
  version: 1,
  identity: Schema.Struct({ threadId: Schema.String }),
});

const toolkit = Toolkit.merge(ContextTools.toolkit, MemoryNotes.toolkit);

export interface EvaluationOptions {
  readonly model: ModelId;
  readonly reasoningEffort: "low" | "medium" | "high";
  readonly seed: number;
  readonly outputDirectory: string;
  readonly sourceCommit: string;
  readonly dirtyWorkingTree: boolean;
  readonly maxCostMicrousd: number;
}

const readLog = Effect.fn("ContextContinuity.readLog")(function* (threadId: ThreadId) {
  const store = yield* ThreadStore;
  const tail = yield* store.inspectTail(ThreadTailRequest.make({ threadId }));
  const records: Array<CanonicalRecordEnvelope> = [];
  let cursor = 0;

  while (cursor < tail.tailSequence) {
    const limit = Math.min(64, tail.tailSequence - cursor);

    const page = yield* store
      .read(
        ThreadRead.make({
          threadId,
          afterSequence: yield* Schema.decodeEffect(CanonicalSequence)(cursor),
          limit,
        }),
      )
      .pipe(Stream.runCollect);

    if (
      page.length !== limit ||
      page.some((record, index) => record.sequence !== cursor + index + 1)
    )
      return yield* EvaluationError.make({
        stage: "evidence",
        message: "Canonical evidence was not contiguous",
      });
    records.push(...page);
    cursor += page.length;
  }

  return records;
});

const readNotes = Effect.fn("ContextContinuity.readNotes")(function* (key: MemoryKey) {
  const reader = yield* MemoryReader;
  const document = yield* reader.get(key);

  if (document?._tag === "WithdrawnMemoryDocument")
    return yield* EvaluationError.make({
      stage: "notes",
      message: "Evaluation notes were unexpectedly withdrawn",
    });

  return { revision: document?.source.revision ?? null, text: document?.content.text ?? "" };
});

/** Independent gate: missing phases, duplicate boundaries, unmetered calls, and partial runs fail. */
export const gateChecks = (report: EvaluationReport): ReadonlyArray<Check> => [
  check(
    "all-user-updates-completed",
    report.phases.map((phase) => phase.index),
    makeScenario(report.seed).map((phase) => phase.index),
  ),
  check("at-least-twelve-native-rollovers", report.windows.length >= REQUIRED_ROLLOVERS, true),
  check(
    "distinct-window-identities",
    new Set(report.windows.map((window) => window.id)).size,
    report.windows.length,
  ),
  check(
    "contiguous-forward-window-coverage",
    report.windows.every(
      (window, index, windows) =>
        window.coversThrough > (windows[index - 1]?.coversThrough ?? 0) &&
        window.coversThrough < window.sequence,
    ),
    true,
  ),
  check(
    "both-recovery-boundaries-exercised",
    report.restarts.map(({ phase, location }) => ({ phase, location })),
    RESTARTS,
  ),
  check(
    "durable-notes-survived-reacquisition",
    report.restarts.every(
      (restart) =>
        restart.notesRevisionBefore !== null &&
        restart.notesRevisionBefore === restart.notesRevisionAfter &&
        restart.notesTextUnchanged,
    ),
    true,
  ),
  check(
    "all-semantic-and-mechanical-checks",
    report.phases.every(
      (phase) => phase.checks.length > 0 && phase.checks.every((item) => item.passed),
    ),
    true,
  ),
  check("all-model-calls-metered", report.usage.completedCalls, report.usage.calls),
  check(
    "native-accounting-survived-recovery",
    report.phases.reduce((total, phase) => total + phase.modelCalls, 0),
    report.usage.completedCalls,
  ),
  check("within-model-call-bound", report.usage.calls <= report.maxModelCalls, true),
  check("model-was-actually-called", report.usage.calls > 0, true),
  check("no-unsettled-provider-reservations", report.usage.reservedCostMicrousd, 0),
  check("bounded-paid-input", report.usage.maxInputTokens <= MAX_INPUT_TOKENS, true),
  check(
    "within-suite-cost-bound",
    report.usage.estimatedCostMicrousd <= report.maxCostMicrousd,
    true,
  ),
  check("no-operational-failure", report.failure, null),
];

export const runEvaluation = Effect.fn("ContextContinuity.runEvaluation")(function* (
  options: EvaluationOptions,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const scenario = makeScenario(options.seed);
  const phaseIndex = yield* Ref.make(0);
  const firstProbeRequests = new Map<number, boolean>();
  const started = yield* Clock.currentTimeMillis;

  const scenarioDigest = yield* digestDefinition({
    version: SCENARIO_VERSION,
    instructions,
    phases: scenario,
  });

  const auditPath = path.join(options.outputDirectory, "requests.ndjson");
  const canonicalPath = path.join(options.outputDirectory, "canonical.ndjson");
  const reportPath = path.join(options.outputDirectory, "report.json");

  const live = yield* makeLiveClient({
    model: options.model,
    maxCostMicrousd: options.maxCostMicrousd,
    phase: phaseIndex,
    audit: Effect.fn("ContextContinuity.audit")(
      function* (event) {
        const probe = scenario[event.phase]?.receipt;

        if (
          event.kind === "request" &&
          probe !== undefined &&
          probe !== null &&
          !firstProbeRequests.has(event.phase)
        )
          firstProbeRequests.set(event.phase, !event.json.includes(probe.code));
        const json = yield* Schema.encodeEffect(Schema.fromJsonString(RequestAudit))(event);

        yield* fs.writeFileString(auditPath, `${json}\n`, { flag: "a" });
      },
      Effect.mapError(() =>
        EvaluationError.make({ stage: "evidence", message: "Could not write request audit" }),
      ),
    ),
  });

  let report: EvaluationReport = {
    version: 2,
    status: "running",
    sourceCommit: options.sourceCommit,
    dirtyWorkingTree: options.dirtyWorkingTree,
    scenarioDigest,
    seed: options.seed,
    provider: "openai",
    model: options.model,
    reasoningEffort: options.reasoningEffort,
    serviceTier: "default",
    pricingVersion: "openai-2026-09-08-conservative",
    contextTokenLimit: CONTEXT_TOKEN_LIMIT,
    maxOutputTokens: MAX_OUTPUT_TOKENS,
    maxCostMicrousd: options.maxCostMicrousd,
    maxModelCalls: MAX_MODEL_CALLS,
    profile: "explicit-rollover-sqlite-v1",
    startedAt: DateTime.formatIso(yield* DateTime.now),
    elapsedMillis: 0,
    phases: [],
    windows: [],
    restarts: [],
    checks: [],
    usage: yield* live.snapshot,
    failure: null,
  };

  const flush = Effect.fn("ContextContinuity.flush")(function* () {
    report = {
      ...report,
      elapsedMillis: (yield* Clock.currentTimeMillis) - started,
      usage: yield* live.snapshot,
    };
    const json = yield* Schema.encodeEffect(Schema.fromJsonString(EvaluationReport))(report);

    yield* fs.writeFileString(reportPath, `${json}\n`);
  });

  yield* flush();
  // Interruption preserves a non-passing partial report. SIGKILL leaves the last running report.
  yield* Effect.addFinalizer(() => flush().pipe(Effect.ignore));

  const threadId = yield* Schema.decodeEffect(ThreadId)(`context-continuity-${options.seed}`);
  const key = MemoryKey.make({ namespace: notesNamespace.make({ threadId }), id: "working-notes" });
  const locator = `memory://context-continuity/${options.seed}/notes`;

  const policy = {
    maxTurns: 16,
    maxToolCalls: 24,
    maxDuration: "8 minutes",
    toolConcurrency: 1,
    contextTokenLimit: CONTEXT_TOKEN_LIMIT,
    onExhaustion: "fail",
  } as const;

  const modelSettings = {
    max_output_tokens: MAX_OUTPUT_TOKENS,
    reasoning: { effort: options.reasoningEffort },
    store: false,
    service_tier: "default",
    strictJsonSchema: true,
  } as const;

  const definition = Agent.make("context-continuity-eval", {
    input: Schema.String,
    output: ProjectStatus,
    instructions,
    toolkit,
    policy: AgentPolicy.make(policy),
  });

  const agent = Agent.withModel(
    definition,
    OpenAiLanguageModel.model(options.model, modelSettings),
  );

  const definitions = yield* digestDefinitions(
    DefinitionDigestInput.make({
      agent: { scenarioDigest, policy, sourceCommit: options.sourceCommit },
      model: { name: options.model, ...modelSettings },
      tools: Object.keys(toolkit.tools).map((name) => ({ name, revision: options.sourceCommit })),
    }),
  );

  let records: ReadonlyArray<CanonicalRecordEnvelope> = [];

  const attempt = Effect.fn("ContextContinuity.attempt")(function* (
    phase: ScenarioPhase,
    restart: (typeof RESTARTS)[number] | undefined,
    previousNotes: { revision: string | null; text: string } | undefined,
  ) {
    const host = NodeDurableAgentRuntime.layer({
      filename: path.join(options.outputDirectory, "thread.sqlite"),
      deploymentId: "context-continuity-eval-v1",
      producerId: `context-eval-${phase.index}-${previousNotes === undefined ? "initial" : "resumed"}`,
      runtimeFailpoint: (location) =>
        location === restart?.location
          ? Effect.fail(DurableRuntimeFailpointError.make({ location }))
          : Effect.void,
    }).pipe(Layer.provide(ContextCompactor.layerRollover));

    const memory = memoryStoreLayer.pipe(Layer.provide(host));

    const handlers = MemoryNotes.layer({
      key,
      locator,
      scopes: [],
      attributions: [
        {
          originId: "context-eval-agent",
          speaker: "Agent",
          observers: [],
          locator,
          activityAt: null,
          interpretation: "private working notes",
        },
      ],
    }).pipe(
      Layer.provide(memory),
      Layer.provide(Layer.succeed(IdGenerator.IdGenerator, IdGenerator.defaultIdGenerator)),
    );

    const services = Layer.mergeAll(
      host,
      memory,
      handlers,
      ContextTools.layer,
      ThreadContextHistory.layer({ maxRecords: 16_384 }).pipe(Layer.provide(host)),
    );

    return yield* Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;
      // Reacquisition reads the same persisted document before any resumed model call.
      const reopenedNotes = yield* readNotes(key);

      const receipt = yield* runtime.submit(agent, phase.message, {
        threadId,
        principal: yield* Schema.decodeEffect(Principal)("context-eval"),
        idempotencyKey: yield* Schema.decodeEffect(IdempotencyKey)(`phase-${phase.index}`),
        definitions,
      });

      const runId = runIdForSubmission(receipt.submissionId);

      if (previousNotes !== undefined) {
        const boundary = RESTARTS.find((item) => item.phase === phase.index);

        if (boundary === undefined)
          return yield* EvaluationError.make({
            stage: "restart",
            message: "Unexpected recovery phase",
          });

        const evidence: RestartEvidence = {
          phase: phase.index,
          location: boundary.location,
          runId,
          notesRevisionBefore: previousNotes.revision,
          notesRevisionAfter: reopenedNotes.revision,
          notesTextUnchanged: previousNotes.text === reopenedNotes.text,
        };

        report = { ...report, restarts: [...report.restarts, evidence] };
        yield* runtime.runRecovery;
      }
      const exit = yield* runtime.processThread(agent, threadId).pipe(Effect.exit);

      records = yield* readLog(threadId);

      const encoded = yield* Effect.forEach(records, (record) =>
        Schema.encodeEffect(Schema.fromJsonString(CanonicalRecordEnvelope))(record),
      );

      yield* fs.writeFileString(canonicalPath, `${encoded.join("\n")}\n`);
      const notes = yield* readNotes(key);

      report = {
        ...report,
        windows: records.flatMap((record) => {
          const payload = record.record.payload;

          return payload._tag === "CompactionCreated" && payload.kind === "rollover"
            ? [
                {
                  id: contextWindowId(payload.runId, payload.turn),
                  recordId: record.record.recordId,
                  sequence: record.sequence,
                  coversThrough: payload.coversThrough,
                },
              ]
            : [];
        }),
      };
      yield* flush();
      if (Exit.isFailure(exit)) {
        if (
          restart !== undefined &&
          Exit.hasFails(exit) &&
          exit.cause.reasons.some(
            (reason) =>
              reason._tag === "Fail" &&
              Schema.is(DurableRuntimeFailpointError)(reason.error) &&
              reason.error.location === restart.location,
          )
        )
          return { kind: "restart", notes, runId } as const;

        return yield* EvaluationError.make({
          stage: "runtime",
          message: "Durable worker failed; inspect canonical evidence",
        });
      }
      const settlement = exit.value.find((value) => value.submissionId === receipt.submissionId);

      const completed = records.flatMap(({ record }) =>
        record.payload._tag === "RunCompleted" && record.payload.runId === runId
          ? [record.payload]
          : [],
      );

      if (
        settlement?.outcome !== "completed" ||
        completed.length !== 1 ||
        completed[0]?.finishReason !== undefined
      )
        return yield* EvaluationError.make({
          stage: "runtime",
          message: `Phase ${phase.index} did not complete normally; inspect canonical evidence`,
        });
      const output = yield* Schema.decodeUnknownEffect(ProjectStatus)(completed[0]?.output);

      return {
        kind: "completed",
        output,
        runId,
        notes,
        modelCalls: settlement.usageSummary?.modelCalls ?? 0,
      } as const;
    }).pipe(Effect.provide(services), Effect.scoped);
  });

  const evaluate = Effect.gen(function* () {
    for (const phase of scenario) {
      yield* Ref.set(phaseIndex, phase.index);
      yield* Console.error(`Context continuity: update ${phase.index + 1}/${scenario.length}`);
      const restart = RESTARTS.find((item) => item.phase === phase.index);
      let result = yield* attempt(phase, restart, undefined);

      if (result.kind === "restart") result = yield* attempt(phase, undefined, result.notes);
      if (result.kind !== "completed")
        return yield* EvaluationError.make({
          stage: "restart",
          message: "Recovered attempt did not finish",
        });

      const runRecords = records.filter(
        ({ record }) => "runId" in record.payload && record.payload.runId === result.runId,
      );

      const boundaries = runRecords.filter(
        ({ record }) => record.payload._tag === "CompactionCreated",
      );

      const windows = boundaries.filter(
        ({ record }) =>
          record.payload._tag === "CompactionCreated" && record.payload.kind === "rollover",
      );

      const lastWindow = windows.at(-1);

      const settledTool = (name: string) =>
        runRecords.filter(
          ({ record }) =>
            record.payload._tag === "ToolCallSettled" &&
            record.payload.toolName === name &&
            !record.payload.isFailure,
        );

      const checks: Array<Check> = [
        ...gradeStatus(phase, result.output),
        check(`phase-${phase.index}/no-summary-or-pruning`, boundaries.length, windows.length),
        check(`phase-${phase.index}/notes-bounded`, result.notes.text.length <= 2_000, true),
        check(`phase-${phase.index}/notes-written`, settledTool("write_notes").length > 0, true),
        check(
          `phase-${phase.index}/single-logical-run`,
          runRecords.filter(({ record }) => record.payload._tag === "RunStarted").length,
          1,
        ),
      ];

      if (phase.index > 0)
        checks.push(
          check(`phase-${phase.index}/native-rollover`, windows.length >= 1, true),
          check(
            `phase-${phase.index}/single-rollover-request`,
            settledTool("new_context").length,
            1,
          ),
          check(
            `phase-${phase.index}/notes-saved-before-rollover`,
            settledTool("write_notes").some(
              (record) => record.sequence < (lastWindow?.sequence ?? 0),
            ),
            true,
          ),
          check(
            `phase-${phase.index}/notes-read-after-rollover`,
            settledTool("read_notes").some(
              (record) => record.sequence > (lastWindow?.sequence ?? Number.MAX_SAFE_INTEGER),
            ),
            true,
          ),
        );
      if (phase.receipt !== null) {
        const answer = result.output.receipts[0];
        const source = records.find((record) => record.record.recordId === answer?.recordId);
        const evidence = source === undefined ? undefined : (yield* project(source)).evidence;

        const searched =
          answer !== undefined &&
          hasSearchPathToRead(
            runRecords,
            lastWindow?.sequence ?? Number.MAX_SAFE_INTEGER,
            answer.recordId,
            phase.receipt.code,
          );

        const read = settledTool("read_context_window").some(({ record, sequence }) => {
          if (
            record.payload._tag !== "ToolCallSettled" ||
            sequence <= (lastWindow?.sequence ?? Number.MAX_SAFE_INTEGER)
          )
            return false;
          const page = Schema.decodeUnknownOption(ContextHistoryPage)(record.payload.result);

          return (
            Option.isSome(page) &&
            page.value.recordId === answer?.recordId &&
            page.value.text.includes(phase.receipt?.code ?? "")
          );
        });

        const closedWindows =
          source === undefined
            ? 0
            : report.windows.filter((window) => window.coversThrough >= source.sequence).length;

        checks.push(
          check(
            `phase-${phase.index}/answer-absent-before-retrieval`,
            firstProbeRequests.get(phase.index),
            true,
          ),
          check(`phase-${phase.index}/search-path-to-original-read`, searched, true),
          check(`phase-${phase.index}/successful-read-after-rollover`, read, true),
          check(
            `phase-${phase.index}/cites-original-evidence`,
            evidence !== undefined &&
              evidence.text.includes(phase.receipt.label) &&
              evidence.text.includes(phase.receipt.code) &&
              source !== undefined &&
              source.sequence < (lastWindow?.sequence ?? 0),
            true,
          ),
          check(
            `phase-${phase.index}/archive-distance`,
            closedWindows >= (phase.index === 12 ? 10 : 4),
            true,
          ),
          check(
            `phase-${phase.index}/receipt-not-copied-into-notes`,
            result.notes.text.includes(phase.receipt.code),
            false,
          ),
        );
      }

      const phaseResult: PhaseResult = {
        index: phase.index,
        runId: result.runId,
        output: result.output,
        modelCalls: result.modelCalls,
        checks,
      };

      report = { ...report, phases: [...report.phases, phaseResult] };
      yield* flush();
    }
  }).pipe(
    Effect.provideService(OpenAiClient.OpenAiClient, live.client),
    Effect.timeout("40 minutes"),
  );

  const exit = yield* evaluate.pipe(Effect.exit);

  if (Exit.isFailure(exit)) {
    const error = Exit.findErrorOption(exit);

    report = {
      ...report,
      failure:
        (yield* live.failure) ??
        (Option.isSome(error) && Schema.is(EvaluationError)(error.value)
          ? `${error.value.stage}: ${error.value.message}`
          : "Evaluation did not finish; inspect canonical records and request usage"),
    };
  }
  yield* flush();
  const checks = gateChecks(report);

  report = { ...report, checks, status: checks.every((item) => item.passed) ? "passed" : "failed" };
  yield* flush();

  return report;
});
