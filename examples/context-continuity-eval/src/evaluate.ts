import * as ContextTools from "@effect-agent/capabilities/ContextTools";
import * as MemoryNotes from "@effect-agent/capabilities/MemoryNotes";
import * as Agent from "@effect-agent/core/Agent";
import { AgentPolicy } from "@effect-agent/core/AgentPolicy";
import { ThreadId } from "@effect-agent/core/Identifiers";
import { MemoryKey, MemoryReader } from "@effect-agent/core/MemoryStore";
import { contextWindowId } from "@effect-agent/engine/Compaction";
import { NodeDurableAgentRuntime } from "@effect-agent/platform-node/NodeDurableAgentRuntime";
import { digestDefinitions, digestDefinition } from "@effect-agent/thread/Digest";
import { DurableAgentRuntime } from "@effect-agent/thread/DurableAgentRuntime";
import { DurableRuntimeFailpointError } from "@effect-agent/thread/DurableFailpoint";
import { CanonicalRecordEnvelope, DefinitionDigestInput } from "@effect-agent/thread/Records";
import { runIdForSubmission } from "@effect-agent/thread/RunJournal";
import { memoryStoreLayer } from "@effect-agent/thread/SqlMemoryStore";
import { IdempotencyKey, Principal } from "@effect-agent/thread/SubmissionLedger";
import * as ThreadContextHistory from "@effect-agent/thread/ThreadContextHistory";
import { ThreadStore } from "@effect-agent/thread/ThreadStore";
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
} from "effect";
import { IdGenerator, Toolkit } from "effect/unstable/ai";

import {
  check,
  EvaluationError,
  EvaluationReport,
  RecoveryCheckpointEvidence,
  ProjectStatus,
  ResumeCheckpoint,
  KillWitness,
  type CompactionEvidence,
  type Check,
  type PhaseResult,
  type RestartEvidence,
} from "./contracts.ts";
import { gradePhase } from "./grade.ts";
import { readLog, readNotes, readRecoveryCheckpoint, notesNamespace } from "./host-evidence.ts";
import {
  makeLiveClient,
  MAX_INPUT_TOKENS,
  MAX_MODEL_CALLS,
  MAX_OUTPUT_TOKENS,
  type ModelId,
} from "./live-model.ts";
import {
  manifestLayer,
  observedCompactor,
  pressureInstructions,
  pressureScenario,
  pressureToolkit,
} from "./pressure.ts";
import {
  DEFAULT_PROFILE,
  MAX_COST_MICROUSD,
  REDUCED_CONTEXT_TOKENS,
  type ProfileId,
} from "./profiles.ts";
import { RequestAudit, RequestAuditSink } from "./request-audit.ts";
import {
  instructions,
  makeScenario,
  REQUIRED_ROLLOVERS,
  RESTARTS,
  SCENARIO_VERSION,
  type ScenarioPhase,
} from "./scenario.ts";

export const CONTEXT_TOKEN_LIMIT = REDUCED_CONTEXT_TOKENS;

const explicitToolkit = Toolkit.merge(ContextTools.toolkit, MemoryNotes.toolkit);

export interface EvaluationOptions {
  readonly model: ModelId;
  readonly profile?: ProfileId;
  readonly processId?: number;
  readonly reasoningEffort: "low" | "medium" | "high";
  readonly seed: number;
  readonly outputDirectory: string;
  readonly sourceCommit: string;
  readonly dirtyWorkingTree: boolean;
  readonly maxCostMicrousd: number;
}

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
  check("hard-spending-ceiling", report.maxCostMicrousd <= MAX_COST_MICROUSD, true),
  check("profile-is-live", report.profile !== "production-capacity-v1", true),
  ...(report.profile === "pressure-restart-sqlite-v1"
    ? [
        check(
          "actual-process-kills",
          report.restarts.every(
            (r) =>
              r.mechanism === "SIGKILL" &&
              r.killConfirmed &&
              r.processBefore !== null &&
              r.processAfter !== null &&
              r.processBefore !== r.processAfter,
          ),
          true,
        ),
      ]
    : []),
  ...(report.profile === "pressure-cloudflare-v1"
    ? [
        check(
          "native-cloudflare-evictions",
          report.restarts.every(
            (r) => r.mechanism === "durable-object-eviction" && r.killConfirmed,
          ),
          true,
        ),
      ]
    : []),
  ...(report.profile !== DEFAULT_PROFILE
    ? [
        check(
          "pressure-caused-committed-windows",
          report.windows.every((w) =>
            report.compactions.some(
              (c) =>
                contextWindowId(c.runId, c.turn) === w.id &&
                c.kind === "rollover" &&
                c.trigger === "pressure" &&
                c.targetTokens !== null &&
                c.estimatedTokens > c.targetTokens,
            ),
          ),
          true,
        ),
      ]
    : []),
  check("no-operational-failure", report.failure, null),
];

export const runEvaluation = Effect.fn("ContextContinuity.runEvaluation")(function* (
  options: EvaluationOptions,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const profile = options.profile ?? DEFAULT_PROFILE;
  const pressure = profile !== DEFAULT_PROFILE;
  const hardRestart = profile === "pressure-restart-sqlite-v1";
  const scenario = pressure ? pressureScenario(options.seed) : makeScenario(options.seed);
  const toolkit = pressure ? pressureToolkit : explicitToolkit;
  const agentInstructions = pressure ? pressureInstructions : instructions;
  const checkpointPath = path.join(options.outputDirectory, "resume.json");

  const checkpoint =
    hardRestart && (yield* fs.exists(checkpointPath))
      ? yield* Schema.decodeUnknownEffect(Schema.fromJsonString(ResumeCheckpoint))(
          yield* fs.readFileString(checkpointPath),
        )
      : undefined;

  if (
    checkpoint !== undefined &&
    (checkpoint.report.sourceCommit !== options.sourceCommit ||
      checkpoint.report.seed !== options.seed ||
      checkpoint.report.profile !== profile ||
      checkpoint.report.model !== options.model ||
      checkpoint.report.maxCostMicrousd !== options.maxCostMicrousd ||
      checkpoint.report.usage.reservedCostMicrousd !== 0 ||
      checkpoint.report.usage.calls !== checkpoint.report.usage.completedCalls)
  )
    return yield* EvaluationError.make({
      stage: "restart",
      message: "Recovery candidate/configuration changed or provider accounting is unresolved",
    });
  const phaseIndex = yield* Ref.make(0);
  const compactions: Array<CompactionEvidence> = [...(checkpoint?.report.compactions ?? [])];
  const started = yield* Clock.currentTimeMillis;

  const scenarioDigest = yield* digestDefinition({
    version: SCENARIO_VERSION,
    instructions: agentInstructions,
    profile,
    phases: scenario,
  });

  if (
    checkpoint !== undefined &&
    (checkpoint.report.scenarioDigest !== scenarioDigest ||
      checkpoint.report.reasoningEffort !== options.reasoningEffort ||
      checkpoint.report.dirtyWorkingTree !== options.dirtyWorkingTree)
  )
    return yield* EvaluationError.make({
      stage: "restart",
      message: "Recovery scenario or model settings changed",
    });

  const auditPath = path.join(options.outputDirectory, "requests.ndjson");
  const canonicalPath = path.join(options.outputDirectory, "canonical.ndjson");
  const reportPath = path.join(options.outputDirectory, "report.json");

  const live = yield* makeLiveClient({
    model: options.model,
    maxCostMicrousd: options.maxCostMicrousd,
    ...(checkpoint === undefined ? {} : { initialUsage: checkpoint.report.usage }),
    phase: phaseIndex,
  }).pipe(Effect.provide(RequestAuditSink.file(auditPath)));

  let report: EvaluationReport = {
    version: 3,
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
    profile,
    compactions: [],
    startedAt: DateTime.formatIso(yield* DateTime.now),
    elapsedMillis: 0,
    phases: [],
    windows: [],
    restarts: [],
    checks: [],
    usage: yield* live.snapshot,
    failure: null,
  };

  if (checkpoint !== undefined) report = checkpoint.report;

  const flush = Effect.fn("ContextContinuity.flush")(function* () {
    report = {
      ...report,
      elapsedMillis:
        (checkpoint?.report.elapsedMillis ?? 0) + (yield* Clock.currentTimeMillis) - started,
      compactions: [...compactions],
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

  const makeDefinition = <T extends Toolkit.Any>(selectedToolkit: T) =>
    Agent.make("context-continuity-eval", {
      input: Schema.String,
      output: ProjectStatus,
      instructions: agentInstructions,
      toolkit: selectedToolkit,
      policy: AgentPolicy.make(policy),
    });

  const agent = Agent.withModel(
    makeDefinition(explicitToolkit),
    OpenAiLanguageModel.model(options.model, modelSettings),
  );

  const pressureAgent = Agent.withModel(
    makeDefinition(pressureToolkit),
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
    let atKill: Effect.Effect<void> = Effect.die("Kill services not acquired");

    const pauseForKill = Effect.gen(function* () {
      if (options.processId === undefined)
        return yield* Effect.die("Missing supervised process identity");
      const notes = yield* readNotes(key);
      const log = yield* readLog(threadId);

      const startedRun = log.findLast(({ record }) => record.payload._tag === "RunStarted")?.record
        .payload;

      if (startedRun?._tag !== "RunStarted") return yield* Effect.die("No run at kill barrier");

      const encodedLog = yield* Effect.forEach(log, (record) =>
        Schema.encodeEffect(Schema.fromJsonString(CanonicalRecordEnvelope))(record),
      );

      yield* fs.writeFileString(
        path.join(options.outputDirectory, `canonical-barrier-${phase.index}.ndjson`),
        `${encodedLog.join("\n")}\n`,
      );
      yield* fs.writeFileString(canonicalPath, `${encodedLog.join("\n")}\n`);
      yield* flush();
      if (
        report.usage.reservedCostMicrousd !== 0 ||
        report.usage.calls !== report.usage.completedCalls
      )
        return yield* Effect.die("Cannot restart an unmetered provider call");

      const saved: ResumeCheckpoint = {
        version: 1,
        report,
        phase: phase.index,
        runId: startedRun.runId,
        processId: options.processId,
        notes,
      };

      const json = yield* Schema.encodeEffect(Schema.fromJsonString(ResumeCheckpoint))(saved);

      yield* fs.writeFileString(`${checkpointPath}.tmp`, json);
      yield* fs.rename(`${checkpointPath}.tmp`, checkpointPath);
      yield* fs.writeFileString(
        path.join(options.outputDirectory, `barrier-${phase.index}.json`),
        json,
      );

      // No scope closes and no failure is returned. Only the supervisor's SIGKILL ends this attempt.
      return yield* Effect.never;
    }).pipe(Effect.orDie);

    const host = NodeDurableAgentRuntime.layer({
      filename: path.join(options.outputDirectory, "thread.sqlite"),
      deploymentId: "context-continuity-eval-v1",
      producerId: `context-eval-${phase.index}-${previousNotes === undefined ? "initial" : "resumed"}`,
      ...(hardRestart ? { ownershipLeaseDuration: 2_000, leaseRenewalInterval: 500 } : {}),
      runtimeFailpoint: (location) =>
        location === restart?.location
          ? hardRestart
            ? Effect.suspend(() => atKill)
            : Effect.fail(DurableRuntimeFailpointError.make({ location }))
          : Effect.void,
    }).pipe(Layer.provide(observedCompactor((evidence) => compactions.push(evidence))));

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
      manifestLayer(CONTEXT_TOKEN_LIMIT),
      ThreadContextHistory.layer({ maxRecords: 16_384 }).pipe(Layer.provide(host)),
    );

    return yield* Effect.gen(function* () {
      const runtime = yield* DurableAgentRuntime;

      atKill = pauseForKill.pipe(
        Effect.provideService(ThreadStore, yield* ThreadStore),
        Effect.provideService(MemoryReader, yield* MemoryReader),
      );
      // Reacquisition reads the same persisted document before any resumed model call.
      const reopenedNotes = yield* readNotes(key);

      const receipt = yield* runtime.submit(
        { definition: pressure ? pressureAgent.definition : agent.definition },
        phase.message,
        {
          threadId,
          principal: yield* Schema.decodeEffect(Principal)("context-eval"),
          idempotencyKey: yield* Schema.decodeEffect(IdempotencyKey)(`phase-${phase.index}`),
          definitions,
        },
      );

      const runId = runIdForSubmission(receipt.submissionId);

      if (previousNotes !== undefined) {
        const boundary = RESTARTS.find((item) => item.phase === phase.index);

        if (boundary === undefined)
          return yield* EvaluationError.make({
            stage: "restart",
            message: "Unexpected recovery phase",
          });

        let evidence: RestartEvidence = {
          phase: phase.index,
          location: boundary.location,
          runId,
          notesRevisionBefore: previousNotes.revision,
          notesRevisionAfter: reopenedNotes.revision,
          notesTextUnchanged: previousNotes.text === reopenedNotes.text,
          mechanism: hardRestart ? "SIGKILL" : "service-reacquisition",
          processBefore: hardRestart ? (checkpoint?.processId ?? null) : null,
          processAfter: hardRestart ? (options.processId ?? null) : null,
          killConfirmed: false,
        };

        if (hardRestart) {
          const witness = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(KillWitness))(
            yield* fs.readFileString(
              path.join(options.outputDirectory, `kill-${phase.index}.json`),
            ),
          );

          if (
            witness.processId !== evidence.processBefore ||
            witness.phase !== phase.index ||
            checkpoint?.runId !== runId
          )
            return yield* EvaluationError.make({
              stage: "restart",
              message: "Kill witness does not match the recovered run",
            });
          evidence = { ...evidence, killConfirmed: true };
        }
        report = { ...report, restarts: [...report.restarts, evidence] };
        yield* runtime.runRecovery;
      }

      const exit = pressure
        ? yield* runtime.processThread(pressureAgent, threadId).pipe(Effect.exit)
        : yield* runtime.processThread(agent, threadId).pipe(Effect.exit);

      records = yield* readLog(threadId);

      const encoded = yield* Effect.forEach(records, (record) =>
        Schema.encodeEffect(Schema.fromJsonString(CanonicalRecordEnvelope))(record),
      );

      yield* fs.writeFileString(canonicalPath, `${encoded.join("\n")}\n`);
      yield* fs.writeFileString(
        path.join(options.outputDirectory, "recovery-checkpoint.json"),
        yield* Schema.encodeEffect(Schema.fromJsonString(RecoveryCheckpointEvidence))(
          yield* readRecoveryCheckpoint(threadId),
        ),
      );
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
    for (const phase of scenario.filter((phase) => phase.index >= (checkpoint?.phase ?? 0))) {
      yield* Ref.set(phaseIndex, phase.index);
      yield* Console.error(`Context continuity: update ${phase.index + 1}/${scenario.length}`);
      const restart = RESTARTS.find((item) => item.phase === phase.index);
      const resuming = checkpoint?.phase === phase.index;

      let result = yield* attempt(
        phase,
        resuming ? undefined : restart,
        resuming ? checkpoint.notes : undefined,
      );

      if (result.kind === "restart") result = yield* attempt(phase, undefined, result.notes);
      if (result.kind !== "completed")
        return yield* EvaluationError.make({
          stage: "restart",
          message: "Recovered attempt did not finish",
        });

      const requestEvidence = yield* Effect.forEach(
        (yield* fs.readFileString(auditPath)).trim().split("\n"),
        (line) => Schema.decodeUnknownEffect(Schema.fromJsonString(RequestAudit))(line),
      );

      const firstRequest = requestEvidence.find(
        (event) => event.kind === "request" && event.phase === phase.index,
      );

      const checks = yield* gradePhase(
        phase,
        result,
        records,
        report.windows,
        firstRequest !== undefined &&
          !firstRequest.json.includes(phase.receipt?.code ?? "missing-receipt"),
        pressure,
        scenario[1]?.message ?? "",
      );

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
