import { contextWindowId } from "@effect-agent/engine/Compaction";
import { digestDefinition } from "@effect-agent/thread/Digest";
import { Receipt } from "@effect-agent/thread/DurableAgentRuntime";
import { CanonicalRecordEnvelope } from "@effect-agent/thread/Records";
import { runIdForSubmission } from "@effect-agent/thread/RunJournal";
import {
  Clock,
  Config,
  Crypto,
  DateTime,
  Effect,
  FileSystem,
  Path,
  Redacted,
  Schema,
} from "effect";
import { HttpClient, HttpClientRequest, HttpClientResponse } from "effect/unstable/http";

import { CloudflareIdentity, CloudflareSnapshot } from "./cloudflare-contracts.ts";
import { EvaluationError, EvaluationReport, ProjectStatus } from "./contracts.ts";
import { gateChecks, type EvaluationOptions } from "./evaluate.ts";
import { gradePhase } from "./grade.ts";
import { MAX_MODEL_CALLS, MAX_OUTPUT_TOKENS } from "./live-model.ts";
import { pressureInstructions, pressureScenario } from "./pressure.ts";

/** One already deployed host, one fresh Thread, one $10 budget. Never deploys or retries inference. */
export const runCloudflareEvaluation = Effect.fn("ContextContinuity.runCloudflareEvaluation")(
  function* (options: EvaluationOptions, baseUrl: string) {
    const client = yield* HttpClient.HttpClient;
    const token = yield* Config.redacted("CONTEXT_EVAL_TOKEN");
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const start = yield* Clock.currentTimeMillis;
    const thread = `context-eval-${yield* (yield* Crypto.Crypto).randomUUIDv4}`;
    const base = new URL(baseUrl);

    if (base.protocol !== "https:")
      return yield* EvaluationError.make({
        stage: "configuration",
        message: "Cloudflare acceptance requires an HTTPS deployed host",
      });

    const get = (route: string) =>
      client.execute(
        HttpClientRequest.get(new URL(`${route}?thread=${thread}`, base).href).pipe(
          HttpClientRequest.bearerToken(token),
        ),
      );

    const identity = yield* get("/identity").pipe(
      Effect.flatMap(HttpClientResponse.schemaBodyJson(CloudflareIdentity)),
    );

    if (
      identity.sourceCommit !== options.sourceCommit ||
      identity.dirtyWorkingTree ||
      identity.model !== options.model ||
      identity.seed !== options.seed ||
      options.reasoningEffort !== "low" ||
      options.maxCostMicrousd !== identity.maxCostMicrousd
    )
      return yield* EvaluationError.make({
        stage: "source",
        message:
          "Deployed Cloudflare bundle does not match the exact clean candidate, model, seed, effort, and budget",
      });
    const scenario = pressureScenario(options.seed);

    let report: EvaluationReport = {
      version: 3,
      status: "running",
      sourceCommit: options.sourceCommit,
      dirtyWorkingTree: options.dirtyWorkingTree,
      scenarioDigest: yield* digestDefinition({
        instructions: pressureInstructions,
        phases: scenario,
        profile: identity.profile,
      }),
      seed: options.seed,
      provider: "openai",
      model: options.model,
      reasoningEffort: options.reasoningEffort,
      serviceTier: "default",
      pricingVersion: "openai-2026-09-08-conservative",
      contextTokenLimit: identity.contextTokenLimit,
      maxOutputTokens: MAX_OUTPUT_TOKENS,
      maxCostMicrousd: identity.maxCostMicrousd,
      maxModelCalls: MAX_MODEL_CALLS,
      profile: identity.profile,
      startedAt: DateTime.formatIso(yield* DateTime.now),
      elapsedMillis: 0,
      phases: [],
      windows: [],
      compactions: [],
      restarts: [],
      checks: [],
      usage: {
        calls: 0,
        completedCalls: 0,
        inputTokens: 0,
        outputTokens: 0,
        maxInputTokens: 0,
        estimatedCostMicrousd: 0,
        reservedCostMicrousd: 0,
        returnedModels: [],
      },
      failure: null,
    };

    const flush = Effect.gen(function* () {
      report = { ...report, elapsedMillis: (yield* Clock.currentTimeMillis) - start };
      yield* fs.writeFileString(
        path.join(options.outputDirectory, "report.json"),
        yield* Schema.encodeEffect(Schema.fromJsonString(EvaluationReport))(report),
      );
    });

    yield* fs.writeFileString(
      path.join(options.outputDirectory, "host.json"),
      JSON.stringify({ ...identity, url: base.origin, thread }),
    );
    yield* flush;

    const run = Effect.gen(function* () {
      for (const phase of scenario) {
        const request = yield* HttpClientRequest.post(
          new URL(`/submit?thread=${thread}`, base).href,
        ).pipe(
          HttpClientRequest.bearerToken(Redacted.value(token)),
          HttpClientRequest.bodyJson({ phase: phase.index, message: phase.message }),
        );

        const receipt = yield* client
          .execute(request)
          .pipe(Effect.flatMap(HttpClientResponse.schemaBodyJson(Receipt)));

        const runId = runIdForSubmission(receipt.submissionId);
        let snapshot: typeof CloudflareSnapshot.Type;

        for (;;) {
          snapshot = yield* get("/snapshot").pipe(
            Effect.flatMap(HttpClientResponse.schemaBodyJson(CloudflareSnapshot)),
          );
          if (JSON.stringify(snapshot.identity) !== JSON.stringify(identity))
            return yield* EvaluationError.make({
              stage: "source",
              message: "Cloudflare host changed during evaluation",
            });
          yield* fs.writeFileString(
            path.join(options.outputDirectory, "host-snapshot.json"),
            yield* Schema.encodeEffect(Schema.fromJsonString(CloudflareSnapshot))(snapshot),
          );
          yield* fs.writeFileString(
            path.join(options.outputDirectory, "canonical.ndjson"),
            (yield* Effect.forEach(
              snapshot.records,
              (record) => Schema.encodeEffect(Schema.fromJsonString(CanonicalRecordEnvelope))(record),
            )).join("\n") + "\n",
          );
          yield* fs.writeFileString(
            path.join(options.outputDirectory, "requests.ndjson"),
            snapshot.audits.map((a) => JSON.stringify(a)).join("\n") + "\n",
          );
          report = {
            ...report,
            usage: snapshot.usage,
            compactions: snapshot.compactions,
            restarts: snapshot.restarts,
            windows: snapshot.records.flatMap(({ record, sequence }) =>
              record.payload._tag === "CompactionCreated" && record.payload.kind === "rollover"
                ? [
                    {
                      id: contextWindowId(record.payload.runId, record.payload.turn),
                      recordId: record.recordId,
                      sequence,
                      coversThrough: record.payload.coversThrough,
                    },
                  ]
                : [],
            ),
          };
          yield* flush;

          if (snapshot.failure !== null) return yield* EvaluationError.make({ stage: "provider", message: snapshot.failure });
          const settled = snapshot.records.find(
            ({ record }) =>
              record.payload._tag === "SubmissionSettled" &&
              record.payload.submissionId === receipt.submissionId,
          )?.record.payload;

          if (settled?._tag === "SubmissionSettled") {
            const completed = snapshot.records.find(
              ({ record }) =>
                record.payload._tag === "RunCompleted" && record.payload.runId === runId,
            )?.record.payload;

            if (
              settled.outcome !== "completed" ||
              completed?._tag !== "RunCompleted" ||
              completed.finishReason !== undefined
            )
              return yield* EvaluationError.make({
                stage: "runtime",
                message: `Cloudflare phase ${phase.index} failed`,
              });
            const output = yield* Schema.decodeUnknownEffect(ProjectStatus)(completed.output);

            const first = snapshot.audits.find(
              (a) => a.kind === "request" && a.phase === phase.index,
            );

            const checks = yield* gradePhase(
              phase,
              { runId, notes: snapshot.notes, output },
              snapshot.records,
              report.windows,
              first !== undefined && !first.json.includes(phase.receipt?.code ?? "missing-receipt"),
              true,
            );

            report = {
              ...report,
              phases: [
                ...report.phases,
                {
                  index: phase.index,
                  runId,
                  output,
                  checks,
                  modelCalls: settled.usageSummary?.modelCalls ?? 0,
                },
              ],
            };
            yield* flush;
            break;
          }
          yield* Effect.sleep("1 second");
        }
      }
    }).pipe(Effect.timeout("35 minutes"));

    yield* run.pipe(
      Effect.catch((error) =>
        Effect.sync(() => {
          report = {
            ...report,
            failure: Schema.is(EvaluationError)(error)
              ? error.message
              : `Cloudflare evaluation failed (${error._tag})`,
          };
        }),
      ),
    );
    const checks = gateChecks(report);

    report = { ...report, checks, status: checks.every((c) => c.passed) ? "passed" : "failed" };
    yield* flush;

    return report;
  },
);
