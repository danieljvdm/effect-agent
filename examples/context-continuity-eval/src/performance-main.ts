import { digestJson } from "@effect-agent/thread/Digest";
import { NodeCrypto, NodeRuntime, NodeServices } from "@effect/platform-node";
import {
  Clock,
  Config,
  Console,
  Crypto,
  Effect,
  Exit,
  FileSystem,
  Layer,
  Option,
  Path,
  Redacted,
  Schedule,
  Schema,
} from "effect";
import { Command, Flag } from "effect/unstable/cli";
import {
  FetchHttpClient,
  HttpClient,
  HttpClientRequest,
  HttpClientResponse,
} from "effect/unstable/http";

import { buildPerformanceCloudflare, PerformanceBuild } from "./build-performance-cloudflare.ts";
import { EvaluationError } from "./contracts.ts";
import { MODEL_IDS } from "./live-model.ts";
import { PerformanceIdentity } from "./performance-contracts.ts";
import type { PerformanceTarget } from "./performance-deployment.ts";
import {
  PerformanceDeployment,
  PerformanceOwnership,
  PerformanceResources,
  loadPerformanceConfig,
  makePerformanceDeployment,
  preparePerformanceTarget,
  withPerformanceDeployment,
} from "./performance-deployment.ts";
import { PerformanceSample, runPerformanceSample } from "./performance-evaluate.ts";

const Report = Schema.Struct({
  version: Schema.Literal(1),
  status: Schema.Literals(["running", "passed", "failed", "dry-run"]),
  live: Schema.Boolean,
  startedAtMillis: Schema.Finite,
  elapsedMillis: Schema.Finite,
  builds: Schema.Array(PerformanceBuild),
  identities: Schema.Array(PerformanceIdentity),
  samples: Schema.Array(PerformanceSample),
  failure: Schema.NullOr(Schema.String),
  conditions: Schema.Record(Schema.String, Schema.Json),
});

const command = Command.make(
  "perf:cloudflare",
  {
    sourceRoot: Flag.directory("source-root").pipe(
      Flag.withDefault("."),
      Flag.withDescription("Clean exact candidate checkout, using its own installed lockfile."),
    ),
    referenceRoot: Flag.directory("reference-root").pipe(
      Flag.optional,
      Flag.withDescription(
        "Optional clean reference checkout. Builds the identical fixture against its dependencies.",
      ),
    ),
    model: Flag.choice("model", MODEL_IDS).pipe(Flag.withDefault("gpt-6-astra")),
    samples: Flag.integer("samples").pipe(
      Flag.withSchema(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 3 }))),
      Flag.withDefault(1),
    ),
    output: Flag.directory("output-dir").pipe(
      Flag.withDefault(".context-continuity-eval/performance"),
    ),
    dryRun: Flag.boolean("dry-run").pipe(
      Flag.withDefault(false),
      Flag.withDescription(
        "Build and show bounded deployment plan without credentials, deployment, or inference.",
      ),
    ),
    cleanup: Flag.boolean("cleanup").pipe(
      Flag.withDefault(false),
      Flag.withDescription(
        "Retry removal of this output directory's recorded disposable namespace/Workers; no inference.",
      ),
    ),
  },
  Effect.fn("Performance.command")(function* (options) {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = path.resolve(options.output);
    const resourcesPath = path.join(directory, "resources.json");

    if (options.cleanup) {
      if (!(yield* fs.exists(resourcesPath))) return;

      let resources = yield* Schema.decodeUnknownEffect(
        Schema.fromJsonString(PerformanceResources),
      )(yield* fs.readFileString(resourcesPath));

      resources = {
        ...resources,
        targets: resources.targets.map((target) => ({
          ...target,
          directory: path.join(directory, target.label),
        })),
      };
      const config = yield* loadPerformanceConfig;

      if (resources.accountDigest !== (yield* digestJson(config.accountId)))
        return yield* EvaluationError.make({
          stage: "cleanup",
          message: "Cleanup account differs from retained ownership manifest",
        });
      const operations = yield* makePerformanceDeployment(undefined);

      for (const target of resources.targets) {
        if (!target.cleanupRequired || target.cleanupComplete) continue;
        yield* operations.remove(target);
        resources = {
          ...resources,
          targets: resources.targets.map((item) =>
            item.name === target.name ? { ...target, cleanupComplete: true } : item,
          ),
        };
        yield* fs.writeFileString(
          resourcesPath,
          Schema.encodeSync(Schema.fromJsonString(PerformanceResources))(resources),
        );
      }

      return;
    }
    if (yield* fs.exists(directory))
      return yield* EvaluationError.make({
        stage: "evidence",
        message:
          "Output directory already exists; choose a new output-dir to retain prior attempts",
      });
    yield* fs.makeDirectory(directory, { recursive: true });
    const start = yield* Clock.currentTimeMillis;

    let report: typeof Report.Type = {
      version: 1,
      status: "running",
      live: !options.dryRun,
      startedAtMillis: start,
      elapsedMillis: 0,
      builds: [],
      identities: [],
      samples: [],
      failure: null,
      conditions: {
        fixture: "cloudflare-order-v1",
        provider: "openai",
        samplesPerTarget: options.samples,
        conservativeMaxCostUsdPerTarget: options.samples * 2,
        inferenceRetries: 0,
        maxTurnsPerSubmission: 4,
        maxToolsPerSubmission: 4,
        toolConcurrency: 2,
        syntheticToolWaitMillis: 20,
        concurrentSubmissions: 1,
        maxInputTokens: 8192,
        maxOutputTokens: 4096,
        maxModelCallsPerThread: 18,
        maxDatabaseBytesPerThread: 16777216,
        maxSampleMillis: 480000,
        pollIntervalMillis: 100,
        optionalProjections: false,
        publicationAdapters: false,
        persistedTimingMarks: true,
        coldStartGuaranteed: false,
        cpu: null,
        cpuUnavailableReason:
          "Worker request APIs do not expose per-event CPU. Attach Cloudflare observability export separately; local profiles are not billing.",
        heap: null,
        heapUnavailableReason: "No deployed per-DO heap API.",
        region: "Ingress CF-Ray suffix recorded in response observation; DO region is not exposed.",
        clock:
          "Separate runner and DO wall clocks; workerd clock advances only after I/O, so zero synchronous durations mean unresolved timer precision.",
        providerCache:
          "Uncontrolled provider prefix cache; cached token usage retained in response audits. No response-id/conversation reuse.",
        timingPolicy:
          "Informational, no latency threshold. Do not sum overlapping intervals or subtract synthetic processing from provider latency.",
        initialization:
          "Fresh-thread probe constructs the host before submission; warm requires unchanged incarnation. Recovery aborts after confirmed tool-result commit.",
      },
    };

    const save = () =>
      Effect.gen(function* () {
        report = { ...report, elapsedMillis: (yield* Clock.currentTimeMillis) - start };
        yield* fs.writeFileString(
          path.join(directory, "report.json"),
          Schema.encodeSync(Schema.fromJsonString(Report))(report),
        );
      });

    yield* save();

    const program = Effect.gen(function* () {
      const candidate = yield* buildPerformanceCloudflare(
        options.sourceRoot,
        path.join(directory, "candidate"),
      );

      const builds = [candidate];

      if (Option.isSome(options.referenceRoot))
        builds.push(
          yield* buildPerformanceCloudflare(
            options.referenceRoot.value,
            path.join(directory, "reference"),
          ),
        );
      report = { ...report, builds };
      yield* save();
      if (options.dryRun) {
        report = { ...report, status: "dry-run" };

        return;
      }
      if (builds.some((build) => build.dirtyWorkingTree))
        return yield* EvaluationError.make({
          stage: "source",
          message: "Live performance requires clean exact candidate and reference checkouts",
        });
      if ((yield* Config.string("EFFECT_AGENT_LIVE").pipe(Config.withDefault("0"))) !== "1")
        return yield* EvaluationError.make({
          stage: "configuration",
          message:
            "Set EFFECT_AGENT_LIVE=1 to authorize deployment and up to $2 per sample per target in provider reservations",
        });
      const config = yield* loadPerformanceConfig;
      const openai = yield* Config.redacted("OPENAI_API_KEY");
      const run = (yield* (yield* Crypto.Crypto).randomUUIDv4).replaceAll("-", "");
      const token = Redacted.make(yield* (yield* Crypto.Crypto).randomUUIDv4);
      const secretPath = yield* fs.makeTempFileScoped({ prefix: "performance-secrets-" });

      yield* fs.writeFileString(
        secretPath,
        JSON.stringify({
          OPENAI_API_KEY: Redacted.value(openai),
          PERFORMANCE_TOKEN: Redacted.value(token),
        }),
        { mode: 0o600 },
      );

      const targets = yield* Effect.forEach(builds, (build, index) =>
        preparePerformanceTarget({
          build,
          label: index === 0 ? "candidate" : "reference",
          run,
          model: options.model,
          samples: options.samples,
          subdomain: config.subdomain,
        }),
      );

      let resources: typeof PerformanceResources.Type = {
        accountDigest: yield* digestJson(config.accountId),
        targets,
      };

      const saveTarget = (target: typeof PerformanceTarget.Type) =>
        Effect.gen(function* () {
          resources = {
            ...resources,
            targets: resources.targets.map((current) =>
              current.name === target.name ? target : current,
            ),
          };
          yield* fs.writeFileString(
            resourcesPath,
            Schema.encodeSync(Schema.fromJsonString(PerformanceResources))(resources),
          );
        }).pipe(
          Effect.mapError(() =>
            EvaluationError.make({
              stage: "evidence",
              message: "Cannot persist deployment ownership",
            }),
          ),
        );

      yield* fs.writeFileString(
        resourcesPath,
        Schema.encodeSync(Schema.fromJsonString(PerformanceResources))(resources),
      );
      const operations = yield* makePerformanceDeployment(secretPath, [openai, token]);
      const client = (yield* HttpClient.HttpClient).pipe(HttpClient.filterStatusOk);

      const evaluate = Effect.gen(function* () {
        for (const target of targets) {
          const response = yield* client
            .execute(
              HttpClientRequest.get(`${target.url}/identity`).pipe(
                HttpClientRequest.bearerToken(token),
              ),
            )
            .pipe(
              Effect.timeout("20 seconds"),
              Effect.retry({ times: 5, schedule: Schedule.spaced("2 seconds") }),
            );

          const identity = yield* HttpClientResponse.schemaBodyJson(PerformanceIdentity)(response);

          if (
            identity.sourceCommit !== target.sourceCommit ||
            identity.dirtyWorkingTree ||
            identity.model !== options.model ||
            identity.fixtureDigest !== candidate.fixtureDigest ||
            identity.deploymentId.length === 0
          )
            return yield* EvaluationError.make({
              stage: "source",
              message:
                "Deployed identity does not match the clean candidate/reference and shared fixture",
            });
          report = {
            ...report,
            identities: [...report.identities, identity],
            conditions: {
              ...report.conditions,
              [`${target.label}IngressCfRay`]: response.headers["cf-ray"] ?? null,
            },
          };
          yield* save();
        }
        for (let sample = 0; sample < options.samples; sample++) {
          const order = sample % 2 === 0 ? targets : [...targets].reverse();

          for (const target of order) {
            const identity = report.identities[targets.indexOf(target)];

            if (identity === undefined)
              return yield* EvaluationError.make({
                stage: "source",
                message: "Target identity missing",
              });

            const result = yield* runPerformanceSample({
              url: target.url,
              token,
              identity,
              target: target.label,
              sample,
              outputDirectory: path.join(directory, `${target.label}-sample-${sample}`),
            });

            report = { ...report, samples: [...report.samples, result] };
            yield* save();
          }
        }
        if (report.samples.some((sample) => !sample.passed))
          return yield* EvaluationError.make({
            stage: "outcome",
            message: "At least one agent/lifecycle sample failed; all observations retained",
          });
      });

      // Nest lifetimes so both targets coexist; measurements alternate without concurrent inference.
      const deployed = targets.reduceRight(
        (use, target) =>
          withPerformanceDeployment(target, use).pipe(
            Effect.provideService(PerformanceDeployment, operations),
            Effect.provideService(PerformanceOwnership, { saveTarget }),
          ),
        evaluate,
      );

      yield* deployed;
      report = { ...report, status: "passed" };
    });

    const exit = yield* program.pipe(
      Effect.onExit((exit) =>
        Effect.gen(function* () {
          if (Exit.isFailure(exit))
            report = {
              ...report,
              status: "failed",
              failure: exit.cause.reasons
                .flatMap((reason) =>
                  reason._tag === "Fail" && Schema.is(EvaluationError)(reason.error)
                    ? [`${reason.error.stage}: ${reason.error.message}`]
                    : [reason._tag],
                )
                .join("; "),
            };
          yield* save();

          const rows = report.samples.flatMap((sample) =>
            sample.phases.map(
              (phase) =>
                `| ${sample.target} | ${sample.sample} | ${phase.condition} | ${phase.passed ? "pass" : "FAIL"} | ${phase.timing.clientValidatedCompletionDeliveryMillis ?? "unavailable"} | ${phase.timing.clientFirstCanonicalFeedbackMillis ?? "unavailable"} |`,
            ),
          );

          yield* fs.writeFileString(
            path.join(directory, "summary.md"),
            `Cloudflare performance: ${report.status}\n\nCandidate: ${report.builds[0]?.sourceCommit ?? "unbuilt"}. Fixture: ${report.builds[0]?.fixtureDigest ?? "unbuilt"}. Timing is informational; every sample is retained.\n\n| Target | Sample | Condition | Outcome | Delivery ms | First canonical feedback ms |\n|---|---:|---|---|---:|---:|\n${rows.join("\n")}\n\nSee report.json for configuration/limits and each phase snapshot for provider cache/usage, durable records, lifecycle, and raw timing. resources.json must show cleanupComplete for every owned resource.\n`,
          );
        }),
      ),
      Effect.exit,
    );

    yield* Console.log(
      `Cloudflare performance ${report.status}: ${path.join(directory, "summary.md")}`,
    );
    if (Exit.isFailure(exit)) return yield* Effect.failCause(exit.cause);
  }),
).pipe(
  Command.withDescription(
    "Manually deploy exact Cloudflare candidates, run real model-selected tool flows, preserve evidence, and delete disposable namespaces/Workers. Requires Cloudflare account/token/subdomain and OPENAI_API_KEY; never scheduled.",
  ),
);

if (import.meta.main)
  NodeRuntime.runMain(
    Command.run(command, { version: "1.0.0" }).pipe(
      Effect.tapError((error) =>
        Console.error(
          Schema.is(EvaluationError)(error)
            ? `${error.stage}: ${error.message}`
            : `Performance command failed (${error._tag}); see retained output and --help.`,
        ),
      ),
      Effect.scoped,
      Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer, FetchHttpClient.layer)),
      Effect.provideService(FetchHttpClient.RequestInit, { redirect: "error" }),
    ),
    { disableErrorReporting: true },
  );
