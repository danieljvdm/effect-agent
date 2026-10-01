import { NodeCrypto, NodeRuntime, NodeServices } from "@effect/platform-node";
import {
  Cause,
  Clock,
  Console,
  Crypto,
  DateTime,
  Effect,
  FileSystem,
  Layer,
  Option,
  Path,
  Random,
  Redacted,
  Schema,
} from "effect";
import { Command, Flag } from "effect/unstable/cli";
import { FetchHttpClient, HttpClient, HttpClientRequest } from "effect/unstable/http";

import { requireReplayCpu, sha256 } from "./replay-cpu-build.ts";
import type { ReplayCpuRole } from "./replay-cpu-contracts.ts";
import {
  REPLAY_CPU_PHASES,
  REPLAY_CPU_PROTOCOL,
  ReplayCpuBuild,
  ReplayCpuEvidence,
  ReplayCpuError,
  ReplayCpuIdentity,
  ReplayCpuOperation,
  ReplayCpuSeed,
} from "./replay-cpu-contracts.ts";
import {
  openReplayCpuDeployment,
  prepareReplayCpuResources,
  writeReplayCpuJson,
} from "./replay-cpu-deployment.ts";
import {
  ReplayCpuSample,
  sanitizeReplayCpuTelemetry,
  summarizeReplayCpu,
  telemetryFor,
  type ReplayCpuTelemetry,
} from "./replay-cpu-results.ts";

const roles = ["baseline", "candidate", "control"] as const;
const sizes = ["small", "large"] as const;
const now = DateTime.now.pipe(Effect.map(DateTime.toEpochMillis));

const options = {
  baseline: Flag.optional(Flag.Directory("baseline-dir")).pipe(
    Flag.withDescription("Prebuilt baseline from perf:cloudflare:cpu:build"),
  ),
  candidate: Flag.optional(Flag.Directory("candidate-dir")).pipe(
    Flag.withDescription("Prebuilt candidate using the identical fixture"),
  ),
  output: Flag.Directory("output-dir").pipe(
    Flag.withDescription("New artifact directory, or existing directory for --cleanup"),
  ),
  dryRun: Flag.Boolean("dry-run").pipe(
    Flag.withDefault(false),
    Flag.withDescription(
      "Verify bundle identities and print the fixed experiment without deploying",
    ),
  ),
  cleanup: Flag.Boolean("cleanup").pipe(
    Flag.withDefault(false),
    Flag.withDescription(
      "Retry destruction of only the recorded stages using retained private state",
    ),
  ),
};

const command = Command.make(
  "perf:cloudflare:cpu",
  options,
  Effect.fn("ReplayCpu.run")(function* (args) {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const crypto = yield* Crypto.Crypto;
    const client = yield* HttpClient.HttpClient;
    const output = path.resolve(args.output);

    if (args.cleanup) {
      yield* requireReplayCpu(!args.dryRun, "Choose --cleanup or --dry-run");
      if (yield* fs.exists(path.join(output, "cleanup.json"))) {
        const receipt = yield* Schema.decodeEffect(
          Schema.fromJsonString(
            Schema.Struct({ complete: Schema.Literal(true), secretRemoved: Schema.Literal(true) }),
          ),
        )(yield* fs.readFileString(path.join(output, "cleanup.json")));

        return yield* Console.log(receipt);
      }

      return yield* (yield* openReplayCpuDeployment(output)).retire;
    }
    if (Option.isNone(args.baseline) || Option.isNone(args.candidate))
      return yield* requireReplayCpu(false, "Supply --baseline-dir and --candidate-dir");

    const directories = {
      baseline: path.resolve(args.baseline.value),
      candidate: path.resolve(args.candidate.value),
    };

    const builds = yield* Effect.all({
      baseline: fs
        .readFileString(path.join(directories.baseline, "build.json"))
        .pipe(Effect.flatMap(Schema.decodeEffect(Schema.fromJsonString(ReplayCpuBuild)))),
      candidate: fs
        .readFileString(path.join(directories.candidate, "build.json"))
        .pipe(Effect.flatMap(Schema.decodeEffect(Schema.fromJsonString(ReplayCpuBuild)))),
    });

    for (const role of ["baseline", "candidate"] as const)
      yield* requireReplayCpu(
        (yield* sha256(yield* fs.readFile(path.join(directories[role], "worker.mjs")))) ===
          builds[role].bundleSha256,
        `${role} bundle differs from its build receipt`,
      );
    yield* requireReplayCpu(
      builds.baseline.fixtureSha256 === builds.candidate.fixtureSha256,
      "Baseline and candidate fixtures differ",
    );
    yield* requireReplayCpu(
      !(yield* fs.exists(output)),
      "Output directory already exists; retain earlier evidence",
    );
    const run = (yield* crypto.randomUUIDv4).replaceAll("-", "").slice(0, 16);

    const schedule = yield* Effect.gen(function* () {
      const blocks = [];

      for (const block of [0, 1, 2]) {
        const deploymentOrder = yield* Random.shuffle(roles);
        const cohorts = [];

        for (const cohort of [0, 1, 2, 3]) {
          const order = yield* Random.shuffle(
            roles.flatMap((role) => sizes.map((size) => ({ role, size }))),
          );

          cohorts.push({ cohort, order });
        }
        blocks.push({ block, deploymentOrder, cohorts });
      }

      return blocks;
    }).pipe(Random.withSeed(run));

    const plan = {
      protocol: REPLAY_CPU_PROTOCOL,
      run,
      builds,
      schedule,
      controlBundleSha256: builds.baseline.bundleSha256,
      objects: 72,
      measuredOperations: 720,
      modelApiCalls: 0,
      workloadDeadlineMinutes: 45,
      primaryMetric:
        "Same-incarnation warmed cycle: sum of two compaction RPC cpuTimeMs per Object",
      initialCycle: "First five operations after seed; not guaranteed cold",
      warmup: "One complete five-operation cycle with no intervening evidence RPC",
      attribution:
        "At least 10% paired-median reduction in all three deployment blocks, exceeding each block's identical-code control shift",
    };

    if (args.dryRun) return yield* Console.log(JSON.stringify(plan, null, 2));
    yield* fs.makeDirectory(output, { recursive: true });
    yield* writeReplayCpuJson(path.join(output, "plan.json"), plan);
    yield* prepareReplayCpuResources(output, directories.baseline, directories.candidate, run);
    const deployment = yield* openReplayCpuDeployment(output);
    const started = yield* now;
    const deadline = started + 45 * 60_000;

    yield* writeReplayCpuJson(path.join(output, "started.json"), {
      startedAtMillis: started,
      deadlineAtMillis: deadline,
    });
    const samples: Array<typeof ReplayCpuSample.Type> = [];
    const promptDigests = new Map<number, string>();

    const request = Effect.fn("ReplayCpu.request")(function* (
      url: string,
      file: string,
      body?: Schema.Json,
    ) {
      yield* requireReplayCpu((yield* now) < deadline, "Workload deadline reached");
      yield* requireReplayCpu(
        !(yield* fs.exists(file)),
        "Refusing to repeat a recorded invocation",
      );
      const startedAtMillis = yield* now;
      const monotonic = yield* Clock.currentTimeNanos;

      const prepared =
        body === undefined
          ? HttpClientRequest.get(url)
          : yield* HttpClientRequest.post(url).pipe(HttpClientRequest.bodyJson(body));

      const response = yield* client
        .execute(prepared.pipe(HttpClientRequest.bearerToken(deployment.token)))
        .pipe(Effect.timeout("110 seconds"));

      const text = (yield* response.text).replaceAll(
        Redacted.value(deployment.token),
        "[redacted]",
      );

      const elapsed = Number((yield* Clock.currentTimeNanos) - monotonic) / 1_000_000;

      const data = yield* Schema.decodeEffect(Schema.fromJsonString(Schema.Json))(text).pipe(
        Effect.catch(() => Effect.succeed({ unparsedResponse: text.slice(0, 2000) })),
      );

      yield* writeReplayCpuJson(file, {
        startedAtMillis,
        endedAtMillis: yield* now,
        clientElapsedMs: elapsed,
        status: response.status,
        cfRay: response.headers["cf-ray"],
        response: data,
      });
      yield* requireReplayCpu(response.status === 200, `Request failed; inspect ${file}`);

      return { data, elapsed };
    });

    yield* Effect.gen(function* () {
      for (const block of schedule) {
        yield* Console.log(`Deploying block ${block.block + 1}/3 through Alchemy`);
        const blockStarted = yield* now;

        const targets = new Map<
          typeof ReplayCpuRole.Type,
          {
            url: string;
            name: string;
            identity: typeof ReplayCpuIdentity.Type;
          }
        >();

        for (const role of block.deploymentOrder) {
          const target = yield* deployment.deploy(block.block, role);
          const file = path.join(output, `block-${block.block}`, role, "identity.json");

          const identity = yield* Schema.decodeUnknownEffect(ReplayCpuIdentity)(
            (yield* request(`${target.url}/identity`, file)).data,
          );

          const expected = builds[role === "candidate" ? "candidate" : "baseline"];

          yield* requireReplayCpu(
            identity.revision === expected.revision &&
              identity.fixtureSha256 === expected.fixtureSha256,
            "Hosted identity mismatch",
          );
          yield* writeReplayCpuJson(
            path.join(output, `block-${block.block}`, role, "deployment.json"),
            target,
          );
          targets.set(role, { ...target, identity });
        }

        const completed: Array<{
          role: typeof ReplayCpuRole.Type;
          cohort: number;
          proof: typeof ReplayCpuEvidence.Type;
          elapsed: ReadonlyArray<number>;
          prompts: ReadonlyArray<string>;
        }> = [];

        const failures: Array<{ role: string; cohort: number; size: string; error: string }> = [];

        for (const cohort of block.cohorts) {
          for (const { role, size } of cohort.order) {
            const target = targets.get(role);

            if (target === undefined)
              return yield* new ReplayCpuError({ message: "Missing deployed role" });
            const object = `${size}-${cohort.cohort}`;

            const directory = path.join(
              output,
              `block-${block.block}`,
              `cohort-${cohort.cohort}`,
              role,
              size,
            );

            yield* fs.makeDirectory(directory, { recursive: true });

            const attempt = yield* Effect.gen(function* () {
              const seeded = yield* Schema.decodeUnknownEffect(ReplayCpuSeed)(
                (yield* request(
                  `${target.url}/seed?object=${object}`,
                  path.join(directory, "seed.json"),
                  { records: size === "small" ? 10 : 1000 },
                )).data,
              );

              const elapsed = [];

              for (const [index, method] of REPLAY_CPU_PHASES.entries()) {
                const receipt = yield* request(
                  `${target.url}/${method}?object=${object}`,
                  path.join(directory, `${method}.json`),
                  {},
                );

                const operation = yield* Schema.decodeUnknownEffect(ReplayCpuOperation)(
                  receipt.data,
                );

                yield* requireReplayCpu(
                  operation.phase === index + 1 &&
                    operation.method === method &&
                    operation.outcome === "completed" &&
                    operation.incarnation.module === seeded.incarnation.module &&
                    operation.incarnation.runtime === seeded.incarnation.runtime,
                  "Operation order, settlement or observed incarnation differs",
                );
                elapsed.push(receipt.elapsed);
              }

              const proof = yield* Schema.decodeUnknownEffect(ReplayCpuEvidence)(
                (yield* request(
                  `${target.url}/evidence?object=${object}`,
                  path.join(directory, "evidence.json"),
                )).data,
              );

              yield* requireReplayCpu(
                proof.valid &&
                  Object.values(proof.checks).every(Boolean) &&
                  proof.checkpoint.present &&
                  proof.objectId === seeded.objectId &&
                  proof.threadId === seeded.threadId &&
                  proof.operations.length === 10 &&
                  proof.revision === target.identity.revision &&
                  proof.fixtureSha256 === target.identity.fixtureSha256 &&
                  proof.deploymentVersion === target.identity.deploymentVersion,
                "Final canonical/provider/checkpoint evidence failed",
              );
              const prompts = [];

              for (const audit of proof.audits.filter((audit) => audit.call === 2)) {
                yield* requireReplayCpu(
                  audit.prompt !== undefined,
                  "Missing captured final provider prompt",
                );
                const text = JSON.stringify(audit.prompt);
                const pattern = /Window: context:run:[^\\]+(?=\\n)/g;

                yield* requireReplayCpu(
                  [...text.matchAll(pattern)].length === 1,
                  "Expected one generated context window ID",
                );

                const digest = yield* sha256(
                  text.replace(pattern, "Window: benchmark-fixed-window"),
                );

                yield* requireReplayCpu(
                  !promptDigests.has(audit.phase) || promptDigests.get(audit.phase) === digest,
                  "Provider prompt differs across comparisons",
                );
                promptDigests.set(audit.phase, digest);
                prompts.push(digest);
              }
              yield* requireReplayCpu(prompts.length === 10, "Missing final prompt captures");
              completed.push({ role, cohort: cohort.cohort, proof, elapsed, prompts });
            }).pipe(Effect.result);

            if (attempt._tag === "Failure") {
              const error = String(attempt.failure).replaceAll(
                Redacted.value(deployment.token),
                "[redacted]",
              );

              failures.push({ role, cohort: cohort.cohort, size, error });
              yield* writeReplayCpuJson(path.join(directory, "failure.json"), { error });
            }
            yield* Console.log(
              `Block ${block.block + 1}, cohort ${cohort.cohort + 1}, ${role}/${size}: ${attempt._tag === "Success" ? "10 operations and final proof passed" : "failed; evidence retained"}`,
            );
          }
        }
        yield* writeReplayCpuJson(
          path.join(output, `block-${block.block}`, "failures.json"),
          failures,
        );
        for (const role of roles) {
          const target = targets.get(role);

          if (target === undefined)
            return yield* new ReplayCpuError({ message: "Missing deployed role" });
          const expected = completed.filter((item) => item.role === role);
          let events: ReplayCpuTelemetry = [];
          let complete = false;

          for (let poll = 0; poll < 13; poll++) {
            const query: Schema.Json = {
              queryId: target.name,
              dry: true,
              view: "events",
              limit: 2000,
              timeframe: { from: blockStarted - 60_000, to: (yield* now) + 60_000 },
              parameters: {
                filterCombination: "and",
                filters: [
                  {
                    key: "$workers.scriptName",
                    operation: "eq",
                    type: "string",
                    value: target.name,
                  },
                  { key: "$workers.cpuTimeMs", operation: "exists", type: "number" },
                ],
              },
            };

            events = yield* sanitizeReplayCpuTelemetry(yield* deployment.queryTelemetry(query));
            yield* writeReplayCpuJson(
              path.join(output, `block-${block.block}`, role, `telemetry-${poll}.json`),
              { query, events },
            );
            complete = expected.every(({ proof }) =>
              REPLAY_CPU_PHASES.every((method) => {
                const matches = telemetryFor(events, proof.objectId, method);

                return (
                  matches.length === 1 &&
                  events.filter(
                    (event) =>
                      event.executionModel === "stateless" && event.traceId === matches[0]?.traceId,
                  ).length === 1
                );
              }),
            );
            if (complete) break;
            if (poll < 12) yield* Effect.sleep("10 seconds");
          }
          yield* requireReplayCpu(
            complete,
            "Invocation telemetry incomplete; retained all exports without rerunning workload",
          );
          for (const { cohort, proof, elapsed, prompts } of expected) {
            for (const [index, operation] of proof.operations.entries()) {
              const event = telemetryFor(events, proof.objectId, operation.method)[0];

              if (event === undefined)
                return yield* new ReplayCpuError({ message: "Missing invocation" });

              const ingress = events.find(
                (candidate) =>
                  candidate.executionModel === "stateless" && candidate.traceId === event.traceId,
              );

              if (ingress === undefined)
                return yield* new ReplayCpuError({ message: "Missing ingress invocation" });

              yield* requireReplayCpu(
                event.scriptVersion?.id === target.identity.deploymentVersion &&
                  event.scriptName === target.name &&
                  event.outcome === "ok" &&
                  event.rpcMethods.every((method) => method === operation.method) &&
                  event.truncated === false &&
                  ingress.outcome === "ok" &&
                  ingress.truncated === false,
                "Invocation version, outcome or completeness differs",
              );
              samples.push(
                yield* Schema.decodeUnknownEffect(ReplayCpuSample)({
                  block: block.block,
                  cohort,
                  role,
                  seedRecords: proof.seedRecords,
                  operation,
                  objectId: proof.objectId,
                  invocationId: event.id,
                  traceId: event.traceId,
                  deploymentVersion: target.identity.deploymentVersion,
                  cpuMs: event.cpuTimeMs,
                  workerWallMs: event.wallTimeMs,
                  ingressCpuMs: ingress.cpuTimeMs,
                  clientElapsedMs: elapsed[index],
                  promptSha256: prompts[index],
                }),
              );
            }
          }
        }
        yield* writeReplayCpuJson(path.join(output, "samples.json"), samples);
        yield* deployment.cleanup();
      }
      yield* requireReplayCpu(
        samples.length === 720 &&
          new Set(samples.map((sample) => sample.invocationId)).size === 720,
        "Experiment incomplete; inspect retained failures",
      );
      const summary = summarizeReplayCpu(samples);

      yield* writeReplayCpuJson(path.join(output, "report.json"), {
        ...plan,
        sampleCount: samples.length,
        summary,
        samples,
      });

      const render = (value: { median: number; min: number; max: number }) =>
        `${value.median.toFixed(1)} ms (${value.min}–${value.max})`;

      const lines = [
        "| Seed records | Cycle | Baseline CPU | Candidate CPU | Identical-code control CPU |",
        "|---:|---|---:|---:|---:|",
        ...summary.map(
          (row) =>
            `| ${row.seedRecords} | ${row.cycle} | ${render(row.baseline)} | ${render(row.candidate)} | ${render(row.control)} |`,
        ),
      ];

      yield* fs.writeFileString(path.join(output, "table.md"), lines.join("\n") + "\n");
      yield* Console.log(lines.join("\n"));
    }).pipe(
      Effect.catchCause((cause) =>
        writeReplayCpuJson(path.join(output, "failure.json"), {
          error: Cause.pretty(cause).replaceAll(Redacted.value(deployment.token), "[redacted]"),
        }).pipe(Effect.andThen(Effect.failCause(cause))),
      ),
      Effect.onExit(() => deployment.retire),
    );
  }),
).pipe(
  Command.withDescription(
    "Run a fixed, provider-free Cloudflare CPU comparison through disposable Alchemy stages",
  ),
);

if (import.meta.main)
  NodeRuntime.runMain(
    Command.run(command, { version: "1" }).pipe(
      Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer, FetchHttpClient.layer)),
    ),
  );
