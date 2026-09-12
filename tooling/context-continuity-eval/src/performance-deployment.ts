import { Config, Context, Effect, FileSystem, Path, Redacted, Schema, Stream } from "effect";
import { HttpClient, HttpClientRequest } from "effect/unstable/http";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import { type PerformanceBuild } from "./build-performance-cloudflare.ts";
import { EvaluationError } from "./contracts.ts";
import { type ModelId } from "./live-model.ts";

export const PerformanceTarget = Schema.Struct({
  label: Schema.Literals(["candidate", "reference"]),
  name: Schema.String.check(
    Schema.isPattern(/^effect-agent-perf-[a-f0-9]{32}-(candidate|reference)$/),
  ),
  url: Schema.String,
  directory: Schema.String,
  sourceCommit: Schema.String,
  cleanupRequired: Schema.Boolean,
  cleanupComplete: Schema.Boolean,
});

export const PerformanceResources = Schema.Struct({
  // Match cleanup credentials without publishing the account identifier in artifacts.
  accountDigest: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
  targets: Schema.Array(PerformanceTarget),
});

export class PerformanceDeployment extends Context.Service<
  PerformanceDeployment,
  {
    exists(target: typeof PerformanceTarget.Type): Effect.Effect<boolean, EvaluationError>;
    deploy(target: typeof PerformanceTarget.Type): Effect.Effect<void, EvaluationError>;
    remove(target: typeof PerformanceTarget.Type): Effect.Effect<void, EvaluationError>;
  }
>()("example/PerformanceDeployment") {}

export class PerformanceOwnership extends Context.Service<
  PerformanceOwnership,
  {
    saveTarget(target: typeof PerformanceTarget.Type): Effect.Effect<void, EvaluationError>;
  }
>()("example/PerformanceOwnership") {}

/** Persist ownership before deployment: an interrupted upload may already have created resources. */
export const withPerformanceDeployment = Effect.fn("Performance.withDeployment")(function* <
  A,
  E,
  R,
>(target: typeof PerformanceTarget.Type, use: Effect.Effect<A, E, R>) {
  const operations = yield* PerformanceDeployment;
  const ownership = yield* PerformanceOwnership;

  if (yield* operations.exists(target))
    return yield* EvaluationError.make({
      stage: "collision",
      message: `Disposable Worker already exists: ${target.name}`,
    });
  const owned = { ...target, cleanupRequired: true };

  return yield* Effect.uninterruptibleMask((restore) =>
    Effect.gen(function* () {
      yield* ownership.saveTarget(owned);

      return yield* restore(operations.deploy(owned).pipe(Effect.andThen(use))).pipe(
        Effect.onExit(() =>
          operations
            .remove(owned)
            .pipe(Effect.andThen(ownership.saveTarget({ ...owned, cleanupComplete: true }))),
        ),
      );
    }),
  );
});

export const loadPerformanceConfig = Config.all({
  accountId: Config.schema(
    Schema.String.check(Schema.isPattern(/^[a-f0-9]{32}$/)),
    "CLOUDFLARE_ACCOUNT_ID",
  ),
  apiToken: Config.redacted("CLOUDFLARE_API_TOKEN"),
  subdomain: Config.schema(
    Schema.String.check(Schema.isPattern(/^[a-z0-9-]{1,63}$/)),
    "CLOUDFLARE_WORKERS_SUBDOMAIN",
  ),
});

export const preparePerformanceTarget = Effect.fn("Performance.prepareTarget")(function* (options: {
  build: typeof PerformanceBuild.Type;
  label: "candidate" | "reference";
  run: string;
  model: ModelId;
  samples: number;
  subdomain: string;
}) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const name = `effect-agent-perf-${options.run}-${options.label}`;

  const target = yield* Schema.decodeEffect(PerformanceTarget)({
    label: options.label,
    name,
    url: `https://${name}.${options.subdomain}.workers.dev`,
    directory: options.build.directory,
    sourceCommit: options.build.sourceCommit,
    cleanupRequired: false,
    cleanupComplete: false,
  });

  const common = {
    name,
    compatibility_date: "2026-08-01",
    compatibility_flags: ["nodejs_compat"],
    workers_dev: true,
    preview_urls: false,
  };

  yield* fs.writeFileString(
    path.join(target.directory, "wrangler.json"),
    JSON.stringify({
      ...common,
      main: "worker.mjs",
      observability: { enabled: true, head_sampling_rate: 1 },
      version_metadata: { binding: "PERFORMANCE_VERSION" },
      vars: {
        PERFORMANCE_MODEL: options.model,
        PERFORMANCE_RUN: options.run,
        PERFORMANCE_SAMPLES: String(options.samples),
      },
      durable_objects: {
        bindings: [{ name: "PERFORMANCE_THREADS", class_name: "PerformanceThread" }],
      },
      exports: { PerformanceThread: { type: "durable-object", storage: "sqlite" } },
    }),
  );
  yield* writeCleanupConfig(target);

  return target;
});

const writeCleanupConfig = Effect.fn("Performance.writeCleanupConfig")(function* (
  target: typeof PerformanceTarget.Type,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;

  yield* fs.makeDirectory(target.directory, { recursive: true });

  const common = {
    name: target.name,
    compatibility_date: "2026-08-01",
    compatibility_flags: ["nodejs_compat"],
    workers_dev: true,
    preview_urls: false,
  };

  yield* fs.writeFileString(
    path.join(target.directory, "cleanup.mjs"),
    "export default { fetch() { return new Response('Evaluation resources retired', { status: 410 }); } };\n",
  );
  yield* fs.writeFileString(
    path.join(target.directory, "cleanup.json"),
    JSON.stringify({
      ...common,
      main: "cleanup.mjs",
      exports: { PerformanceThread: { type: "durable-object", state: "deleted" } },
    }),
  );
});

/** Wrangler owns uploads and namespace retirement. Secret file is scoped outside the evidence directory. */
export const makePerformanceDeployment = Effect.fn("Performance.deployment")(function* (
  secretsFile: string | undefined,
  sensitiveValues: ReadonlyArray<Redacted.Redacted<string>> = [],
) {
  const config = yield* loadPerformanceConfig;
  const client = yield* HttpClient.HttpClient;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const cwd = path.resolve(path.dirname(yield* path.fromFileUrl(new URL(import.meta.url))), "..");
  const executableSearchPath = yield* Config.string("PATH");
  const userHome = yield* Config.string("HOME");

  const run = (
    target: typeof PerformanceTarget.Type,
    operation: string,
    args: ReadonlyArray<string>,
  ) =>
    Effect.suspend(() => {
      let output = "";

      return Effect.scoped(
        Effect.gen(function* () {
          const handle = yield* spawner.spawn(
            ChildProcess.make("vp", ["exec", "wrangler", ...args], {
              cwd,
              stdin: "ignore",
              stdout: "pipe",
              stderr: "pipe",
              extendEnv: false,
              env: {
                PATH: executableSearchPath,
                HOME: userHome,
                CLOUDFLARE_ACCOUNT_ID: config.accountId,
                CLOUDFLARE_API_TOKEN: Redacted.value(config.apiToken),
                CI: "true",
                NO_COLOR: "1",
                WRANGLER_SEND_METRICS: "false",
                WRANGLER_WRITE_LOGS: "false",
              },
            }),
          );

          yield* Stream.runFoldEffect(
            handle.all,
            () => "",
            (all, bytes) => {
              const next = all + new TextDecoder().decode(bytes);

              output = next;

              return next.length > 256 * 1024
                ? Effect.fail(
                    EvaluationError.make({
                      stage: operation,
                      message: "Wrangler output bound exceeded",
                    }),
                  )
                : Effect.succeed(next);
            },
          );
          if (Number(yield* handle.exitCode) !== 0)
            return yield* EvaluationError.make({
              stage: operation,
              message: `Wrangler ${operation} failed for ${target.name}; inspect the retained log`,
            });
        }),
      ).pipe(
        Effect.timeout("3 minutes"),
        Effect.ensuring(
          Effect.suspend(() =>
            fs.writeFileString(
              path.join(target.directory, `${operation}.log`),
              [config.apiToken, ...sensitiveValues]
                .reduce(
                  (text, value) => text.replaceAll(Redacted.value(value), "[redacted]"),
                  output,
                )
                .replaceAll(config.accountId, "[account]")
                .slice(0, 256 * 1024),
              { flag: "a" },
            ),
          ).pipe(Effect.orDie),
        ),
        Effect.mapError(() =>
          EvaluationError.make({
            stage: operation,
            message: `Wrangler ${operation} failed for ${target.name}; cleanup may need retry`,
          }),
        ),
      );
    });

  const exists = (target: typeof PerformanceTarget.Type) =>
    client
      .execute(
        HttpClientRequest.get(
          `https://api.cloudflare.com/client/v4/accounts/${config.accountId}/workers/scripts/${target.name}`,
        ).pipe(HttpClientRequest.bearerToken(config.apiToken)),
      )
      .pipe(
        Effect.timeout("20 seconds"),
        Effect.flatMap((response) =>
          response.status === 404
            ? Effect.succeed(false)
            : response.status >= 200 && response.status < 300
              ? Effect.succeed(true)
              : Effect.fail(
                  EvaluationError.make({
                    stage: "collision",
                    message: "Could not establish disposable Worker ownership",
                  }),
                ),
        ),
        Effect.mapError(() =>
          EvaluationError.make({
            stage: "collision",
            message: "Cloudflare Worker existence check failed",
          }),
        ),
      );

  return PerformanceDeployment.of({
    exists,
    deploy: (target) =>
      secretsFile === undefined
        ? Effect.fail(
            EvaluationError.make({
              stage: "secrets",
              message: "Deployment requires a scoped secret file",
            }),
          )
        : run(target, "deploy", [
            "deploy",
            "--config",
            path.join(target.directory, "wrangler.json"),
            "--no-bundle",
            "--secrets-file",
            secretsFile,
          ]),
    remove: (target) =>
      Effect.gen(function* () {
        if (!(yield* exists(target))) return;
        // Downloaded artifact configuration cannot redirect deletion.
        yield* writeCleanupConfig(target).pipe(
          Effect.provideService(FileSystem.FileSystem, fs),
          Effect.provideService(Path.Path, path),
          Effect.mapError(() =>
            EvaluationError.make({
              stage: "cleanup",
              message: "Cannot prepare the fixed namespace deletion config",
            }),
          ),
        );
        // This removes the DO namespace and stored data before removing the Worker.
        yield* run(target, "delete-namespace", [
          "deploy",
          "--config",
          path.join(target.directory, "cleanup.json"),
          "--no-bundle",
        ]);
        // The fixture owns no KV assets. Wrangler's delete command also scans legacy KV
        // namespaces after deleting the Worker, requiring unrelated account permissions.
        yield* client
          .execute(
            HttpClientRequest.delete(
              `https://api.cloudflare.com/client/v4/accounts/${config.accountId}/workers/scripts/${target.name}`,
            ).pipe(HttpClientRequest.bearerToken(config.apiToken)),
          )
          .pipe(
            Effect.timeout("20 seconds"),
            Effect.flatMap((response) =>
              response.status === 404 || (response.status >= 200 && response.status < 300)
                ? Effect.void
                : Effect.fail(
                    EvaluationError.make({
                      stage: "delete-worker",
                      message: `Worker deletion failed: ${target.name}`,
                    }),
                  ),
            ),
            Effect.mapError(() =>
              EvaluationError.make({
                stage: "delete-worker",
                message: `Worker deletion failed: ${target.name}; cleanup may need retry`,
              }),
            ),
          );
        if (yield* exists(target))
          return yield* EvaluationError.make({
            stage: "cleanup",
            message: `Worker deletion was not confirmed: ${target.name}`,
          });
      }),
  });
});
