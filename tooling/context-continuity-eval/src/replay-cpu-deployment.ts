import { Config, Crypto, Effect, FileSystem, Path, Redacted, Schema, Stream } from "effect";
import { HttpClient, HttpClientRequest } from "effect/http";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

import { requireReplayCpu, sha256 } from "./replay-cpu-build.ts";
import { ReplayCpuError, ReplayCpuRole, ReplayCpuStage } from "./replay-cpu-contracts.ts";

export const ReplayCpuTarget = Schema.Struct({
  block: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 2 })),
  role: ReplayCpuRole,
  stage: ReplayCpuStage,
  cleanupRequired: Schema.Boolean,
  cleanupComplete: Schema.Boolean,
});

export const ReplayCpuResources = Schema.Struct({
  run: Schema.String.check(Schema.isPattern(/^[a-f0-9]{16}$/)),
  accountDigest: Schema.String,
  privateDirectory: Schema.String,
  targets: Schema.Array(ReplayCpuTarget),
});

const PrivateState = Schema.Struct({
  run: Schema.String,
  accountDigest: Schema.String,
  output: Schema.String,
  baseline: Schema.String,
  candidate: Schema.String,
  token: Schema.String,
});

const credentials = Config.all({
  accountId: Config.schema(
    Schema.String.check(Schema.isPattern(/^[a-f0-9]{32}$/)),
    "CLOUDFLARE_ACCOUNT_ID",
  ),
  apiToken: Config.Redacted("CLOUDFLARE_API_TOKEN"),
});

const Namespaces = Schema.Struct({
  success: Schema.Boolean,
  result: Schema.Array(
    Schema.Struct({
      id: Schema.optionalKey(Schema.NullOr(Schema.String)),
      script: Schema.optionalKey(Schema.NullOr(Schema.String)),
    }),
  ),
});

export const workerName = (target: typeof ReplayCpuTarget.Type) =>
  `effect-agent-cpu-${target.stage}`;

export const writeReplayCpuJson = Effect.fnUntraced(function* (file: string, value: unknown) {
  const fs = yield* FileSystem.FileSystem;

  yield* fs.writeFileString(`${file}.tmp`, JSON.stringify(value, null, 2) + "\n");
  yield* fs.rename(`${file}.tmp`, file);
});

/** State contains secrets. Keep it private and retain it until remote deletion is verified. */
export const prepareReplayCpuResources = Effect.fn("ReplayCpu.prepareResources")(function* (
  output: string,
  baseline: string,
  candidate: string,
  run: string,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const crypto = yield* Crypto.Crypto;
  const config = yield* credentials;
  const accountDigest = yield* sha256(config.accountId);
  const privateDirectory = yield* fs.makeTempDirectory({ prefix: "effect-agent-cpu-" });

  yield* fs.chmod(privateDirectory, 0o700);

  const repository = path.resolve(
    path.dirname(yield* path.fromFileUrl(new URL(import.meta.url))),
    "../../..",
  );

  yield* fs.symlink(
    path.join(repository, "node_modules"),
    path.join(privateDirectory, "node_modules"),
  );
  yield* fs.writeFileString(
    path.join(privateDirectory, "package.json"),
    '{"private":true,"type":"module"}\n',
  );

  const privateState = {
    run,
    accountDigest,
    output,
    baseline,
    candidate,
    token:
      (yield* crypto.randomUUIDv4).replaceAll("-", "") +
      (yield* crypto.randomUUIDv4).replaceAll("-", ""),
  };

  yield* writeReplayCpuJson(path.join(privateDirectory, "private.json"), privateState);
  yield* fs.chmod(path.join(privateDirectory, "private.json"), 0o600);
  const resources = ReplayCpuResources.make({ run, accountDigest, privateDirectory, targets: [] });

  yield* writeReplayCpuJson(path.join(output, "resources.json"), resources);

  return resources;
});

export const openReplayCpuDeployment = Effect.fn("ReplayCpu.openDeployment")(function* (
  output: string,
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const client = yield* HttpClient.HttpClient;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const config = yield* credentials;
  const accountDigest = yield* sha256(config.accountId);
  const executablePath = yield* Config.String("PATH");
  const userHome = yield* Config.String("HOME");
  const stack = yield* path.fromFileUrl(new URL("./replay-cpu.stack.ts", import.meta.url));

  let resources = yield* Schema.decodeEffect(Schema.fromJsonString(ReplayCpuResources))(
    yield* fs.readFileString(path.join(output, "resources.json")),
  );

  yield* requireReplayCpu(
    resources.accountDigest === accountDigest,
    "Cleanup/deployment account differs from ownership receipt",
  );

  const privateState = yield* Schema.decodeEffect(Schema.fromJsonString(PrivateState))(
    yield* fs.readFileString(path.join(resources.privateDirectory, "private.json")),
  );

  yield* requireReplayCpu(
    privateState.run === resources.run &&
      privateState.accountDigest === accountDigest &&
      privateState.output === output,
    "Private state does not match this experiment",
  );
  for (const target of resources.targets)
    yield* requireReplayCpu(
      target.stage === `${resources.run}-b${target.block}-${target.role}`,
      "Invalid stage ownership",
    );

  const save = Effect.fnUntraced(function* (target: typeof ReplayCpuTarget.Type) {
    resources = {
      ...resources,
      targets: [...resources.targets.filter((item) => item.stage !== target.stage), target],
    };
    yield* writeReplayCpuJson(path.join(output, "resources.json"), resources);
  });

  const api = (route: string) =>
    client
      .execute(
        HttpClientRequest.get(
          `https://api.cloudflare.com/client/v4/accounts/${config.accountId}/${route}`,
        ).pipe(HttpClientRequest.bearerToken(config.apiToken)),
      )
      .pipe(Effect.timeout("30 seconds"));

  const exists = Effect.fnUntraced(function* (target: typeof ReplayCpuTarget.Type) {
    const response = yield* api(`workers/scripts/${workerName(target)}`);

    yield* requireReplayCpu(
      response.status === 404 || response.status === 200,
      "Cannot establish Worker existence",
    );

    return response.status === 200;
  });

  const namespaces = Effect.fnUntraced(function* (target: typeof ReplayCpuTarget.Type) {
    const matching: Array<string> = [];

    for (let page = 1; page <= 100; page++) {
      const response = yield* api(`workers/durable_objects/namespaces?page=${page}&per_page=100`);

      yield* requireReplayCpu(response.status === 200, "Cannot verify Durable Object namespaces");
      const result = yield* Schema.decodeUnknownEffect(Namespaces)(yield* response.json);

      yield* requireReplayCpu(result.success, "Namespace listing failed");
      for (const item of result.result.filter((item) => item.script === workerName(target))) {
        if (typeof item.id !== "string")
          return yield* new ReplayCpuError({ message: "Owned namespace has no identity" });
        matching.push(item.id);
      }
      if (result.result.length < 100) return matching;
    }

    return yield* new ReplayCpuError({ message: "Namespace listing exceeded its bound" });
  });

  const alchemy = Effect.fn("ReplayCpu.alchemy")(function* (
    target: typeof ReplayCpuTarget.Type,
    action: "deploy" | "destroy",
  ) {
    let captured = "";
    const directory = path.join(output, `block-${target.block}`, target.role);

    yield* fs.makeDirectory(directory, { recursive: true });
    yield* Effect.scoped(
      Effect.gen(function* () {
        const handle = yield* spawner.spawn(
          ChildProcess.make(
            "vp",
            ["exec", "alchemy", action, stack, "--stage", target.stage, "--yes"],
            {
              cwd: resources.privateDirectory,
              stdin: "ignore",
              stdout: "pipe",
              stderr: "pipe",
              extendEnv: false,
              env: {
                PATH: executablePath,
                HOME: userHome,
                CI: "true",
                NO_COLOR: "1",
                ALCHEMY_HOME: path.join(resources.privateDirectory, "auth"),
                CLOUDFLARE_ACCOUNT_ID: config.accountId,
                CLOUDFLARE_API_TOKEN: Redacted.value(config.apiToken),
                REPLAY_CPU_RUN: target.stage,
                REPLAY_CPU_BUNDLE: path.join(
                  target.role === "candidate" ? privateState.candidate : privateState.baseline,
                  "worker.mjs",
                ),
                REPLAY_CPU_TOKEN: privateState.token,
              },
            },
          ),
        );

        yield* Stream.runForEach(handle.all, (bytes) =>
          Effect.gen(function* () {
            captured += new TextDecoder().decode(bytes);
            yield* requireReplayCpu(captured.length < 512 * 1024, "Alchemy output bound exceeded");
          }),
        );
        yield* requireReplayCpu(
          Number(yield* handle.exitCode) === 0,
          `Alchemy ${action} failed; inspect ${directory}/${action}.log`,
        );
      }),
    ).pipe(
      Effect.timeout("4 minutes"),
      Effect.ensuring(
        Effect.suspend(() =>
          fs.writeFileString(
            path.join(directory, `${action}.log`),
            captured
              .replaceAll(Redacted.value(config.apiToken), "[redacted]")
              .replaceAll(privateState.token, "[redacted]")
              .replaceAll(config.accountId, "[account]"),
            { flag: "a" },
          ),
        ).pipe(Effect.orDie),
      ),
    );
  });

  const remove = Effect.fn("ReplayCpu.remove")(function* (target: typeof ReplayCpuTarget.Type) {
    yield* alchemy(target, "destroy");
    yield* requireReplayCpu(!(yield* exists(target)), `Worker still exists: ${workerName(target)}`);
    yield* requireReplayCpu(
      (yield* namespaces(target)).length === 0,
      `Durable Object namespace still exists: ${workerName(target)}`,
    );
    yield* save({ ...target, cleanupComplete: true });
  });

  const cleanup = Effect.fn("ReplayCpu.cleanup")(function* () {
    const results = yield* Effect.forEach(
      resources.targets.filter((target) => target.cleanupRequired && !target.cleanupComplete),
      (target) => remove(target).pipe(Effect.result),
      { concurrency: 1 },
    );

    yield* requireReplayCpu(
      results.every((result) => result._tag === "Success"),
      "Cleanup incomplete; retain private state and rerun --cleanup",
    );
  });

  return {
    token: Redacted.make(privateState.token),
    queryTelemetry: Effect.fn("ReplayCpu.telemetryQuery")(function* (query: Schema.Json) {
      const request = yield* HttpClientRequest.post(
        `https://api.cloudflare.com/client/v4/accounts/${config.accountId}/workers/observability/telemetry/query`,
      ).pipe(HttpClientRequest.bearerToken(config.apiToken), HttpClientRequest.bodyJson(query));

      const response = yield* client.execute(request).pipe(Effect.timeout("40 seconds"));

      yield* requireReplayCpu(response.status === 200, "Cloudflare telemetry query failed");

      return yield* response.json;
    }),
    deploy: Effect.fn("ReplayCpu.deploy")(function* (
      block: number,
      role: typeof ReplayCpuRole.Type,
    ) {
      const target = ReplayCpuTarget.make({
        block,
        role,
        stage: Schema.decodeSync(ReplayCpuStage)(`${resources.run}-b${block}-${role}`),
        cleanupRequired: true,
        cleanupComplete: false,
      });

      yield* requireReplayCpu(
        !resources.targets.some((item) => item.stage === target.stage),
        "Stage already attempted",
      );
      yield* requireReplayCpu(
        !(yield* exists(target)),
        `Worker already exists: ${workerName(target)}`,
      );
      yield* Effect.uninterruptible(save(target));
      yield* alchemy(target, "deploy");
      yield* requireReplayCpu(yield* exists(target), "Deployed Worker is missing");
      const ids = yield* namespaces(target);

      yield* requireReplayCpu(
        ids.length === 1,
        "Expected one isolated SQLite Durable Object namespace",
      );
      const subdomainResponse = yield* api("workers/subdomain");

      yield* requireReplayCpu(subdomainResponse.status === 200, "Cannot read Workers subdomain");

      const subdomain = yield* Schema.decodeUnknownEffect(
        Schema.Struct({
          success: Schema.Literal(true),
          result: Schema.Struct({
            subdomain: Schema.String.check(Schema.isPattern(/^[a-z0-9-]+$/)),
          }),
        }),
      )(yield* subdomainResponse.json);

      return {
        ...target,
        name: workerName(target),
        url: `https://${workerName(target)}.${subdomain.result.subdomain}.workers.dev`,
        namespaces: ids,
      };
    }),
    cleanup,
    retire: Effect.gen(function* () {
      yield* cleanup();
      yield* requireReplayCpu(
        resources.targets.every((target) => target.cleanupComplete),
        "Remote cleanup is not complete",
      );
      yield* fs.remove(resources.privateDirectory, { recursive: true });
      yield* writeReplayCpuJson(path.join(output, "cleanup.json"), {
        complete: true,
        secretRemoved: true,
        targets: resources.targets,
      });
    }),
  };
});
