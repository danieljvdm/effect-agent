import {
  Cause,
  Config,
  Crypto,
  Duration,
  Effect,
  Exit,
  Option,
  Path,
  Redacted,
  Ref,
  Schema,
  Stream,
} from "effect";
import { FetchHttpClient, HttpClient, HttpClientRequest } from "effect/unstable/http";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";

import { BrowserRunWorkerProofFailure, BrowserRunWorkerProofResult } from "./contract.ts";

const CLOUDFLARE_ACCOUNT_ID = "CLOUDFLARE_ACCOUNT_ID";
const CLOUDFLARE_API_TOKEN = "CLOUDFLARE_API_TOKEN";
const BROWSER_RENDERING_API_TOKEN = "BROWSER_RENDERING_API_TOKEN";
const CLOUDFLARE_WORKERS_SUBDOMAIN = "CLOUDFLARE_WORKERS_SUBDOMAIN";
const MAX_PROCESS_OUTPUT_BYTES = 64 * 1_024;
const PROCESS_TIMEOUT = Duration.seconds(120);
const DEPLOYMENT_PROPAGATION_DELAY = Duration.seconds(15);
const INVOCATION_TIMEOUT = Duration.seconds(150);

const AccountId = Schema.NonEmptyString.check(
  Schema.isMaxLength(64),
  Schema.isPattern(/^[a-f0-9]{32}$/),
);

const WorkersSubdomain = Schema.NonEmptyString.check(
  Schema.isMaxLength(63),
  Schema.isPattern(/^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/),
);

const WorkerName = Schema.NonEmptyString.check(
  Schema.isMaxLength(63),
  Schema.isPattern(/^effect-agent-browser-proof-[a-f0-9]{32}$/),
);

export class WorkerProofError extends Schema.TaggedError<WorkerProofError>()("WorkerProofError", {
  reason: Schema.Literals([
    "configuration",
    "name-generation",
    "name-check",
    "name-collision",
    "deployment",
    "invocation",
    "invocation-timeout",
    "response",
    "deletion",
  ]),
  operation: Schema.String,
  message: Schema.String,
  status: Schema.optionalKey(Schema.Int),
  cause: Schema.optionalKey(Schema.Defect()),
}) {}

export interface WorkerDeploymentOperations {
  readonly nameExists: (name: string) => Effect.Effect<boolean, WorkerProofError>;
  readonly deploy: (name: string) => Effect.Effect<void, WorkerProofError>;
  readonly invoke: (name: string) => Effect.Effect<BrowserRunWorkerProofResult, WorkerProofError>;
  readonly delete: (name: string) => Effect.Effect<void, WorkerProofError>;
}

const proofConfig = Config.all({
  accountId: Config.schema(AccountId, CLOUDFLARE_ACCOUNT_ID),
  apiToken: Config.redacted(CLOUDFLARE_API_TOKEN),
  browserToken: Config.redacted(BROWSER_RENDERING_API_TOKEN),
  workersSubdomain: Config.schema(WorkersSubdomain, CLOUDFLARE_WORKERS_SUBDOMAIN),
  executableSearchPath: Config.nonEmptyString("PATH"),
  userHome: Config.nonEmptyString("HOME"),
});

const loadProofConfig = proofConfig.pipe(
  Effect.mapError((cause) =>
    WorkerProofError.make({
      reason: "configuration",
      operation: "load Cloudflare proof configuration",
      message: `Set ${CLOUDFLARE_ACCOUNT_ID}, ${CLOUDFLARE_API_TOKEN}, ${BROWSER_RENDERING_API_TOKEN}, and ${CLOUDFLARE_WORKERS_SUBDOMAIN} before running the live proof`,
      cause,
    }),
  ),
);

const workerProofError = (
  reason: WorkerProofError["reason"],
  operation: string,
  message: string,
  options?: { readonly status?: number; readonly cause?: unknown },
): WorkerProofError =>
  WorkerProofError.make({
    reason,
    operation,
    message,
    ...(options?.status === undefined ? {} : { status: options.status }),
    ...(options?.cause === undefined ? {} : { cause: options.cause }),
  });

const runProcess = Effect.fn("BrowserRunWorkerProof.runProcess")(function* (input: {
  readonly spawner: ChildProcessSpawner.ChildProcessSpawner["Service"];
  readonly executable: string;
  readonly args: ReadonlyArray<string>;
  readonly cwd: string;
  readonly env: Readonly<Record<string, string>>;
  readonly operation: "deployment" | "deletion";
}) {
  const process = Effect.scoped(
    Effect.gen(function* () {
      const handle = yield* input.spawner.spawn(
        ChildProcess.make(input.executable, input.args, {
          cwd: input.cwd,
          env: input.env,
          extendEnv: false,
          stdin: "ignore",
          stdout: "pipe",
          stderr: "pipe",
        }),
      );

      const [outputBytes, exitCode] = yield* Effect.all([
        Stream.runFoldEffect(
          handle.all,
          () => 0,
          (observed, chunk) => {
            const next = observed + chunk.length;

            return next <= MAX_PROCESS_OUTPUT_BYTES
              ? Effect.succeed(next)
              : workerProofError(
                  input.operation,
                  `run Wrangler ${input.operation}`,
                  `Wrangler output exceeded ${String(MAX_PROCESS_OUTPUT_BYTES)} bytes`,
                );
          },
        ),
        handle.exitCode,
      ]);

      void outputBytes;
      if (Number(exitCode) !== 0) {
        return yield* workerProofError(
          input.operation,
          `run Wrangler ${input.operation}`,
          `Wrangler ${input.operation} exited with status ${String(exitCode)}`,
          { status: Number(exitCode) },
        );
      }
    }),
  ).pipe(
    Effect.mapError((cause) =>
      Schema.is(WorkerProofError)(cause)
        ? cause
        : workerProofError(
            input.operation,
            `run Wrangler ${input.operation}`,
            `Wrangler ${input.operation} could not run`,
          ),
    ),
  );

  return yield* process.pipe(
    Effect.timeoutOrElse({
      duration: PROCESS_TIMEOUT,
      orElse: () =>
        workerProofError(
          input.operation,
          `run Wrangler ${input.operation}`,
          `Wrangler ${input.operation} exceeded 120 seconds`,
        ),
    }),
  );
});

export const makeLiveOperations = Effect.fn("BrowserRunWorkerProof.makeLiveOperations")(
  function* (): Effect.fn.Return<
    WorkerDeploymentOperations,
    WorkerProofError,
    HttpClient.HttpClient | ChildProcessSpawner.ChildProcessSpawner | Path.Path
  > {
    const config = yield* loadProofConfig;
    const client = yield* HttpClient.HttpClient;
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const path = yield* Path.Path;
    const repositoryRoot = path.resolve(import.meta.dirname, "../../..");
    const exampleRoot = path.join(repositoryRoot, "examples", "browser-run-worker-proof");
    const wranglerExecutable = path.join(exampleRoot, "node_modules", ".bin", "wrangler");
    const wranglerConfig = path.join(exampleRoot, "wrangler.jsonc");

    const subprocessEnv = {
      CLOUDFLARE_ACCOUNT_ID: config.accountId,
      CLOUDFLARE_API_TOKEN: Redacted.value(config.apiToken),
      HOME: config.userHome,
      NO_COLOR: "1",
      PATH: config.executableSearchPath,
      WRANGLER_SEND_METRICS: "false",
      WRANGLER_WRITE_LOGS: "false",
    };

    const scriptUrl = (name: string) =>
      `https://api.cloudflare.com/client/v4/accounts/${config.accountId}/workers/scripts/${name}`;

    const invocationUrl = (name: string) =>
      `https://${name}.${config.workersSubdomain}.workers.dev/`;

    const execute = <E>(
      request: HttpClientRequest.HttpClientRequest,
      mapError: (cause: unknown) => E,
    ) =>
      client
        .execute(HttpClientRequest.bearerToken(request, config.apiToken))
        .pipe(Effect.mapError(mapError));

    const nameExists = Effect.fn("BrowserRunWorkerProof.nameExists")(function* (name: string) {
      const response = yield* execute(HttpClientRequest.get(scriptUrl(name)), (cause) =>
        workerProofError(
          "name-check",
          "check temporary Worker name",
          "Cloudflare could not check whether the temporary Worker name exists",
          { cause },
        ),
      );

      if (response.status === 404) return false;
      if (response.status >= 200 && response.status < 300) return true;

      return yield* workerProofError(
        "name-check",
        "check temporary Worker name",
        `Cloudflare returned HTTP ${String(response.status)} while checking the temporary Worker name`,
        { status: response.status },
      );
    });

    const deploy = (name: string) =>
      runProcess({
        spawner,
        executable: wranglerExecutable,
        args: [
          "deploy",
          "--name",
          name,
          "--config",
          wranglerConfig,
          "--var",
          `CLOUDFLARE_ACCOUNT_ID:${config.accountId}`,
        ],
        cwd: repositoryRoot,
        env: subprocessEnv,
        operation: "deployment",
      });

    const invoke = Effect.fn("BrowserRunWorkerProof.invoke")(function* (name: string) {
      // The deployment finalizer is registered before provisioning its secret.
      // This request's body contains a token; never include it or its cause in diagnostics.
      const secretResponse = yield* execute(
        HttpClientRequest.put(`${scriptUrl(name)}/secrets`).pipe(
          HttpClientRequest.bodyJsonUnsafe({
            name: BROWSER_RENDERING_API_TOKEN,
            text: Redacted.value(config.browserToken),
            type: "secret_text",
          }),
        ),
        () =>
          workerProofError(
            "deployment",
            "provision browser token",
            "Cloudflare could not provision the temporary Worker's browser token",
          ),
      ).pipe(
        Effect.withTracerEnabled(false),
        Effect.provideService(FetchHttpClient.RequestInit, { redirect: "error" }),
      );

      if (secretResponse.status < 200 || secretResponse.status >= 300) {
        return yield* workerProofError(
          "deployment",
          "provision browser token",
          "Cloudflare rejected the temporary Worker's browser token",
          { status: secretResponse.status },
        );
      }
      yield* Effect.sleep(DEPLOYMENT_PROPAGATION_DELAY);

      const response = yield* client
        .execute(HttpClientRequest.get(invocationUrl(name)))
        .pipe(
          Effect.mapError((cause) =>
            workerProofError(
              "invocation",
              "invoke temporary Worker",
              "The temporary Worker invocation failed",
              { cause },
            ),
          ),
        );

      if (response.status < 200 || response.status >= 300) {
        const failure = yield* response.json.pipe(
          Effect.flatMap(Schema.decodeUnknownEffect(BrowserRunWorkerProofFailure)),
          Effect.option,
        );

        return yield* workerProofError(
          "invocation",
          "invoke temporary Worker",
          Option.isSome(failure)
            ? `The temporary Worker proof failed at ${failure.value.stage}${failure.value.cleanupReason === undefined ? "" : ` (${failure.value.cleanupReason}, HTTP ${failure.value.cleanupStatus ?? "none"})`}`
            : `The temporary Worker returned HTTP ${String(response.status)}`,
          { status: response.status },
        );
      }

      const value = yield* response.json.pipe(
        Effect.mapError((cause) =>
          workerProofError(
            "response",
            "decode temporary Worker response",
            "The temporary Worker did not return JSON",
            { cause },
          ),
        ),
      );

      return yield* Schema.decodeUnknownEffect(BrowserRunWorkerProofResult)(value).pipe(
        Effect.mapError((cause) =>
          workerProofError(
            "response",
            "validate temporary Worker response",
            "The temporary Worker did not return the expected Browser Run fact, scrape, screenshot, and interactive metadata",
            { cause },
          ),
        ),
      );
    });

    const deleteWorker = (name: string) =>
      runProcess({
        spawner,
        executable: wranglerExecutable,
        args: ["delete", name, "--config", wranglerConfig, "--force"],
        cwd: repositoryRoot,
        env: subprocessEnv,
        operation: "deletion",
      });

    return { nameExists, deploy, invoke, delete: deleteWorker };
  },
);

export const temporaryWorker = Effect.fn("BrowserRunWorkerProof.temporaryWorker")(function* (
  operations: WorkerDeploymentOperations,
  name: string,
  deletionFailure: Ref.Ref<Option.Option<WorkerProofError>>,
) {
  return yield* Effect.acquireRelease(
    Effect.gen(function* () {
      if (yield* operations.nameExists(name)) {
        return yield* workerProofError(
          "name-collision",
          "acquire temporary Worker",
          `The generated temporary Worker name ${name} already exists`,
        );
      }
      yield* operations.deploy(name);

      return name;
    }),
    (deployedName) =>
      operations.delete(deployedName).pipe(
        Effect.tap(() => Effect.logInfo(`Temporary Worker ${deployedName} was deleted`)),
        Effect.catch((error) => Ref.set(deletionFailure, Option.some(error))),
      ),
  );
});

const makeWorkerName = Effect.fn("BrowserRunWorkerProof.makeWorkerName")(function* () {
  const crypto = yield* Crypto.Crypto;

  const uuid = yield* crypto.randomUUIDv4.pipe(
    Effect.mapError((cause) =>
      workerProofError(
        "name-generation",
        "generate temporary Worker name",
        "A collision-resistant temporary Worker name could not be generated",
        { cause },
      ),
    ),
  );

  return yield* Schema.decodeUnknownEffect(WorkerName)(
    `effect-agent-browser-proof-${uuid.replaceAll("-", "")}`,
  ).pipe(
    Effect.mapError((cause) =>
      workerProofError(
        "name-generation",
        "validate temporary Worker name",
        "The generated temporary Worker name was invalid",
        { cause },
      ),
    ),
  );
});

export const runWorkerProofWith = Effect.fn("BrowserRunWorkerProof.runWorkerProofWith")(function* (
  operations: WorkerDeploymentOperations,
) {
  const name = yield* makeWorkerName();
  const deletionFailure = yield* Ref.make<Option.Option<WorkerProofError>>(Option.none());

  const proofExit = yield* Effect.scoped(
    Effect.gen(function* () {
      const deployedName = yield* temporaryWorker(operations, name, deletionFailure);

      const result = yield* operations.invoke(deployedName).pipe(
        Effect.timeoutOrElse({
          duration: INVOCATION_TIMEOUT,
          orElse: () =>
            workerProofError(
              "invocation-timeout",
              "invoke temporary Worker",
              "The unresolved temporary Worker invocation exceeded 150 seconds and was not retried",
            ),
        }),
      );

      return { name: deployedName, result } as const;
    }),
  ).pipe(Effect.exit);

  const cleanup = yield* Ref.get(deletionFailure);

  if (Option.isSome(cleanup)) {
    if (Exit.isFailure(proofExit)) {
      return yield* Effect.failCause(Cause.combine(proofExit.cause, Cause.fail(cleanup.value)));
    }

    return yield* cleanup.value;
  }

  return yield* proofExit;
});

export const liveWorkerProof = Effect.gen(function* () {
  const operations = yield* makeLiveOperations();

  return yield* runWorkerProofWith(operations);
}).pipe(Effect.provide(FetchHttpClient.layer));
