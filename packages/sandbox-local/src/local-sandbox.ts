import {
  SANDBOX_DIAGNOSTIC_MAX_LENGTH,
  Sandbox,
  SandboxExited,
  SandboxExitError,
  SandboxImplementation,
  SandboxOutput,
  SandboxOutputLimitError,
  SandboxResourceUse,
  SandboxSpawnError,
  SandboxStarted,
  SandboxTimeoutError,
  SandboxUnsupportedRequestError,
  type SandboxEvent,
  type SandboxExecute,
  type SandboxRequest,
} from "@effect-agent/sandbox";
import { NodeServices } from "@effect/platform-node";
import { Clock, Config, Duration, Effect, Layer, Option, Ref, Stream } from "effect";
import { ChildProcess } from "effect/unstable/process";
import { ChildProcessSpawner } from "effect/unstable/process/ChildProcessSpawner";

/**
 * The only implementation identity produced by this package. It deliberately states that local
 * process execution is unisolated development tooling, not a security sandbox.
 */
export const unisolatedImplementation = SandboxImplementation.make({
  isolation: "unisolated",
  identity: "local-process",
});

type OutputStream = "stdout" | "stderr";
type OutputCounts = Readonly<Record<OutputStream, number>>;

const zeroOutput: OutputCounts = { stdout: 0, stderr: 0 };

const boundedDiagnostic = (message: string): string =>
  message.slice(0, SANDBOX_DIAGNOSTIC_MAX_LENGTH);

const unsupported = (
  feature: Parameters<typeof SandboxUnsupportedRequestError.make>[0]["feature"],
  message: string,
) =>
  SandboxUnsupportedRequestError.make({
    implementation: unisolatedImplementation,
    feature,
    message: boundedDiagnostic(message),
  });

const validateRequest = Effect.fn("LocalSandbox.validateRequest")(function* (
  request: SandboxRequest,
) {
  if (
    request.runtime.kind !== "unisolated-process" ||
    request.runtime.identity !== unisolatedImplementation.identity
  ) {
    return yield* unsupported(
      "runtime",
      "The local process runner only accepts runtime kind 'unisolated-process' with identity 'local-process'.",
    );
  }
  if (request.mounts.length > 0) {
    return yield* unsupported(
      "mounts",
      "The unisolated local process runner cannot enforce mount access modes.",
    );
  }
  if (request.network._tag !== "NetworkDisabled") {
    return yield* unsupported(
      "network",
      "The unisolated local process runner cannot enforce workload network policy.",
    );
  }
  if (request.limits.cpuCores !== undefined) {
    return yield* unsupported(
      "cpu-limit",
      "The unisolated local process runner cannot enforce CPU limits.",
    );
  }
  if (request.limits.memoryBytes !== undefined) {
    return yield* unsupported(
      "memory-limit",
      "The unisolated local process runner cannot enforce memory limits.",
    );
  }
  if (request.secretHandles.length > 0) {
    return yield* unsupported(
      "secret-handles",
      "The unisolated local process runner does not resolve secret handles into an environment.",
    );
  }
  if (request.artifactRules.length > 0) {
    return yield* unsupported(
      "artifacts",
      "The unisolated local process runner does not collect artifacts.",
    );
  }
});

const spawnError = (request: SandboxRequest, message: string, cause?: unknown) =>
  SandboxSpawnError.make({
    implementation: unisolatedImplementation,
    command: request.command,
    message: boundedDiagnostic(message),
    ...(cause === undefined ? {} : { cause }),
  });

const exitError = (message: string, cause?: unknown, exitCode = -1) =>
  SandboxExitError.make({
    implementation: unisolatedImplementation,
    exitCode,
    message: boundedDiagnostic(message),
    ...(cause === undefined ? {} : { cause }),
  });

const environmentFromAllowlist = Effect.fn("LocalSandbox.environmentFromAllowlist")(function* (
  request: SandboxRequest,
) {
  const environment: Record<string, string> = {};

  for (const name of request.environment.allow) {
    const value = yield* Config.option(Config.string(name)).pipe(
      Effect.mapError((error) =>
        spawnError(
          request,
          `Could not read allowed environment variable '${name}': ${error.message}`,
          error,
        ),
      ),
    );

    if (Option.isSome(value)) {
      environment[name] = value.value;
    }
  }

  return environment;
});

const outputEvent = (
  counts: Ref.Ref<OutputCounts>,
  stream: OutputStream,
  bytes: Uint8Array,
  decoder: TextDecoder,
  limit: number,
): Effect.Effect<SandboxOutput, SandboxOutputLimitError> =>
  Ref.modify(counts, (current) => {
    const streamBytes = current[stream] + bytes.byteLength;
    const next = { ...current, [stream]: streamBytes };

    return [next.stdout + next.stderr, next] as const;
  }).pipe(
    Effect.flatMap((observed) =>
      observed > limit
        ? Effect.fail(
            SandboxOutputLimitError.make({
              implementation: unisolatedImplementation,
              stream,
              limit,
              observed,
            }),
          )
        : Effect.succeed(
            SandboxOutput.make({
              eventVersion: 1,
              implementation: unisolatedImplementation,
              stream,
              text: decoder.decode(bytes, { stream: true }),
              bytes: bytes.byteLength,
            }),
          ),
    ),
  );

/**
 * Emits any text the streaming decoder still holds once its source stream ends. The final decode
 * must run at end-of-stream, not at pipeline construction, so the flush is suspended; a trailing
 * incomplete UTF-8 sequence surfaces as replacement text. Its raw bytes were already counted when
 * the chunk arrived, so the flush event carries zero bytes.
 */
const flushDecoder = (stream: OutputStream, decoder: TextDecoder): Stream.Stream<SandboxOutput> =>
  Stream.suspend(() => {
    const text = decoder.decode();

    return text.length === 0
      ? Stream.empty
      : Stream.succeed(
          SandboxOutput.make({
            eventVersion: 1,
            implementation: unisolatedImplementation,
            stream,
            text,
            bytes: 0,
          }),
        );
  });

const makeExecute =
  (spawner: ChildProcessSpawner["Service"]): SandboxExecute =>
  (request) =>
    Stream.unwrap(
      Effect.gen(function* () {
        yield* validateRequest(request);
        const startedAt = yield* Clock.currentTimeMillis;
        const counts = yield* Ref.make(zeroOutput);
        const stdoutDecoder = new TextDecoder();
        const stderrDecoder = new TextDecoder();
        const environment = yield* environmentFromAllowlist(request);

        const command = ChildProcess.make(request.command, request.args, {
          cwd: request.cwd,
          env: environment,
          extendEnv: false,
          stdout: "pipe",
          stderr: "pipe",
        });

        const child = yield* spawner
          .spawn(command)
          .pipe(Effect.mapError((error) => spawnError(request, error.message, error)));

        const streamOutput = (stream: OutputStream, decoder: TextDecoder) => {
          const source = stream === "stdout" ? child.stdout : child.stderr;

          return source.pipe(
            Stream.mapError((error) => exitError(error.message, error)),
            Stream.mapEffect((bytes) =>
              outputEvent(counts, stream, bytes, decoder, request.limits.maxOutputBytes),
            ),
            Stream.concat(flushDecoder(stream, decoder)),
          );
        };

        const terminal = Stream.unwrap(
          Effect.gen(function* () {
            const exitCode = yield* child.exitCode.pipe(
              Effect.mapError((error) => exitError(error.message, error)),
            );

            const endedAt = yield* Clock.currentTimeMillis;
            const output = yield* Ref.get(counts);

            const resourceUse = SandboxResourceUse.make({
              wallTime: Duration.millis(endedAt - startedAt),
              stdoutBytes: output.stdout,
              stderrBytes: output.stderr,
            });

            const exited = SandboxExited.make({
              eventVersion: 1,
              implementation: unisolatedImplementation,
              exitCode,
              resourceUse,
              artifacts: [],
            });

            return exitCode === 0
              ? Stream.succeed<SandboxEvent>(exited)
              : Stream.concat(
                  Stream.succeed<SandboxEvent>(exited),
                  Stream.fail(
                    exitError(
                      `Unisolated local process exited with code ${exitCode}.`,
                      undefined,
                      exitCode,
                    ),
                  ),
                );
          }),
        );

        const execution = Stream.concat(
          Stream.succeed<SandboxEvent>(
            SandboxStarted.make({
              eventVersion: 1,
              implementation: unisolatedImplementation,
              runtime: request.runtime,
            }),
          ),
          Stream.concat(
            Stream.merge(
              streamOutput("stdout", stdoutDecoder),
              streamOutput("stderr", stderrDecoder),
            ),
            terminal,
          ),
        );

        return execution.pipe(
          Stream.interruptWhen(
            Effect.sleep(request.limits.maxWallTime).pipe(
              Effect.andThen(
                Effect.fail(
                  SandboxTimeoutError.make({
                    implementation: unisolatedImplementation,
                    maxWallTime: request.limits.maxWallTime,
                  }),
                ),
              ),
            ),
          ),
        );
      }),
    ).pipe(Stream.withSpan("LocalSandbox.execute"));

/**
 * Development-only local process adapter with its `ChildProcessSpawner` requirement kept visible
 * so composition roots and tests can inject a spawner double. It is unisolated and must never be
 * used as a security boundary for untrusted code or commands.
 */
export const sandboxLayer: Layer.Layer<Sandbox, never, ChildProcessSpawner> = Layer.effect(Sandbox)(
  Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner;

    return Sandbox.of({ execute: makeExecute(spawner) });
  }),
);

/** A scoped Node process implementation whose events are always labeled `unisolated`. */
export const layer: Layer.Layer<Sandbox> = sandboxLayer.pipe(Layer.provide(NodeServices.layer));
