import { NodeServices } from "@effect/platform-node";
import { Clock, Duration, Effect, Layer, Ref, Stream } from "effect";
import { ChildProcess } from "effect/unstable/process";
import { ChildProcessSpawner } from "effect/unstable/process/ChildProcessSpawner";
import {
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
  type SandboxError,
  type SandboxEvent,
  type SandboxExecute,
  type SandboxRequest,
} from "@effect-agent/sandbox";

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

const unsupported = (
  feature: Parameters<typeof SandboxUnsupportedRequestError.make>[0]["feature"],
  message: string,
) =>
  SandboxUnsupportedRequestError.make({
    implementation: unisolatedImplementation,
    feature,
    message,
  });

const validateRequest = (request: SandboxRequest): Effect.Effect<void, SandboxError> => {
  if (request.runtime.kind !== "unisolated-process") {
    return Effect.fail(
      unsupported(
        "runtime",
        "The local process runner only accepts runtime.kind 'unisolated-process'.",
      ),
    );
  }
  if (request.mounts.length > 0) {
    return Effect.fail(
      unsupported(
        "mounts",
        "The unisolated local process runner cannot enforce mount access modes.",
      ),
    );
  }
  if (request.network._tag !== "NetworkDisabled") {
    return Effect.fail(
      unsupported(
        "network",
        "The unisolated local process runner cannot enforce workload network policy.",
      ),
    );
  }
  if (request.limits.cpuCores !== undefined) {
    return Effect.fail(
      unsupported("cpu-limit", "The unisolated local process runner cannot enforce CPU limits."),
    );
  }
  if (request.limits.memoryBytes !== undefined) {
    return Effect.fail(
      unsupported(
        "memory-limit",
        "The unisolated local process runner cannot enforce memory limits.",
      ),
    );
  }
  if (request.secretHandles.length > 0) {
    return Effect.fail(
      unsupported(
        "secret-handles",
        "The unisolated local process runner does not resolve secret handles into an environment.",
      ),
    );
  }
  if (request.artifactRules.length > 0) {
    return Effect.fail(
      unsupported("artifacts", "The unisolated local process runner does not collect artifacts."),
    );
  }
  return Effect.void;
};

const environmentFromAllowlist = (allowlist: ReadonlyArray<string>): Record<string, string> => {
  const environment: Record<string, string> = {};
  for (const name of allowlist) {
    const value = process.env[name];
    if (value !== undefined) {
      environment[name] = value;
    }
  }
  return environment;
};

const spawnError = (request: SandboxRequest, message: string) =>
  SandboxSpawnError.make({
    implementation: unisolatedImplementation,
    command: request.command,
    message,
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

const flushDecoder = (stream: OutputStream, decoder: TextDecoder): Stream.Stream<SandboxOutput> => {
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
};

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
        const command = ChildProcess.make(request.command, request.args, {
          cwd: request.cwd,
          env: environmentFromAllowlist(request.environment.allow),
          extendEnv: false,
          stdout: "pipe",
          stderr: "pipe",
        });
        const child = yield* spawner
          .spawn(command)
          .pipe(Effect.mapError((error) => spawnError(request, error.message)));

        const streamOutput = (stream: OutputStream, decoder: TextDecoder) => {
          const source = stream === "stdout" ? child.stdout : child.stderr;
          return source.pipe(
            Stream.mapError((error) => spawnError(request, error.message)),
            Stream.mapEffect((bytes) =>
              outputEvent(counts, stream, bytes, decoder, request.limits.maxOutputBytes),
            ),
            Stream.concat(flushDecoder(stream, decoder)),
          );
        };

        const terminal = Stream.unwrap(
          Effect.gen(function* () {
            const exitCode = yield* child.exitCode.pipe(
              Effect.mapError((error) =>
                SandboxExitError.make({
                  implementation: unisolatedImplementation,
                  exitCode: -1,
                  message: error.message,
                }),
              ),
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
                    SandboxExitError.make({
                      implementation: unisolatedImplementation,
                      exitCode,
                      message: `Unisolated local process exited with code ${exitCode}.`,
                    }),
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
    );

/**
 * Development-only local process adapter. It is unisolated and must never be used as a security
 * boundary for untrusted code or commands.
 */
const localLayer = Layer.effect(Sandbox)(
  Effect.gen(function* () {
    const spawner = yield* ChildProcessSpawner;
    return Sandbox.of({ execute: makeExecute(spawner) });
  }),
);

/** A scoped Node process implementation whose events are always labeled `unisolated`. */
export const layer = localLayer.pipe(Layer.provide(NodeServices.layer));
