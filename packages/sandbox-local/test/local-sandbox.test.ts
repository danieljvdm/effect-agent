import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";

import {
  SANDBOX_DIAGNOSTIC_MAX_LENGTH,
  Sandbox,
  type SandboxEvent,
  type SandboxRequest,
} from "@effect-agent/sandbox";
import { describe, expect, it, layer } from "@effect/vitest";
import {
  Cause,
  ConfigProvider,
  Deferred,
  Duration,
  Effect,
  Exit,
  Fiber,
  Layer,
  Option,
  Ref,
  Schema,
  Sink,
  Stream,
  type Scope,
} from "effect";
import { PlatformError, SystemError } from "effect/PlatformError";
import { ChildProcessSpawner } from "effect/unstable/process";

import { layer as localSandboxLayer, sandboxLayer } from "../src/index.ts";

const AllowedEnvironmentResult = Schema.Struct({
  allowed: Schema.String,
  hasHidden: Schema.Boolean,
});

const request = (
  args: ReadonlyArray<string>,
  overrides: Partial<SandboxRequest> = {},
): SandboxRequest => ({
  runtime: { kind: "unisolated-process", identity: "local-process" },
  command: process.execPath,
  args,
  cwd: process.cwd(),
  environment: { allow: [] },
  mounts: [],
  network: { _tag: "NetworkDisabled" },
  limits: {
    maxOutputBytes: 1_024,
    maxWallTime: Duration.seconds(5),
  },
  secretHandles: [],
  artifactRules: [],
  ...overrides,
});

const failureFrom = <E>(exit: Exit.Exit<unknown, E>): E => {
  expect(Exit.isFailure(exit)).toBe(true);
  if (Exit.isSuccess(exit)) {
    throw new Error("Expected the sandbox stream to fail");
  }
  const failure = Cause.findErrorOption(exit.cause);

  expect(Option.isSome(failure)).toBe(true);
  if (Option.isNone(failure)) {
    throw new Error("Expected a typed sandbox error");
  }

  return failure.value;
};

const withTempDirectory = <A, E, R>(
  use: (directory: string) => Effect.Effect<A, E, R>,
): Effect.Effect<A, E, Exclude<R, Scope.Scope>> =>
  Effect.scoped(
    Effect.acquireRelease(
      Effect.sync(() => fs.mkdtempSync(path.join(os.tmpdir(), "effect-agent-sandbox-"))),
      (directory) => Effect.sync(() => fs.rmSync(directory, { recursive: true, force: true })),
    ).pipe(Effect.flatMap(use)),
  );

/** Writes the child's own pid to `pidFile` before running `script`, so tests can observe it. */
const recordPidThen = (pidFile: string, script: string): string =>
  `require('node:fs').writeFileSync(${JSON.stringify(pidFile)}, String(process.pid)); ${script}`;

const readRecordedPid = (pidFile: string): Effect.Effect<number> =>
  Effect.sync(() => Number.parseInt(fs.readFileSync(pidFile, "utf8"), 10));

const isProcessAlive = (pid: number): Effect.Effect<boolean> =>
  Effect.sync(() => {
    try {
      process.kill(pid, 0);

      return true;
    } catch {
      return false;
    }
  });

layer(localSandboxLayer, { excludeTestServices: true })("unisolated local Sandbox", (it) => {
  it.effect("labels streamed stdout and successful completion as unisolated", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox;

      const events = yield* sandbox
        .execute(request(["-e", "process.stdout.write('hello'); process.stderr.write('warning')"]))
        .pipe(Stream.runCollect);

      expect(events.map((event) => event._tag)).toEqual([
        "SandboxStarted",
        "SandboxOutput",
        "SandboxOutput",
        "SandboxExited",
      ]);
      expect(events[0]).toMatchObject({
        _tag: "SandboxStarted",
        runtime: { kind: "unisolated-process", identity: "local-process" },
      });
      expect(events.every((event) => event.implementation.isolation === "unisolated")).toBe(true);
      expect(
        events.find((event) => event._tag === "SandboxOutput" && event.stream === "stdout"),
      ).toMatchObject({
        text: "hello",
      });
      expect(
        events.find((event) => event._tag === "SandboxOutput" && event.stream === "stderr"),
      ).toMatchObject({
        text: "warning",
      });
    }),
  );

  it.effect("surfaces trailing incomplete UTF-8 stdout with byte accounting intact", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox;

      const events = yield* sandbox
        .execute(request(["-e", "process.stdout.write(Buffer.from([0xe2, 0x82]))"]))
        .pipe(Stream.runCollect);

      const stdout = events.flatMap((event) =>
        event._tag === "SandboxOutput" && event.stream === "stdout" ? [event] : [],
      );

      expect(stdout.map((event) => event.text).join("")).toBe("\uFFFD");
      expect(stdout.reduce((total, event) => total + event.bytes, 0)).toBe(2);
      expect(events.at(-1)).toMatchObject({
        _tag: "SandboxExited",
        exitCode: 0,
        resourceUse: { stdoutBytes: 2 },
      });
    }),
  );

  it.effect("emits the exit record before returning a typed non-zero exit failure", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox;
      const events = yield* Ref.make<ReadonlyArray<SandboxEvent>>([]);

      const exit = yield* sandbox
        .execute(request(["-e", "process.stderr.write('failed'); process.exit(3)"]))
        .pipe(
          Stream.runForEach((event) => Ref.update(events, (all) => [...all, event])),
          Effect.exit,
        );

      expect(failureFrom(exit)).toMatchObject({ _tag: "SandboxExitError", exitCode: 3 });
      expect((yield* Ref.get(events)).at(-1)).toMatchObject({
        _tag: "SandboxExited",
        exitCode: 3,
      });
    }),
  );

  it.effect("enforces a bounded stderr limit and terminates the owned process", () =>
    withTempDirectory((directory) =>
      Effect.gen(function* () {
        const sandbox = yield* Sandbox;
        const pidFile = path.join(directory, "pid");

        const exit = yield* sandbox
          .execute(
            request(
              [
                "-e",
                recordPidThen(
                  pidFile,
                  "process.stderr.write('12345'); setInterval(() => undefined, 1_000)",
                ),
              ],
              {
                limits: { maxOutputBytes: 4, maxWallTime: Duration.seconds(5) },
              },
            ),
          )
          .pipe(Stream.runDrain, Effect.exit);

        expect(failureFrom(exit)).toMatchObject({
          _tag: "SandboxOutputLimitError",
          stream: "stderr",
          limit: 4,
          observed: 5,
        });
        const pid = yield* readRecordedPid(pidFile);

        expect(yield* isProcessAlive(pid)).toBe(false);
      }),
    ),
  );

  it.effect("applies the output limit across stdout and stderr together", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox;

      const exit = yield* sandbox
        .execute(
          request(["-e", "process.stdout.write('1234'); process.stderr.write('5678')"], {
            limits: { maxOutputBytes: 5, maxWallTime: Duration.seconds(5) },
          }),
        )
        .pipe(Stream.runDrain, Effect.exit);

      expect(failureFrom(exit)).toMatchObject({
        _tag: "SandboxOutputLimitError",
        limit: 5,
        observed: 8,
      });
    }),
  );

  it.effect("rejects request features the unisolated adapter cannot enforce", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox;

      const exit = yield* sandbox
        .execute(
          request(["-e", "process.exit(0)"], {
            mounts: [{ source: "/tmp", target: "/tmp", access: "read-only" }],
          }),
        )
        .pipe(Stream.runDrain, Effect.exit);

      expect(failureFrom(exit)).toMatchObject({
        _tag: "SandboxUnsupportedRequestError",
        feature: "mounts",
      });
    }),
  );

  it.effect("rejects a runtime identity the local adapter cannot honor", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox;

      const exit = yield* sandbox
        .execute(
          request(["-e", "process.exit(0)"], {
            runtime: { kind: "unisolated-process", identity: "claimed-runtime" },
          }),
        )
        .pipe(Stream.runDrain, Effect.exit);

      expect(failureFrom(exit)).toMatchObject({
        _tag: "SandboxUnsupportedRequestError",
        feature: "runtime",
      });
    }),
  );

  it.effect("returns a typed spawn failure for a missing executable", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox;

      const exit = yield* sandbox
        .execute(
          request([], {
            command: "/effect-agent/definitely-missing-executable",
          }),
        )
        .pipe(Stream.runDrain, Effect.exit);

      expect(failureFrom(exit)).toMatchObject({
        _tag: "SandboxSpawnError",
        cause: expect.anything(),
      });
    }),
  );

  it.effect("copies only explicitly allowed environment variables", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox;

      const provider = ConfigProvider.fromEnvRecord({
        EFFECT_AGENT_ALLOWED: "visible",
        EFFECT_AGENT_HIDDEN: "hidden",
      });

      const events = yield* sandbox
        .execute(
          request(
            [
              "-e",
              "process.stdout.write(JSON.stringify({ allowed: process.env.EFFECT_AGENT_ALLOWED, hasHidden: typeof process.env.EFFECT_AGENT_HIDDEN === 'string' }))",
            ],
            { environment: { allow: ["EFFECT_AGENT_ALLOWED"] } },
          ),
        )
        .pipe(Stream.runCollect, Effect.provide(ConfigProvider.layer(provider)));

      const stdout = events
        .flatMap((event) =>
          event._tag === "SandboxOutput" && event.stream === "stdout" ? [event.text] : [],
        )
        .join("");

      const result = yield* Schema.decodeEffect(Schema.fromJsonString(AllowedEnvironmentResult))(
        stdout,
      );

      expect(result).toEqual({
        allowed: "visible",
        hasHidden: false,
      });
    }),
  );

  it.effect("maps configuration source failures to typed spawn errors", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox;

      const failingProvider = ConfigProvider.make(() =>
        Effect.fail(
          new ConfigProvider.SourceError({
            message: "environment source unavailable",
          }),
        ),
      );

      const exit = yield* sandbox
        .execute(
          request(["-e", "process.exit(0)"], {
            environment: { allow: ["EFFECT_AGENT_ALLOWED"] },
          }),
        )
        .pipe(Stream.runDrain, Effect.provide(ConfigProvider.layer(failingProvider)), Effect.exit);

      expect(failureFrom(exit)).toMatchObject({
        _tag: "SandboxSpawnError",
        message: expect.stringContaining("environment source unavailable"),
        cause: expect.anything(),
      });
    }),
  );

  it.effect(
    "fails wall-clock timeout through the typed channel and finalizes the process scope",
    () =>
      withTempDirectory((directory) =>
        Effect.gen(function* () {
          const sandbox = yield* Sandbox;
          const pidFile = path.join(directory, "pid");

          const exit = yield* sandbox
            .execute(
              request(["-e", recordPidThen(pidFile, "setInterval(() => undefined, 1_000)")], {
                limits: { maxOutputBytes: 1_024, maxWallTime: Duration.millis(500) },
              }),
            )
            .pipe(Stream.runDrain, Effect.exit);

          expect(failureFrom(exit)).toMatchObject({ _tag: "SandboxTimeoutError" });
          const pid = yield* readRecordedPid(pidFile);

          expect(yield* isProcessAlive(pid)).toBe(false);
        }),
      ),
  );

  it.effect("propagates consumer interruption while scope finalization owns process cleanup", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox;
      const startedPid = yield* Deferred.make<number>();

      const fiber = yield* Effect.forkChild(
        sandbox
          .execute(
            request([
              "-e",
              "process.stdout.write(String(process.pid) + '\\n'); setInterval(() => undefined, 1_000)",
            ]),
          )
          .pipe(
            Stream.runForEach((event) =>
              event._tag === "SandboxOutput" && event.stream === "stdout"
                ? Deferred.succeed(startedPid, Number.parseInt(event.text, 10)).pipe(Effect.asVoid)
                : Effect.void,
            ),
          ),
      );

      const pid = yield* Deferred.await(startedPid);

      yield* Fiber.interrupt(fiber);
      const exit = yield* Fiber.await(fiber);
      const processAlive = yield* isProcessAlive(pid);

      expect(Exit.isFailure(exit)).toBe(true);
      if (Exit.isFailure(exit)) {
        expect(Cause.hasInterrupts(exit.cause)).toBe(true);
      }
      expect(processAlive).toBe(false);
    }),
  );
});

const spawnerWithStdout = (
  stdout: Stream.Stream<Uint8Array, PlatformError>,
): ChildProcessSpawner.ChildProcessSpawner["Service"] =>
  ChildProcessSpawner.make(() =>
    Effect.succeed(
      ChildProcessSpawner.makeHandle({
        pid: ChildProcessSpawner.ProcessId(4_242),
        exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(0)),
        isRunning: Effect.succeed(false),
        kill: () => Effect.void,
        stdin: Sink.drain,
        stdout,
        stderr: Stream.empty,
        all: stdout,
        getInputFd: () => Sink.drain,
        getOutputFd: () => Stream.empty,
        unref: Effect.succeed(Effect.void),
      }),
    ),
  );

const scriptedSpawner = (
  stdoutChunks: ReadonlyArray<Uint8Array>,
): ChildProcessSpawner.ChildProcessSpawner["Service"] =>
  spawnerWithStdout(Stream.fromArray(stdoutChunks));

describe("unisolated local Sandbox with an injected spawner double", () => {
  it.effect("decodes UTF-8 sequences split across chunk boundaries and flushes the tail", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox;
      const events = yield* sandbox.execute(request([])).pipe(Stream.runCollect);

      const stdout = events.flatMap((event) =>
        event._tag === "SandboxOutput" && event.stream === "stdout" ? [event] : [],
      );

      expect(stdout.map((event) => event.text).join("")).toBe("\u20AC\uFFFD");
      expect(stdout.reduce((total, event) => total + event.bytes, 0)).toBe(5);
      expect(events.at(-1)).toMatchObject({
        _tag: "SandboxExited",
        exitCode: 0,
        resourceUse: { stdoutBytes: 5, stderrBytes: 0 },
      });
    }).pipe(
      Effect.provide(
        sandboxLayer.pipe(
          Layer.provide(
            Layer.succeed(ChildProcessSpawner.ChildProcessSpawner)(
              // "€" (0xe2 0x82 0xac) split mid-sequence, then a trailing incomplete sequence.
              scriptedSpawner([
                new Uint8Array([0xe2]),
                new Uint8Array([0x82, 0xac]),
                new Uint8Array([0xe2, 0x82]),
              ]),
            ),
          ),
        ),
      ),
    ),
  );

  it.effect("reports post-start output transport failures as bounded exit errors", () => {
    const outputFailure = new PlatformError(
      new SystemError({
        _tag: "Unknown",
        module: "ChildProcess",
        method: "stdout",
        description: "x".repeat(16 * 1024),
      }),
    );

    return Effect.gen(function* () {
      const sandbox = yield* Sandbox;
      const exit = yield* sandbox.execute(request([])).pipe(Stream.runDrain, Effect.exit);
      const failure = failureFrom(exit);

      expect(failure).toMatchObject({
        _tag: "SandboxExitError",
        exitCode: -1,
        cause: outputFailure,
      });
      if (failure._tag !== "SandboxExitError") {
        throw new Error("Expected a SandboxExitError for a post-start output failure");
      }
      expect(failure.message).toHaveLength(SANDBOX_DIAGNOSTIC_MAX_LENGTH);
    }).pipe(
      Effect.provide(
        sandboxLayer.pipe(
          Layer.provide(
            Layer.succeed(ChildProcessSpawner.ChildProcessSpawner)(
              spawnerWithStdout(Stream.fail(outputFailure)),
            ),
          ),
        ),
      ),
    );
  });
});
