import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";

import {
  layer as localSandboxLayer,
  sandboxLayer,
} from "@effect-agent/sandbox-local/local-sandbox";
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
  Schema,
  Sink,
  Stream,
  type Scope,
} from "effect";
import { Sandbox, type SandboxEvent, type SandboxRequest } from "effect-agent/sandbox";
import type { PlatformError } from "effect/PlatformError";
import { TestClock } from "effect/testing";
import { ChildProcessSpawner } from "effect/unstable/process";

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
  it.effect.each(["configuration", "spawn"] as const)(
    "times out pending %s setup without emitting process events",
    (phase) =>
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const events: Array<SandboxEvent> = [];
        let interrupted = false;
        let spawned = 0;
        let finalized = 0;

        const pending = Deferred.succeed(entered, undefined).pipe(
          Effect.andThen(Effect.never),
          Effect.onInterrupt(() =>
            Effect.sync(() => {
              interrupted = true;
            }),
          ),
        );

        const spawner = ChildProcessSpawner.make((command) =>
          Effect.gen(function* () {
            spawned++;
            yield* Effect.acquireRelease(Effect.void, () =>
              Effect.sync(() => {
                finalized++;
              }),
            );
            if (phase === "spawn") return yield* pending;

            return yield* scriptedSpawner([]).spawn(command);
          }),
        );

        const provider =
          phase === "configuration"
            ? ConfigProvider.make(() => pending)
            : ConfigProvider.fromEnvRecord({ EFFECT_AGENT_ALLOWED: "visible" });

        const fiber = yield* Effect.gen(function* () {
          const sandbox = yield* Sandbox;

          yield* sandbox
            .execute(
              request([], {
                environment: { allow: ["EFFECT_AGENT_ALLOWED"] },
                limits: { maxOutputBytes: 1_024, maxWallTime: Duration.seconds(1) },
              }),
            )
            .pipe(
              Stream.runForEach((event) =>
                Effect.sync(() => {
                  events.push(event);
                }),
              ),
            );
        }).pipe(
          Effect.provide(sandboxLayer),
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
          Effect.provide(ConfigProvider.layer(provider)),
          Effect.forkChild,
        );

        yield* Deferred.await(entered);
        yield* TestClock.adjust("1 second");
        const exit = yield* Fiber.await(fiber);

        expect(failureFrom(exit)).toMatchObject({ _tag: "SandboxTimeoutError" });
        expect(events).toEqual([]);
        expect(interrupted).toBe(true);
        expect(spawned).toBe(phase === "spawn" ? 1 : 0);
        expect(finalized).toBe(phase === "spawn" ? 1 : 0);
      }),
  );

  it.effect("shares the setup deadline with active output and finalizes the child once", () =>
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const spawning = yield* Deferred.make<void>();
      const started = yield* Deferred.make<void>();
      const output = yield* Deferred.make<void>();
      const events: Array<SandboxEvent> = [];
      const values = ConfigProvider.fromEnvRecord({ EFFECT_AGENT_ALLOWED: "visible" });
      let finalized = 0;

      const provider = ConfigProvider.make((path) =>
        Deferred.succeed(entered, undefined).pipe(
          Effect.andThen(Effect.sleep("200 millis")),
          Effect.andThen(values.load(path)),
        ),
      );

      const stdout = Stream.fromEffectRepeat(
        Effect.sleep("100 millis").pipe(Effect.as(new TextEncoder().encode("output"))),
      );

      const spawner = ChildProcessSpawner.make((command) =>
        Effect.gen(function* () {
          yield* Deferred.succeed(spawning, undefined);
          yield* Effect.sleep("300 millis");

          return yield* Effect.acquireRelease(spawnerWithStdout(stdout).spawn(command), () =>
            Effect.sync(() => {
              finalized++;
            }),
          );
        }),
      );

      const fiber = yield* Effect.gen(function* () {
        const sandbox = yield* Sandbox;

        yield* sandbox
          .execute(
            request([], {
              environment: { allow: ["EFFECT_AGENT_ALLOWED"] },
              limits: { maxOutputBytes: 1_024, maxWallTime: Duration.seconds(1) },
            }),
          )
          .pipe(
            Stream.runForEach((event) =>
              Effect.sync(() => {
                events.push(event);
              }).pipe(
                Effect.andThen(
                  event._tag === "SandboxStarted"
                    ? Deferred.succeed(started, undefined)
                    : event._tag === "SandboxOutput"
                      ? Deferred.succeed(output, undefined)
                      : Effect.void,
                ),
              ),
            ),
          );
      }).pipe(
        Effect.provide(sandboxLayer),
        Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        Effect.provide(ConfigProvider.layer(provider)),
        Effect.forkChild,
      );

      yield* Deferred.await(entered);
      yield* TestClock.adjust("200 millis");
      yield* Deferred.await(spawning);
      yield* TestClock.adjust("300 millis");
      yield* Deferred.await(started);
      yield* TestClock.adjust("100 millis");
      yield* Deferred.await(output);
      yield* TestClock.adjust("300 millis");

      expect(events[0]?._tag).toBe("SandboxStarted");
      expect(events.some((event) => event._tag === "SandboxOutput")).toBe(true);
      expect(finalized).toBe(0);

      yield* TestClock.adjust("100 millis");
      const exit = yield* Fiber.await(fiber);

      expect(failureFrom(exit)).toMatchObject({ _tag: "SandboxTimeoutError" });
      expect(events.some((event) => event._tag === "SandboxExited")).toBe(false);
      expect(finalized).toBe(1);
    }),
  );

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
});
