import { expect, layer } from "@effect/vitest";

import { Cause, Deferred, Duration, Effect, Exit, Fiber, Option, Ref, Stream } from "effect";
import { Sandbox, type SandboxEvent, type SandboxRequest } from "@effect-agent/sandbox";

import { layer as localSandboxLayer } from "../src/index.ts";

const request = (
  args: ReadonlyArray<string>,
  overrides: Partial<SandboxRequest> = {},
): SandboxRequest => ({
  runtime: { kind: "unisolated-process", identity: "node" },
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
    Effect.gen(function* () {
      const sandbox = yield* Sandbox;
      const exit = yield* sandbox
        .execute(
          request(["-e", "process.stderr.write('12345')"], {
            limits: { maxOutputBytes: 4, maxWallTime: Duration.seconds(5) },
          }),
        )
        .pipe(Stream.runDrain, Effect.exit);

      expect(failureFrom(exit)).toMatchObject({
        _tag: "SandboxOutputLimitError",
        stream: "stderr",
        limit: 4,
        observed: 5,
      });
    }),
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

      expect(failureFrom(exit)).toMatchObject({ _tag: "SandboxSpawnError" });
    }),
  );

  it.effect("copies only explicitly allowed environment variables", () =>
    Effect.gen(function* () {
      const sandbox = yield* Sandbox;
      const events = yield* sandbox
        .execute(
          request(
            [
              "-e",
              "process.stdout.write(JSON.stringify({ hasPath: typeof process.env.PATH === 'string', hasHome: typeof process.env.HOME === 'string' }))",
            ],
            { environment: { allow: ["PATH"] } },
          ),
        )
        .pipe(Stream.runCollect);
      const stdout = events
        .flatMap((event) =>
          event._tag === "SandboxOutput" && event.stream === "stdout" ? [event.text] : [],
        )
        .join("");

      expect(JSON.parse(stdout)).toEqual({ hasPath: true, hasHome: false });
    }),
  );

  it.effect(
    "fails wall-clock timeout through the typed channel and finalizes the process scope",
    () =>
      Effect.gen(function* () {
        const sandbox = yield* Sandbox;
        const exit = yield* sandbox
          .execute(
            request(["-e", "setInterval(() => undefined, 1_000)"], {
              limits: { maxOutputBytes: 1_024, maxWallTime: Duration.millis(100) },
            }),
          )
          .pipe(Stream.runDrain, Effect.exit);

        expect(failureFrom(exit)).toMatchObject({ _tag: "SandboxTimeoutError" });
      }),
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
      const processAlive = yield* Effect.sync(() => {
        try {
          process.kill(pid, 0);
          return true;
        } catch {
          return false;
        }
      });

      expect(Exit.isFailure(exit)).toBe(true);
      if (Exit.isFailure(exit)) {
        expect(Cause.hasInterrupts(exit.cause)).toBe(true);
      }
      expect(processAlive).toBe(false);
    }),
  );
});
