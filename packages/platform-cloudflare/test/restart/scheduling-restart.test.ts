import { NodeFileSystem } from "@effect/platform-node";
import { it } from "@effect/vitest";
import { Clock, Effect, FileSystem, Schema } from "effect";
import { build } from "esbuild";
import { convertV4MiniflareOptions, Miniflare } from "miniflare";
import { expect } from "vite-plus/test";

class RestartHarnessError extends Schema.TaggedError<RestartHarnessError>()("RestartHarnessError", {
  operation: Schema.String,
}) {}

const workerEntry = `${import.meta.dirname}/scheduling-restart-worker.ts`;

const buildWorker = Effect.tryPromise({
  try: () =>
    build({
      entryPoints: [workerEntry],
      bundle: true,
      write: false,
      format: "esm",
      target: "es2022",
      platform: "browser",
      conditions: ["workerd", "worker", "browser"],
      external: ["cloudflare:*", "node:*"],
      logLevel: "silent",
    }),
  catch: () => RestartHarnessError.make({ operation: "bundle restart worker" }),
}).pipe(
  Effect.flatMap((bundled) => {
    const output = bundled.outputFiles[0];
    if (output === undefined) {
      return Effect.fail(RestartHarnessError.make({ operation: "read restart worker bundle" }));
    }
    return Effect.succeed(
      `const __disabledDynamicImport = () => Promise.reject(new Error("dynamic import is disabled in the restart fixture"));\n${output.text.replaceAll(
        /\bimport\s*\(/g,
        "__disabledDynamicImport(",
      )}`,
    );
  }),
);

const openRuntime = (persistDirectory: string, script: string) =>
  Effect.acquireRelease(
    Effect.try({
      try: () =>
        new Miniflare(
          convertV4MiniflareOptions({
            modules: true,
            script,
            compatibilityDate: "2025-05-01",
            compatibilityFlags: ["nodejs_compat"],
            durableObjects: {
              RESTART_THREADS: {
                className: "SchedulingRestartThread",
                useSQLite: true,
              },
              RESTART_SCHEDULES: { className: "SchedulingRestartOwner", useSQLite: true },
            },
            resourcePersistencePath: persistDirectory,
          }),
        ),
      catch: () => RestartHarnessError.make({ operation: "open Miniflare runtime" }),
    }),
    (runtime) =>
      Effect.tryPromise({
        try: () => runtime.dispose(),
        catch: () => RestartHarnessError.make({ operation: "dispose Miniflare runtime" }),
      }).pipe(Effect.orDie),
  );

const call = (runtime: Miniflare, path: string, body?: unknown) =>
  Effect.tryPromise({
    try: () =>
      runtime.dispatchFetch(`http://placeholder${path}`, {
        method: body === undefined ? "GET" : "POST",
        ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      }),
    catch: () => RestartHarnessError.make({ operation: `dispatch ${path}` }),
  }).pipe(
    Effect.flatMap((response) =>
      response.ok
        ? Effect.tryPromise({
            try: () => response.json(),
            catch: () => RestartHarnessError.make({ operation: `decode ${path}` }),
          })
        : Effect.fail(RestartHarnessError.make({ operation: `receive ${path}` })),
    ),
  );

const Introspection = Schema.Struct({
  alarmDeliveries: Schema.Natural,
  completedAlarmDeliveries: Schema.Natural,
  admissionReplyFailures: Schema.Natural,
});
const Status = Schema.Struct({
  delivered: Schema.Boolean,
  pending: Schema.Boolean,
  receiptSubmissionId: Schema.NullOr(Schema.String),
  submissionIds: Schema.Array(Schema.String),
});

const waitForAlarm = (runtime: Miniflare) =>
  Effect.gen(function* () {
    for (let attempt = 0; attempt < 200; attempt += 1) {
      const raw = yield* call(runtime, "/introspect");
      const observed = yield* Schema.decodeUnknownEffect(Introspection)(raw).pipe(
        Effect.mapError(() => RestartHarnessError.make({ operation: "validate introspection" })),
      );
      if (observed.completedAlarmDeliveries > 0) return observed;
      yield* Effect.sleep("50 millis");
    }
    return yield* RestartHarnessError.make({ operation: "wait for persisted Schedule alarm" });
  });

const waitForLostReply = (runtime: Miniflare) =>
  Effect.gen(function* () {
    for (let attempt = 0; attempt < 200; attempt += 1) {
      const raw = yield* call(runtime, "/introspect");
      const observed = yield* Schema.decodeUnknownEffect(Introspection)(raw).pipe(
        Effect.mapError(() => RestartHarnessError.make({ operation: "validate introspection" })),
      );
      if (observed.admissionReplyFailures > 0) return observed;
      yield* Effect.sleep("50 millis");
    }
    return yield* RestartHarnessError.make({ operation: "wait for lost admission reply" });
  });

it.live(
  "re-delivers a persisted Schedule Owner alarm after a full Miniflare restart",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const persistDirectory = yield* fs.makeTempDirectoryScoped({
          prefix: "effect-agent-cf-schedule-restart-",
        });
        const script = yield* buildWorker;
        yield* Effect.scoped(
          Effect.gen(function* () {
            const first = yield* openRuntime(persistDirectory, script);
            yield* call(first, "/introspect");
            const nowMillis = yield* Clock.currentTimeMillis;
            const deadlineAtMillis = nowMillis + 5_000;
            yield* call(first, "/create", { deadlineAtMillis });
          }),
        );

        yield* Effect.scoped(
          Effect.gen(function* () {
            const second = yield* openRuntime(persistDirectory, script);
            yield* waitForAlarm(second);
            const rawStatus = yield* call(second, "/status");
            const status = yield* Schema.decodeUnknownEffect(Status)(rawStatus).pipe(
              Effect.mapError(() => RestartHarnessError.make({ operation: "validate status" })),
            );
            expect(status.delivered).toBe(true);
            expect(status.pending).toBe(false);
            expect(status.submissionIds).toEqual([status.receiptSubmissionId]);
          }),
        );
      }),
    ).pipe(Effect.provide(NodeFileSystem.layer)),
  120_000,
);

it.live(
  "recovers a lost admission reply from only the persisted wake after Miniflare restart",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const persistDirectory = yield* fs.makeTempDirectoryScoped({
          prefix: "effect-agent-cf-schedule-lost-reply-",
        });
        const script = yield* buildWorker;

        yield* Effect.scoped(
          Effect.gen(function* () {
            const first = yield* openRuntime(persistDirectory, script);
            yield* call(first, "/introspect");
            yield* call(first, "/arm-lost-reply", {});
            const nowMillis = yield* Clock.currentTimeMillis;
            yield* call(first, "/create", { deadlineAtMillis: nowMillis + 500 });
            yield* waitForLostReply(first);
          }),
        );

        yield* Effect.scoped(
          Effect.gen(function* () {
            const second = yield* openRuntime(persistDirectory, script);
            yield* waitForAlarm(second);
            const rawStatus = yield* call(second, "/status");
            const status = yield* Schema.decodeUnknownEffect(Status)(rawStatus).pipe(
              Effect.mapError(() => RestartHarnessError.make({ operation: "validate status" })),
            );
            expect(status.delivered).toBe(true);
            expect(status.pending).toBe(false);
            expect(status.submissionIds).toEqual([status.receiptSubmissionId]);
          }),
        );
      }),
    ).pipe(Effect.provide(NodeFileSystem.layer)),
  120_000,
);
