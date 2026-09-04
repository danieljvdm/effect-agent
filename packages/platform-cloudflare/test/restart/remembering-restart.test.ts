import { NodeFileSystem } from "@effect/platform-node";
import { it } from "@effect/vitest";
import { Clock, Effect, Fiber, FileSystem, Schema } from "effect";
import { build } from "esbuild";
import { convertV4MiniflareOptions, Miniflare } from "miniflare";
import { expect } from "vite-plus/test";

import {
  ForegroundSample,
  Profile,
  type Request,
  type Source,
  Status,
} from "./remembering-contract.ts";

class ProbeFailure extends Schema.TaggedError<ProbeFailure>()("RememberingProbeFailure", {
  operation: Schema.String,
}) {}

const bundle = Effect.tryPromise({
  try: () =>
    build({
      entryPoints: [`${import.meta.dirname}/remembering-worker.ts`],
      bundle: true,
      write: false,
      format: "esm",
      target: "es2022",
      platform: "browser",
      conditions: ["workerd", "worker", "browser"],
      external: ["cloudflare:*", "node:*"],
      logLevel: "silent",
    }),
  catch: () => ProbeFailure.make({ operation: "bundle remembering fixture" }),
}).pipe(
  Effect.flatMap((built) => {
    const output = built.outputFiles[0];

    return output === undefined
      ? Effect.fail(ProbeFailure.make({ operation: "bundle output" }))
      : Effect.succeed(
          `const __disabledDynamicImport = () => Promise.reject(new Error("dynamic import disabled"));\n${output.text.replaceAll(/\bimport\s*\(/g, "__disabledDynamicImport(")}`,
        );
  }),
);

const open = (script: string, directory: string, lineage: string | null = "fixture-lineage-1") =>
  Effect.acquireRelease(
    Effect.try({
      try: () =>
        new Miniflare(
          convertV4MiniflareOptions({
            modules: true,
            script,
            compatibilityDate: "2025-05-01",
            compatibilityFlags: ["nodejs_compat"],
            bindings: lineage === null ? {} : { REMEMBERING_LINEAGE: lineage },
            durableObjects: { REMEMBERING: { className: "RememberingOwner", useSQLite: true } },
            resourcePersistencePath: directory,
          }),
        ),
      catch: () => ProbeFailure.make({ operation: "open workerd" }),
    }),
    (runtime) =>
      Effect.tryPromise({
        try: () => runtime.dispose(),
        catch: () => ProbeFailure.make({ operation: "dispose workerd" }),
      }).pipe(Effect.orDie),
  );

const call = Effect.fn("rememberingProbe.call")(function* (
  runtime: Miniflare,
  owner: string,
  request: Request,
) {
  const response = yield* Effect.tryPromise({
    try: () =>
      runtime.dispatchFetch(`http://fixture/${owner}`, {
        method: "POST",
        body: JSON.stringify(request),
      }),
    catch: () => ProbeFailure.make({ operation: `dispatch ${request._tag}` }),
  });

  return yield* Effect.tryPromise({
    try: () => response.json(),
    catch: () => ProbeFailure.make({ operation: `decode ${request._tag}` }),
  });
});

const status = Effect.fn("rememberingProbe.status")(function* (runtime: Miniflare, owner: string) {
  return yield* Schema.decodeUnknownEffect(Status)(yield* call(runtime, owner, { _tag: "Status" }));
});

const until = Effect.fn("rememberingProbe.until")(function* (
  runtime: Miniflare,
  owner: string,
  predicate: (value: Status) => boolean,
) {
  for (let attempt = 0; attempt < 300; attempt++) {
    const current = yield* status(runtime, owner);

    if (predicate(current)) return current;
    yield* Effect.sleep("25 millis");
  }

  return yield* ProbeFailure.make({ operation: `wait ${owner}` });
});

const source = (
  id: string,
  text = "I prefer morning meetings and concise agendas.",
  sequence = 1,
): Source => ({ id, text, sequence, revision: `r${sequence}`, author: "human" });

const ReadResult = Schema.Struct({
  profile: Schema.NullOr(Profile),
  recalled: Schema.String,
  generation: Schema.Natural,
});

const read = Effect.fn("rememberingProbe.read")(function* (runtime: Miniflare, owner: string) {
  return yield* Schema.decodeUnknownEffect(ReadResult)(
    yield* call(runtime, owner, { _tag: "Read", authorized: true }),
  );
});

const admit = Effect.fn("rememberingProbe.admit")(function* (
  runtime: Miniflare,
  owner: string,
  value: Source,
  id = value.id,
) {
  expect(yield* call(runtime, owner, { _tag: "Commit", source: value, automatic: false })).toEqual({
    committed: true,
  });
  expect(yield* call(runtime, owner, { _tag: "Remember", id, sourceId: value.id })).toEqual({
    id,
    status: "queued",
  });
});

const fixture = Effect.fn("rememberingProbe.fixture")(function* () {
  const fs = yield* FileSystem.FileSystem;
  const directory = yield* fs.makeTempDirectoryScoped({ prefix: "effect-agent-remembering-" });
  const script = yield* bundle;

  return { script, directory };
});

it.live(
  "settles foreground streams while extraction and profile writes independently block, with bounded admission and local timing samples",
  () =>
    Effect.gen(function* () {
      const { script, directory } = yield* fixture();
      const runtime = yield* open(script, directory);
      const samples: Array<ForegroundSample & { requestMillis: number; state: string }> = [];
      const recallDelays: Array<{ state: string; milliseconds: number | null }> = [];

      for (const state of ["off", "idle", "blocked"] as const) {
        yield* call(runtime, state, {
          _tag: "Configure",
          automaticWake: false,
          extractionBlocked: state === "blocked",
        });
        yield* call(runtime, state, { _tag: "Correct", text: "Human preference used for recall." });
        let blocked: Fiber.Fiber<unknown, ProbeFailure> | undefined;

        if (state === "blocked") {
          yield* admit(runtime, state, source("held-a"));
          yield* admit(runtime, state, source("held-b"));
          blocked = yield* call(runtime, state, { _tag: "Work" }).pipe(Effect.forkChild);
          yield* until(runtime, state, (value) => value.extractionWaiting === 2);
          expect(yield* call(runtime, state, { _tag: "Work" })).toEqual({ busy: true });
          expect(
            yield* call(runtime, state, { _tag: "Remember", id: "held-a", sourceId: "held-a" }),
          ).toEqual({ id: "held-a", status: "duplicate" });
          yield* call(runtime, state, {
            _tag: "Commit",
            source: source("overflow"),
            automatic: false,
          });
          expect(
            yield* call(runtime, state, { _tag: "Remember", id: "overflow", sourceId: "overflow" }),
          ).toMatchObject({
            _tag: "Failure",
            failure: { _tag: "RememberingAdmissionError", reason: "capacity" },
          });
        }
        const firstSourceCommitted = yield* Clock.currentTimeMillis;

        for (let sample = 0; sample < 3; sample++) {
          const start = yield* Clock.currentTimeMillis;

          const raw = yield* call(runtime, state, {
            _tag: "Foreground",
            source: source(`chat-${sample}`),
            learning: state === "off" ? "off" : "automatic",
          });

          const measured = yield* Schema.decodeUnknownEffect(ForegroundSample)(raw);

          samples.push({
            ...measured,
            requestMillis: (yield* Clock.currentTimeMillis) - start,
            state,
          });
          expect(measured.output).toBe("Thanks, I can help with that.");
          expect(measured.recalled).toContain("Human preference used for recall.");
          expect(measured.promptIncludesRecall).toBe(true);
          expect(measured.completedResponseMillis).toBeGreaterThanOrEqual(
            measured.firstTokenMillis,
          );
          if (state === "idle") yield* call(runtime, state, { _tag: "Work" });
        }
        const observed = yield* status(runtime, state);

        expect(observed.foregroundFinalizers).toBe(3);
        if (state === "blocked") {
          expect(observed).toMatchObject({ extractionWaiting: 2, writeCalls: 0, outbox: 3 });
          expect((yield* read(runtime, state)).profile?.facts).toHaveLength(1);
          yield* call(runtime, state, { _tag: "Configure", extractionBlocked: false });
          if (blocked !== undefined) yield* Fiber.join(blocked);
          yield* call(runtime, state, { _tag: "Work" });
          yield* call(runtime, state, { _tag: "Work" });
          expect((yield* status(runtime, state)).active).toBe(0);
        }
        const recalled = yield* read(runtime, state);

        if (state !== "off") expect(recalled.recalled).toContain("morning meetings");
        recallDelays.push({
          state,
          milliseconds:
            state === "off" ? null : (yield* Clock.currentTimeMillis) - firstSourceCommitted,
        });
      }
      yield* call(runtime, "explicit", {
        _tag: "Configure",
        automaticWake: false,
        extractionBlocked: true,
      });
      yield* admit(runtime, "explicit", source("held"));
      const held = yield* call(runtime, "explicit", { _tag: "Work" }).pipe(Effect.forkChild);

      yield* until(runtime, "explicit", (value) => value.extractionWaiting === 1);
      const explicitStart = yield* Clock.currentTimeMillis;

      const explicit = yield* Schema.decodeUnknownEffect(ForegroundSample)(
        yield* call(runtime, "explicit", {
          _tag: "Foreground",
          source: source("explicit-chat"),
          learning: "explicit",
        }),
      );

      samples.push({
        ...explicit,
        state: "explicit-with-blocked-extraction",
        requestMillis: (yield* Clock.currentTimeMillis) - explicitStart,
      });
      expect(explicit.output).toBe("Thanks, I can help with that.");
      expect(yield* status(runtime, "explicit")).toMatchObject({
        active: 2,
        extractionWaiting: 1,
        foregroundFinalizers: 1,
      });
      yield* call(runtime, "explicit", { _tag: "Configure", extractionBlocked: false });
      yield* Fiber.join(held);
      // Use the identical foreground workload with both conditional writes independently held.
      const owner = "writes";

      yield* call(runtime, owner, { _tag: "Configure", automaticWake: false, writeBlocked: true });
      yield* call(runtime, owner, { _tag: "Correct", text: "Keep the human-authored preference." });
      yield* admit(runtime, owner, source("a", "I prefer tea."));
      yield* admit(runtime, owner, source("b", "I live in Seattle."));
      const writing = yield* call(runtime, owner, { _tag: "Work" }).pipe(Effect.forkChild);

      yield* until(runtime, owner, (value) => value.writeWaiting === 2);

      const raw = yield* call(runtime, owner, {
        _tag: "Foreground",
        source: source("write-chat"),
        learning: "automatic",
      });

      const writeBlockedSample = yield* Schema.decodeUnknownEffect(ForegroundSample)(raw);

      expect(writeBlockedSample.recalled).toContain("Keep the human-authored preference.");
      expect(writeBlockedSample.promptIncludesRecall).toBe(true);
      expect(writeBlockedSample.output).toBe("Thanks, I can help with that.");
      expect(yield* status(runtime, owner)).toMatchObject({
        writeWaiting: 2,
        writeCalls: 0,
        foregroundFinalizers: 1,
      });
      // Human correction and two writers force real revision conflicts after the gates open.
      yield* call(runtime, owner, { _tag: "Correct", text: "Keep the human-authored preference." });
      yield* call(runtime, owner, { _tag: "Configure", writeBlocked: false });
      yield* Fiber.join(writing);
      const learned = yield* read(runtime, owner);

      expect(learned.profile?.facts.map((fact) => fact.text).sort()).toEqual(
        ["I live in Seattle.", "I prefer tea.", "Keep the human-authored preference."].sort(),
      );
      expect(learned.recalled).toContain("I prefer tea.");
      expect(yield* status(runtime, owner)).toMatchObject({
        extractionCalls: 2,
        workerStarts: 1,
        workerFinalizers: 1,
      });
      expect((yield* status(runtime, owner)).mergeCalls).toBeGreaterThan(2);
      expect(yield* call(runtime, owner, { _tag: "Read", authorized: false })).toMatchObject({
        failure: { reason: "denied" },
      });
      yield* Effect.logInfo(
        "Remembering local timing samples; workerd event clocks may quantize synchronous intervals to zero",
        samples,
      );
      yield* Effect.logInfo(
        "Remembering local first-source-commit to authorized recall; blocked case includes deliberate barrier time",
        recallDelays,
      );
    }).pipe(Effect.scoped, Effect.provide(NodeFileSystem.layer)),
  120_000,
);

it.live(
  "recovers proposal, exact command, committed write with lost reply, and completion through full workerd restarts",
  () =>
    Effect.gen(function* () {
      const script = yield* bundle;
      const fs = yield* FileSystem.FileSystem;

      for (const [failAt, expectedTag] of [
        ["source:after", "Outbox"],
        ["save:Proposed:after", "Proposed"],
        ["save:Prepared:after", "Prepared"],
        ["memory:change:after", "Prepared"],
        ["save:Completed:after", "Completed"],
      ] as const) {
        const directory = yield* fs.makeTempDirectoryScoped({
          prefix: "effect-agent-remembering-restart-",
        });

        yield* Effect.gen(function* () {
          const first = yield* open(script, directory);

          yield* call(first, "owner", { _tag: "Configure", failAt });
          if (expectedTag === "Outbox") {
            expect(
              yield* call(first, "owner", {
                _tag: "Commit",
                source: source("fact"),
                automatic: true,
              }),
            ).toMatchObject({ _tag: "Failure" });
            expect(yield* status(first, "owner")).toMatchObject({ outbox: 1, active: 0 });
          } else {
            yield* admit(first, "owner", source("fact"));
            expect(yield* call(first, "owner", { _tag: "Work" })).toMatchObject({
              _tag: "Failure",
            });
            expect((yield* status(first, "owner")).checkpoints).toEqual([
              { id: "fact", tag: expectedTag },
            ]);
          }
          // Dispose with no post-failure mutation. The wake predates the durable mutation.
        }).pipe(Effect.scoped);
        yield* Effect.gen(function* () {
          const second = yield* open(script, directory);
          const recovered = yield* until(second, "owner", (value) => value.archived === 1);

          expect(recovered.extractionCalls).toBe(expectedTag === "Outbox" ? 1 : 0);
          const current = yield* read(second, "owner");

          expect(current.generation).toBe(1);
          expect(current.profile?.facts).toHaveLength(1);
          expect(current.recalled).toContain("morning meetings");
          expect(
            yield* call(second, "owner", {
              _tag: "Remember",
              id: expectedTag === "Outbox" ? "automatic:fact:r1" : "fact",
              sourceId: "fact",
            }),
          ).toEqual({
            id: expectedTag === "Outbox" ? "automatic:fact:r1" : "fact",
            status: "duplicate",
          });
        }).pipe(Effect.scoped);
      }
    }).pipe(Effect.scoped, Effect.provide(NodeFileSystem.layer)),
  120_000,
);

it.live(
  "repairs lost wakes by durable alarm without restart and admits source outbox work without a foreground model tool call",
  () =>
    Effect.gen(function* () {
      const { script, directory } = yield* fixture();
      const runtime = yield* open(script, directory);
      const start = yield* Clock.currentTimeMillis;

      expect(
        yield* call(runtime, "alarm", {
          _tag: "Commit",
          source: source("alarm-fact"),
          automatic: true,
        }),
      ).toEqual({ committed: true });
      expect(yield* status(runtime, "alarm")).toMatchObject({
        extractionCalls: 0,
        active: 0,
        outbox: 1,
      });
      yield* until(runtime, "alarm", (value) => value.archived === 1);
      expect((yield* read(runtime, "alarm")).recalled).toContain("morning meetings");
      yield* Effect.logInfo("Remembering local source-to-authorized-recall delay", {
        milliseconds: (yield* Clock.currentTimeMillis) - start,
        environment: "local workerd/Miniflare",
        durableAlarmDelayMillis: 2500,
      });
    }).pipe(Effect.scoped, Effect.provide(NodeFileSystem.layer)),
  30_000,
);

it.live(
  "fences uncertain writes and cleans retained references after job pruning without removing human corrections or newer source contributions",
  () =>
    Effect.gen(function* () {
      const { script, directory } = yield* fixture();
      const runtime = yield* open(script, directory);
      const owner = "forget";

      yield* call(runtime, owner, {
        _tag: "Configure",
        automaticWake: false,
        failAt: "memory:change:after",
      });
      yield* admit(runtime, owner, source("old", "I prefer tea."));
      expect(yield* call(runtime, owner, { _tag: "Work" })).toMatchObject({ _tag: "Failure" });
      yield* call(runtime, owner, { _tag: "Correct", text: "Human correction stays." });
      yield* call(runtime, owner, { _tag: "Forget", sourceId: "old", sequence: 2 });
      expect((yield* read(runtime, owner)).profile?.facts.map((fact) => fact.text)).toEqual([
        "Human correction stays.",
      ]);
      yield* call(runtime, owner, { _tag: "Work" });
      expect(yield* status(runtime, owner)).toMatchObject({
        active: 0,
        archived: 1,
        extractionCalls: 1,
      });
      expect((yield* read(runtime, owner)).profile?.facts.map((fact) => fact.text)).toEqual([
        "Human correction stays.",
      ]);
      yield* call(runtime, owner, {
        _tag: "Commit",
        source: source("old", "Delayed source edit must not undo Forget.", 3),
        automatic: false,
      });
      expect(
        yield* call(runtime, owner, { _tag: "Remember", id: "late", sourceId: "old" }),
      ).toMatchObject({ failure: { reason: "suppressed" } });

      yield* admit(runtime, owner, source("edited", "Old version."));
      yield* call(runtime, owner, { _tag: "Work" });
      expect((yield* status(runtime, owner)).active).toBe(0);
      yield* admit(runtime, owner, source("edited", "New version.", 2), "new-edited");
      yield* call(runtime, owner, { _tag: "Work" });
      expect((yield* read(runtime, owner)).profile?.facts.map((fact) => fact.text).sort()).toEqual(
        ["Human correction stays.", "New version."].sort(),
      );
      yield* call(runtime, owner, { _tag: "Forget", sourceId: "edited", sequence: 3 });
      expect((yield* status(runtime, owner)).active).toBeGreaterThan(0);
      yield* call(runtime, owner, { _tag: "Work" });
      expect((yield* read(runtime, owner)).profile?.facts.map((fact) => fact.text)).toEqual([
        "Human correction stays.",
      ]);
      expect((yield* status(runtime, owner)).active).toBe(0);

      yield* call(runtime, "late-writer", {
        _tag: "Configure",
        automaticWake: false,
        writeBlocked: true,
      });
      yield* call(runtime, "late-writer", { _tag: "Correct", text: "Human fact." });
      yield* admit(runtime, "late-writer", source("late-source", "Old delayed fact."));
      const late = yield* call(runtime, "late-writer", { _tag: "Work" }).pipe(Effect.forkChild);

      yield* until(runtime, "late-writer", (value) => value.writeWaiting === 1);
      yield* call(runtime, "late-writer", { _tag: "Forget", sourceId: "late-source", sequence: 2 });
      yield* call(runtime, "late-writer", { _tag: "Configure", writeBlocked: false });
      expect(yield* Fiber.join(late)).toMatchObject({ failure: { reason: "fenced" } });
      yield* call(runtime, "late-writer", { _tag: "Work" });
      expect((yield* read(runtime, "late-writer")).profile?.facts.map((fact) => fact.text)).toEqual(
        ["Human fact."],
      );

      yield* call(runtime, "empty-profile", { _tag: "Configure", automaticWake: false });
      yield* admit(runtime, "empty-profile", source("a", "Only old fact."));
      yield* call(runtime, "empty-profile", { _tag: "Work" });
      yield* call(runtime, "empty-profile", { _tag: "Forget", sourceId: "a", sequence: 2 });
      yield* call(runtime, "empty-profile", { _tag: "Work" });
      expect((yield* read(runtime, "empty-profile")).profile).toBeNull();
      yield* admit(runtime, "empty-profile", source("b", "New unrelated fact."));
      yield* call(runtime, "empty-profile", { _tag: "Work" });
      expect(
        (yield* read(runtime, "empty-profile")).profile?.facts.map((fact) => fact.text),
      ).toEqual(["New unrelated fact."]);
    }).pipe(Effect.scoped, Effect.provide(NodeFileSystem.layer)),
  30_000,
);

it.live(
  "keeps retry, timeout and defect work durable and finalizes each finite background scope",
  () =>
    Effect.gen(function* () {
      const { script, directory } = yield* fixture();
      const runtime = yield* open(script, directory);

      yield* call(runtime, "failures", {
        _tag: "Configure",
        automaticWake: false,
        extractionFailure: "retry",
      });
      yield* admit(runtime, "failures", source("retry"));
      expect(yield* call(runtime, "failures", { _tag: "Work" })).toMatchObject({
        failure: { reason: "retry" },
      });
      yield* call(runtime, "failures", { _tag: "Configure", extractionFailure: "defect" });
      expect(yield* call(runtime, "failures", { _tag: "Work" })).toEqual({ _tag: "Defect" });
      yield* call(runtime, "failures", {
        _tag: "Configure",
        extractionFailure: "none",
        extractionBlocked: true,
        workerTimeoutMillis: 20,
      });
      expect(yield* call(runtime, "failures", { _tag: "Work" })).toMatchObject({
        failure: { reason: "timeout" },
      });
      expect(yield* status(runtime, "failures")).toMatchObject({
        active: 1,
        running: false,
        workerStarts: 3,
        workerFinalizers: 3,
        extractionFinalizers: 3,
        extractionWaiting: 0,
      });
      yield* call(runtime, "failures", {
        _tag: "Configure",
        extractionBlocked: false,
        workerTimeoutMillis: 10_000,
      });
      yield* call(runtime, "failures", { _tag: "Work" });
      expect((yield* read(runtime, "failures")).profile?.facts).toHaveLength(1);
      expect(yield* status(runtime, "failures")).toMatchObject({
        active: 0,
        workerStarts: 4,
        workerFinalizers: 4,
      });
    }).pipe(Effect.scoped, Effect.provide(NodeFileSystem.layer)),
  30_000,
);

it.live(
  "refuses missing, invalid and changed owner lineage before initialization or command replay",
  () =>
    Effect.gen(function* () {
      const { script, directory } = yield* fixture();

      yield* Effect.gen(function* () {
        const first = yield* open(script, directory);

        yield* call(first, "owner", {
          _tag: "Configure",
          automaticWake: false,
          failAt: "save:Prepared:after",
        });
        yield* admit(first, "owner", source("lineage"));
        expect(yield* call(first, "owner", { _tag: "Work" })).toMatchObject({ _tag: "Failure" });
        expect((yield* status(first, "owner")).checkpoints).toEqual([
          { id: "lineage", tag: "Prepared" },
        ]);
      }).pipe(Effect.scoped);
      for (const [index, lineage] of [
        null,
        "",
        "x".repeat(257),
        "different-external-lineage",
      ].entries()) {
        yield* Effect.gen(function* () {
          const incompatible = yield* open(script, directory, lineage);

          const failure = {
            _tag: "Failure",
            failure: { _tag: "RememberingFixtureFailure", reason: "corrupt" },
          };

          expect(yield* call(incompatible, "owner", { _tag: "Work" })).toMatchObject(failure);
          expect(
            yield* call(incompatible, "owner", { _tag: "Read", authorized: true }),
          ).toMatchObject(failure);
          expect(
            yield* call(incompatible, "owner", {
              _tag: "Remember",
              id: "lineage",
              sourceId: "lineage",
            }),
          ).toMatchObject(failure);
          // Invalid config must not initialize a fresh owner's header either.
          if (lineage !== "different-external-lineage")
            expect(yield* call(incompatible, `fresh-${index}`, { _tag: "Work" })).toMatchObject(
              failure,
            );
        }).pipe(Effect.scoped);
        yield* Effect.gen(function* () {
          const original = yield* open(script, directory);

          expect((yield* read(original, "owner")).generation).toBe(0);
          if (lineage !== "different-external-lineage")
            expect((yield* read(original, `fresh-${index}`)).generation).toBe(0);
        }).pipe(Effect.scoped);
      }
      yield* Effect.gen(function* () {
        const original = yield* open(script, directory);

        expect((yield* read(original, "owner")).generation).toBe(0);
        yield* call(original, "owner", { _tag: "Work" });
        expect((yield* read(original, "owner")).generation).toBe(1);
        expect((yield* status(original, "owner")).extractionCalls).toBe(0);
      }).pipe(Effect.scoped);
    }).pipe(Effect.scoped, Effect.provide(NodeFileSystem.layer)),
  30_000,
);

it.live(
  "refuses existing remembering tables with a missing or incompatible owner header",
  () =>
    Effect.gen(function* () {
      const script = yield* bundle;
      const fs = yield* FileSystem.FileSystem;

      for (const kind of ["missing", "incompatible"] as const) {
        const directory = yield* fs.makeTempDirectoryScoped({
          prefix: "effect-agent-remembering-header-",
        });

        yield* Effect.gen(function* () {
          const first = yield* open(script, directory);

          yield* call(first, "owner", {
            _tag: "Configure",
            automaticWake: false,
            failAt: "save:Prepared:after",
          });
          yield* admit(first, "owner", source("header"));
          expect(yield* call(first, "owner", { _tag: "Work" })).toMatchObject({ _tag: "Failure" });
          expect((yield* read(first, "owner")).generation).toBe(0);
          expect(yield* call(first, "owner", { _tag: "CorruptHeader", kind })).toEqual({
            corrupted: true,
          });
        }).pipe(Effect.scoped);
        yield* Effect.gen(function* () {
          const reopened = yield* open(script, directory);

          expect(yield* call(reopened, "owner", { _tag: "Work" })).toMatchObject({
            _tag: "Failure",
            failure: { _tag: "RememberingFixtureFailure", reason: "corrupt" },
          });
          expect(yield* call(reopened, "owner", { _tag: "Read", authorized: true })).toMatchObject({
            failure: { reason: "corrupt" },
          });
        }).pipe(Effect.scoped);
      }
    }).pipe(Effect.scoped, Effect.provide(NodeFileSystem.layer)),
  30_000,
);
