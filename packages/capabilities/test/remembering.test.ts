import * as MemoryNamespace from "@effect-agent/core/MemoryNamespace";
import { type MemoryContent } from "@effect-agent/core/MemoryReference";
import type { MemoryDocument } from "@effect-agent/core/MemoryStore";
import {
  applyMemoryWrite,
  MemoryKey,
  MemoryOperationConflict,
  MemoryReader,
  MemoryScope,
  MemoryStorageError,
  MemoryWrite,
  MemoryWriter,
} from "@effect-agent/core/MemoryStore";
import * as Protocol from "@effect-agent/core/RememberingStore";
import { it } from "@effect/vitest";
import { Cause, Deferred, Effect, Exit, Fiber, Schema } from "effect";
import { TestClock } from "effect/testing";
import { describe, expect } from "vite-plus/test";

import * as Remembering from "../src/Remembering.ts";

const Sources = MemoryNamespace.define({
  name: "test/messages",
  version: 1,
  identity: Schema.String,
});

const Targets = MemoryNamespace.define({
  name: "test/profiles",
  version: 1,
  identity: Schema.String,
});

const sourceNamespace = Sources.make("tenant");
const targetNamespace = Targets.make("tenant");
const target = MemoryKey.make({ namespace: targetNamespace, id: "profile" });

type Intent = Protocol.Intent<typeof sourceNamespace, typeof targetNamespace>;

const intent = (id: string, sourceId = id, sequence = 1): Intent =>
  Protocol.Intent.make({
    version: 1,
    id,
    invocationId: `invocation-${id}`,
    source: {
      key: MemoryKey.make({ namespace: sourceNamespace, id: sourceId }),
      locator: `message://${sourceId}`,
      revision: `opaque-${sequence}`,
      position: { authorityGeneration: "authority", sequence },
    },
    target,
  });

const invalidation = (
  source: Intent,
  reason: Protocol.Invalidation["reason"] = "forget",
  sequence = 2,
) =>
  Protocol.Invalidation.make({
    version: 1,
    id: `${reason}-${source.id}-${sequence}`,
    source: source.source.key,
    position: { authorityGeneration: "authority", sequence },
    reason,
  });

const limits = Remembering.Limits.make({
  maxSourceBytes: 1_024,
  maxProposalBytes: 4_096,
  timeoutMillis: 100,
});

const Fact = Schema.Struct({ text: Schema.NonEmptyString });

const Entry = Schema.Struct({
  source: Schema.String,
  revision: Schema.String,
  text: Schema.String,
  human: Schema.Boolean,
});

const Profile = Schema.Array(Entry);
const ProfileJson = Schema.fromJsonString(Profile);

const content = (entries: typeof Profile.Type): MemoryContent => ({
  text: Schema.encodeSync(ProfileJson)(entries),
  recordedAt: 7,
  extractedAt: 11,
  metadata: {},
  attributions: [
    {
      originId: "source",
      speaker: "speaker",
      observers: [],
      locator: "message://source",
      activityAt: 3,
      interpretation: "reported preference",
    },
  ],
});

class SourceFailure extends Schema.TaggedError<SourceFailure>()("SourceFailure", {}) {}

/** Local public-port fixture. Canonical profile state exists only in the MemoryWriter map;
 * the job map keeps immutable intent/proposal/command and retained cleanup references.
 */
const fixture = Effect.fn("remembering.fixture")(function* () {
  const jobs = new Map<string, Protocol.Checkpoint>();
  const events = new Map<string, Protocol.Invalidation>();
  const admitted = new Map<string, string>();
  const receipts = new Map<string, { command: string; document: MemoryDocument }>();
  let document: MemoryDocument | null = null;
  const writes: Array<MemoryWrite> = [];

  const state = {
    extractions: 0,
    reads: 0,
    loads: 0,
    merges: 0,
    cleanupCalls: 0,
    loseAck: false,
    unavailable: false,
    noFact: false,
    rejectCleanup: false,
    failAt: null as Protocol.MutationPoint | null,
    pauseAt: null as Protocol.MutationPoint | null,
    transitionStarted: yield* Deferred.make<void>(),
    resumeTransition: yield* Deferred.make<void>(),
    invalidateDuringExtract: null as Protocol.Invalidation | null,
    sourceEvent: null as Protocol.Invalidation | null,
    snapshotOverride: null as Remembering.SourceSnapshot | null,
    extractDefect: false,
    badQuote: false,
    pauseWrite: false,
    writeStarted: yield* Deferred.make<void>(),
    release: 0,
  };

  const keyOf = (key: MemoryKey) => `${key.namespace.address}/${key.id}`;

  const signature = (value: Intent | Protocol.Intent) =>
    JSON.stringify(Schema.encodeSync(Protocol.Intent.Wire)(value));

  const store: Protocol.Store = {
    admit: Effect.fn("fixture.admit")(function* (value) {
      const previous = admitted.get(value.id);

      if (previous !== undefined) {
        if (previous !== signature(value))
          return yield* Protocol.AdmissionError.make({ reason: "conflict" });

        return Protocol.Admission.make({ id: value.id, status: "duplicate" });
      }
      const event = events.get(keyOf(value.source.key));

      if (
        event !== undefined &&
        (event.reason === "forget" ||
          (Protocol.comparePosition(value.source.position, event.position) ?? -1) <= 0)
      )
        return yield* Protocol.AdmissionError.make({ reason: "suppressed" });
      if (admitted.size >= 16) return yield* Protocol.AdmissionError.make({ reason: "capacity" });
      admitted.set(value.id, signature(value));
      jobs.set(
        value.id,
        Protocol.Checkpoint.make({
          intent: value,
          version: 0,
          suppression: null,
          progress: { _tag: "Pending" },
        }),
      );

      return Protocol.Admission.make({ id: value.id, status: "queued" });
    }),
    read: Effect.fn("fixture.read")(function* (value) {
      const checkpoint = jobs.get(value.id);

      if (checkpoint === undefined)
        return yield* Protocol.CheckpointError.make({ reason: "missing" });
      if (signature(checkpoint.intent) !== signature(value))
        return yield* Protocol.CheckpointError.make({ reason: "corrupt" });

      return checkpoint;
    }),
    save: Effect.fn("fixture.save")(function* ({ intent: value, expectedVersion, progress }) {
      const checkpoint = yield* store.read(value);

      if (checkpoint.version !== expectedVersion)
        return yield* Protocol.CheckpointError.make({ reason: "fenced" });

      const next = Protocol.Checkpoint.make({
        ...checkpoint,
        progress,
        version: checkpoint.version + 1,
      });

      jobs.set(value.id, next);

      return next;
    }),
    invalidate: Effect.fn("fixture.invalidate")((event) =>
      Effect.sync(() => {
        const previous = events.get(keyOf(event.source));

        if (previous?.id === event.id)
          return Protocol.InvalidationReceipt.make({
            id: event.id,
            status: "duplicate",
            affected: 0,
          });
        if (
          previous !== undefined &&
          (previous.reason === "forget" ||
            (Protocol.comparePosition(event.position, previous.position) ?? -1) < 0)
        )
          return Protocol.InvalidationReceipt.make({ id: event.id, status: "stale", affected: 0 });
        events.set(keyOf(event.source), event);
        let affected = 0;

        for (const [id, checkpoint] of jobs) {
          if (keyOf(checkpoint.intent.source.key) !== keyOf(event.source)) continue;

          const comparison = Protocol.comparePosition(
            checkpoint.intent.source.position,
            event.position,
          );

          if (comparison === undefined) continue;
          if (
            event.reason !== "forget" &&
            (comparison > 0 || (comparison === 0 && event.reason === "source-edit"))
          )
            continue;
          jobs.set(
            id,
            Protocol.Checkpoint.make({
              ...checkpoint,
              version: checkpoint.version + 1,
              suppression: event,
            }),
          );
          affected++;
        }

        return Protocol.InvalidationReceipt.make({ id: event.id, status: "accepted", affected });
      }),
    ),
  };

  const writer = MemoryWriter.fromAdapter({
    change: Effect.fn("fixture.change")(function* (write) {
      writes.push(write);
      const previous = receipts.get(write.operationId);
      const command = JSON.stringify(Schema.encodeSync(MemoryWrite.Wire)(write));

      if (previous !== undefined) {
        if (previous.command !== command)
          return yield* MemoryOperationConflict.make({
            key: write.key,
            operationId: write.operationId,
          });

        return previous.document;
      }
      const next = yield* applyMemoryWrite(document, write, 19);

      document = next;
      receipts.set(write.operationId, { command, document: next });
      if (state.loseAck) {
        state.loseAck = false;

        return yield* MemoryStorageError.make({
          operation: "lost acknowledgement",
          reason: "unavailable",
        });
      }
      if (state.pauseWrite) {
        state.pauseWrite = false;

        return yield* Effect.acquireUseRelease(
          Effect.void,
          () => Deferred.succeed(state.writeStarted, undefined).pipe(Effect.andThen(Effect.never)),
          () =>
            Effect.sync(() => {
              state.release++;
            }),
        );
      }

      return next;
    }),
  });

  const reader = MemoryReader.fromAdapter({
    get: () =>
      Effect.sync(() => {
        state.reads++;

        return document;
      }),
  });

  const failpoint = Protocol.MutationFailpoint.of({
    hit: Effect.fn("fixture.failpoint")(function* (point) {
      if (state.pauseAt === point) {
        state.pauseAt = null;
        yield* Deferred.succeed(state.transitionStarted, undefined);
        yield* Deferred.await(state.resumeTransition);
      }
      if (state.failAt !== point) return;
      state.failAt = null;

      return yield* Protocol.MutationFailure.make({ point });
    }),
  });

  const entries = (value: MemoryDocument | null) =>
    value?._tag === "ActiveMemoryDocument"
      ? Schema.decodeUnknownEffect(ProfileJson)(value.content.text)
      : Effect.succeed([]);

  const processor = Remembering.make({
    proposal: Fact,
    loadSource: (value: Intent) =>
      Effect.suspend(
        (): Effect.Effect<Remembering.SourceSnapshot | Protocol.Invalidation, SourceFailure> => {
          state.loads++;
          if (state.unavailable) return Effect.fail(SourceFailure.make({}));
          if (state.sourceEvent !== null) return Effect.succeed(state.sourceEvent);

          return Effect.succeed(
            state.snapshotOverride ??
              Remembering.SourceSnapshot.make({
                source: value.source,
                text: `fact-${value.source.key.id}`,
              }),
          );
        },
      ),
    extract: (source) =>
      Effect.suspend(() => {
        state.extractions++;
        if (state.invalidateDuringExtract !== null)
          state.sourceEvent = state.invalidateDuringExtract;
        if (state.extractDefect) return Effect.die("extraction defect");
        if (state.noFact) return Effect.succeed(null);

        return Effect.succeed({
          value: { text: source.text },
          evidence: [
            {
              source: {
                id: source.source.key.id,
                locator: source.source.locator,
                revision: source.source.revision,
              },
              quote: state.badQuote ? "invented" : source.text,
              startByte: 0,
              endByte: new TextEncoder().encode(source.text).byteLength,
            },
          ],
        });
      }),
    merge: Effect.fn("fixture.merge")(function* ({ intent: value, proposal, current }) {
      state.merges++;
      const existing = yield* entries(current);

      if (
        existing.some(
          (entry) =>
            entry.source === value.source.key.id && entry.revision === value.source.revision,
        )
      )
        return { _tag: "NoChange" };

      return {
        _tag: "Put",
        locator: "profile://person",
        content: content([
          ...existing,
          {
            source: value.source.key.id,
            revision: value.source.revision,
            text: proposal.value.text,
            human: false,
          },
        ]),
        scopes: [MemoryScope.make("private")],
      };
    }),
    cleanup: Effect.fn("fixture.cleanup")(function* ({ intent: value, current }) {
      state.cleanupCalls++;
      if (state.rejectCleanup) return { _tag: "Reject" };
      const existing = yield* entries(current);

      const retained = existing.filter(
        (entry) =>
          entry.human ||
          entry.source !== value.source.key.id ||
          entry.revision !== value.source.revision,
      );

      if (retained.length === existing.length) return { _tag: "NoChange" };

      return {
        _tag: "Put",
        locator: "profile://person",
        content: content(retained),
        scopes: [MemoryScope.make("private")],
      };
    }),
  });

  const provide = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    effect.pipe(
      Effect.provideService(MemoryReader, reader),
      Effect.provideService(MemoryWriter, writer),
      Effect.provideService(Protocol.MutationFailpoint, failpoint),
    );

  const advance = (value: Intent, extractionEnabled = true, bounds = limits) =>
    provide(processor.advance({ intent: value, store, limits: bounds, extractionEnabled }));

  const admit = (value: Intent) => provide(Remembering.admit(store, value));

  const invalidate = (event: Protocol.Invalidation) =>
    provide(Remembering.invalidate(store, event));

  const drain = Effect.fn("fixture.drain")(function* (value: Intent, enabled = true) {
    for (let steps = 0; steps < 12; steps++) {
      const checkpoint = yield* advance(value, enabled);

      if (
        checkpoint.progress._tag === "Completed" &&
        (checkpoint.suppression === null || checkpoint.progress.cleaned)
      )
        return checkpoint;
    }

    return yield* Effect.die("remembering did not finish within 12 steps");
  });

  const correct = Effect.fn("fixture.correct")(function* (rows: typeof Profile.Type) {
    return yield* writer.change(
      MemoryWrite.make({
        _tag: "Put",
        key: target,
        operationId: `human-${writes.length}`,
        expectedRevision: document?.source.revision ?? null,
        locator: "profile://person",
        content: content(rows),
        scopes: [MemoryScope.make("private")],
      }),
    );
  });

  return {
    state,
    store,
    jobs,
    writes,
    receipts,
    writer,
    reader,
    processor,
    provide,
    admit,
    advance,
    drain,
    invalidate,
    correct,
    profile: () => entries(document),
  };
});

describe("background remembering", () => {
  it.effect(
    "admits without foreground work, saves proposals before target reads, and rebases two jobs without extracting again",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        const a = intent("a");
        const b = intent("b");

        expect(yield* f.admit(a)).toMatchObject({ status: "queued" });
        yield* f.admit(b);
        expect([f.state.loads, f.state.extractions, f.state.reads, f.writes.length]).toEqual([
          0, 0, 0, 0,
        ]);
        yield* f.advance(a);
        yield* f.advance(b);
        expect(f.state.reads).toBe(0);
        yield* f.advance(a);
        yield* f.advance(b);
        yield* f.advance(a);
        expect((yield* f.advance(b)).progress._tag).toBe("Proposed");
        yield* f.drain(b);
        expect((yield* f.profile()).map((row) => row.source)).toEqual(["a", "b"]);
        expect(f.state.extractions).toBe(2);
        expect(f.writes[1]?.operationId).not.toBe(f.writes[2]?.operationId);
      }),
  );

  it.effect(
    "replays a lost acknowledgement byte-for-byte after a human correction even when the source loader fails",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        const a = intent("a");

        yield* f.admit(a);
        yield* f.advance(a);
        yield* f.advance(a);
        f.state.loseAck = true;
        expect(yield* f.advance(a).pipe(Effect.flip)).toMatchObject({ reason: "unavailable" });
        const saved = f.writes[0];

        yield* f.correct([{ source: "a", revision: "human", text: "corrected", human: true }]);
        f.state.unavailable = true;
        yield* f.advance(a, false);
        expect(f.writes[2]).toEqual(saved);
        expect(yield* f.profile()).toEqual([
          { source: "a", revision: "human", text: "corrected", human: true },
        ]);
        expect(f.state.extractions).toBe(1);
      }),
  );

  it.effect("keeps concrete operation conflicts pending without rebasing or extracting", () =>
    Effect.gen(function* () {
      const f = yield* fixture();
      const a = intent("a");

      yield* f.admit(a);
      yield* f.advance(a);
      const saved = yield* f.advance(a);

      if (saved.progress._tag !== "Prepared") return yield* Effect.die("missing command");

      const forged = MemoryWrite.make({
        ...saved.progress.command,
        key: target,
        operationId: saved.progress.command.operationId,
        ...(saved.progress.command._tag === "Put" ? { content: content([]) } : {}),
      });

      yield* f.writer.change(forged);
      expect(yield* f.advance(a).pipe(Effect.flip)).toMatchObject({
        _tag: "MemoryOperationConflict",
      });
      expect((yield* f.store.read(a)).progress).toEqual(saved.progress);
      expect(f.state.extractions).toBe(1);
    }),
  );

  it.effect(
    "reconciles a suppressed prepared write and cleans after restart while preserving human and newer source contributions",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        const a = intent("a");

        yield* f.admit(a);
        yield* f.advance(a);
        yield* f.advance(a);
        f.state.loseAck = true;
        yield* f.advance(a).pipe(Effect.flip);
        yield* f.correct([
          { source: "a", revision: "human", text: "human correction", human: true },
          { source: "a", revision: "opaque-3", text: "new revision", human: false },
          { source: "a", revision: "opaque-1", text: "old revision", human: false },
        ]);
        yield* f.invalidate(invalidation(a, "source-edit"));

        const checkpoint = Schema.decodeUnknownSync(Schema.fromJsonString(Protocol.Checkpoint))(
          Schema.encodeSync(Schema.fromJsonString(Protocol.Checkpoint))(yield* f.store.read(a)),
        );

        f.jobs.set(a.id, checkpoint);
        f.state.unavailable = true;
        yield* f.drain(a, false);
        expect((yield* f.profile()).map((row) => row.text)).toEqual([
          "human correction",
          "new revision",
        ]);
        expect(f.writes[2]).toEqual(f.writes[0]);
        expect(f.state.extractions).toBe(1);
      }),
  );

  it.effect(
    "suppression before the first dispatch still reconciles the saved command, then removes its contribution",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        const a = intent("a");

        yield* f.admit(a);
        yield* f.advance(a);
        yield* f.advance(a);
        yield* f.invalidate(invalidation(a));
        f.state.unavailable = true;
        yield* f.drain(a, false);
        expect(f.writes).toHaveLength(2);
        expect(yield* f.profile()).toEqual([]);
      }),
  );

  it.effect(
    "retains no-change contribution references and does not discharge rejected cleanup",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        const a = intent("a");

        yield* f.correct([
          { source: "a", revision: a.source.revision, text: "fact-a", human: false },
        ]);
        yield* f.admit(a);
        const done = yield* f.drain(a);

        expect(done.progress).toMatchObject({
          outcome: "no-change",
          applied: { _tag: "ActiveMemoryDocument" },
        });
        yield* f.invalidate(invalidation(a));
        f.state.rejectCleanup = true;
        expect(yield* f.advance(a, false).pipe(Effect.flip)).toMatchObject({
          reason: "cleanup-rejected",
        });
        expect((yield* f.store.read(a)).progress).toMatchObject({ cleaned: false });
        f.state.rejectCleanup = false;
        yield* f.drain(a, false);
        expect(yield* f.profile()).toEqual([]);
      }),
  );

  it.effect(
    "rejects invalid quote provenance and mismatched source revisions before reading a target",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        const a = intent("a");

        yield* f.admit(a);
        f.state.badQuote = true;
        expect(yield* f.advance(a).pipe(Effect.flip)).toMatchObject({ reason: "invalid-evidence" });
        f.state.badQuote = false;
        f.state.snapshotOverride = Remembering.SourceSnapshot.make({
          source: { ...a.source, revision: "other" },
          text: "fact-a",
        });
        expect(yield* f.advance(a).pipe(Effect.flip)).toMatchObject({ reason: "source-changed" });
        expect(f.state.reads).toBe(0);
        expect(f.writes).toEqual([]);
        const text = "I prefer 🍵 and café.";

        f.state.snapshotOverride = Remembering.SourceSnapshot.make({ source: a.source, text });
        expect(
          yield* f.advance(a, true, { ...limits, maxSourceBytes: text.length }).pipe(Effect.flip),
        ).toMatchObject({ reason: "budget" });
        expect((yield* f.advance(a)).progress._tag).toBe("Proposed");
        yield* f.drain(a);
        expect(yield* f.profile()).toEqual([
          { source: "a", revision: a.source.revision, text, human: false },
        ]);
      }),
  );

  it.effect(
    "persists explicit source invalidation before materialization and keeps extraction disablement independent",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        const a = intent("a");

        yield* f.admit(a);
        expect(yield* f.advance(a, false).pipe(Effect.flip)).toMatchObject({ reason: "disabled" });
        yield* f.advance(a);
        f.state.sourceEvent = invalidation(a, "source-revocation");
        expect((yield* f.advance(a)).suppression?.reason).toBe("source-revocation");
        yield* f.drain(a, false);
        expect(f.writes).toEqual([]);
        expect(f.state.reads).toBe(0);
        const b = intent("b");

        yield* f.admit(b);
        f.state.sourceEvent = null;
        f.state.noFact = true;
        expect((yield* f.advance(b)).progress).toMatchObject({
          _tag: "Completed",
          outcome: "no-change",
        });
      }),
  );

  it.effect(
    "keeps expected failures and defects distinct, and applies input/source/proposal bounds",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        const a = intent("a");

        yield* f.admit(a);
        f.state.unavailable = true;
        expect(yield* f.advance(a).pipe(Effect.flip)).toMatchObject({ _tag: "SourceFailure" });
        f.state.unavailable = false;
        f.state.extractDefect = true;
        const defect = yield* f.advance(a).pipe(Effect.exit);

        expect(Exit.isFailure(defect)).toBe(true);
        if (Exit.isFailure(defect))
          expect(Cause.pretty(defect.cause)).toContain("extraction defect");
        f.state.extractDefect = false;
        expect(yield* f.advance({ ...a, id: "" }).pipe(Effect.flip)).toMatchObject({
          reason: "invalid-input",
        });
        expect(
          yield* f.advance(a, true, { ...limits, maxSourceBytes: 1 }).pipe(Effect.flip),
        ).toMatchObject({ reason: "budget" });
        expect(
          yield* f.advance(a, true, { ...limits, maxProposalBytes: 1 }).pipe(Effect.flip),
        ).toMatchObject({ reason: "budget" });
        expect((yield* f.store.read(a)).progress._tag).toBe("Pending");
      }),
  );

  it.effect(
    "timeout and interruption finalize resources and retain the exact uncertain command",
    () =>
      Effect.gen(function* () {
        for (const mode of ["timeout", "interruption"] as const) {
          const f = yield* fixture();
          const a = intent(mode);

          yield* f.admit(a);
          yield* f.advance(a);
          yield* f.advance(a);
          f.state.pauseWrite = true;
          const fiber = yield* f.advance(a).pipe(Effect.forkChild);

          yield* Deferred.await(f.state.writeStarted);
          if (mode === "timeout") {
            yield* TestClock.adjust(100);
            expect(yield* Fiber.join(fiber).pipe(Effect.flip)).toMatchObject({ reason: "timeout" });
          } else yield* Fiber.interrupt(fiber);
          expect(f.state.release).toBe(1);
          expect((yield* f.store.read(a)).progress._tag).toBe("Prepared");
          yield* f.drain(a);
          expect(f.writes[1]).toEqual(f.writes[0]);
          expect(f.state.extractions).toBe(1);
        }
      }),
  );

  it.effect(
    "before/after durable transition failures resume without dropping proposal, command, or cleanup",
    () =>
      Effect.gen(function* () {
        for (const point of Protocol.MutationPoint.literals) {
          const f = yield* fixture();
          const a = intent("a");

          if (point.startsWith("admission")) {
            f.state.failAt = point;
            yield* f.admit(a).pipe(Effect.flip);
            yield* f.admit(a);
          } else yield* f.admit(a);
          if (point.startsWith("proposal")) {
            f.state.failAt = point;
            yield* f.advance(a).pipe(Effect.flip);
          }
          yield* f.advance(a);
          if (point.startsWith("command")) {
            f.state.failAt = point;
            yield* f.advance(a).pipe(Effect.flip);
          }
          if ((yield* f.store.read(a)).progress._tag === "Proposed") yield* f.advance(a);
          if (point.startsWith("rebase")) {
            yield* f.correct([{ source: "human", revision: "1", text: "human", human: true }]);
            f.state.failAt = point;
            yield* f.advance(a).pipe(Effect.flip);
          }
          if (point.startsWith("completion")) {
            f.state.failAt = point;
            yield* f.advance(a).pipe(Effect.flip);
          }
          yield* f.drain(a);
          if (point.startsWith("suppression")) {
            f.state.failAt = point;
            yield* f.invalidate(invalidation(a)).pipe(Effect.flip);
          }
          yield* f.invalidate(invalidation(a));
          if (point.startsWith("cleanup")) {
            f.state.failAt = point;
            yield* f.advance(a, false).pipe(Effect.flip);
          }
          const done = yield* f.drain(a, false);

          expect(done.progress).toMatchObject({ outcome: "suppressed", cleaned: true });
          expect((yield* f.profile()).filter((row) => !row.human)).toEqual([]);
          if (point !== "proposal:before") expect(f.state.extractions).toBe(1);
        }
      }),
  );

  it.effect(
    "fences late proposal, command and completion commits against source invalidation",
    () =>
      Effect.gen(function* () {
        for (const point of ["proposal:before", "command:before", "completion:before"] as const) {
          const f = yield* fixture();
          const a = intent(point);

          yield* f.admit(a);
          if (point !== "proposal:before") yield* f.advance(a);
          if (point === "completion:before") yield* f.advance(a);
          f.state.pauseAt = point;
          const worker = yield* f.advance(a).pipe(Effect.forkChild);

          yield* Deferred.await(f.state.transitionStarted);
          yield* f.invalidate(invalidation(a));
          yield* Deferred.succeed(f.state.resumeTransition, undefined);
          expect(yield* Fiber.join(worker).pipe(Effect.flip)).toMatchObject({ reason: "fenced" });
          f.state.unavailable = true;
          yield* f.drain(a, false);
          expect(yield* f.profile()).toEqual([]);
          expect(f.state.extractions).toBe(1);
          if (point === "completion:before") expect(f.writes[1]).toEqual(f.writes[0]);
          else expect(f.writes).toEqual([]);
        }
      }),
  );

  it.effect(
    "rechecks authoritative source after extraction before retaining a materializable proposal",
    () =>
      Effect.gen(function* () {
        const f = yield* fixture();
        const a = intent("a");

        yield* f.admit(a);
        f.state.invalidateDuringExtract = invalidation(a, "source-deletion");
        const checkpoint = yield* f.advance(a);

        expect(checkpoint.suppression?.reason).toBe("source-deletion");
        yield* f.drain(a, false);
        expect(f.state.extractions).toBe(1);
        expect(f.state.reads).toBe(0);
        expect(f.writes).toEqual([]);
      }),
  );
});
