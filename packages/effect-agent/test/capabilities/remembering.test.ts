import { it } from "@effect/vitest";
import { Effect, Schema } from "effect";
import * as MemoryNamespace from "effect-agent/memory-namespace";
import { type MemoryContent } from "effect-agent/memory-reference";
import type { MemoryDocument } from "effect-agent/memory-store";
import {
  applyMemoryWrite,
  MemoryKey,
  MemoryOperationConflict,
  MemoryReader,
  MemoryScope,
  MemoryStorageError,
  MemoryWrite,
  MemoryWriter,
} from "effect-agent/memory-store";
import * as Protocol from "effect-agent/remembering-store";
import { describe, expect } from "vite-plus/test";

import * as Remembering from "../../src/capabilities/Remembering.ts";

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
const fixture = Effect.fn("remembering.fixture")(() =>
  Effect.sync(() => {
    const jobs = new Map<string, Protocol.Checkpoint>();
    const events = new Map<string, Protocol.Invalidation>();
    const admitted = new Map<string, string>();
    const receipts = new Map<string, { command: string; document: MemoryDocument }>();
    let document: MemoryDocument | null = null;
    const writes: Array<MemoryWrite> = [];

    const state = {
      extractions: 0,
      reads: 0,
      loseAck: false,
      unavailable: false,
      invalidateDuringExtract: null as Protocol.Invalidation | null,
      sourceEvent: null as Protocol.Invalidation | null,
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
            return Protocol.InvalidationReceipt.make({
              id: event.id,
              status: "stale",
              affected: 0,
            });
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

    const entries = (value: MemoryDocument | null) =>
      value?._tag === "ActiveMemoryDocument"
        ? Schema.decodeEffect(ProfileJson)(value.content.text)
        : Effect.succeed([]);

    const processor = Remembering.make({
      proposal: Fact,
      loadSource: (value: Intent) =>
        Effect.suspend(
          (): Effect.Effect<Remembering.SourceSnapshot | Protocol.Invalidation, SourceFailure> => {
            if (state.unavailable) return Effect.fail(SourceFailure.make({}));
            if (state.sourceEvent !== null) return Effect.succeed(state.sourceEvent);

            return Effect.succeed(
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

          return Effect.succeed({
            value: { text: source.text },
            evidence: [
              {
                source: {
                  id: source.source.key.id,
                  locator: source.source.locator,
                  revision: source.source.revision,
                },
                quote: source.text,
                startByte: 0,
                endByte: new TextEncoder().encode(source.text).byteLength,
              },
            ],
          });
        }),
      merge: Effect.fn("fixture.merge")(function* ({ intent: value, proposal, current }) {
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
        Effect.provide(Protocol.MutationFailpoint.layer),
      );

    const advance = (value: Intent, extractionEnabled = true) =>
      provide(processor.advance({ intent: value, store, limits, extractionEnabled }));

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
  }),
);

describe("background remembering", () => {
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
