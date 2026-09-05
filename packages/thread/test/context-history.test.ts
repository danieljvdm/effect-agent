import { ThreadId } from "@effect-agent/core/Identifiers";
import {
  ContextHistory,
  ContextHistoryError,
  ContextHistoryRead,
  ContextHistorySearch,
} from "@effect-agent/engine/ContextHistory";
import { EMPTY_TAIL_DIGEST } from "@effect-agent/thread/Digest";
import type { RecordEnvelope } from "@effect-agent/thread/Records";
import {
  CanonicalRecordEnvelope,
  CanonicalSequence,
  ProducerEpoch,
  PersistedJson,
} from "@effect-agent/thread/Records";
import * as ThreadContextHistory from "@effect-agent/thread/ThreadContextHistory";
import { ThreadStore, ThreadStoreError, ThreadTail } from "@effect-agent/thread/ThreadStore";
import { describe, expect, it } from "@effect/vitest";
import { Cause, Deferred, Effect, Exit, Fiber, Layer, Schema, Stream } from "effect";
import { TestClock } from "effect/testing";
import { Prompt } from "effect/unstable/ai";

const threadId = Schema.decodeSync(ThreadId)("context-thread");
const sequence = Schema.decodeSync(CanonicalSequence);
const digest = "a".repeat(64);

const record = (position: number, payload: typeof RecordEnvelope.Encoded.payload) =>
  Schema.decodeSync(CanonicalRecordEnvelope)({
    threadId,
    batchId: `batch:${position}`,
    sequence: position,
    offset: `offset:${position}`,
    record: {
      recordId: `record:${position}`,
      family: "thread",
      schemaVersion: 1,
      createdAt: "2026-09-04T00:00:00.000Z",
      deploymentId: "test",
      payload,
    },
  });

const promptJson = (prompt: Prompt.Prompt) =>
  Schema.decodeUnknownSync(PersistedJson)(Schema.encodeSync(Prompt.Prompt)(prompt));

const response = (position: number, text: string, runId = "run-1") =>
  record(position, {
    _tag: "ModelResponseRecorded",
    runId,
    turnId: `turn:${position}`,
    turn: position,
    messages: promptJson(
      Prompt.make([
        Prompt.makeMessage("assistant", { content: [Prompt.makePart("text", { text })] }),
      ]),
    ),
    messagesDigest: digest,
  });

/** Read-only port probe; storage adapter suites own persistence and atomic append behavior. */
const probe = (initial: ReadonlyArray<CanonicalRecordEnvelope>) => {
  const state = { records: [...initial], pages: [] as Array<number> };

  const store = ThreadStore.of({
    materialize: () => Effect.die("History lookup cannot materialize Threads"),
    append: () => Effect.die("History lookup cannot append records"),
    observe: () => Stream.die("History lookup cannot observe unbounded history"),
    export: () => Effect.die("History lookup must use bounded reads"),
    inspectTail: (request) =>
      Effect.sync(() =>
        ThreadTail.make({
          threadId: request.threadId,
          tailSequence: sequence(state.records.length),
          tailDigest: EMPTY_TAIL_DIGEST,
          producerEpoch: Schema.decodeSync(ProducerEpoch)(0),
        }),
      ),
    read: (request) => {
      state.pages.push(request.limit);

      return Stream.fromIterable(
        state.records.slice(
          request.afterSequence ?? 0,
          (request.afterSequence ?? 0) + request.limit,
        ),
      );
    },
  });

  return { state, store };
};

const provide = (
  store: ThreadStore["Service"],
  options: ThreadContextHistory.ThreadContextHistoryOptions = {},
) => ThreadContextHistory.layer(options).pipe(Layer.provide(Layer.succeed(ThreadStore, store)));

const search = (query: string, limit = 20) => ContextHistorySearch.make({ threadId, query, limit });

const read = (recordId: string, offset = 0, maxChars = 20_000) =>
  ContextHistoryRead.make({
    threadId,
    recordId,
    offset,
    maxChars,
  });

const archived = [
  response(1, "needle[1] prior Run", "run-0"),
  response(2, "needle[1] first window"),
  record(3, {
    _tag: "ToolCallSettled",
    runId: "run-1",
    toolCallId: "call-1",
    toolName: "read_file",
    result: { text: "needle[1] original tool evidence" },
    isFailure: false,
  }),
  record(4, {
    _tag: "CompactionCreated",
    runId: "run-1",
    turn: 2,
    kind: "rollover",
    coversThrough: 3,
    handoff: "Continue from notes",
  }),
  response(5, "needle[1] second window"),
  record(6, {
    _tag: "CompactionCreated",
    runId: "run-1",
    turn: 3,
    kind: "rollover",
    coversThrough: 5,
  }),
  response(7, "needle[1] third window"),
  response(8, "needle[1] later Run", "run-2"),
];

describe("canonical context history", () => {
  it.effect("searches literal text newest first across retained windows and Runs", () => {
    const p = probe(archived);

    return Effect.gen(function* () {
      const history = yield* ContextHistory;
      const hits = yield* history.search(search("NEEDLE[1]"));

      expect(hits.map(({ recordId, windowId }) => ({ recordId, windowId }))).toEqual([
        { recordId: "record:8", windowId: "context:run-1:3" },
        { recordId: "record:7", windowId: "context:run-1:3" },
        { recordId: "record:5", windowId: "context:run-1:2" },
        { recordId: "record:3", windowId: "context:run-1:0" },
        { recordId: "record:2", windowId: "context:run-1:0" },
        { recordId: "record:1", windowId: "context:run-0:0" },
      ]);
      expect((yield* history.search(search("needle[1]", 2))).map((hit) => hit.recordId)).toEqual([
        "record:8",
        "record:7",
      ]);
      expect(yield* history.search(search("needle.*"))).toEqual([]);
      expect((yield* history.read(read("record:3"))).text).toContain("original tool evidence");
    }).pipe(Effect.provide(provide(p.store)));
  });

  it.effect("pages retained text exactly and returns a bounded snippet around a late match", () => {
    const retained = `start-${"x".repeat(22_000)}-target-end`;

    const p = probe([
      record(1, {
        _tag: "ToolCallSettled",
        runId: "run-1",
        toolCallId: "call-1",
        toolName: "read_file",
        result: retained,
        isFailure: false,
      }),
    ]);

    return Effect.gen(function* () {
      const history = yield* ContextHistory;
      const first = yield* history.read(read("record:1"));

      expect(first.text).toHaveLength(20_000);
      expect(first.nextOffset).toBe(20_000);
      const rest = yield* history.read(read("record:1", first.nextOffset ?? 0));

      expect(rest.nextOffset).toBe(null);
      expect(first.text + rest.text).toBe(
        `tool result read_file (call-1; success):\n${JSON.stringify(retained)}`,
      );
      const hits = yield* history.search(search("target"));

      expect(hits[0]?.text).toContain("target-end");
      expect(hits[0]?.text.length).toBeLessThanOrEqual(2_000);
    }).pipe(Effect.provide(provide(p.store)));
  });

  it.effect(
    "excludes raw submitted fields, system instructions, reasoning, and Step outputs",
    () => {
      const p = probe([
        record(1, {
          _tag: "UserInputRecorded",
          kind: "user",
          runId: "run-1",
          input: { secret: "hidden" },
        }),
        record(2, {
          _tag: "ModelResponseRecorded",
          runId: "run-1",
          turn: 1,
          turnId: "turn-1",
          messagesDigest: digest,
          messages: promptJson(
            Prompt.make([
              Prompt.makeMessage("system", { content: "secret system value" }),
              Prompt.makeMessage("user", {
                content: [Prompt.makePart("text", { text: "projected request" })],
              }),
              Prompt.makeMessage("assistant", {
                content: [
                  Prompt.makePart("reasoning", { text: "secret reasoning value" }),
                  Prompt.makePart("text", { text: "visible answer" }),
                ],
              }),
            ]),
          ),
          runScopedPrefixLength: 2,
        }),
        record(3, {
          _tag: "ToolStepSettled",
          runId: "run-1",
          toolCallId: "call-1",
          stepName: "prepare",
          output: { secret: "private command" },
          outputDigest: digest,
        }),
      ]);

      return Effect.gen(function* () {
        const history = yield* ContextHistory;

        expect(yield* history.search(search("secret"))).toEqual([]);
        expect(
          (yield* history.search(search("projected request"))).map((hit) => hit.recordId),
        ).toEqual(["record:2"]);
        const hidden = yield* Effect.exit(history.read(read("record:3")));

        expect(Exit.isFailure(hidden) && Cause.squash(hidden.cause)).toMatchObject({
          reason: "not-found",
        });
      }).pipe(Effect.provide(provide(p.store)));
    },
  );

  it.effect("captures a fixed tail and requests bounded pages even when new records arrive", () => {
    const p = probe(Array.from({ length: 65 }, (_, i) => response(i + 1, `entry ${i + 1}`)));

    const store = ThreadStore.of({
      ...p.store,
      read: (request) => {
        if (p.state.records.length === 65) p.state.records.push(response(66, "late arrival"));

        return p.store.read(request);
      },
    });

    return Effect.gen(function* () {
      const history = yield* ContextHistory;

      expect(yield* history.search(search("late arrival"))).toEqual([]);
      expect(p.state.pages).toEqual([64, 1]);
      expect((yield* history.search(search("late arrival"))).map((hit) => hit.recordId)).toEqual([
        "record:66",
      ]);
    }).pipe(Effect.provide(provide(store)));
  });

  it.effect("fails explicitly before scanning an archive above the configured ceiling", () => {
    const p = probe(archived);

    return Effect.gen(function* () {
      const history = yield* ContextHistory;
      const exit = yield* Effect.exit(history.search(search("needle")));

      expect(Exit.isFailure(exit) && Cause.squash(exit.cause)).toMatchObject({ reason: "limit" });
      expect(p.state.pages).toEqual([]);
    }).pipe(Effect.provide(provide(p.store, { maxRecords: 2 })));
  });

  it.effect("rejects whitespace queries and reads beyond retained text", () => {
    const p = probe([response(1, "short")]);

    return Effect.gen(function* () {
      const history = yield* ContextHistory;

      const requests: ReadonlyArray<Effect.Effect<unknown, ContextHistoryError>> = [
        history.search(search("  ")),
        history.read(read("record:1", 100)),
      ];

      for (const request of requests) {
        const exit = yield* Effect.exit(request);

        expect(Exit.isFailure(exit) && Cause.squash(exit.cause)).toMatchObject({
          reason: "invalid-input",
        });
      }
    }).pipe(Effect.provide(provide(p.store)));
  });

  it.effect("fails closed on foreign or noncontiguous records and hides storage diagnostics", () =>
    Effect.gen(function* () {
      const p = probe([response(1, "private")]);

      const foreign = CanonicalRecordEnvelope.make({
        ...p.state.records[0]!,
        threadId: Schema.decodeSync(ThreadId)("foreign"),
      });

      for (const stream of [
        Stream.succeed(foreign),
        Stream.succeed(response(2, "gap")),
        Stream.fail(
          ThreadStoreError.make({ operation: "read", message: "secret backend diagnostic" }),
        ),
      ]) {
        const exit = yield* Effect.gen(function* () {
          return yield* (yield* ContextHistory).search(search("private"));
        }).pipe(Effect.provide(provide({ ...p.store, read: () => stream })), Effect.exit);

        expect(Exit.isFailure(exit) && Cause.squash(exit.cause)).toEqual(
          ContextHistoryError.make({
            reason: "unavailable",
            message: "Canonical context history is unavailable",
          }),
        );
      }
    }),
  );

  it.effect("preserves defects and finalizes a read on timeout or caller interruption", () =>
    Effect.gen(function* () {
      const p = probe([response(1, "text")]);

      const defect = yield* Effect.gen(function* () {
        return yield* (yield* ContextHistory).search(search("text"));
      }).pipe(
        Effect.provide(provide({ ...p.store, read: () => Stream.die("backend defect") })),
        Effect.exit,
      );

      expect(Exit.isFailure(defect) && Cause.squash(defect.cause)).toBe("backend defect");
      for (const mode of ["timeout", "interrupt"] as const) {
        const entered = yield* Deferred.make<void>();
        let released = 0;

        const waiting = Stream.unwrap(
          Effect.gen(function* () {
            yield* Effect.acquireRelease(Deferred.succeed(entered, undefined), () =>
              Effect.sync(() => {
                released += 1;
              }),
            );

            return Stream.never;
          }),
        );

        const fiber = yield* Effect.gen(function* () {
          return yield* (yield* ContextHistory).read(read("record:1"));
        }).pipe(
          Effect.provide(provide({ ...p.store, read: () => waiting }, { timeoutMillis: 100 })),
          Effect.forkChild,
        );

        yield* Deferred.await(entered);
        if (mode === "timeout") yield* TestClock.adjust(100);
        else yield* Fiber.interrupt(fiber);
        const exit = yield* Fiber.await(fiber);

        expect(Exit.isFailure(exit)).toBe(true);
        if (Exit.isFailure(exit)) {
          if (mode === "timeout")
            expect(Cause.squash(exit.cause)).toMatchObject({ reason: "limit" });
          else expect(Cause.hasInterrupts(exit.cause)).toBe(true);
        }
        expect(released).toBe(1);
      }
    }),
  );
});

type Equal<L, R> =
  (<T>() => T extends L ? 1 : 2) extends <T>() => T extends R ? 1 : 2
    ? (<T>() => T extends R ? 1 : 2) extends <T>() => T extends L ? 1 : 2
      ? true
      : false
    : false;
type Assert<T extends true> = T;
type HistoryLayer = ReturnType<typeof ThreadContextHistory.layer>;
type SuccessProof = Assert<Equal<Layer.Success<HistoryLayer>, ContextHistory>>;
type FailureProof = Assert<Equal<Layer.Error<HistoryLayer>, ContextHistoryError>>;
type RequirementProof = Assert<Equal<Layer.Services<HistoryLayer>, ThreadStore>>;
it("keeps the archive dependency and typed lookup failures visible", () => {
  const proof: readonly [SuccessProof, FailureProof, RequirementProof] = [true, true, true];

  expect(proof).toEqual([true, true, true]);
});
