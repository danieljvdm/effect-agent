import { describe, expect, it } from "@effect/vitest";
import { Cause, Deferred, Effect, Exit, Fiber, Layer, Schema, Stream } from "effect";
import {
  ContextHistory,
  ContextHistoryError,
  ContextHistoryRead,
  ContextHistorySearch,
} from "effect-agent/context-history";
import { EMPTY_TAIL_DIGEST } from "effect-agent/digest";
import { ThreadId } from "effect-agent/identifiers";
import type { RecordEnvelope } from "effect-agent/records";
import {
  CanonicalRecordEnvelope,
  CanonicalSequence,
  ProducerEpoch,
  PersistedJson,
} from "effect-agent/records";
import * as ThreadContextHistory from "effect-agent/thread-context-history";
import { ThreadStore, ThreadStoreError, ThreadTail } from "effect-agent/thread-store";
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
    readIdentity: () => Effect.die("History lookup must not read worker identity"),
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
      if ("selection" in request)
        return Stream.die("History fixture does not implement selected reads");
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

const search = (query: string, limit = 20, beforeRecordId?: string) =>
  ContextHistorySearch.make({
    threadId,
    query,
    limit,
    ...(beforeRecordId === undefined ? {} : { beforeRecordId }),
  });

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
  it.effect(
    "reaches the original receipt behind newer recall and notes matches despite appends",
    () => {
      // Regression for #372: the original remains at sequence 16 while later recall and notes
      // occupy every newest-first result. Use synthetic evidence, independent of the live oracle.
      const p = probe([
        ...Array.from({ length: 15 }, (_, i) => response(i + 1, "unrelated")),
        record(16, {
          _tag: "ModelResponseRecorded",
          runId: "run-1",
          turnId: "turn:16",
          turn: 16,
          messages: promptJson(
            Prompt.make([
              Prompt.makeMessage("user", {
                content: [
                  Prompt.makePart("text", {
                    text: "Archive RECEIPTS: dock-01 | verification code receipt-original-7",
                  }),
                ],
              }),
            ]),
          ),
          messagesDigest: digest,
        }),
        record(17, {
          _tag: "CompactionCreated",
          runId: "run-1",
          turn: 17,
          kind: "rollover",
          coversThrough: 16,
        }),
        ...Array.from({ length: 48 }, (_, i) =>
          record(i + 18, {
            _tag: "ToolCallSettled",
            runId: "run-1",
            toolCallId: `call:${i}`,
            toolName: i % 2 === 0 ? "read_notes" : "search_context_windows",
            result: { text: `Look up RECEIPTS for dock-01. ${"x".repeat(2_500)}` },
            isFailure: false,
          }),
        ),
      ]);

      return Effect.gen(function* () {
        const history = yield* ContextHistory;
        let hits = yield* history.search(search("RECEIPTS", 3));

        expect(hits.map((hit) => hit.recordId)).toEqual(["record:65", "record:64", "record:63"]);
        expect(hits.every((hit) => !hit.text.includes("receipt-original-7"))).toBe(true);
        const visited = hits.map((hit) => hit.recordId);

        for (let page = 0; page < 16; page++) {
          // Persisting the preceding recall creates another newer literal match on every page.
          p.state.records.push(response(p.state.records.length + 1, "RECEIPTS recall"));
          hits = yield* history.search(search("RECEIPTS", 3, hits.at(-1)!.recordId));
          expect(hits.length).toBe(page === 15 ? 1 : 3);
          for (const hit of hits) {
            expect(visited).not.toContain(hit.recordId);
            expect(hit.text.length).toBeLessThanOrEqual(2_000);
            visited.push(hit.recordId);
          }
        }

        expect(visited).toEqual([
          ...Array.from({ length: 48 }, (_, i) => `record:${65 - i}`),
          "record:16",
        ]);
        expect(hits[0]).toMatchObject({ recordId: "record:16", windowId: "context:run-1:0" });
        expect((yield* history.read(read(hits[0]!.recordId))).text).toContain("receipt-original-7");
        expect(yield* history.search(search("RECEIPTS", 3, "record:16"))).toEqual([]);
        expect(p.state.pages.every((limit) => limit <= 64)).toBe(true);
      }).pipe(Effect.provide(provide(p.store, { maxRecords: 128 })));
    },
  );

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

  it.effect("only accepts anchors in the captured tail and verifies records after the anchor", () =>
    Effect.gen(function* () {
      const p = probe(archived);

      const appending = ThreadStore.of({
        ...p.store,
        read: (request) => {
          if (p.state.records.length === 8) p.state.records.push(response(9, "late anchor"));

          return p.store.read(request);
        },
      });

      yield* Effect.gen(function* () {
        const history = yield* ContextHistory;
        const missing = yield* Effect.exit(history.search(search("needle", 3, "record:9")));

        expect(Exit.isFailure(missing) && Cause.squash(missing.cause)).toMatchObject({
          reason: "not-found",
        });
        expect(
          (yield* history.search(search("needle", 3, "record:9"))).map((hit) => hit.recordId),
        ).toEqual(["record:8", "record:7", "record:5"]);
      }).pipe(Effect.provide(provide(appending)));

      const corrupt = probe([
        ...archived.slice(0, 7),
        // A later corrupt rollover must not be skipped even when an early anchor is found.
        record(8, {
          _tag: "CompactionCreated",
          runId: "run-1",
          turn: 4,
          kind: "rollover",
          coversThrough: 2,
        }),
      ]);

      const result = yield* Effect.gen(function* () {
        return yield* (yield* ContextHistory).search(search("needle", 3, "record:2"));
      }).pipe(Effect.provide(provide(corrupt.store)), Effect.exit);

      expect(Exit.isFailure(result) && Cause.squash(result.cause)).toMatchObject({
        reason: "unavailable",
      });
    }),
  );

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
        for (const request of [search("private"), search("private", 3, "record:1")]) {
          const exit = yield* Effect.gen(function* () {
            return yield* (yield* ContextHistory).search(request);
          }).pipe(Effect.provide(provide({ ...p.store, read: () => stream })), Effect.exit);

          expect(Exit.isFailure(exit) && Cause.squash(exit.cause)).toEqual(
            ContextHistoryError.make({
              reason: "unavailable",
              message: "Canonical context history is unavailable",
            }),
          );
        }
      }
    }),
  );

  it.effect("preserves defects and finalizes a read on timeout or caller interruption", () =>
    Effect.gen(function* () {
      const p = probe([response(1, "text")]);

      const defect = yield* Effect.gen(function* () {
        return yield* (yield* ContextHistory).search(search("text", 3, "record:1"));
      }).pipe(
        Effect.provide(provide({ ...p.store, read: () => Stream.die("backend defect") })),
        Effect.exit,
      );

      expect(Exit.isFailure(defect) && Cause.squash(defect.cause)).toBe("backend defect");
      for (const mode of ["read-timeout", "search-interrupt"] as const) {
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
          const history = yield* ContextHistory;

          return mode.startsWith("read")
            ? yield* history.read(read("record:1"))
            : yield* history.search(search("text", 3, "record:1"));
        }).pipe(
          Effect.provide(provide({ ...p.store, read: () => waiting }, { timeoutMillis: 100 })),
          Effect.forkChild,
        );

        yield* Deferred.await(entered);
        if (mode.endsWith("timeout")) yield* TestClock.adjust(100);
        else yield* Fiber.interrupt(fiber);
        const exit = yield* Fiber.await(fiber);

        expect(Exit.isFailure(exit)).toBe(true);
        if (Exit.isFailure(exit)) {
          if (mode.endsWith("timeout"))
            expect(Cause.squash(exit.cause)).toMatchObject({ reason: "limit" });
          else expect(Cause.hasInterrupts(exit.cause)).toBe(true);
        }
        expect(released).toBe(1);
      }
    }),
  );
});
