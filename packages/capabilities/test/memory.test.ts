import type { MemoryLookup } from "@effect-agent/core";
import {
  MemoryAttribution,
  MemoryContent,
  MemoryPassage,
  MemoryRecallError,
  MemoryRecallLimits,
  MemorySourceReference,
} from "@effect-agent/core";
import { describe, expect, it } from "@effect/vitest";
import { Cause, Context, Deferred, Effect, Exit, Fiber, Ref } from "effect";
import { TestClock } from "effect/testing";

import { recallMemory, type MemoryRecallSource } from "../src/index.ts";

const limits = MemoryRecallLimits.make({
  maxSources: 8,
  maxItems: 8,
  maxBytes: 16_384,
  maxTokens: 16_384,
  timeoutMillis: 1_000,
});

const passage = (id: string, text: string) =>
  MemoryPassage.make({
    version: 1,
    source: MemorySourceReference.make({
      id,
      locator: `https://corpus.example/${id}`,
      revision: "r1",
    }),
    passageId: "document",
    content: MemoryContent.make({
      text,
      attributions: [
        MemoryAttribution.make({
          originId: `dan:${id}`,
          speaker: "Dan",
          observers: ["Chad"],
          locator: "chat://engineering/42",
          activityAt: 10,
          interpretation: "proposal, not an agreed outcome",
        }),
      ],
      metadata: { topic: "queue", confidence: "uncertain" },
      recordedAt: 20,
      extractedAt: 30,
    }),
  });

const found = (...passages: ReadonlyArray<MemoryPassage>): MemoryLookup => ({
  _tag: "Found",
  passages,
});
const source = (id: string, result: MemoryLookup, essential = false): MemoryRecallSource => ({
  id,
  essential,
  read: Effect.succeed(result),
});

describe("bounded memory recall", () => {
  it.effect(
    "reads Markdown and external passages without a store, retaining attribution and uncertainty",
    () =>
      Effect.gen(function* () {
        const markdown = passage("notes", "# Proposal\nUse a queue for retries.");
        const external = passage("external", "Adam proposes synchronous delivery.");
        const recalled = yield* recallMemory(
          [source("known", found(markdown), true), source("remote", found(external, markdown))],
          limits,
        );
        expect(recalled.passages).toEqual([markdown, external]);
        expect(recalled.outcomes[1]).toMatchObject({ selected: 1, deduplicated: 1 });
        expect(recalled.text).toContain("Untrusted reference material");
        expect(recalled.text).toContain('"citation":"memory:1"');
        expect(recalled.text).toContain('"speaker":"Dan"');
        expect(recalled.text).toContain('"observers":["Chad"]');
        expect(recalled.text).toContain('"activityAt":10');
        expect(recalled.text).toContain("not an agreed outcome");
        expect(recalled.bytes).toBeLessThanOrEqual(limits.maxBytes);
        expect(recalled.estimatedTokens).toBeLessThanOrEqual(limits.maxTokens);
        expect(yield* recallMemory([], limits)).toMatchObject({
          text: "",
          passages: [],
          estimatedTokens: 0,
        });
      }),
  );

  it.effect("selects whole ranked passages under item, UTF-8 byte, and tokenizer bounds", () =>
    Effect.gen(function* () {
      const small = passage("small", "A bounded passage 🌊");
      const large = passage("large", "oversize ".repeat(2_000));
      const one = yield* recallMemory([source("known", found(small))], limits);
      const recalled = yield* recallMemory(
        [source("ranked", found(large, small, passage("third", "later")))],
        MemoryRecallLimits.make({
          ...limits,
          maxItems: 1,
          maxBytes: one.bytes,
          maxTokens: one.bytes,
        }),
      );
      expect(recalled.passages).toEqual([small]);
      expect(recalled.bytes).toBe(one.bytes);
      expect(recalled.outcomes[0]).toMatchObject({ selected: 1, omitted: 2 });
      const tokenLimited = yield* recallMemory(
        [source("known", found(small))],
        limits,
        () => limits.maxTokens + 1,
      );
      expect(tokenLimited.passages).toEqual([]);
      const essential = yield* recallMemory([source("essential", found(large), true)], limits).pipe(
        Effect.flip,
      );
      expect(essential).toMatchObject({
        _tag: "MemoryRecallError",
        reason: "budget",
        sourceId: "essential",
      });
    }),
  );

  it.effect(
    "keeps optional failure outcomes distinct and fails essential unavailable or stale sources",
    () =>
      Effect.gen(function* () {
        const unavailable: MemoryLookup = { _tag: "Unavailable", message: "offline" };
        const stale: MemoryLookup = {
          _tag: "InsufficientFreshness",
          message: "view has not caught up",
        };
        const result = yield* recallMemory(
          [
            source("none", { _tag: "NoMatch" }, true),
            source("offline", unavailable),
            source("stale", stale),
          ],
          limits,
        );
        expect(result.outcomes.map((outcome) => outcome.status)).toEqual([
          "NoMatch",
          "Unavailable",
          "InsufficientFreshness",
        ]);
        expect(
          yield* recallMemory([source("offline", unavailable, true)], limits).pipe(Effect.flip),
        ).toMatchObject({ reason: "unavailable" });
        expect(
          yield* recallMemory([source("stale", stale, true)], limits).pipe(Effect.flip),
        ).toMatchObject({ reason: "insufficient-freshness" });
      }),
  );

  it.effect(
    "preserves unknown-revision disagreement and rejects conflicting claims to one known revision",
    () =>
      Effect.gen(function* () {
        const first = passage("claim", "use a queue");
        const second = passage("claim", "deliver synchronously");
        const unknownFirst = MemoryPassage.make({
          ...first,
          source: MemorySourceReference.make({ ...first.source, revision: null }),
        });
        const unknownSecond = MemoryPassage.make({ ...second, source: unknownFirst.source });
        const result = yield* recallMemory(
          [source("unknown", found(unknownFirst, unknownSecond, unknownFirst))],
          limits,
        );
        expect(result.passages).toEqual([unknownFirst, unknownSecond]);
        expect(result.outcomes[0]).toMatchObject({ selected: 2, deduplicated: 1 });
        expect(result.text).toContain("not independent corroboration");
        expect(
          yield* recallMemory([source("conflicting", found(first, second))], limits).pipe(
            Effect.flip,
          ),
        ).toMatchObject({ reason: "invalid-input" });
      }),
  );

  it.effect(
    "bounds source identities from a single retriever and retains the validated tokenizer result",
    () =>
      Effect.gen(function* () {
        const first = passage("first", "first source");
        const second = passage("second", "second source");
        const result = yield* recallMemory(
          [source("remote", found(first, second))],
          MemoryRecallLimits.make({ ...limits, maxSources: 1 }),
        );
        expect(result.passages).toEqual([first]);
        let calls = 0;
        const estimated = yield* recallMemory([source("remote", found(first))], limits, () =>
          ++calls === 1 ? 100 : Number.NaN,
        );
        expect(calls).toBe(1);
        expect(estimated.estimatedTokens).toBe(100);
      }),
  );

  it.effect("rejects malformed adapter results and invalid token estimates", () =>
    Effect.gen(function* () {
      const malformed = { ...passage("malformed", "text") };
      Reflect.set(malformed, "source", { id: "missing provenance" });
      const error = yield* recallMemory([source("remote", found(malformed))], limits).pipe(
        Effect.flip,
      );
      expect(error).toMatchObject({ reason: "invalid-input", sourceId: "remote" });
      const metadata = {
        ...passage("metadata", "text"),
        content: { ...passage("metadata", "text").content },
      };
      Reflect.set(metadata.content, "metadata", { giant: "x".repeat(8_193) });
      expect(
        yield* recallMemory([source("metadata", found(metadata))], limits).pipe(Effect.flip),
      ).toMatchObject({ reason: "invalid-input" });
      for (const estimate of [Number.NaN, -1, 0, 1.5, Number.POSITIVE_INFINITY]) {
        expect(
          yield* recallMemory(
            [source("known", found(passage("a", "text")))],
            limits,
            () => estimate,
          ).pipe(Effect.flip),
        ).toMatchObject({ reason: "invalid-input" });
      }
    }),
  );

  it.effect("retains expected reader errors and defects and finalizes temporary readers", () =>
    Effect.gen(function* () {
      const finalized = yield* Ref.make(0);
      for (const failure of [Effect.fail("offline" as const), Effect.die("reader defect")]) {
        const read = Effect.acquireRelease(Effect.void, () =>
          Ref.update(finalized, (n) => n + 1),
        ).pipe(Effect.andThen(failure));
        const exit = yield* recallMemory([{ id: "remote", essential: false, read }], limits).pipe(
          Effect.exit,
        );
        expect(Exit.isFailure(exit)).toBe(true);
        if (Exit.isFailure(exit)) {
          expect(Cause.pretty(exit.cause)).toMatch(/offline|reader defect/);
        }
      }
      expect(yield* Ref.get(finalized)).toBe(2);
      yield* recallMemory(
        [
          {
            id: "success",
            essential: false,
            read: Effect.acquireRelease(Effect.succeed(found()), () =>
              Ref.update(finalized, (n) => n + 1),
            ),
          },
        ],
        limits,
      );
      expect(yield* Ref.get(finalized)).toBe(3);
    }),
  );

  it.effect("enforces its deadline and finalizes readers on timeout and interruption", () =>
    Effect.gen(function* () {
      const finalized = yield* Ref.make(0);
      const started = yield* Deferred.make<void>();
      const read = Effect.acquireRelease(Deferred.succeed(started, undefined), () =>
        Ref.update(finalized, (n) => n + 1),
      ).pipe(Effect.andThen(Effect.never));
      const timeout = yield* recallMemory([{ id: "slow", essential: false, read }], limits).pipe(
        Effect.forkChild,
      );
      yield* Deferred.await(started);
      yield* TestClock.adjust(1_000);
      expect(yield* Fiber.join(timeout).pipe(Effect.flip)).toMatchObject({ reason: "timeout" });
      expect(yield* Ref.get(finalized)).toBe(1);
      const entered = yield* Deferred.make<void>();
      const interrupted = yield* recallMemory(
        [
          {
            id: "interrupted",
            essential: false,
            read: Effect.acquireRelease(Deferred.succeed(entered, undefined), () =>
              Ref.update(finalized, (n) => n + 1),
            ).pipe(Effect.andThen(Effect.never)),
          },
        ],
        limits,
      ).pipe(Effect.forkChild);
      yield* Deferred.await(entered);
      yield* Fiber.interrupt(interrupted);
      expect(yield* Ref.get(finalized)).toBe(2);
    }),
  );
});

class Corpus extends Context.Service<
  Corpus,
  { readonly read: Effect.Effect<MemoryLookup, "remote-failure"> }
>()("memory-test/Corpus") {}

it("preserves reader E/R and discharges only the recall-owned Scope", () => {
  const program = recallMemory(
    [
      {
        id: "typed",
        essential: true,
        read: Effect.flatMap(Corpus, (corpus) => corpus.read),
      },
    ],
    limits,
  );
  const error: Effect.Error<typeof program> = "remote-failure";
  const taggedError: Effect.Error<typeof program> = MemoryRecallError.make({
    reason: "timeout",
    message: "timed out",
  });
  const requirement: [Effect.Services<typeof program>] extends [Corpus] ? true : false = true;
  const readerRequired: [Corpus] extends [Effect.Services<typeof program>] ? true : false = true;
  const exactErrors: [Effect.Error<typeof program>] extends ["remote-failure" | MemoryRecallError]
    ? true
    : false = true;
  expect([error, taggedError._tag, requirement, readerRequired, exactErrors]).toEqual([
    "remote-failure",
    "MemoryRecallError",
    true,
    true,
    true,
  ]);
});
