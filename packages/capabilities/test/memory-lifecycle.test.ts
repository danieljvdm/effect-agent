import {
  ActiveMemoryDocument,
  MemoryAttribution,
  MemoryContent,
  MemoryKey,
  type MemoryLookup,
  MemoryPassage,
  MemoryReader,
  MemoryRecallLimits,
  MemoryStorageError,
  WithdrawnMemoryDocument,
} from "@effect-agent/core";
import { describe, expect, it } from "@effect/vitest";
import { Cause, Deferred, Effect, Exit, Fiber, Layer, Ref } from "effect";
import { TestClock } from "effect/testing";

import { MemoryAccess, recallMemory, revalidateMemoryLookup } from "../src/index.ts";

const key = MemoryKey.make({ namespace: "team-a", id: "queue-discussion" });
const access = MemoryAccess.make({ namespace: "team-a", scope: "participating-channels" });
const document = ActiveMemoryDocument.make({
  version: 1,
  key,
  source: { id: key.id, locator: "chat://engineering/42", revision: "1" },
  generation: 1,
  predecessor: null,
  modifiedAt: 30,
  scopes: [access.scope],
  content: MemoryContent.make({
    text: "Dan proposes a queue. No decision yet.",
    attributions: [
      MemoryAttribution.make({
        originId: "engineering:42",
        speaker: "Dan",
        observers: ["Chad"],
        locator: "chat://engineering/42",
        activityAt: 10,
        interpretation: "proposal",
      }),
    ],
    metadata: { topic: "delivery" },
    recordedAt: 20,
    extractedAt: 25,
  }),
});
const candidate = MemoryPassage.make({
  version: 1,
  source: document.source,
  passageId: "claim",
  content: document.content,
});
const candidates: MemoryLookup = { _tag: "Found", passages: [candidate] };
const limits = MemoryRecallLimits.make({
  maxSources: 4,
  maxItems: 8,
  maxBytes: 16_384,
  maxTokens: 16_384,
  timeoutMillis: 100,
});

const recall = (lookup = candidates) =>
  recallMemory(
    [
      {
        id: "stale-cache",
        essential: true,
        read: revalidateMemoryLookup(lookup, access),
      },
    ],
    limits,
  );

describe("authoritative memory validation", () => {
  it.effect(
    "replaces stale passages with the correction, preserving source attribution and original times",
    () =>
      Effect.gen(function* () {
        const corrected = ActiveMemoryDocument.make({
          ...document,
          source: { ...document.source, revision: "2" },
          generation: 2,
          predecessor: document.source,
          modifiedAt: 1_000,
          content: {
            ...document.content,
            text: "Dan retracts the queue proposal. The approach is unresolved.",
          },
        });
        const result = yield* recall().pipe(
          Effect.provideService(MemoryReader, { get: () => Effect.succeed(corrected) }),
        );
        expect(result.passages).toHaveLength(1);
        expect(result.passages[0]).toMatchObject({
          source: { revision: "2" },
          passageId: "document",
          content: {
            text: corrected.content.text,
            recordedAt: 20,
            extractedAt: 25,
            attributions: [
              { speaker: "Dan", observers: ["Chad"], activityAt: 10, interpretation: "proposal" },
            ],
          },
        });
        expect(result.text).not.toContain(document.content.text);
      }),
  );

  it.effect(
    "omits withdrawn, revoked, absent, or wrong-namespace sources regardless of cached text",
    () =>
      Effect.gen(function* () {
        const withdrawn = WithdrawnMemoryDocument.make({
          version: 1,
          key,
          source: { ...document.source, revision: "2" },
          generation: 2,
          predecessor: document.source,
          modifiedAt: 100,
          reason: "withdrawn by the source owner",
        });
        for (const current of [
          withdrawn,
          ActiveMemoryDocument.make({ ...document, scopes: [] }),
          null,
        ]) {
          const result = yield* recall().pipe(
            Effect.provideService(MemoryReader, { get: () => Effect.succeed(current) }),
          );
          expect(result.passages).toEqual([]);
          expect(result.outcomes[0]?.status).toBe("NoMatch");
        }
        const leaked = ActiveMemoryDocument.make({
          ...document,
          key: { ...key, namespace: "other-tenant" },
        });
        const error = yield* recall().pipe(
          Effect.provideService(MemoryReader, { get: () => Effect.succeed(leaked) }),
          Effect.flip,
        );
        expect(error).toMatchObject({ _tag: "MemoryStorageError", reason: "corrupt" });
      }),
  );

  it.effect(
    "checks every recall again and replaces forged provenance and nonexistent excerpts",
    () =>
      Effect.gen(function* () {
        const reads = yield* Ref.make(0);
        const reader = MemoryReader.of({
          get: () =>
            Ref.getAndUpdate(reads, (n) => n + 1).pipe(
              Effect.map((n) => (n === 0 ? document : null)),
            ),
        });
        const forged = MemoryPassage.make({
          ...candidate,
          content: {
            ...candidate.content,
            text: "An invented agreement",
            attributions: [{ ...candidate.content.attributions[0], speaker: "Adam" }],
          },
        });
        const first = yield* recall({ _tag: "Found", passages: [forged, forged] }).pipe(
          Effect.provideService(MemoryReader, reader),
        );
        expect(first.passages).toHaveLength(1);
        expect(first.passages[0]?.content).toEqual(document.content);
        expect(yield* Ref.get(reads)).toBe(1);
        const second = yield* recall().pipe(Effect.provideService(MemoryReader, reader));
        expect(second.passages).toEqual([]);
        expect(yield* Ref.get(reads)).toBe(2);
      }),
  );

  it.effect("does not turn expected reader failure or defects into a missing source", () =>
    Effect.gen(function* () {
      const failure = MemoryStorageError.make({ operation: "read", reason: "unavailable" });
      expect(
        yield* recall().pipe(
          Effect.provideService(MemoryReader, { get: () => Effect.fail(failure) }),
          Effect.flip,
        ),
      ).toEqual(failure);
      const defect = yield* recall().pipe(
        Effect.provideService(MemoryReader, { get: () => Effect.die("reader defect") }),
        Effect.exit,
      );
      expect(Exit.isFailure(defect)).toBe(true);
      if (Exit.isFailure(defect)) expect(Cause.pretty(defect.cause)).toContain("reader defect");
    }),
  );

  it.effect("bounds validation time and releases a reader on timeout and interruption", () =>
    Effect.gen(function* () {
      const releases = yield* Ref.make(0);
      for (const mode of ["timeout", "interruption"] as const) {
        const started = yield* Deferred.make<void>();
        const layer = Layer.effect(
          MemoryReader,
          Effect.acquireRelease(
            Effect.succeed(
              MemoryReader.of({
                get: () => Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never)),
              }),
            ),
            () => Ref.update(releases, (n) => n + 1),
          ),
        );
        const fiber = yield* recallMemory(
          [
            {
              id: "slow-view",
              essential: false,
              read: revalidateMemoryLookup(candidates, access).pipe(Effect.provide(layer)),
            },
          ],
          limits,
        ).pipe(Effect.forkChild);
        yield* Deferred.await(started);
        if (mode === "timeout") {
          yield* TestClock.adjust(100);
          expect(yield* Fiber.join(fiber).pipe(Effect.flip)).toMatchObject({ reason: "timeout" });
        } else {
          yield* Fiber.interrupt(fiber);
        }
      }
      expect(yield* Ref.get(releases)).toBe(2);
    }),
  );
});
