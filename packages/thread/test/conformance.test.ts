import { ThreadId } from "@effect-agent/core";
import { expect, describe, it } from "@effect/vitest";
import { Effect, Schema } from "effect";

import {
  AppendConflict,
  BatchId,
  CanonicalSequence,
  ThreadTail,
  ThreadTailRequest,
  Digest,
  EMPTY_TAIL_DIGEST,
  ProducerEpoch,
} from "../src/index.ts";
import { threadStoreConformanceCases } from "../src/testing.ts";

const threadId = Schema.decodeSync(ThreadId)("thread-port-1");
const batchId = Schema.decodeSync(BatchId)("batch-port-1");
const sequence = Schema.decodeSync(CanonicalSequence);
const epoch = Schema.decodeSync(ProducerEpoch);
const digest = Schema.decodeSync(Digest)("a".repeat(64));

describe("ThreadStore port schemas", () => {
  it.effect("round-trips tail inspection requests and results", () =>
    Effect.gen(function* () {
      const request = ThreadTailRequest.make({ threadId });

      const decodedRequest = yield* Schema.encodeEffect(ThreadTailRequest)(request).pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(ThreadTailRequest)),
      );

      expect(decodedRequest).toEqual(request);

      const tail = ThreadTail.make({
        threadId,
        tailSequence: sequence(3),
        tailDigest: digest,
        producerEpoch: epoch(2),
      });

      const decodedTail = yield* Schema.encodeEffect(ThreadTail)(tail).pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(ThreadTail)),
      );

      expect(decodedTail).toEqual(tail);

      const emptyTail = ThreadTail.make({
        threadId,
        tailSequence: sequence(0),
        tailDigest: EMPTY_TAIL_DIGEST,
        producerEpoch: epoch(1),
      });

      expect(
        yield* Schema.encodeEffect(ThreadTail)(emptyTail).pipe(
          Effect.flatMap(Schema.decodeUnknownEffect(ThreadTail)),
        ),
      ).toEqual(emptyTail);
    }),
  );

  it.effect("round-trips every append conflict reason with and without the tail hint", () =>
    Effect.gen(function* () {
      const reasons = ["batch-digest", "record-identity", "tail"] as const;

      yield* Effect.forEach(
        reasons,
        (reason) =>
          Effect.gen(function* () {
            const bare = AppendConflict.make({ threadId, batchId, reason });

            const decodedBare = yield* Schema.encodeEffect(AppendConflict)(bare).pipe(
              Effect.flatMap(Schema.decodeUnknownEffect(AppendConflict)),
            );

            expect(decodedBare.reason).toBe(reason);
            expect(decodedBare.actualTailSequence).toBeUndefined();
            expect(decodedBare.actualTailDigest).toBeUndefined();
          }),
        { discard: true },
      );

      const hinted = AppendConflict.make({
        threadId,
        batchId,
        reason: "tail",
        actualTailSequence: sequence(7),
        actualTailDigest: digest,
      });

      const decodedHinted = yield* Schema.encodeEffect(AppendConflict)(hinted).pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(AppendConflict)),
      );

      expect(decodedHinted.actualTailSequence).toBe(sequence(7));
      expect(decodedHinted.actualTailDigest).toBe(digest);
    }),
  );

  it.effect("rejects append conflict reasons outside the declared literals", () =>
    Effect.gen(function* () {
      const failure = yield* Schema.decodeUnknownEffect(AppendConflict)({
        _tag: "AppendConflict",
        threadId,
        batchId,
        reason: "unknown-reason",
      }).pipe(Effect.flip);

      expect(failure._tag).toBe("SchemaError");
    }),
  );

  it("names every shared conformance case uniquely", () => {
    const names = threadStoreConformanceCases.map((conformanceCase) => conformanceCase.name);

    expect(new Set(names).size).toBe(names.length);
  });
});
