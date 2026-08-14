import { ConversationId } from "@effect-agent/core";
import { expect, describe, it } from "@effect/vitest";
import { Effect, Schema } from "effect";

import {
  AppendConflict,
  BatchId,
  CanonicalSequence,
  ConversationTail,
  ConversationTailRequest,
  conversationStoreConformanceCases,
  Digest,
  EMPTY_TAIL_DIGEST,
  ProducerEpoch,
} from "../src/index.ts";

const conversationId = Schema.decodeSync(ConversationId)("conversation-port-1");
const batchId = Schema.decodeSync(BatchId)("batch-port-1");
const sequence = Schema.decodeSync(CanonicalSequence);
const epoch = Schema.decodeSync(ProducerEpoch);
const digest = Schema.decodeSync(Digest)("a".repeat(64));

describe("ConversationStore port schemas", () => {
  it.effect("round-trips tail inspection requests and results", () =>
    Effect.gen(function* () {
      const request = ConversationTailRequest.make({ conversationId });
      const decodedRequest = yield* Schema.encodeEffect(ConversationTailRequest)(request).pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(ConversationTailRequest)),
      );
      expect(decodedRequest).toEqual(request);

      const tail = ConversationTail.make({
        conversationId,
        tailSequence: sequence(3),
        tailDigest: digest,
        producerEpoch: epoch(2),
      });
      const decodedTail = yield* Schema.encodeEffect(ConversationTail)(tail).pipe(
        Effect.flatMap(Schema.decodeUnknownEffect(ConversationTail)),
      );
      expect(decodedTail).toEqual(tail);

      const emptyTail = ConversationTail.make({
        conversationId,
        tailSequence: sequence(0),
        tailDigest: EMPTY_TAIL_DIGEST,
        producerEpoch: epoch(1),
      });
      expect(
        yield* Schema.encodeEffect(ConversationTail)(emptyTail).pipe(
          Effect.flatMap(Schema.decodeUnknownEffect(ConversationTail)),
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
            const bare = AppendConflict.make({ conversationId, batchId, reason });
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
        conversationId,
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
        conversationId,
        batchId,
        reason: "unknown-reason",
      }).pipe(Effect.flip);
      expect(failure._tag).toBe("SchemaError");
    }),
  );

  it("names every shared conformance case uniquely", () => {
    const names = conversationStoreConformanceCases.map((conformanceCase) => conformanceCase.name);
    expect(new Set(names).size).toBe(names.length);
    expect(names.length).toBeGreaterThanOrEqual(8);
  });
});
