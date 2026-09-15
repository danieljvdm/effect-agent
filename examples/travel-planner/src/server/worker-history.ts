import { Effect, Schema, Stream } from "effect";
import type { ThreadId } from "effect-agent/identifiers";
import { CanonicalSequence } from "effect-agent/records";
import { ThreadRead, ThreadStore, ThreadTailRequest } from "effect-agent/thread-store";

import { PlannerError } from "../domain.ts";

/** Read a finite recent window, bounding storage/decoding before projecting activity. */
export const workerHistory = Effect.fn("workerHistory")(
  function* (threadId: ThreadId) {
    const store = yield* ThreadStore;
    const tail = yield* store.inspectTail(ThreadTailRequest.make({ threadId }));

    const afterSequence = yield* Schema.decodeEffect(CanonicalSequence)(
      Math.max(0, tail.tailSequence - 100),
    );

    const count = tail.tailSequence - afterSequence;

    if (count === 0) return [];

    const records = yield* store
      .read(ThreadRead.make({ threadId, afterSequence, limit: count }))
      .pipe(Stream.take(count), Stream.runCollect);

    if (
      records.length !== count ||
      records.some(
        (entry, index) =>
          entry.threadId !== threadId || entry.sequence !== afterSequence + index + 1,
      )
    )
      return yield* new PlannerError({
        code: "unavailable",
        message: "Worker history could not be read.",
      });

    return records;
  },
  Effect.catchTag("ThreadNotMaterialized", () => Effect.succeed([])),
);
