import { Effect, Option, Stream } from "effect";

import {
  ConversationExportRequest,
  ConversationObservation,
  ConversationRead,
  ConversationStore,
  type ConversationStoreFailure,
  LoadCheckpointRequest,
} from "@effect-agent/session";
import type { ConversationId } from "@effect-agent/core";

/** Adapter-neutral evidence returned by the shared ConversationStore smoke contract. */
export interface ConversationStoreConformanceSnapshot {
  readonly readCount: number;
  readonly observedCount: number;
  readonly exportCount: number;
  readonly hasCheckpoint: boolean;
}

/**
 * Reusable public-port probe for adapter suites. The caller performs
 * materialization, append, and checkpoint setup before invoking this probe.
 */
export const inspectConversationStoreConformance = (
  conversationId: ConversationId,
): Effect.Effect<
  ConversationStoreConformanceSnapshot,
  ConversationStoreFailure,
  ConversationStore
> =>
  Effect.gen(function* () {
    const store = yield* ConversationStore;
    const read = yield* store
      .read(ConversationRead.make({ conversationId, limit: 1_024 }))
      .pipe(Stream.runCollect);
    const observed = yield* store
      .observe(ConversationObservation.make({ conversationId }))
      .pipe(Stream.take(read.length), Stream.runCollect);
    const exported = yield* store.export(ConversationExportRequest.make({ conversationId }));
    const checkpoint = yield* store.loadCheckpoint(LoadCheckpointRequest.make({ conversationId }));
    return {
      readCount: read.length,
      observedCount: observed.length,
      exportCount: exported.records.length,
      hasCheckpoint: Option.isSome(checkpoint),
    };
  });
