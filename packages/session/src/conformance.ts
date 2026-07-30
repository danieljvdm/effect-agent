import type { ConversationId } from "@effect-agent/core";
import { Effect, Option, Schema, Stream } from "effect";

import {
  CheckpointRejected,
  ConversationExportRequest,
  ConversationNotMaterialized,
  ConversationObservation,
  ConversationRead,
  ConversationStore,
  ConversationStoreError,
  LoadCheckpointRequest,
} from "./store.ts";

/** Adapter-neutral evidence returned by the shared ConversationStore contract probe. */
export class ConversationStoreConformanceSnapshot extends Schema.Class<ConversationStoreConformanceSnapshot>(
  "@effect-agent/session/ConversationStoreConformanceSnapshot",
)({
  readCount: Schema.Natural,
  observedCount: Schema.Natural,
  exportCount: Schema.Natural,
  hasCheckpoint: Schema.Boolean,
}) {}

/**
 * Reusable public-port probe for every ConversationStore adapter suite. The caller performs
 * materialization, append, and checkpoint setup before invoking this probe.
 */
export const inspectConversationStoreConformance = Effect.fn(
  "ConversationStore.inspectConformance",
)(function* (
  conversationId: ConversationId,
): Effect.fn.Return<
  ConversationStoreConformanceSnapshot,
  ConversationStoreError | ConversationNotMaterialized | CheckpointRejected,
  ConversationStore
> {
  const store = yield* ConversationStore;
  const read = yield* store
    .read(ConversationRead.make({ conversationId, limit: 1_024 }))
    .pipe(Stream.runCollect);
  const observed = yield* store
    .observe(ConversationObservation.make({ conversationId }))
    .pipe(Stream.take(read.length), Stream.runCollect);
  const exported = yield* store.export(ConversationExportRequest.make({ conversationId }));
  const checkpoint = yield* store.loadCheckpoint(LoadCheckpointRequest.make({ conversationId }));

  return ConversationStoreConformanceSnapshot.make({
    readCount: read.length,
    observedCount: observed.length,
    exportCount: exported.records.length,
    hasCheckpoint: Option.isSome(checkpoint),
  });
});
