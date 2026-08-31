import { ConversationId, type RunCompleted, type RunId } from "@effect-agent/core";
import { Context, Effect, Layer, Schema } from "effect";
import type { Prompt } from "effect/unstable/ai";

/** A history adapter rejected a read, staged value, or completed Run commit. */
export class ConversationHistoryError extends Schema.TaggedError<ConversationHistoryError>()(
  "ConversationHistoryError",
  {
    conversationId: ConversationId,
    reason: Schema.Literals([
      "not-found",
      "conflict",
      "fenced",
      "incompatible",
      "limit",
      "encoding",
      "storage",
    ]),
    message: Schema.String,
    cause: Schema.optionalKey(Schema.Defect()),
  },
) {}

/**
 * One Run's private staging area. Staging never commits history. The interpreter calls commit
 * only after successful execution and model resource closure, before publishing RunCompleted.
 * Schema-encoded input is opaque here; the adapter owns its persistence boundary.
 */
export interface ConversationHistoryRun {
  readonly prompt: Prompt.Prompt;
  readonly stageInput: (input: unknown) => Effect.Effect<void, ConversationHistoryError>;
  readonly stageHistory: (history: Prompt.Prompt) => Effect.Effect<void, ConversationHistoryError>;
  readonly commit: (completed: RunCompleted) => Effect.Effect<void, ConversationHistoryError>;
}

/**
 * History policy provided to AgentRuntime.run, start, and stream. Adapters capture storage
 * dependencies in their Layer; every Run gets its own staging area and append ownership.
 * No implementation may retry model or Tool execution or claim interrupted-work recovery.
 */
export class ConversationHistory extends Context.Service<
  ConversationHistory,
  {
    readonly open: (request: {
      readonly conversationId: ConversationId;
      readonly runId: RunId;
    }) => Effect.Effect<ConversationHistoryRun | undefined, ConversationHistoryError>;
    readonly load: (
      conversationId: ConversationId,
    ) => Effect.Effect<Prompt.Prompt, ConversationHistoryError>;
  }
>()("@effect-agent/engine/ConversationHistory") {
  /**
   * Retain no shared history. Undefined staging leaves explicit history/onHistory and durable
   * journal hooks under their caller's ownership. Use a memory store with PersistentHistory
   * when completed Runs should share history for the lifetime of a process Scope.
   */
  static readonly layerTransient = Layer.succeed(ConversationHistory, {
    open: () => Effect.succeed(undefined),
    load: (conversationId) =>
      Effect.fail(
        ConversationHistoryError.make({
          conversationId,
          reason: "not-found",
          message: "Transient execution does not retain Conversation history",
        }),
      ),
  });
}
