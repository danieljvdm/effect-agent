import { ThreadId, type RunId } from "@effect-agent/core/Identifiers";
import { type RunCompleted } from "@effect-agent/core/RunEvent";
import { Context, Effect, Layer, Schema } from "effect";
import type { Prompt } from "effect/unstable/ai";

/** A history adapter rejected a read, staged value, or completed Run commit. */
export class ThreadHistoryError extends Schema.TaggedError<ThreadHistoryError>()(
  "ThreadHistoryError",
  {
    threadId: ThreadId,
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
 * One Run's private staging area. Staging never commits history. Before commit, the interpreter
 * finishes run-owned work and closes resources acquired for that Run, then validates the result
 * for run/start.await. Shared model, provider, and client services supplied by an enclosing
 * application Layer remain owned by that application's Scope and may outlive multiple Runs.
 * Commit must succeed before RunCompleted becomes observable. Schema-encoded input is opaque
 * here; the adapter owns its persistence boundary. A failed commit may have reached storage;
 * callers must inspect history before retrying external execution.
 */
export interface ThreadHistoryRun {
  readonly prompt: Prompt.Prompt;
  readonly stageInput: (input: unknown) => Effect.Effect<void, ThreadHistoryError>;
  readonly stageHistory: (history: Prompt.Prompt) => Effect.Effect<void, ThreadHistoryError>;
  readonly commit: (completed: RunCompleted) => Effect.Effect<void, ThreadHistoryError>;
}

/**
 * Standard public API for retaining successful Runs through AgentRuntime.run, start, and stream.
 * Provide PersistentHistory.layer from thread/history with a memory or SQLite ThreadStore
 * Layer. Adapters capture storage dependencies; every Run gets private staging and append ownership.
 * Retaining adapters cannot share ownership with explicit history/onHistory, input queues, or
 * durable recovery hooks. Use layerTransient for those advanced integrations.
 * No implementation may retry model or Tool execution or claim interrupted-work recovery.
 */
export class ThreadHistory extends Context.Service<
  ThreadHistory,
  {
    readonly open: (request: {
      readonly threadId: ThreadId;
      readonly runId: RunId;
    }) => Effect.Effect<ThreadHistoryRun | undefined, ThreadHistoryError>;
    readonly load: (threadId: ThreadId) => Effect.Effect<Prompt.Prompt, ThreadHistoryError>;
  }
>()("@effect-agent/engine/ThreadHistory") {
  /**
   * Retain no shared history. Undefined staging leaves explicit history/onHistory and durable
   * journal hooks under their caller's ownership. Use a memory store with PersistentHistory
   * when completed Runs should share history for the lifetime of a process Scope.
   */
  static readonly layerTransient = Layer.succeed(ThreadHistory, {
    open: () => Effect.succeed(undefined),
    load: (threadId) =>
      Effect.fail(
        ThreadHistoryError.make({
          threadId,
          reason: "not-found",
          message: "Transient execution does not retain Thread history",
        }),
      ),
  });
}
