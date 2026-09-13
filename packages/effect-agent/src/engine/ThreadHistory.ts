import { Context, Effect, Layer, Schema } from "effect";
import type { Prompt } from "effect/unstable/ai";

import { ThreadId, type RunId } from "../core/Identifiers.ts";
import { type RunCompleted } from "../core/RunEvent.ts";
import {
  EphemeralThreads,
  EphemeralThreadsLive,
  threadPrompt,
  type ThreadError,
} from "./EphemeralThreads.ts";

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
 * One Run's history owner. Incremental adapters retain each stageHistory update; on-success
 * adapters stage privately until commit. Before commit, the interpreter
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
 * History shared by Runs with the same Thread ID. The default layer retains native messages
 * incrementally in memory for its application Scope, including completed updates before a failure.
 * PersistentHistory.layer from @effect-agent/thread/persistent-history instead commits successful
 * Runs to an explicit ThreadStore. Durable hosts retain history through their journal hooks.
 * No implementation may retry model or Tool execution or claim interrupted-work recovery.
 */
export class ThreadHistory extends Context.Service<
  ThreadHistory,
  {
    readonly retention: "incremental" | "on-success";
    readonly open: (request: {
      readonly threadId: ThreadId;
      readonly runId: RunId;
    }) => Effect.Effect<ThreadHistoryRun, ThreadHistoryError>;
    readonly load: (threadId: ThreadId) => Effect.Effect<Prompt.Prompt, ThreadHistoryError>;
  }
>()("@effect-agent/engine/ThreadHistory") {
  /**
   * Retain bounded in-memory history across Runs. Provide once around the application so
   * all Runs share the same store. The underlying EphemeralThreads service is exposed for
   * snapshots and interactive hooks; separate Layer builds own separate stores.
   */
  static readonly layer = Layer.effect(
    ThreadHistory,
    Effect.gen(function* () {
      const threads = yield* EphemeralThreads;

      const historyError = (cause: ThreadError): ThreadHistoryError =>
        ThreadHistoryError.make({
          threadId: cause.threadId,
          reason:
            cause._tag === "ThreadNotFound"
              ? "not-found"
              : cause._tag === "ThreadLimitExceeded"
                ? "limit"
                : cause._tag === "ThreadHistoryDiverged"
                  ? "conflict"
                  : "encoding",
          message:
            cause._tag === "ThreadNotFound"
              ? "Thread history is not present in this application Scope"
              : cause._tag === "ThreadLimitExceeded"
                ? `In-memory history exceeds the ${cause.limit} limit of ${cause.limitValue}`
                : cause.message,
          cause,
        });

      return ThreadHistory.of({
        retention: "incremental",
        load: (threadId) =>
          threads.snapshot(threadId).pipe(Effect.map(threadPrompt), Effect.mapError(historyError)),
        open: Effect.fn("ThreadHistory.open")(function* ({ threadId, runId }) {
          const snapshot = yield* threads.create(threadId).pipe(Effect.mapError(historyError));

          return {
            prompt: threadPrompt(snapshot),
            stageInput: () => Effect.void,
            stageHistory: (history: Prompt.Prompt) =>
              threads
                .recordHistory(threadId, runId, history)
                .pipe(Effect.asVoid, Effect.mapError(historyError)),
            commit: () => Effect.void,
          };
        }),
      });
    }),
  ).pipe(Layer.provideMerge(EphemeralThreadsLive));
}

/** Retain in-memory conversation history for the application Scope. */
export const layer = ThreadHistory.layer;
