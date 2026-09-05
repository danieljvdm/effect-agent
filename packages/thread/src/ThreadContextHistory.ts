import type { RunId, ThreadId } from "@effect-agent/core/Identifiers";
import { contextWindowId } from "@effect-agent/engine/Compaction";
import {
  ContextHistory,
  ContextHistoryError,
  ContextHistoryHit,
  ContextHistoryPage,
  ContextHistoryRead,
  ContextHistorySearch,
} from "@effect-agent/engine/ContextHistory";
import { Effect, Layer, Schema, Stream } from "effect";
import { Prompt } from "effect/unstable/ai";

import { CanonicalRecordEnvelope, CanonicalSequence, PersistedJson } from "./Records.ts";
import { ThreadRead, ThreadStore, ThreadTail, ThreadTailRequest } from "./ThreadStore.ts";

/** Bounds each lookup; a larger archive needs an explicitly configured or indexed adapter. */
export const ThreadContextHistoryOptions = Schema.Struct({
  maxRecords: Schema.optionalKey(
    Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 65_536 })),
  ),
  timeoutMillis: Schema.optionalKey(
    Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 300_000 })),
  ),
});

export type ThreadContextHistoryOptions = typeof ThreadContextHistoryOptions.Type;

const unavailable = () =>
  ContextHistoryError.make({
    reason: "unavailable",
    message: "Canonical context history is unavailable",
  });

const invalid = (message: string) => ContextHistoryError.make({ reason: "invalid-input", message });

interface WindowBoundary {
  readonly after: number;
  readonly windowId: string;
}
interface Evidence {
  readonly recordId: string;
  readonly sequence: number;
  readonly runId: RunId;
  readonly text: string;
}

const jsonText = Effect.fn("ThreadContextHistory.jsonText")(function* (value: unknown) {
  const json = yield* Schema.decodeUnknownEffect(PersistedJson)(value).pipe(
    Effect.mapError(unavailable),
  );

  return JSON.stringify(json);
});

const promptText = Effect.fn("ThreadContextHistory.promptText")(function* (value: PersistedJson) {
  const prompt = yield* Schema.decodeUnknownEffect(Prompt.Prompt)(value).pipe(
    Effect.mapError(unavailable),
  );

  const messages: Array<string> = [];

  for (const message of prompt.content) {
    // Provider options, system instructions, reasoning, and attachment bytes are not recall data.
    if (message.role === "system") continue;
    const parts: Array<string> = [];

    for (const part of message.content) {
      switch (part.type) {
        case "text":
          parts.push(part.text);
          break;
        case "tool-call":
          parts.push(`tool call ${part.name} (${part.id}):\n${yield* jsonText(part.params)}`);
          break;
        case "tool-result":
          parts.push(
            `tool result ${part.name} (${part.id}; ${part.isFailure ? "failure" : "success"}):\n${yield* jsonText(part.result)}`,
          );
          break;
        case "file":
          parts.push("[attachment omitted]");
          break;
        case "reasoning":
        case "tool-approval-request":
        case "tool-approval-response":
          break;
      }
    }
    if (parts.length > 0) messages.push(`${message.role}:\n${parts.join("\n")}`);
  }

  return messages.join("\n\n");
});

const evidence = Effect.fn("ThreadContextHistory.evidence")(function* (
  envelope: CanonicalRecordEnvelope,
): Effect.fn.Return<Evidence | undefined, ContextHistoryError> {
  const payload = envelope.record.payload;
  let text: string;

  if (payload._tag === "ModelResponseRecorded") {
    text = yield* promptText(payload.messages);
  } else if (payload._tag === "ModelCompleted") {
    text =
      payload.messages === undefined
        ? `assistant output:\n${yield* jsonText(payload.output)}`
        : yield* promptText(payload.messages);
  } else if (payload._tag === "ToolCallSettled") {
    text = `tool result ${payload.toolName} (${payload.toolCallId}; ${payload.isFailure ? "failure" : "success"}):\n${yield* jsonText(payload.result)}`;
  } else {
    // Raw submission input can contain fields excluded by the Agent's input projection.
    // Step outputs, approvals, failure diagnostics, and operational records stay private.
    return undefined;
  }

  return text.length === 0
    ? undefined
    : {
        recordId: envelope.record.recordId,
        sequence: envelope.sequence,
        runId: payload.runId,
        text,
      };
});

const windowFor = (record: Evidence, boundaries: ReadonlyArray<WindowBoundary>): string => {
  let windowId = contextWindowId(record.runId, 0);

  for (const boundary of boundaries) {
    if (record.sequence <= boundary.after) break;
    windowId = boundary.windowId;
  }

  return windowId;
};

/**
 * Provides bounded lexical search and paged reads over retained canonical transcript evidence.
 * Each operation captures one tail and scans at most `maxRecords` (default 16,384), in pages of
 * 64, with a `timeoutMillis` deadline (default 10 seconds). An oversized history fails explicitly
 * instead of returning an incomplete search. No index, mutable archive, or background work is created.
 *
 * Rollover coverage boundaries assign subsequent records to the new window, including later Runs.
 * Before the first rollover, each Run uses its initial `context:<runId>:0` identity. Pruning and
 * summarization never remove searchable source records. Tool results remain exactly as retained,
 * including any truncation envelope; discarded output bytes and transient recall are unavailable.
 *
 * The host must supply an authorized ThreadStore. This adapter does not grant access based on a
 * Thread ID. Context Tools select the current Run's Thread, never a model-selected Thread.
 */
export const layer = (
  options: ThreadContextHistoryOptions = {},
): Layer.Layer<ContextHistory, ContextHistoryError, ThreadStore> =>
  Layer.effect(
    ContextHistory,
    Effect.gen(function* () {
      const decoded = yield* Schema.decodeUnknownEffect(ThreadContextHistoryOptions)(options).pipe(
        Effect.mapError(() => invalid("Invalid context history limits")),
      );

      const maxRecords = decoded.maxRecords ?? 16_384;
      const timeoutMillis = decoded.timeoutMillis ?? 10_000;
      const store = yield* ThreadStore;

      const scan = Effect.fn("ThreadContextHistory.scan")(function* (
        threadId: ThreadId,
        visit: (record: CanonicalRecordEnvelope) => Effect.Effect<void, ContextHistoryError>,
      ) {
        const tail = yield* store.inspectTail(ThreadTailRequest.make({ threadId })).pipe(
          Effect.mapError(unavailable),
          Effect.flatMap((value) =>
            Schema.decodeUnknownEffect(Schema.toType(ThreadTail))(value).pipe(
              Effect.mapError(unavailable),
            ),
          ),
        );

        if (tail.threadId !== threadId) return yield* unavailable();
        if (tail.tailSequence > maxRecords)
          return yield* ContextHistoryError.make({
            reason: "limit",
            message: `Context history exceeds the configured ${maxRecords} record scan limit`,
          });
        const boundaries: Array<WindowBoundary> = [];
        let cursor = Schema.decodeSync(CanonicalSequence)(0);

        while (cursor < tail.tailSequence) {
          const limit = Math.min(64, tail.tailSequence - cursor);

          const page = yield* store
            .read(ThreadRead.make({ threadId, afterSequence: cursor, limit }))
            .pipe(Stream.take(limit + 1), Stream.runCollect, Effect.mapError(unavailable));

          if (page.length !== limit) return yield* unavailable();
          for (const raw of page) {
            const record = yield* Schema.decodeUnknownEffect(
              Schema.toType(CanonicalRecordEnvelope),
            )(raw).pipe(Effect.mapError(unavailable));

            if (record.threadId !== threadId || record.sequence !== cursor + 1)
              return yield* unavailable();
            const payload = record.record.payload;

            if (payload._tag === "CompactionCreated" && payload.kind === "rollover") {
              if (
                payload.coversThrough >= record.sequence ||
                payload.coversThrough < (boundaries.at(-1)?.after ?? 0)
              ) {
                return yield* unavailable();
              }
              boundaries.push({
                after: payload.coversThrough,
                windowId: contextWindowId(payload.runId, payload.turn),
              });
            }
            yield* visit(record);
            cursor = record.sequence;
          }
        }

        return boundaries;
      });

      const search = Effect.fn("ThreadContextHistory.search")(
        function* (input: ContextHistorySearch) {
          const request = yield* Schema.decodeUnknownEffect(Schema.toType(ContextHistorySearch))(
            input,
          ).pipe(Effect.mapError(() => invalid("Invalid context history search")));

          const query = request.query.trim().toLowerCase();

          if (query.length === 0)
            return yield* invalid("Context history search requires non-whitespace text");
          const matches: Array<Evidence> = [];

          const boundaries = yield* scan(
            request.threadId,
            Effect.fn(function* (record) {
              const item = yield* evidence(record);

              if (item === undefined) return;
              const index = item.text.toLowerCase().indexOf(query);

              if (index < 0) return;
              const start = Math.max(0, index - 200);

              matches.push({ ...item, text: item.text.slice(start, start + 2_000) });
              if (matches.length > request.limit) matches.shift();
            }),
          );

          return matches.reverse().map((item) =>
            ContextHistoryHit.make({
              recordId: item.recordId,
              windowId: windowFor(item, boundaries),
              text: item.text,
            }),
          );
        },
        Effect.timeoutOrElse({
          duration: timeoutMillis,
          orElse: () =>
            Effect.fail(
              ContextHistoryError.make({
                reason: "limit",
                message: "Context history search exceeded its time limit",
              }),
            ),
        }),
      );

      const read = Effect.fn("ThreadContextHistory.read")(
        function* (input: ContextHistoryRead) {
          const request = yield* Schema.decodeUnknownEffect(Schema.toType(ContextHistoryRead))(
            input,
          ).pipe(Effect.mapError(() => invalid("Invalid context history read")));

          let selected: Evidence | undefined;

          const boundaries = yield* scan(
            request.threadId,
            Effect.fn(function* (record) {
              if (record.record.recordId === request.recordId) selected = yield* evidence(record);
            }),
          );

          if (selected === undefined)
            return yield* ContextHistoryError.make({
              reason: "not-found",
              message: "Retained context record was not found in this Thread",
            });
          if (request.offset > selected.text.length)
            return yield* invalid("Context history offset is beyond the retained record");
          const end = Math.min(selected.text.length, request.offset + request.maxChars);

          return ContextHistoryPage.make({
            recordId: selected.recordId,
            windowId: windowFor(selected, boundaries),
            text: selected.text.slice(request.offset, end),
            nextOffset: end < selected.text.length ? end : null,
          });
        },
        Effect.timeoutOrElse({
          duration: timeoutMillis,
          orElse: () =>
            Effect.fail(
              ContextHistoryError.make({
                reason: "limit",
                message: "Context history read exceeded its time limit",
              }),
            ),
        }),
      );

      return ContextHistory.of({ search, read });
    }),
  );
