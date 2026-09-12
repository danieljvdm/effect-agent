import type { ThreadId } from "@effect-agent/core/Identifiers";
import {
  ContextHistory,
  ContextHistoryError,
  ContextHistoryHit,
  ContextHistoryPage,
  ContextHistoryRead,
  ContextHistorySearch,
} from "@effect-agent/engine/ContextHistory";
import { Effect, Layer, Schema, Stream } from "effect";

import { CanonicalRecordEnvelope, CanonicalSequence } from "./Records.ts";
import type { ContextHistoryBoundary } from "./ThreadContextHistoryProjection.ts";
import {
  ContextHistoryEvidence,
  project,
  normalizeQuery,
  matchText,
  windowIdFor,
} from "./ThreadContextHistoryProjection.ts";
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

/**
 * Provides bounded lexical search and paged reads over retained canonical transcript evidence.
 * Each operation captures one tail and scans at most `maxRecords` (default 16,384), in pages of
 * 64, with a `timeoutMillis` deadline (default 10 seconds). An oversized history fails explicitly
 * instead of returning an incomplete search. No index, mutable archive, or background work is created.
 * Search resolves `beforeRecordId` against eligible canonical evidence in that same scan and
 * returns only older matches. It still verifies the entire captured tail and its boundaries.
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
      const decoded = yield* Schema.decodeEffect(ThreadContextHistoryOptions)(options).pipe(
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
            Schema.decodeEffect(Schema.toType(ThreadTail))(value).pipe(
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
        const boundaries: Array<ContextHistoryBoundary> = [];
        let cursor = Schema.decodeSync(CanonicalSequence)(0);

        while (cursor < tail.tailSequence) {
          const limit = Math.min(64, tail.tailSequence - cursor);

          const page = yield* store
            .read(ThreadRead.make({ threadId, afterSequence: cursor, limit }))
            .pipe(Stream.take(limit + 1), Stream.runCollect, Effect.mapError(unavailable));

          if (page.length !== limit) return yield* unavailable();
          for (const raw of page) {
            const record = yield* Schema.decodeEffect(Schema.toType(CanonicalRecordEnvelope))(
              raw,
            ).pipe(Effect.mapError(unavailable));

            if (record.threadId !== threadId || record.sequence !== cursor + 1)
              return yield* unavailable();
            if (record.record.payload._tag === "CompactionCreated") {
              const { boundary } = yield* project(record);

              if (boundary !== undefined) {
                if (boundary.coversThrough < (boundaries.at(-1)?.coversThrough ?? 0))
                  return yield* unavailable();
                boundaries.push(boundary);
              }
            }
            yield* visit(record);
            cursor = record.sequence;
          }
        }

        return boundaries;
      });

      const search = Effect.fn("ThreadContextHistory.search")(
        function* (input: ContextHistorySearch) {
          const request = yield* Schema.decodeEffect(Schema.toType(ContextHistorySearch))(
            input,
          ).pipe(Effect.mapError(() => invalid("Invalid context history search")));

          const query = yield* normalizeQuery(request.query);
          const matches: Array<ContextHistoryEvidence> = [];
          let anchorFound = false;

          const boundaries = yield* scan(
            request.threadId,
            Effect.fn(function* (record) {
              const item = (yield* project(record)).evidence;

              if (item === undefined) return;
              if (item.recordId === request.beforeRecordId) anchorFound = true;
              // The scan is ascending: retain older candidates until the exclusive anchor.
              // Continue scanning to verify the captured tail and all window boundaries.
              if (anchorFound) return;
              const text = matchText(item.text, query);

              if (text === undefined) return;
              matches.push(ContextHistoryEvidence.make({ ...item, text }));
              if (matches.length > request.limit) matches.shift();
            }),
          );

          if (request.beforeRecordId !== undefined && !anchorFound)
            return yield* ContextHistoryError.make({
              reason: "not-found",
              message: "Retained context record was not found in this Thread",
            });

          return matches.reverse().map((item) =>
            ContextHistoryHit.make({
              recordId: item.recordId,
              windowId: windowIdFor(item, boundaries),
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
          const request = yield* Schema.decodeEffect(Schema.toType(ContextHistoryRead))(input).pipe(
            Effect.mapError(() => invalid("Invalid context history read")),
          );

          let selected: ContextHistoryEvidence | undefined;

          const boundaries = yield* scan(
            request.threadId,
            Effect.fn(function* (record) {
              if (record.record.recordId === request.recordId)
                selected = (yield* project(record)).evidence;
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
            windowId: windowIdFor(selected, boundaries),
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
