import { ThreadId, RunId } from "@effect-agent/core/Identifiers";
import {
  ContextHistory,
  ContextHistoryError,
  ContextHistoryHit,
  ContextHistoryPage,
  type ContextHistoryRead,
  type ContextHistorySearch,
} from "@effect-agent/engine/ContextHistory";
import {
  ContextRolloverTool,
  ContextWindow,
  ContextWindowStatus,
} from "@effect-agent/engine/ContextWindow";
import { getToolExecutionClass } from "@effect-agent/engine/DurableStep";
import { describe, expect, it } from "@effect/vitest";
import { Context, Effect, Ref, Stream } from "effect";

import * as ContextTools from "../src/ContextTools.ts";

const status = ContextWindowStatus.make({
  threadId: ThreadId.make("current-thread"),
  runId: RunId.make("current-run"),
  windowId: "initial",
  estimatedTokens: 40,
  contextTokenLimit: 100,
  remainingTokens: 60,
});

describe("context window tools", () => {
  it.effect("rejects archive requests above the tools' result bounds before reading", () =>
    Effect.gen(function* () {
      const tools = yield* ContextTools.toolkit;

      const searchFailure = yield* tools
        .handle("search_context_windows", { query: "saved", limit: 4 }, "search")
        .pipe(Effect.flip);

      const readFailure = yield* tools
        .handle("read_context_window", { recordId: "evidence", maxChars: 5_001 }, "read")
        .pipe(Effect.flip);

      expect(searchFailure).toMatchObject({ reason: { _tag: "ToolParameterValidationError" } });
      expect(readFailure).toMatchObject({ reason: { _tag: "ToolParameterValidationError" } });
    }).pipe(
      Effect.provide(ContextTools.layer),
      Effect.provideService(ContextWindow, { status: Effect.die("Unexpected status read") }),
      Effect.provideService(ContextHistory, {
        search: () => Effect.die("Unexpected archive search"),
        read: () => Effect.die("Unexpected archive read"),
      }),
    ),
  );

  it.effect("returns a designated rollover request without performing a hidden mutation", () =>
    Effect.gen(function* () {
      const tools = yield* ContextTools.toolkit;

      const results = yield* tools
        .handle("new_context", { handoff: "Continue from the saved notes." }, "rotate")
        .pipe(Effect.flatMap(Stream.runCollect));

      expect(results).toMatchObject([
        { isFailure: false, result: { handoff: "Continue from the saved notes." } },
      ]);
      expect(Context.get(ContextTools.NewContext.annotations, ContextRolloverTool)).toBe(true);
      expect(getToolExecutionClass(ContextTools.NewContext)).toBe("idempotent");
    }).pipe(Effect.provide(ContextTools.layer)),
  );

  it.effect("binds archive queries to the current Thread on every invocation", () =>
    Effect.gen(function* () {
      const current = yield* Ref.make(status);
      const searches: Array<ContextHistorySearch> = [];
      const reads: Array<ContextHistoryRead> = [];
      const tools = yield* ContextTools.toolkit;

      const archive = ContextHistory.of({
        search: (request) =>
          Effect.sync(() => {
            searches.push(request);

            return [
              ContextHistoryHit.make({ recordId: "evidence", windowId: "old", text: "saved" }),
            ];
          }),
        read: (request) =>
          Effect.sync(() => {
            reads.push(request);

            return ContextHistoryPage.make({
              recordId: request.recordId,
              windowId: "old",
              text: "saved evidence",
              nextOffset: null,
            });
          }),
      });

      const run = Effect.gen(function* () {
        const untrustedSearch = { query: "saved", threadId: "another-thread" };

        yield* tools
          .handle("search_context_windows", untrustedSearch, "search")
          .pipe(Effect.flatMap(Stream.runCollect));

        yield* Ref.set(
          current,
          ContextWindowStatus.make({
            ...status,
            threadId: ThreadId.make("next-thread"),
            estimatedTokens: 55,
            remainingTokens: 45,
          }),
        );

        const remaining = yield* tools
          .handle("get_context_remaining", {}, "status")
          .pipe(Effect.flatMap(Stream.runCollect));

        const untrustedRead = { recordId: "evidence", threadId: "another-thread" };

        const page = yield* tools
          .handle("read_context_window", untrustedRead, "read")
          .pipe(Effect.flatMap(Stream.runCollect));

        expect(remaining).toMatchObject([{ result: { estimatedTokens: 55, remainingTokens: 45 } }]);
        expect(page).toMatchObject([{ result: { text: "saved evidence", nextOffset: null } }]);
      });

      yield* run.pipe(
        Effect.provideService(ContextWindow, { status: Ref.get(current) }),
        Effect.provideService(ContextHistory, archive),
      );
      expect(searches).toMatchObject([{ threadId: "current-thread", query: "saved", limit: 3 }]);
      expect(reads).toMatchObject([
        { threadId: "next-thread", recordId: "evidence", offset: 0, maxChars: 5_000 },
      ]);
    }).pipe(Effect.provide(ContextTools.layer)),
  );

  it.effect("returns an unavailable archive as a typed tool failure", () =>
    Effect.gen(function* () {
      const tools = yield* ContextTools.toolkit;

      const failure = ContextHistoryError.make({
        reason: "unavailable",
        message: "Archive offline",
      });

      const results = yield* tools
        .handle("search_context_windows", { query: "saved" }, "search")
        .pipe(Effect.flatMap(Stream.runCollect));

      expect(results).toMatchObject([{ isFailure: true, result: failure }]);
    }).pipe(
      Effect.provide(ContextTools.layer),
      Effect.provideService(ContextWindow, { status: Effect.succeed(status) }),
      Effect.provideService(ContextHistory, {
        search: () =>
          Effect.fail(
            ContextHistoryError.make({ reason: "unavailable", message: "Archive offline" }),
          ),
        read: () =>
          Effect.fail(
            ContextHistoryError.make({ reason: "unavailable", message: "Archive offline" }),
          ),
      }),
    ),
  );
});
